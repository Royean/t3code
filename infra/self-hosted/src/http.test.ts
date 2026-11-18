// @effect-diagnostics-next-line nodeBuiltinImport:off -- The integration fixture owns an ephemeral native HTTP listener.
import * as NodeHttp from "node:http";
import { expect, it } from "@effect/vitest";
import {
  createSelfHostedConnectClient,
  normalizeSelfHostedServerUrl,
  SelfHostedConnectClientError,
} from "@t3tools/client-runtime/self-hosted-connect";
import * as Effect from "effect/Effect";
import type { SelfHostedConfig } from "./config.ts";
import { createHttpHandler } from "./http.ts";
import * as SelfHostedConnect from "./SelfHostedConnect.ts";

const config: SelfHostedConfig = {
  databasePath: ":memory:",
  account: {
    accountId: "owner",
    username: "owner",
    passwordHash:
      "scrypt$32768$8$1$ca33483ce1102b6d2057afec1a065ced$935b09158b33db6019b3441ef559389dd9ffdb09409da5ac46829d640d7c58af",
  },
  environments: [
    {
      environmentId: "office",
      label: "Office",
      origin: "https://office.example.com",
      pairingOrigin: "http://127.0.0.1:3773",
      credential: "private-environment-credential",
    },
  ],
  allowedOrigins: ["https://app.example.com", "t3code://app", "t3code-dev://app"],
};

it.live(
  "serves the runtime client over HTTP with auth, strict bodies, CORS, and no-store responses",
  () =>
    Effect.gen(function* () {
      const service = yield* SelfHostedConnect.SelfHostedConnect;
      const server = yield* Effect.acquireRelease(
        Effect.promise(
          () =>
            new Promise<ReturnType<typeof NodeHttp.createServer>>((resolve, reject) => {
              const handler = createHttpHandler(service, config);
              const listener = NodeHttp.createServer((request, response) => {
                void handler(request, response);
              });
              listener.once("error", reject);
              listener.listen(0, "127.0.0.1", () => resolve(listener));
            }),
        ),
        (listener) =>
          Effect.promise(
            () =>
              new Promise<void>((resolve) => {
                listener.closeAllConnections();
                listener.close(() => resolve());
              }),
          ),
      );
      const address = server.address();
      if (!address || typeof address === "string") throw new Error("Missing test listener.");
      const origin = `http://127.0.0.1:${address.port}`;
      yield* Effect.promise(async () => {
        for (const [path, init] of [
          ["/v1/environments", {}],
          [
            "/v1/environments/office/connect",
            { method: "POST", headers: { "Content-Type": "application/json" }, body: "{}" },
          ],
        ] as const) {
          // @effect-diagnostics-next-line globalFetchInEffect:off -- The test drives the native HTTP boundary with the same fetch API as browser clients.
          const response = await fetch(new URL(path, origin), init);
          expect(response.status).toBe(401);
          expect(response.headers.get("cache-control")).toBe("no-store");
        }
        // @effect-diagnostics-next-line globalFetchInEffect:off -- The test must inspect raw HTTP CORS headers before runtime decoding.
        const blockedOrigin = await fetch(new URL("/v1/auth/login", origin), {
          method: "POST",
          headers: { Origin: "https://attacker.example.com" },
        });
        expect(blockedOrigin.status).toBe(403);
        expect(blockedOrigin.headers.get("access-control-allow-origin")).toBeNull();
        // @effect-diagnostics-next-line globalFetchInEffect:off -- The test exercises the browser's HTTP preflight contract.
        const preflight = await fetch(origin, {
          method: "OPTIONS",
          headers: { Origin: "https://app.example.com" },
        });
        expect(preflight.status).toBe(204);
        expect(preflight.headers.get("access-control-allow-origin")).toBe(
          "https://app.example.com",
        );
        for (const desktopOrigin of ["t3code://app", "t3code-dev://app"]) {
          // @effect-diagnostics-next-line globalFetchInEffect:off -- Electron sends its registered custom origin on account API requests.
          const desktopPreflight = await fetch(origin, {
            method: "OPTIONS",
            headers: { Origin: desktopOrigin },
          });
          expect(desktopPreflight.status).toBe(204);
          expect(desktopPreflight.headers.get("access-control-allow-origin")).toBe(desktopOrigin);
        }
        const client = createSelfHostedConnectClient({ serverUrl: origin });
        const session = await client.login("owner", "test-password-123");
        expect(
          (await client.listEnvironments(session.accessToken)).environments[0]?.available,
        ).toBe(true);
        expect((await client.connect(session.accessToken, "office")).pairingUrl).toBe(
          "https://office.example.com/pair#token=one-time-pairing",
        );
        // @effect-diagnostics-next-line globalFetchInEffect:off -- The test submits a body the typed client intentionally cannot create.
        const extraFields = await fetch(new URL("/v1/environments/office/connect", origin), {
          method: "POST",
          headers: {
            Authorization: `Bearer ${session.accessToken}`,
            "Content-Type": "application/json",
          },
          body: JSON.stringify({ targetUrl: "https://attacker.example.com" }),
        });
        expect(extraFields.status).toBe(400);
        expect(await extraFields.text()).not.toContain("private-environment-credential");
        // @effect-diagnostics-next-line globalFetchInEffect:off -- The test verifies the native HTTP body-size guard before decoding.
        const oversized = await fetch(new URL("/v1/auth/login", origin), {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: "x".repeat(16_385),
        });
        expect(oversized.status).toBe(413);
        await client.logout(session.refreshToken);
        await expect(client.listEnvironments(session.accessToken)).rejects.toMatchObject({
          status: 401,
          code: "unauthorized",
        });
      });
    }).pipe(
      Effect.scoped,
      Effect.provide(
        SelfHostedConnect.layer(config, {
          fetch: async () =>
            Response.json({
              credential: "one-time-pairing",
              expiresAt: "2099-01-01T00:00:00.000Z",
            }),
        }),
      ),
    ),
);

it("normalizes HTTPS origins and only permits loopback HTTP for local testing", () => {
  expect(normalizeSelfHostedServerUrl(" https://connect.example.com/ ")).toBe(
    "https://connect.example.com",
  );
  expect(normalizeSelfHostedServerUrl("http://127.0.0.1:3050")).toBe("http://127.0.0.1:3050");
  for (const origin of [
    "http://connect.example.com",
    "https://user:password@connect.example.com",
    "https://connect.example.com/path",
    "https://connect.example.com?secret=value",
    "https://connect.example.com#token=value",
    "http://localhost.example.com",
  ])
    expect(() => normalizeSelfHostedServerUrl(origin)).toThrow();
});

it("validates runtime responses and rejects redirects before sending credentials elsewhere", async () => {
  const client = createSelfHostedConnectClient({
    serverUrl: "https://connect.example.com",
    fetch: async (_url, init) => {
      expect(init?.redirect).toBe("error");
      expect(init?.credentials).toBe("omit");
      return Response.json({
        accountId: "owner",
        accessToken: "invalid",
        refreshToken: "invalid",
        expiresAt: "bad-date",
      });
    },
  });
  await expect(client.login("owner", "test-password-123")).rejects.toBeInstanceOf(
    SelfHostedConnectClientError,
  );
});
