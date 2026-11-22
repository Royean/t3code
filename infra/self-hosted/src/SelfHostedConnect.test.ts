// @effect-diagnostics-next-line nodeBuiltinImport:off -- The persistence test creates and inspects its own temporary native SQLite fixture.
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The persistence fixture uses an OS temporary directory path.
import * as NodePath from "node:path";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Signs the fixture webhook link.
import * as NodeCrypto from "node:crypto";
import { expect, it } from "@effect/vitest";
import {
  AuthStandardClientScopes,
  AuthAccessTokenType,
  EnvironmentId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";
import type { SelfHostedConfig } from "./config.ts";
import { decodeConfig } from "./config.ts";
import { hashPassword, verifyPassword } from "./password.ts";
import * as SelfHostedConnect from "./SelfHostedConnect.ts";

const PASSWORD = "test-password-123";
const PASSWORD_HASH =
  "scrypt$32768$8$1$ca33483ce1102b6d2057afec1a065ced$935b09158b33db6019b3441ef559389dd9ffdb09409da5ac46829d640d7c58af";
const config = (overrides: Partial<SelfHostedConfig> = {}): SelfHostedConfig => ({
  databasePath: ":memory:",
  account: { accountId: "owner", username: "owner", passwordHash: PASSWORD_HASH },
  environments: [
    {
      environmentId: "office",
      label: "Office computer",
      origin: "https://office.example.com",
      pairingOrigin: "http://127.0.0.1:3773",
      credential: "private-environment-credential",
    },
  ],
  ...overrides,
});
const credentials = { username: "owner", password: PASSWORD };
const ACTIVITY_KEY = "a".repeat(43);
const packageName = "com.example.t3pro";
const online: typeof fetch = async () =>
  Response.json({
    credential: "one-time-pairing",
    expiresAt: "2099-01-01T00:00:00.000Z",
  });

it.effect("verifies scrypt passwords without accepting malformed hashes or password changes", () =>
  Effect.promise(async () => {
    const encoded = await hashPassword(PASSWORD);
    expect(await verifyPassword(PASSWORD, encoded)).toBe(true);
    expect(await verifyPassword("wrong-password", encoded)).toBe(false);
    expect(await verifyPassword(PASSWORD, "scrypt$9999999999$8$1$salt$hash")).toBe(false);
    expect(encoded).not.toContain(PASSWORD);
  }),
);

it.effect("denies incorrect credentials, rate-limits logins, and expires the budget", () =>
  Effect.gen(function* () {
    const service = yield* SelfHostedConnect.SelfHostedConnect;
    for (let attempt = 0; attempt < 5; attempt++) {
      expect(
        (yield* Effect.flip(
          service.login({ ...credentials, password: "incorrect-password" }, "client"),
        ))._tag,
      ).toBe("Unauthorized");
    }
    expect((yield* Effect.flip(service.login(credentials, "client")))._tag).toBe("RateLimited");
    yield* TestClock.adjust("1 minute");
    expect((yield* service.login(credentials, "client")).accountId).toBe("owner");
  }).pipe(Effect.provide(SelfHostedConnect.layer(config(), { fetch: online }))),
);

it.effect("lists named devices and revokes their complete session family", () =>
  Effect.gen(function* () {
    const service = yield* SelfHostedConnect.SelfHostedConnect;
    const phone = yield* service.login({ ...credentials, deviceLabel: "Android phone" }, "phone");
    const desktop = yield* service.login({ ...credentials, deviceLabel: "MacBook Air" }, "mac");
    const rotated = yield* service.refresh(phone.refreshToken);
    const devices = yield* service.listDevices(desktop.accessToken);
    const phoneDevice = devices.devices.find((device) => device.label === "Android phone")!;
    expect(phoneDevice.current).toBe(false);
    expect(devices.devices).toContainEqual(
      expect.objectContaining({ label: "MacBook Air", current: true }),
    );
    expect((yield* Effect.flip(service.revokeDevice("invalid", phoneDevice.id)))._tag).toBe(
      "Unauthorized",
    );
    yield* service.revokeDevice(desktop.accessToken, phoneDevice.id);
    expect((yield* Effect.flip(service.listEnvironments(rotated.accessToken)))._tag).toBe(
      "Unauthorized",
    );
    expect((yield* Effect.flip(service.refresh(rotated.refreshToken)))._tag).toBe("Unauthorized");
    expect((yield* service.listDevices(desktop.accessToken)).devices).toHaveLength(1);
  }).pipe(Effect.provide(SelfHostedConnect.layer(config()))),
);

it.effect("revokes the environment credential issued to an account device", () => {
  const revoked: unknown[] = [];
  const fetchRequest: typeof fetch = async (url, init) => {
    switch (new URL(String(url)).pathname) {
      case "/api/auth/clients/revoke":
        revoked.push(JSON.parse(String(init?.body)));
        return Response.json({ revoked: true });
      case "/oauth/token":
        return Response.json({
          session_id: "host-session",
          access_token: "host-access",
          issued_token_type: AuthAccessTokenType,
          token_type: "Bearer",
          expires_in: 600,
          scope: "orchestration:read",
        });
      default:
        return online(url, init);
    }
  };
  return Effect.gen(function* () {
    const service = yield* SelfHostedConnect.SelfHostedConnect;
    const phone = yield* service.login({ ...credentials, deviceLabel: "Phone" }, "phone");
    const mac = yield* service.login(credentials, "mac");
    expect(yield* service.connectAccount(phone.accessToken, "office")).toEqual({
      environmentId: "office",
      httpBaseUrl: "https://office.example.com",
      accessToken: "host-access",
    });
    const device = (yield* service.listDevices(mac.accessToken)).devices.find(
      ({ label }) => label === "Phone",
    )!;
    yield* service.revokeDevice(mac.accessToken, device.id);
    expect(revoked).toEqual([{ sessionId: "host-session" }]);
    expect((yield* Effect.flip(service.connectAccount(phone.accessToken, "office")))._tag).toBe(
      "Unauthorized",
    );
  }).pipe(Effect.provide(SelfHostedConnect.layer(config(), { fetch: fetchRequest })));
});

it.effect("delivers and acknowledges Android activity using a revocable device credential", () => {
  const settings = config({
    environments: [{ ...config().environments[0]!, activityKey: ACTIVITY_KEY }],
    androidPackageName: packageName,
  });
  return Effect.gen(function* () {
    const service = yield* SelfHostedConnect.SelfHostedConnect;
    const phone = yield* service.login({ ...credentials, deviceLabel: "Phone" }, "phone");
    const mac = yield* service.login(credentials, "mac");
    const deliveryToken = "d".repeat(43);
    const deliveryTokenHash = NodeCrypto.createHash("sha256").update(deliveryToken).digest("hex");
    const registration = {
      deviceId: "fixture-installation",
      label: "Phone",
      platform: "android",
      androidApiLevel: 34,
      bundleId: packageName,
      preferences: {
        liveActivitiesEnabled: true,
        notificationsEnabled: true,
        notifyOnApproval: true,
        notifyOnCompletion: true,
        notifyOnInput: true,
        notifyOnFailure: true,
      },
    } as const;
    yield* service.registerAndroid(phone.accessToken, registration, deliveryTokenHash);
    const state = {
      environmentId: EnvironmentId.make("office"),
      threadId: ThreadId.make("thread"),
      projectTitle: "Project",
      threadTitle: "Task",
      phase: "completed",
      headline: "Done",
      modelTitle: "Replay",
      updatedAt: "1970-01-01T00:00:00.000Z",
      deepLink: "//untrusted.example",
    } as const;
    expect(
      (yield* Effect.flip(service.publishActivity(phone.accessToken, "office", "thread", state)))
        ._tag,
    ).toBe("Unauthorized");
    yield* service.publishActivity(ACTIVITY_KEY, "office", "thread", state);
    const deliveries = (yield* service.readAndroid(deliveryToken, 0)).messages;
    const alerts = deliveries.filter(({ data }) => data.alert_id);
    expect(alerts).toEqual([
      expect.objectContaining({
        data: expect.objectContaining({
          alert_title: "Task",
          alert_path: "/threads/office/thread",
          device_id: "fixture-installation",
        }),
      }),
    ]);
    expect((yield* service.readAndroid(deliveryToken, 0)).messages).toEqual(deliveries);
    expect((yield* Effect.flip(service.readAndroid(phone.accessToken, 0)))._tag).toBe(
      "Unauthorized",
    );
    expect((yield* Effect.flip(service.listDevices(deliveryToken)))._tag).toBe("Unauthorized");
    const rotated = yield* service.refresh(phone.refreshToken);
    yield* service.registerAndroid(rotated.accessToken, registration, deliveryTokenHash);
    yield* service.publishActivity(ACTIVITY_KEY, "office", "thread", { ...state, phase: "failed" });
    const batch = (yield* service.readAndroid(deliveryToken, alerts[0]!.id)).messages;
    const next = batch.filter(({ data }) => data.alert_id);
    expect(next).toHaveLength(1);
    expect(next[0]!.id).toBeGreaterThan(alerts[0]!.id);
    yield* service.publishActivity(ACTIVITY_KEY, "office", "thread", {
      ...state,
      phase: "running",
    });
    yield* TestClock.adjust("6 minutes");
    expect((yield* service.readAndroid(deliveryToken, next[0]!.id, true)).messages).toEqual([
      expect.objectContaining({
        data: expect.objectContaining({
          active: "true",
          activity_phase: "running",
          activity_expires_at: "7200000",
        }),
      }),
    ]);
    const phoneId = (yield* service.listDevices(mac.accessToken)).devices.find(
      (device) => device.label === "Phone",
    )!.id;
    yield* service.revokeDevice(mac.accessToken, phoneId);
    expect((yield* Effect.flip(service.readAndroid(deliveryToken, 0)))._tag).toBe("Unauthorized");
  }).pipe(Effect.provide(SelfHostedConnect.layer(settings)));
});

it.effect(
  "holds a signed webhook while its environment is offline and replays the same delivery",
  () => {
    const attempts: Array<{
      readonly url: string;
      readonly proof: string | null;
      readonly id: string | null;
    }> = [];
    const fetchRequest: typeof fetch = async (url, init) => {
      const headers = new Headers(init?.headers);
      attempts.push({
        url: String(url),
        id: headers.get("x-t3-relay-delivery-id"),
        proof: headers.get("x-t3-self-hosted-delivery"),
      });
      return Response.json({}, { status: attempts.length === 1 ? 502 : 202 });
    };
    const settings = config({
      environments: [{ ...config().environments[0]!, activityKey: ACTIVITY_KEY }],
    });
    return Effect.gen(function* () {
      const service = yield* SelfHostedConnect.SelfHostedConnect;
      const delivery = {
        environmentId: "office",
        hookId: "hook",
        token: "x".repeat(43),
        signature: "",
        method: "POST",
        query: "source=fixture",
        headers: { "content-type": "application/json" },
        body: new TextEncoder().encode("{}"),
      };
      expect((yield* Effect.flip(service.queueWebhook(delivery)))._tag).toBe("EnvironmentNotFound");
      const signature = NodeCrypto.createHmac("sha256", ACTIVITY_KEY)
        .update(
          JSON.stringify(["ingress", delivery.environmentId, delivery.hookId, delivery.token]),
        )
        .digest("base64url");
      const { deliveryId } = yield* service.queueWebhook({ ...delivery, signature });
      yield* TestClock.adjust("4 seconds");
      expect(attempts).toHaveLength(1);
      expect(attempts[0]).toMatchObject({
        id: deliveryId,
        url: `http://127.0.0.1:3773/api/hooks/hook/${delivery.token}?source=fixture`,
        proof: expect.stringMatching(/^[A-Za-z0-9_-]{43}$/),
      });
      yield* TestClock.adjust("32 seconds");
      expect(attempts).toEqual([attempts[0], attempts[0]]);
    }).pipe(Effect.provide(SelfHostedConnect.layer(settings, { fetch: fetchRequest })));
  },
);

it.effect("rotates refresh tokens atomically and revokes descendants after replay", () =>
  Effect.gen(function* () {
    const service = yield* SelfHostedConnect.SelfHostedConnect;
    const initial = yield* service.login(credentials, "client");
    const rotated = yield* service.refresh(initial.refreshToken);
    expect(rotated.refreshToken).not.toBe(initial.refreshToken);
    expect((yield* Effect.flip(service.listEnvironments(initial.accessToken)))._tag).toBe(
      "Unauthorized",
    );
    expect((yield* service.listEnvironments(rotated.accessToken)).environments).toHaveLength(1);
    expect((yield* Effect.flip(service.refresh(initial.refreshToken)))._tag).toBe("Unauthorized");
    expect((yield* Effect.flip(service.refresh(rotated.refreshToken)))._tag).toBe("Unauthorized");
    expect((yield* Effect.flip(service.connect(rotated.accessToken, "office")))._tag).toBe(
      "Unauthorized",
    );
  }).pipe(Effect.provide(SelfHostedConnect.layer(config(), { fetch: online }))),
);

it.effect("logs out the complete session family and expires access and refresh tokens", () =>
  Effect.gen(function* () {
    const service = yield* SelfHostedConnect.SelfHostedConnect;
    const initial = yield* service.login(credentials, "client");
    const rotated = yield* service.refresh(initial.refreshToken);
    yield* service.logout(initial.refreshToken);
    yield* service.logout(initial.refreshToken);
    expect((yield* Effect.flip(service.listEnvironments(rotated.accessToken)))._tag).toBe(
      "Unauthorized",
    );
    expect((yield* Effect.flip(service.refresh(rotated.refreshToken)))._tag).toBe("Unauthorized");
    const expiring = yield* service.login(credentials, "client");
    yield* TestClock.adjust("1 minute");
    expect((yield* Effect.flip(service.listEnvironments(expiring.accessToken)))._tag).toBe(
      "Unauthorized",
    );
    const refreshed = yield* service.refresh(expiring.refreshToken);
    yield* TestClock.adjust("1 minute");
    expect((yield* Effect.flip(service.refresh(refreshed.refreshToken)))._tag).toBe("Unauthorized");
  }).pipe(
    Effect.provide(
      SelfHostedConnect.layer(config({ accessTokenTtlSeconds: 60, refreshTokenTtlSeconds: 120 }), {
        fetch: online,
      }),
    ),
  ),
);

it.effect(
  "authorizes before contacting configured targets and returns only a one-time credential",
  () => {
    const calls: { url: string; init: RequestInit | undefined }[] = [];
    return Effect.gen(function* () {
      const service = yield* SelfHostedConnect.SelfHostedConnect;
      expect((yield* Effect.flip(service.listEnvironments("missing")))._tag).toBe("Unauthorized");
      expect((yield* Effect.flip(service.connect("missing", "office")))._tag).toBe("Unauthorized");
      const session = yield* service.login(credentials, "client");
      expect(
        (yield* Effect.flip(service.connect(session.accessToken, "https://attacker.example")))._tag,
      ).toBe("EnvironmentNotFound");
      expect(calls).toHaveLength(0);
      const environments = yield* service.listEnvironments(session.accessToken);
      expect(JSON.stringify(environments)).not.toContain("private-environment-credential");
      expect(environments.environments[0]).toEqual({
        environmentId: "office",
        label: "Office computer",
        httpBaseUrl: "https://office.example.com",
        wsBaseUrl: "wss://office.example.com",
        available: true,
      });
      const connected = yield* service.connect(session.accessToken, "office");
      expect(connected.pairingUrl).toBe("https://office.example.com/pair#token=one-time-pairing");
      expect(JSON.stringify(connected)).not.toContain("private-environment-credential");
      expect(calls[1]!.url).toBe("http://127.0.0.1:3773/api/auth/pairing-token");
      expect(calls[1]!.init?.redirect).toBe("error");
      expect(calls[1]!.init?.headers).toEqual({
        Authorization: "Bearer private-environment-credential",
        "Content-Type": "application/json",
      });
      expect(JSON.parse(String(calls[1]!.init?.body))).toEqual({
        label: "Office computer",
        scopes: AuthStandardClientScopes,
      });
      expect(calls.map((call) => new URL(call.url).pathname)).toEqual([
        "/.well-known/t3/environment",
        "/api/auth/pairing-token",
      ]);
    }).pipe(
      Effect.provide(
        SelfHostedConnect.layer(config(), {
          fetch: async (url, init) => {
            calls.push({ url: String(url), init });
            return online(url, init);
          },
        }),
      ),
    );
  },
);

it.effect("marks offline environments unavailable and maps a rejected pairing request", () =>
  Effect.gen(function* () {
    const service = yield* SelfHostedConnect.SelfHostedConnect;
    const session = yield* service.login(credentials, "client");
    expect((yield* service.listEnvironments(session.accessToken)).environments[0]?.available).toBe(
      false,
    );
    expect((yield* Effect.flip(service.connect(session.accessToken, "office")))._tag).toBe(
      "EnvironmentUnavailable",
    );
  }).pipe(
    Effect.provide(
      SelfHostedConnect.layer(config(), {
        fetch: async () => new Response("offline", { status: 503 }),
      }),
    ),
  ),
);

it.effect("times out pairing and aborts the upstream request", () =>
  Effect.gen(function* () {
    const service = yield* SelfHostedConnect.SelfHostedConnect;
    const session = yield* service.login(credentials, "client");
    const pending = yield* service
      .connect(session.accessToken, "office")
      .pipe(Effect.flip, Effect.forkChild);
    yield* TestClock.adjust("100 millis");
    expect((yield* Fiber.join(pending))._tag).toBe("UpstreamTimeout");
  }).pipe(
    Effect.provide(
      SelfHostedConnect.layer(config({ requestTimeoutMs: 100 }), {
        fetch: (_url, init) =>
          new Promise((_resolve, reject) => {
            init?.signal?.addEventListener("abort", () => reject(new Error("aborted")), {
              once: true,
            });
          }),
      }),
    ),
  ),
);

it.effect(
  "persists token hashes without plaintext tokens and supports restarting the service",
  () =>
    Effect.gen(function* () {
      const directory = yield* Effect.acquireRelease(
        Effect.promise(() =>
          NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "self-hosted-connect-")),
        ),
        (path) => Effect.promise(() => NodeFSP.rm(path, { recursive: true, force: true })),
      );
      const databasePath = NodePath.join(directory, "sessions.sqlite");
      const session = yield* Effect.scoped(
        Effect.gen(function* () {
          const service = yield* SelfHostedConnect.SelfHostedConnect;
          return yield* service.login(credentials, "client");
        }).pipe(
          Effect.provide(SelfHostedConnect.layer(config({ databasePath }), { fetch: online })),
        ),
      );
      const bytes = yield* Effect.promise(() => NodeFSP.readFile(databasePath));
      expect(bytes.includes(Buffer.from(session.accessToken))).toBe(false);
      expect(bytes.includes(Buffer.from(session.refreshToken))).toBe(false);
      const resumed = yield* Effect.gen(function* () {
        const service = yield* SelfHostedConnect.SelfHostedConnect;
        return yield* service.refresh(session.refreshToken);
      }).pipe(Effect.provide(SelfHostedConnect.layer(config({ databasePath }), { fetch: online })));
      expect(resumed.accountId).toBe("owner");
    }).pipe(Effect.scoped),
);

it("rejects untrusted pairing targets, public HTTP, duplicate ids, and userinfo", () => {
  const base = config({ databasePath: "/private/state/sessions.sqlite" });
  for (const override of [
    { origin: "http://office.example.com" },
    { origin: "https://user:secret@office.example.com" },
    { pairingOrigin: "http://internal.example.com" },
    { pairingOrigin: "https://office.example.com/path" },
    { credential: "secret\nheader" },
  ]) {
    expect(() =>
      decodeConfig({ ...base, environments: [{ ...base.environments[0], ...override }] }),
    ).toThrow();
  }
  expect(() =>
    decodeConfig({ ...base, environments: [base.environments[0], base.environments[0]] }),
  ).toThrow();
});
