// @effect-diagnostics nodeBuiltinImport:off -- Isolated HTTP fixtures prove that a mutation is never redirected or replayed.
import * as NodeHttp from "node:http";
import * as NodeFSP from "node:fs/promises";
import * as NodeOS from "node:os";
import * as NodePath from "node:path";
import * as NodeServices from "@effect/platform-node/NodeServices";
import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { FetchHttpClient, HttpClient, HttpClientRequest } from "effect/http";
import * as TestSourceControlHost from "@t3tools/source-control-testing/TestSourceControlHost";

import * as GitHubHttpClient from "./GitHubHttpClient.ts";

it.live.each(["socket", "fetch"] as const)(
  "keeps an exact mutation to one request over %s while ordinary redirects still work",
  (transport) =>
    Effect.gen(function* () {
      const directory = yield* Effect.acquireRelease(
        Effect.promise(() => NodeFSP.mkdtemp(NodePath.join(NodeOS.tmpdir(), "issue18-http-"))),
        (directory) =>
          Effect.promise(() => NodeFSP.rm(directory, { recursive: true, force: true })),
      );
      const socket = NodePath.join(directory, "api.sock");
      const paths: Array<string> = [];
      const server = NodeHttp.createServer(async (request, response) => {
        for await (const _chunk of request) {
          // Drain the body before sending the redirect.
        }
        paths.push(request.url ?? "");
        response.writeHead(request.url === "/start" ? 307 : 200, {
          "content-type": "application/json",
          ...(request.url === "/start" ? { location: "/target" } : {}),
        });
        response.end("{}");
      });
      yield* Effect.acquireRelease(
        Effect.promise(
          () =>
            new Promise<void>((resolve, reject) => {
              server.once("error", reject);
              if (transport === "socket") server.listen(socket, resolve);
              else server.listen(0, "127.0.0.1", resolve);
            }),
        ),
        () =>
          Effect.promise(
            () =>
              new Promise<void>((resolve, reject) => {
                server.closeAllConnections();
                server.close((error) => (error ? reject(error) : resolve()));
              }),
          ),
      );
      const address = server.address();
      const origin =
        transport === "socket"
          ? "https://api.github.com"
          : `http://127.0.0.1:${typeof address === "object" && address ? address.port : 0}`;
      const host = TestSourceControlHost.layer({
        process: {
          run: () =>
            Effect.succeed(
              TestSourceControlHost.processOutput(transport === "socket" ? socket : ""),
            ),
        },
      });
      yield* Effect.gen(function* () {
        const http = yield* HttpClient.HttpClient;
        const request = HttpClientRequest.post(`${origin}/start`).pipe(
          HttpClientRequest.bodyJsonUnsafe({ query: "mutation { updateRefs }" }),
        );
        const ordinary = yield* http.execute(request);
        yield* ordinary.text;
        expect(ordinary.status).toBe(200);
        expect(paths).toEqual(["/start", "/target"]);

        paths.length = 0;
        const exact = yield* http
          .execute(request)
          .pipe(
            Effect.provideService(GitHubHttpClient.NoRedirects, true),
            Effect.provideService(FetchHttpClient.RequestInit, { redirect: "manual" }),
          );
        yield* exact.text;
        expect(exact.status).toBe(307);
        expect(paths).toEqual(["/start"]);
      }).pipe(
        Effect.provide(
          GitHubHttpClient.layer.pipe(
            Layer.provide(Layer.mergeAll(host, FetchHttpClient.layer, NodeServices.layer)),
          ),
        ),
      );
    }).pipe(Effect.scoped),
);
