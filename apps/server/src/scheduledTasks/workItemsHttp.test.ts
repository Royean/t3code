import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { expect, it } from "@effect/vitest";
import {
  AuthSessionId,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentHttpApi,
  EnvironmentId,
  type AuthEnvironmentScope,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpPlatform from "effect/http/HttpPlatform";
import * as Etag from "effect/http/Etag";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as WorkItems from "./WorkbenchWorkItems.ts";
import * as ExecutionsHttp from "./workItemsHttp.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import { failEnvironmentAuthInvalid } from "../auth/http.ts";

class TestApi extends HttpApi.make("environment").add(
  EnvironmentHttpApi.groups.workbenchWorkItems,
) {}

it.effect(
  "requires authentication and both scopes and decodes a bounded business delivery selector",
  () =>
    Effect.gen(function* () {
      let reads = 0;
      const web = HttpRouter.toWebHandler(
        HttpApiBuilder.layer(TestApi).pipe(
          Layer.provide(ExecutionsHttp.layer),
          Layer.provide(
            Layer.mock(WorkItems.WorkbenchWorkItems)({
              get: (taskId) =>
                Effect.sync(() => {
                  reads++;
                  return {
                    trackingId: "epoch",
                    taskId,
                    taskCreatedAt: "2026-11-26T00:00:00.000Z",
                    projectId: "project",
                    workItems: [],
                  };
                }),
            }),
          ),
          Layer.provide(
            Layer.succeed(ServerEnvironment.ServerEnvironmentIdentity, {
              getEnvironmentId: Effect.succeed(EnvironmentId.make("env-test")),
            }),
          ),
          Layer.provide(
            Layer.succeed(EnvironmentAuthenticatedAuth, (effect) =>
              Effect.gen(function* () {
                const request = yield* HttpServerRequest.HttpServerRequest;
                if (request.headers["test-scopes"] === undefined)
                  return yield* failEnvironmentAuthInvalid("missing_credential");
                return yield* effect.pipe(
                  Effect.provideService(EnvironmentAuthenticatedPrincipal, {
                    sessionId: AuthSessionId.make("test"),
                    subject: "test",
                    method: "bearer-access-token",
                    scopes: new Set(
                      request.headers["test-scopes"].split(",") as AuthEnvironmentScope[],
                    ),
                  }),
                );
              }),
            ),
          ),
          Layer.provide(
            HttpPlatform.layer.pipe(
              Layer.provide(NodeServices.layer),
              Layer.provide(Etag.layerWeak),
            ),
          ),
          Layer.provide(Etag.layerWeak),
          Layer.provide([NodeServices.layer, NodeCrypto.layer]),
        ),
        { disableLogger: true },
      );
      yield* Effect.addFinalizer(() => Effect.promise(() => web.dispose()));
      const call = (query: string, scopes?: string) =>
        Effect.promise(() =>
          web.handler(
            new Request(`http://env.local/api/scheduled-tasks/task/work-items?${query}`, {
              headers: scopes === undefined ? {} : { "test-scopes": scopes },
            }),
          ),
        );
      expect((yield* call("businessDeliveryId=one")).status).toBe(401);
      expect((yield* call("businessDeliveryId=one", "orchestration:read")).status).toBe(403);
      expect((yield* call("businessDeliveryId=one", "orchestration:operate")).status).toBe(403);
      expect(reads).toBe(0);
      const scopes = "orchestration:read,orchestration:operate";
      expect((yield* call("", scopes)).status).toBe(400);
      expect((yield* call(`businessDeliveryId=${"a".repeat(201)}`, scopes)).status).toBe(400);
      const response = yield* call("businessDeliveryId=one", scopes);
      expect(response.status).toBe(200);
      expect(yield* Effect.promise(() => response.json())).toEqual({
        trackingId: "epoch",
        environmentId: "env-test",
        taskId: "task",
        taskCreatedAt: "2026-11-26T00:00:00.000Z",
        projectId: "project",
        workItems: [],
      });
      expect((yield* call("deliveryId=delivery%3Anative", scopes)).status).toBe(400);
      expect(reads).toBe(1);
    }),
);
