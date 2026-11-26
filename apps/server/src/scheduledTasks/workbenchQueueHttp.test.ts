import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { expect, it } from "@effect/vitest";
import {
  AuthSessionId,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentHttpApi,
  EnvironmentId,
  WorkbenchQueueError,
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
import * as WorkItems from "./WorkbenchQueue.ts";
import * as ExecutionsHttp from "./workbenchQueueHttp.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import { failEnvironmentAuthInvalid } from "../auth/http.ts";

class TestApi extends HttpApi.make("environment").add(EnvironmentHttpApi.groups.workbenchQueue) {}

it.effect("requires both scopes before queue reads or control mutations", () =>
  Effect.gen(function* () {
    let calls = 0;
    let failure: WorkbenchQueueError | undefined;
    const snapshot = {
      trackingId: "epoch",
      queueId: "queue",
      controllerId: "controller",
      projectId: "project" as never,
      concurrency: 2 as const,
      occupied: 0,
      bindings: [],
      items: [],
    };
    const receipt = {
      trackingId: "epoch",
      queueId: "queue",
      controllerId: "controller",
      projectId: "project" as never,
      taskId: "task" as never,
      taskCreatedAt: "2026-11-26T00:00:00.000Z",
      dispatcherThreadId: "thread" as never,
      intentId: "intent",
      subscriptionScopeId: "scope",
      businessDeliveryId: "event",
      bodySha256: "a".repeat(64),
    };
    const prepare = () =>
      Effect.gen(function* () {
        calls++;
        if (failure !== undefined) return yield* failure;
        return receipt;
      });
    const result = () =>
      Effect.gen(function* () {
        calls++;
        if (failure !== undefined) return yield* failure;
        return snapshot;
      });
    const web = HttpRouter.toWebHandler(
      HttpApiBuilder.layer(TestApi).pipe(
        Layer.provide(ExecutionsHttp.layer),
        Layer.provide(
          Layer.mock(WorkItems.WorkbenchQueue)({
            get: result,
            bind: result,
            reconcile: result,
            priority: result,
            prepareDeliveryIntent: prepare,
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
          HttpPlatform.layer.pipe(Layer.provide(NodeServices.layer), Layer.provide(Etag.layerWeak)),
        ),
        Layer.provide(Etag.layerWeak),
        Layer.provide([NodeServices.layer, NodeCrypto.layer]),
      ),
      { disableLogger: true },
    );
    yield* Effect.addFinalizer(() => Effect.promise(() => web.dispose()));
    const requests = [
      new Request(
        "http://env.local/api/workbench/queues/queue?controllerId=controller&projectId=project",
      ),
      ...["bind", "reconcile", "priority"].map(
        (action) =>
          new Request("http://env.local/api/workbench/queues/queue/" + action, {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              controllerId: "controller",
              projectId: "project",
              taskId: "task",
              dispatcherThreadId: "thread",
              workItemId: "one",
              priority: 1,
            }),
          }),
      ),
    ];
    requests.push(
      new Request("http://env.local/api/workbench/queues/queue/delivery-intents", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({ ...receipt, expectedTrackingId: "epoch", matter: null }),
      }),
    );
    for (const request of requests) {
      expect((yield* Effect.promise(() => web.handler(request.clone()))).status).toBe(401);
      for (const scope of ["orchestration:read", "orchestration:operate"]) {
        const denied = new Request(request.clone(), {
          headers: { ...Object.fromEntries(request.headers), "test-scopes": scope },
        });
        expect((yield* Effect.promise(() => web.handler(denied))).status).toBe(403);
      }
    }
    expect(calls).toBe(0);
    for (const request of requests) {
      const allowed = new Request(request.clone(), {
        headers: {
          ...Object.fromEntries(request.headers),
          "test-scopes": "orchestration:read,orchestration:operate",
        },
      });
      const response = yield* Effect.promise(() => web.handler(allowed));
      expect(response.status).toBe(200);
      expect(yield* Effect.promise(() => response.json())).toEqual({
        ...(request.url.endsWith("/delivery-intents") ? receipt : snapshot),
        environmentId: "env-test",
      });
    }
    expect(calls).toBe(5);
    failure = Object.defineProperty(new WorkbenchQueueError({ reason: "unavailable" }), "cause", {
      value: new Error("private SQL connection credential"),
    });
    for (const request of requests) {
      const allowed = new Request(request.clone(), {
        headers: {
          ...Object.fromEntries(request.headers),
          "test-scopes": "orchestration:read,orchestration:operate",
        },
      });
      const response = yield* Effect.promise(() => web.handler(allowed));
      expect(response.status).toBe(409);
      expect(yield* Effect.promise(() => response.json())).toEqual({
        _tag: "WorkbenchQueueError",
        reason: "unavailable",
      });
    }
  }),
);
