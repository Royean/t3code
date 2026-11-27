import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { expect, it } from "@effect/vitest";
import {
  AuthSessionId,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentHttpApi,
  WorkbenchQueueError,
  type AuthEnvironmentScope,
  type WorkbenchTaskProgressResult,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpPlatform from "effect/http/HttpPlatform";
import * as Etag from "effect/http/Etag";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as Progress from "./WorkbenchTaskProgress.ts";
import * as ProgressHttp from "./workbenchTaskProgressHttp.ts";
import { failEnvironmentAuthInvalid } from "../auth/http.ts";

class TestApi extends HttpApi.make("environment").add(
  EnvironmentHttpApi.groups.workbenchTaskProgress,
) {}

it.effect(
  "authenticates and checks both scopes before reading an exact bounded progress target",
  () =>
    Effect.gen(function* () {
      let reads = 0;
      let failure: WorkbenchQueueError | undefined;
      const sample: WorkbenchTaskProgressResult = {
        environmentId: "environment",
        trackingId: "tracking",
        queueId: "queue",
        controllerId: "controller",
        projectId: "project" as never,
        task: {
          id: "item",
          taskId: "task" as never,
          taskCreatedAt: "2026-11-27T00:00:00.000Z",
          dispatcherThreadId: "dispatcher" as never,
          workThreadId: null,
          previousTaskId: null,
          cycleNumber: 1,
          version: 0,
          businessState: "active",
          queueState: "queued",
          workState: "unknown",
          blockedReason: null,
          matter: null,
          branch: null,
          head: {
            state: "not_created",
            sha: null,
            observedAt: "2026-11-27T00:00:00.000Z",
            checksBound: false,
          },
          checks: { state: "not_configured", passed: 0, failed: 0, used: null, sourceRunId: null },
          nextStep: "await_dispatch",
          threadUrl: null,
          updatedAt: "2026-11-27T00:00:00.000Z",
        },
      };
      const web = HttpRouter.toWebHandler(
        HttpApiBuilder.layer(TestApi).pipe(
          Layer.provide(ProgressHttp.layer),
          Layer.provide(
            Layer.mock(Progress.WorkbenchTaskProgress)({
              get: (queueId, workItemId, scope) =>
                Effect.gen(function* () {
                  reads++;
                  expect({ queueId, workItemId, ...scope }).toEqual({
                    queueId: "queue",
                    workItemId: "item",
                    controllerId: "controller",
                    projectId: "project",
                    expectedTrackingId: "tracking",
                  });
                  if (failure) return yield* failure;
                  return sample;
                }),
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
                    sessionId: AuthSessionId.make("fixture"),
                    subject: "fixture",
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
      const query = "controllerId=controller&projectId=project&expectedTrackingId=tracking";
      const call = (selector = query, scopes?: string) =>
        Effect.promise(() =>
          web.handler(
            new Request(`http://env.local/api/workbench/queues/queue/progress/item?${selector}`, {
              headers: scopes === undefined ? {} : { "test-scopes": scopes },
            }),
          ),
        );
      expect((yield* call()).status).toBe(401);
      expect((yield* call(query, "orchestration:read")).status).toBe(403);
      expect((yield* call(query, "orchestration:operate")).status).toBe(403);
      expect(reads).toBe(0);
      const scopes = "orchestration:read,orchestration:operate";
      expect((yield* call("controllerId=controller&projectId=project", scopes)).status).toBe(400);
      expect(
        (yield* call(
          `controllerId=${"x".repeat(201)}&projectId=project&expectedTrackingId=tracking`,
          scopes,
        )).status,
      ).toBe(400);
      const response = yield* call(query, scopes);
      expect(response.status).toBe(200);
      expect(yield* Effect.promise(() => response.json())).toEqual(sample);
      failure = new WorkbenchQueueError({ reason: "invalid_binding" });
      const rejected = yield* call(query, scopes);
      expect(rejected.status).toBe(409);
      expect(yield* Effect.promise(() => rejected.json())).toMatchObject({
        reason: "invalid_binding",
      });
      expect(reads).toBe(2);
    }),
);
