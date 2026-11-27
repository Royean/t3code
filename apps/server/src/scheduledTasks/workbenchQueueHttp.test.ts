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
import * as Responses from "./WorkbenchResponses.ts";
import * as ResponsesHttp from "./workbenchResponsesHttp.ts";
import { digest } from "./workbenchLifecycle.ts";
import * as Attention from "./WorkbenchAttention.ts";
import * as AttentionHttp from "./workbenchAttentionHttp.ts";
import * as WorkItems from "./WorkbenchQueue.ts";
import * as ExecutionsHttp from "./workbenchQueueHttp.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import { failEnvironmentAuthInvalid } from "../auth/http.ts";

class TestApi extends HttpApi.make("environment")
  .add(EnvironmentHttpApi.groups.workbenchQueue)
  .add(EnvironmentHttpApi.groups.workbenchAttention)
  .add(EnvironmentHttpApi.groups.workbenchResponses) {}

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
    const attention = {
      id: "attention",
      workItemId: "one",
      taskId: "task",
      taskCreatedAt: "2026-11-26T00:00:00.000Z",
      version: 1,
      phase: "awaiting_user",
      reason: {
        code: "manual",
        summary: "Needs a repair",
        requiredAction: "Repair and record the result",
      },
      priority: 1,
      slotState: "released",
      checkpoint: { status: "ready", capturedAt: "2026-11-26T00:00:00.000Z" },
      resolution: { summary: null, recordedAt: null },
      canRequeue: false,
      blockedReason: null,
      dispatcherThreadId: "thread",
      workThreadId: "child",
      sourceRunId: "run",
      resumeRunId: null,
      createdAt: "2026-11-26T00:00:00.000Z",
      updatedAt: "2026-11-26T00:00:00.000Z",
    } as const;
    const attentionSnapshot = {
      trackingId: snapshot.trackingId,
      queueId: snapshot.queueId,
      controllerId: snapshot.controllerId,
      projectId: snapshot.projectId,
      attentions: [attention],
      nextCursor: null,
    };
    const attentionResult = (kind: "pause" | "record" | "requeue") => ({
      trackingId: snapshot.trackingId,
      queueId: snapshot.queueId,
      controllerId: snapshot.controllerId,
      projectId: snapshot.projectId,
      attention,
      operation: { clientRequestId: "request", kind, state: "pending" as const },
      queue: snapshot,
    });
    const attentionRead = () =>
      Effect.gen(function* () {
        calls++;
        if (failure) return yield* failure;
        return attentionSnapshot as never;
      });
    const attentionWrite = (kind: "pause" | "record" | "requeue") => () =>
      Effect.gen(function* () {
        calls++;
        if (failure) return yield* failure;
        return attentionResult(kind) as never;
      });
    let nativeAuditId: string | undefined;
    const responseSnapshot = {
      trackingId: "epoch",
      queueId: "queue",
      controllerId: "controller",
      projectId: "project",
      requests: [],
      nextCursor: null,
    };
    const responseReceipt = {
      trackingId: "epoch",
      queueId: "queue",
      controllerId: "controller",
      projectId: "project",
      response: {
        id: "answer",
        sourceIdentity: "source",
        sourceVersion: 1,
        workItemId: "one",
        requestId: "request",
        status: "recorded",
        blockedReason: null,
        recordedAt: "2026-11-27T00:00:00.000Z",
        updatedAt: "2026-11-27T00:00:00.000Z",
        delivery: { evidence: "none", confirmedAt: null },
        claimedActor: { kind: "workbench_cookie", id: "claimed" },
        nativePrincipal: { kind: "environment_session", id: digest("test") },
        answerMessageId: null,
        answerRunId: null,
      },
    };
    const responseRead = (list: boolean) =>
      Effect.gen(function* () {
        calls++;
        if (failure) return yield* failure;
        return (list ? responseSnapshot : responseReceipt) as never;
      });
    const web = HttpRouter.toWebHandler(
      HttpApiBuilder.layer(TestApi).pipe(
        Layer.provide(ExecutionsHttp.layer),
        Layer.provide(AttentionHttp.layer),
        Layer.provide(ResponsesHttp.layer),
        Layer.provide(
          Layer.mock(Responses.WorkbenchResponses)({
            list: () => responseRead(true),
            get: () => responseRead(false),
            respond: (_q, _input, principal) => {
              nativeAuditId = principal;
              return responseRead(false);
            },
          }),
        ),
        Layer.provide(
          Layer.mock(Attention.WorkbenchAttention)({
            get: attentionRead,
            pause: attentionWrite("pause"),
            record: attentionWrite("record"),
            requeue: attentionWrite("requeue"),
          }),
        ),
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
    requests.push(
      new Request(
        "http://env.local/api/workbench/queues/queue/attention?controllerId=controller&projectId=project&expectedTrackingId=epoch",
      ),
    );
    for (const action of ["pause", "record", "requeue"]) {
      requests.push(
        new Request(
          `http://env.local/api/workbench/queues/queue/attention/${action === "pause" ? "one" : "attention"}/${action}`,
          {
            method: "POST",
            headers: { "content-type": "application/json" },
            body: JSON.stringify({
              controllerId: "controller",
              projectId: "project",
              expectedTrackingId: "epoch",
              clientRequestId: "request",
              expectedVersion: 1,
              expectedWorkItemVersion: 1,
              expectedLatestEventSequence: 1,
              reason: {
                code: "manual",
                summary: "Needs a repair",
                requiredAction: "Repair and record the result",
              },
              result: "Repaired",
              priority: 1,
            }),
          },
        ),
      );
    }
    requests.push(
      new Request(
        "http://env.local/api/workbench/queues/queue/requests?controllerId=controller&projectId=project&expectedTrackingId=epoch",
      ),
      new Request(
        "http://env.local/api/workbench/queues/queue/responses/answer?controllerId=controller&projectId=project&expectedTrackingId=epoch",
      ),
      new Request("http://env.local/api/workbench/queues/queue/responses", {
        method: "POST",
        headers: { "content-type": "application/json" },
        body: JSON.stringify({
          controllerId: "controller",
          projectId: "project",
          expectedTrackingId: "epoch",
          responseId: "answer",
          sourceIdentity: "source",
          expectedVersion: 1,
          claimedActor: { kind: "workbench_cookie", id: "claimed" },
          response: { type: "decision", decision: "decline" },
        }),
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
        ...(request.url.includes("/requests")
          ? responseSnapshot
          : request.url.includes("/responses")
            ? responseReceipt
            : request.url.includes("/attention")
              ? request.method === "GET"
                ? attentionSnapshot
                : attentionResult(request.url.split("/").at(-1) as "pause" | "record" | "requeue")
              : request.url.endsWith("/delivery-intents")
                ? receipt
                : snapshot),
        environmentId: "env-test",
      });
    }
    expect(calls).toBe(12);
    expect(nativeAuditId).toBe(digest("test"));
    expect(nativeAuditId).not.toBe("claimed");
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
