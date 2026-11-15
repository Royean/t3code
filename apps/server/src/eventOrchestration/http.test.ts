import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { expect, it } from "@effect/vitest";
import {
  AuthSessionId,
  CommandId,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentHttpApi,
  ProjectId,
  ThreadId,
  type AuthEnvironmentScope,
  type EventOrchestrationConfig,
  type EventOrchestrationJobDetail,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpPlatform from "effect/http/HttpPlatform";
import * as Etag from "effect/http/Etag";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";

import * as EventOrchestrationService from "./EventOrchestrationService.ts";
import * as EventOrchestrationHttp from "./http.ts";

class TestApi extends HttpApi.make("environment").add(
  EnvironmentHttpApi.groups.eventOrchestration,
) {}

const config = {
  enabled: false,
  projectId: ProjectId.make("project:events-http"),
  coordinatorExecutorId: "primary",
  executors: [],
  roles: [],
  allowedRepositories: ["acme/app"],
  allowedEventTypes: ["issues.ready"],
  runtimeMode: "auto-accept-edits",
  interactionMode: "default",
} satisfies EventOrchestrationConfig;

it.effect("HTTP grants protect configuration and submission before any execution side effect", () =>
  Effect.gen(function* () {
    const writes = yield* Ref.make(0);
    const dependencies = Layer.mock(EventOrchestrationService.EventOrchestrationService)({
      ownsThread: () => false,
      configure: (input) => Ref.update(writes, (count) => count + 1).pipe(Effect.as(input)),
      list: () => Effect.succeed([]),
      submit: (event) =>
        Ref.update(writes, (count) => count + 1).pipe(
          Effect.as({
            id: "event:admission",
            event,
            coordinatorThreadId: ThreadId.make("event:admission:coordinator"),
            coordinatorCommandId: CommandId.make("event:admission:launch"),
            coordinatorLaunchMode: "launch",
            coordinatorLaunchCount: 1,
            coordinatorStatus: "queued",
            coordinatorFailureKind: null,
            coordinatorStopped: false,
            coordinatorLastHeartbeatAt: null,
            coordinatorLastProgressAt: null,
            status: "queued",
            summary: null,
            evidence: [],
            tasks: [],
            createdAt: "2026-11-15T00:00:00.000Z",
            updatedAt: "2026-11-15T00:00:00.000Z",
          } satisfies EventOrchestrationJobDetail),
        ),
    });
    const web = HttpRouter.toWebHandler(
      HttpApiBuilder.layer(TestApi).pipe(
        Layer.provide(EventOrchestrationHttp.layer),
        Layer.provide(dependencies),
        Layer.provide(
          Layer.succeed(EnvironmentAuthenticatedAuth, (effect) =>
            Effect.flatMap(HttpServerRequest.HttpServerRequest, (request) =>
              effect.pipe(
                Effect.provideService(EnvironmentAuthenticatedPrincipal, {
                  sessionId: AuthSessionId.make("session:events-http"),
                  subject: "test-operator",
                  method: "bearer-access-token",
                  scopes: new Set(
                    (request.headers["test-scopes"]?.split(",") ?? []) as AuthEnvironmentScope[],
                  ),
                }),
              ),
            ),
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
    const request = (
      path: string,
      scopes: ReadonlyArray<AuthEnvironmentScope>,
      payload?: unknown,
    ) =>
      new Request(`http://env.local/api/event-orchestration/${path}`, {
        method: payload === undefined ? "GET" : "POST",
        headers: { "content-type": "application/json", "test-scopes": scopes.join(",") },
        ...(payload === undefined ? {} : { body: JSON.stringify(payload) }),
      });
    const call = (input: Request) => Effect.promise(() => web.handler(input));
    const readOnly = yield* call(request("config", ["orchestration:read"], config));
    expect(readOnly.status).toBe(403);
    const settingsOnly = yield* call(request("config", ["settings:write"], config));
    expect(settingsOnly.status).toBe(403);
    expect(yield* Ref.get(writes)).toBe(0);
    const configured = yield* call(
      request("config", ["settings:write", "orchestration:operate"], config),
    );
    expect(configured.status).toBe(200);
    expect(yield* Ref.get(writes)).toBe(1);
    const deniedSubmission = yield* call(
      request("events", ["orchestration:read"], {
        source: "github",
        deliveryId: "delivery:one",
        repository: "acme/app",
        eventType: "issues.ready",
        text: "Fix the issue.",
      }),
    );
    expect(deniedSubmission.status).toBe(403);
    const listed = yield* call(request("jobs", ["orchestration:read"]));
    expect(listed.status).toBe(200);
    expect(yield* Effect.promise(() => listed.json())).toEqual([]);
    const malformedCursor = yield* call(request("changes?after=-1", ["orchestration:read"]));
    expect(malformedCursor.status).toBe(400);
    expect(yield* Ref.get(writes)).toBe(1);
    const accepted = yield* call(
      request("events", ["orchestration:operate"], {
        source: "github",
        deliveryId: "delivery:accepted",
        repository: "acme/app",
        eventType: "issues.ready",
        text: "Approved work.",
      }),
    );
    expect(accepted.status).toBe(200);
    expect(yield* Effect.promise(() => accepted.json())).toEqual({
      jobId: "event:admission",
      accepted: true,
    });
    expect(yield* Ref.get(writes)).toBe(2);
  }).pipe(Effect.scoped),
);
