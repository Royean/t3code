import { expect, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as NodeServices from "@effect/platform-node/NodeServices";
import {
  AlwaysOnRoutingError,
  AuthSessionId,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentHttpApi,
  EnvironmentId,
  ThreadId,
  type AuthEnvironmentScope,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Etag from "effect/http/Etag";
import * as HttpPlatform from "effect/http/HttpPlatform";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";

import { AlwaysOnRouting } from "./AlwaysOnRouting.ts";
import * as RoutingHttp from "./routingHttp.ts";

class TestApi extends HttpApi.make("environment").add(EnvironmentHttpApi.groups.alwaysOn) {}

it.effect("requires an operate grant and returns the routed thread without an agent id", () =>
  Effect.gen(function* () {
    const submissions = yield* Ref.make(0);
    const receipt = {
      environmentId: EnvironmentId.make("environment:target"),
      threadId: ThreadId.make("always-on-event-test"),
      duplicate: false,
    };
    const modelSelection = {
      instanceId: "codex",
      model: "test-model",
      options: [{ id: "reasoningEffort", value: "high" }],
    };
    const routing = Layer.mock(AlwaysOnRouting)({
      submit: (event) =>
        Effect.sync(() => expect(event.modelSelection).toEqual(modelSelection)).pipe(
          Effect.andThen(Ref.update(submissions, (count) => count + 1)),
          Effect.andThen(
            event.sourceEventId === "peer-offline"
              ? Effect.fail(
                  new AlwaysOnRoutingError({
                    reason: "unavailable",
                    message: "The destination did not answer.",
                  }),
                )
              : Effect.succeed({
                  ...receipt,
                  environmentId: event.environmentId ?? receipt.environmentId,
                }),
          ),
        ),
    });
    const auth = Layer.succeed(EnvironmentAuthenticatedAuth, (effect) =>
      Effect.flatMap(HttpServerRequest.HttpServerRequest, (request) =>
        effect.pipe(
          Effect.provideService(EnvironmentAuthenticatedPrincipal, {
            sessionId: AuthSessionId.make("session:always-on-http"),
            subject: "test-operator",
            method: "bearer-access-token",
            scopes: new Set(
              (request.headers["test-scopes"]?.split(",") ?? []) as AuthEnvironmentScope[],
            ),
          }),
        ),
      ),
    );
    const web = HttpRouter.toWebHandler(
      HttpApiBuilder.layer(TestApi).pipe(
        Layer.provide(RoutingHttp.layer),
        Layer.provide(routing),
        Layer.provide(auth),
        Layer.provide(
          HttpPlatform.layer.pipe(Layer.provide(NodeServices.layer), Layer.provide(Etag.layerWeak)),
        ),
        Layer.provide(Etag.layerWeak),
        Layer.provide([NodeServices.layer, NodeCrypto.layer]),
      ),
      { disableLogger: true },
    );
    yield* Effect.addFinalizer(() => Effect.promise(() => web.dispose()));
    const call = (scope: AuthEnvironmentScope, sourceEventId: string) =>
      Effect.promise(() =>
        web.handler(
          new Request("http://env.local/api/always-on/events", {
            method: "POST",
            headers: { "content-type": "application/json", "test-scopes": scope },
            body: JSON.stringify({
              environmentId: "environment:requested",
              modelSelection,
              source: "fixture",
              sourceEventId,
              text: "Check the build.",
            }),
          }),
        ),
      );

    const refused = yield* call("orchestration:read", "first");
    expect(refused.status).toBe(403);
    expect(yield* Ref.get(submissions)).toBe(0);

    const accepted = yield* call("orchestration:operate", "first");
    expect(accepted.status).toBe(202);
    expect(yield* Effect.promise(() => accepted.json())).toEqual({
      ...receipt,
      environmentId: "environment:requested",
    });

    const offline = yield* call("orchestration:operate", "peer-offline");
    expect(offline.status).toBe(503);
    expect(yield* Effect.promise(() => offline.json())).toMatchObject({
      message: "The destination did not answer.",
    });
    expect(yield* Ref.get(submissions)).toBe(2);
  }).pipe(Effect.scoped),
);
