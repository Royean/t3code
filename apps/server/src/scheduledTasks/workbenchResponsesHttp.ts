import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentHttpApi,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import { requireEnvironmentScope } from "../auth/http.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as Responses from "./WorkbenchResponses.ts";
import { digest } from "./workbenchLifecycle.ts";
export const layer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "workbenchResponses",
  Effect.fnUntraced(function* (handlers) {
    const responses = yield* Responses.WorkbenchResponses,
      identity = yield* ServerEnvironment.ServerEnvironmentIdentity;
    const authorize = requireEnvironmentScope(AuthOrchestrationReadScope).pipe(
      Effect.andThen(requireEnvironmentScope(AuthOrchestrationOperateScope)),
    );
    return handlers
      .handle(
        "list",
        Effect.fnUntraced(function* ({ params, query }) {
          yield* authorize;
          return {
            ...(yield* responses.list(params.queueId, query)),
            environmentId: yield* identity.getEnvironmentId,
          };
        }),
      )
      .handle(
        "get",
        Effect.fnUntraced(function* ({ params, query }) {
          yield* authorize;
          return {
            ...(yield* responses.get(params.queueId, params.responseId, query)),
            environmentId: yield* identity.getEnvironmentId,
          };
        }),
      )
      .handle(
        "respond",
        Effect.fnUntraced(function* ({ params, payload }) {
          yield* authorize;
          const principal = yield* EnvironmentAuthenticatedPrincipal;
          return {
            ...(yield* responses.respond(params.queueId, payload, digest(principal.sessionId))),
            environmentId: yield* identity.getEnvironmentId,
          };
        }),
      );
  }),
);
