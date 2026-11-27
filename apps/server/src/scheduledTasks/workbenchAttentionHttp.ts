import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  EnvironmentHttpApi,
  EnvironmentAuthenticatedPrincipal,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import { requireEnvironmentScope } from "../auth/http.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as Attention from "./WorkbenchAttention.ts";
import { digest } from "./workbenchLifecycle.ts";
export const layer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "workbenchAttention",
  Effect.fnUntraced(function* (handlers) {
    const attention = yield* Attention.WorkbenchAttention,
      identity = yield* ServerEnvironment.ServerEnvironmentIdentity;
    const authorize = requireEnvironmentScope(AuthOrchestrationReadScope).pipe(
      Effect.andThen(requireEnvironmentScope(AuthOrchestrationOperateScope)),
    );
    return handlers
      .handle(
        "get",
        Effect.fnUntraced(function* ({ params, query }) {
          yield* authorize;
          return {
            ...(yield* attention.get(params.queueId, query)),
            environmentId: yield* identity.getEnvironmentId,
          };
        }),
      )
      .handle(
        "pause",
        Effect.fnUntraced(function* ({ params, payload }) {
          yield* authorize;
          return {
            ...(yield* attention.pause(params.queueId, params.workItemId, payload)),
            environmentId: yield* identity.getEnvironmentId,
          };
        }),
      )
      .handle(
        "record",
        Effect.fnUntraced(function* ({ params, payload }) {
          yield* authorize;
          return {
            ...(yield* attention.record(
              params.queueId,
              params.attentionId,
              payload,
              digest((yield* EnvironmentAuthenticatedPrincipal).sessionId),
            )),
            environmentId: yield* identity.getEnvironmentId,
          };
        }),
      )
      .handle(
        "requeue",
        Effect.fnUntraced(function* ({ params, payload }) {
          yield* authorize;
          return {
            ...(yield* attention.requeue(
              params.queueId,
              params.attentionId,
              payload,
              digest((yield* EnvironmentAuthenticatedPrincipal).sessionId),
            )),
            environmentId: yield* identity.getEnvironmentId,
          };
        }),
      );
  }),
);
