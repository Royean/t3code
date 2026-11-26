import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  EnvironmentHttpApi,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import { requireEnvironmentScope } from "../auth/http.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as Queue from "./WorkbenchQueue.ts";

export const layer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "workbenchQueue",
  Effect.fnUntraced(function* (handlers) {
    const queue = yield* Queue.WorkbenchQueue;
    const identity = yield* ServerEnvironment.ServerEnvironmentIdentity;
    return handlers
      .handle(
        "prepareDeliveryIntent",
        Effect.fnUntraced(function* ({ params, payload }) {
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
          return {
            ...(yield* queue.prepareDeliveryIntent(params.queueId, payload)),
            environmentId: yield* identity.getEnvironmentId,
          };
        }),
      )
      .handle(
        "get",
        Effect.fnUntraced(function* ({ params, query }) {
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
          return {
            ...(yield* queue.get(params.queueId, query)),
            environmentId: yield* identity.getEnvironmentId,
          };
        }),
      )
      .handle(
        "bind",
        Effect.fnUntraced(function* ({ params, payload }) {
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
          return {
            ...(yield* queue.bind(params.queueId, payload)),
            environmentId: yield* identity.getEnvironmentId,
          };
        }),
      )
      .handle(
        "reconcile",
        Effect.fnUntraced(function* ({ params, payload }) {
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
          return {
            ...(yield* queue.reconcile(params.queueId, payload)),
            environmentId: yield* identity.getEnvironmentId,
          };
        }),
      )
      .handle(
        "priority",
        Effect.fnUntraced(function* ({ params, payload }) {
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
          return {
            ...(yield* queue.priority(params.queueId, payload)),
            environmentId: yield* identity.getEnvironmentId,
          };
        }),
      );
  }),
);
