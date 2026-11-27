import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  EnvironmentHttpApi,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import { requireEnvironmentScope } from "../auth/http.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as Repairs from "./WorkbenchRepairs.ts";

export const layer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "workbenchRepairs",
  Effect.fnUntraced(function* (handlers) {
    const repairs = yield* Repairs.WorkbenchRepairs,
      identity = yield* ServerEnvironment.ServerEnvironmentIdentity;
    return handlers.handle(
      "list",
      Effect.fnUntraced(function* ({ params, query }) {
        yield* requireEnvironmentScope(AuthOrchestrationReadScope);
        yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
        return {
          ...(yield* repairs.list(params.queueId, query)),
          environmentId: yield* identity.getEnvironmentId,
        };
      }),
    );
  }),
);
