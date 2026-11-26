import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  EnvironmentHttpApi,
  EnvironmentScheduledTaskExecutionError,
  ScheduledTaskId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import { failEnvironmentInternal, requireEnvironmentScope } from "../auth/http.ts";
import * as ServerEnvironment from "../environment/ServerEnvironment.ts";
import * as ScheduledTaskService from "./ScheduledTaskService.ts";

export const layer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "scheduledTaskExecutions",
  Effect.fnUntraced(function* (handlers) {
    const service = yield* ScheduledTaskService.ScheduledTaskService;
    const environment = yield* ServerEnvironment.ServerEnvironmentIdentity;
    return handlers.handle(
      "get",
      Effect.fnUntraced(function* ({ params, query }) {
        yield* requireEnvironmentScope(AuthOrchestrationReadScope);
        yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
        const result = yield* service
          .getExecutions({ id: ScheduledTaskId.make(params.taskId), ...query })
          .pipe(
            Effect.catchTags({
              ScheduledTaskError: (cause) =>
                cause.cause === undefined
                  ? Effect.fail(
                      new EnvironmentScheduledTaskExecutionError({
                        reason: "tracking_unavailable",
                      }),
                    )
                  : failEnvironmentInternal("internal_error", cause),
            }),
          );
        return { ...result, environmentId: yield* environment.getEnvironmentId };
      }),
    );
  }),
);
