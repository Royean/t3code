import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  AuthSettingsWriteScope,
  EnvironmentEventOrchestrationError,
  EnvironmentHttpApi,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";

import { failEnvironmentInternal, requireEnvironmentScope } from "../auth/http.ts";
import * as EventOrchestrationService from "./EventOrchestrationService.ts";

const failRequest = Effect.fn("environment.events.failRequest")(function* (
  cause: EventOrchestrationService.EventOrchestrationError,
) {
  if (cause.reason === "storage" || cause.reason === "runtime") {
    return yield* failEnvironmentInternal("internal_error", cause);
  }
  return yield* new EnvironmentEventOrchestrationError({ reason: cause.reason });
});

export const layer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "eventOrchestration",
  Effect.fnUntraced(function* (handlers) {
    const service = yield* EventOrchestrationService.EventOrchestrationService;
    return handlers
      .handle(
        "configure",
        Effect.fn("environment.events.configure")(function* ({ payload }) {
          yield* requireEnvironmentScope(AuthSettingsWriteScope);
          yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
          return yield* service.configure(payload).pipe(Effect.catch(failRequest));
        }),
      )
      .handle(
        "submit",
        Effect.fn("environment.events.submit")(function* ({ payload }) {
          yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
          return yield* service.submit(payload).pipe(
            Effect.map((job) => ({ jobId: job.id, accepted: true as const })),
            Effect.catch(failRequest),
          );
        }),
      )
      .handle(
        "list",
        Effect.fn("environment.events.list")(function* () {
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          return yield* service.list().pipe(Effect.catch(failRequest));
        }),
      )
      .handle(
        "get",
        Effect.fn("environment.events.get")(function* ({ params }) {
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          return yield* service.get(params.jobId).pipe(Effect.catch(failRequest));
        }),
      )
      .handle(
        "changes",
        Effect.fn("environment.events.changes")(function* ({ query }) {
          yield* requireEnvironmentScope(AuthOrchestrationReadScope);
          return yield* service
            .changes(query.after ?? 0, query.limit)
            .pipe(Effect.catch(failRequest));
        }),
      )
      .handle(
        "resume",
        Effect.fn("environment.events.resume")(function* ({ payload }) {
          yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
          return yield* service.resume(payload.jobId).pipe(Effect.catch(failRequest));
        }),
      )
      .handle(
        "cancel",
        Effect.fn("environment.events.cancel")(function* ({ payload }) {
          yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
          return yield* service.cancel(payload.jobId).pipe(Effect.catch(failRequest));
        }),
      );
  }),
);
