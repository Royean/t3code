import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Effect from "effect/Effect";

import * as EventOrchestrationService from "../../../eventOrchestration/EventOrchestrationService.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { EventOrchestrationToolkit } from "./tools.ts";

const failRequest = (cause: EventOrchestrationService.EventOrchestrationError) =>
  new OrchestratorMcpFailure({
    code: cause.reason === "forbidden" ? "capability_denied" : "invalid_request",
    message: cause.message,
  });

const caller = Effect.gen(function* () {
  const scope = yield* McpInvocationContext.McpInvocationContext;
  if (!scope.capabilities.has("orchestration")) {
    return yield* new OrchestratorMcpFailure({
      code: "capability_denied",
      message: "Event orchestration is unavailable in this provider session.",
    });
  }
  const threadScope = yield* McpInvocationContext.requireThreadScope(scope, "event_task");
  const service = yield* EventOrchestrationService.EventOrchestrationService;
  return { threadId: threadScope.thread.threadId, service };
});

export const layer = McpToolAccess.toLayer(EventOrchestrationToolkit, {
  event_task_assign: McpToolAccess.actsAsCaller((input) =>
    Effect.gen(function* () {
      const { threadId, service } = yield* caller;
      return yield* service.assign(threadId, input).pipe(Effect.mapError(failRequest));
    }),
  ),
  event_task_report: McpToolAccess.actsAsCaller(({ report }) =>
    Effect.gen(function* () {
      const { threadId, service } = yield* caller;
      return yield* service.report(threadId, report).pipe(Effect.mapError(failRequest));
    }),
  ),
  event_task_read: McpToolAccess.reads(({ jobId }) =>
    Effect.gen(function* () {
      const { threadId, service } = yield* caller;
      const job = yield* service.getForCaller(threadId).pipe(Effect.mapError(failRequest));
      if (jobId !== undefined && jobId !== job.id) {
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "This thread is not assigned to that event job.",
        });
      }
      return job;
    }),
  ),
});
