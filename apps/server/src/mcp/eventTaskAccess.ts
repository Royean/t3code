import { OrchestratorMcpFailure } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";

import * as WorkbenchGuard from "../scheduledTasks/WorkbenchQueueGuard.ts";
import * as EventOrchestrationService from "../eventOrchestration/EventOrchestrationService.ts";
import * as McpInvocationContext from "./McpInvocationContext.ts";

const unmanagedHandoffs = new Set([
  "delegate_task",
  "create_threads",
  "t3_thread_launch",
  "t3_thread_send",
  "t3_thread_send_attachments",
  "t3_thread_transfer",
  "t3_thread_configure",
  "t3_worktree_handoff",
  "schedule_task",
  "run_scheduled_task_now",
]);

export const requireManagedEventHandoff = Effect.fn("McpEventTasks.requireManagedHandoff")(
  function* (toolName: string) {
    if (!unmanagedHandoffs.has(toolName)) return;
    const service = yield* Effect.serviceOption(
      EventOrchestrationService.EventOrchestrationService,
    );
    const scope = yield* Effect.serviceOption(McpInvocationContext.McpInvocationContext);
    const threadId = Option.getOrUndefined(scope)?.thread?.threadId;
    if (threadId === undefined) return;
    const workbenchGuard = yield* Effect.serviceOption(WorkbenchGuard.WorkbenchQueueGuard);
    if (Option.isSome(workbenchGuard) && (yield* workbenchGuard.value.ownsThread(threadId)))
      return yield* new OrchestratorMcpFailure({
        code: "capability_denied",
        message:
          "Workbench dispatchers must use t3_workbench_dispatch so persistent queue claims and execution limits stay enforced.",
      });
    if (Option.isSome(service) && service.value.ownsThread(threadId)) {
      return yield* new OrchestratorMcpFailure({
        code: "capability_denied",
        message:
          "Managed event work must use event_task_assign and event_task_report so role, dependency and retry limits stay enforced.",
      });
    }
  },
);
