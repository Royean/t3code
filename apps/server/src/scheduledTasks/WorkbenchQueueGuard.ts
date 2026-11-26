import { CommandId, type OrchestrationV2ServerCommand, type ThreadId } from "@t3tools/contracts";
import { derive } from "@t3tools/provider-core/server/IdAllocator";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import { OrchestratorDispatchError } from "../orchestration-v2/Orchestrator.ts";

export const WorkbenchQueuePermit = Context.Reference<
  { readonly workItemId: string; readonly claimId: string } | undefined
>("t3/workbench/QueuePermit", { defaultValue: () => undefined });

export class WorkbenchQueueGuard extends Context.Service<
  WorkbenchQueueGuard,
  {
    readonly ownsThread: (threadId: ThreadId) => Effect.Effect<boolean>;
    readonly preservePreparation: (
      threadId: ThreadId,
      runId: string,
      projectId: string,
      recoverySignal?: string,
    ) => Effect.Effect<boolean>;
    readonly markWakeRecovery: (
      threadId: ThreadId,
      messageId: string,
      recoverySignal: string,
    ) => Effect.Effect<void>;
    readonly assertCommand: (
      command: OrchestrationV2ServerCommand,
    ) => Effect.Effect<void, OrchestratorDispatchError>;
  }
>()("t3/scheduledTasks/WorkbenchQueueGuard") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const ownsThread = Effect.fn("WorkbenchQueueGuard.ownsThread")(function* (threadId: ThreadId) {
    const rows =
      yield* sql`SELECT 1 FROM workbench_queue_bindings WHERE dispatcher_thread_id=${threadId}
      UNION ALL SELECT 1 FROM workbench_work_items WHERE dispatcher_thread_id=${threadId} LIMIT 1`;
    return rows.length > 0;
  }, Effect.orDie);
  const assertCommand = Effect.fn("WorkbenchQueueGuard.assertCommand")(
    function* (command: OrchestrationV2ServerCommand) {
      if (command.type !== "delegated_task.request") return;
      if (!(yield* ownsThread(command.parentThreadId))) return;
      const permit = yield* WorkbenchQueuePermit;
      const rows =
        permit === undefined
          ? []
          : yield* sql`SELECT 1 FROM workbench_queue_entries e JOIN workbench_work_items w ON w.id=e.work_item_id
      JOIN workbench_queue_bindings b ON b.queue_id=e.queue_id AND b.task_id=w.task_id AND b.task_created_at=w.task_created_at
      WHERE e.state='occupied' AND e.work_item_id=${permit.workItemId} AND e.claim_id=${permit.claimId} AND w.request_id=${command.commandId}
      AND b.dispatcher_thread_id=${command.parentThreadId} AND w.dispatcher_thread_id=b.dispatcher_thread_id AND w.project_id=b.project_id LIMIT 1`;
      if (rows.length !== 1)
        return yield* new OrchestratorDispatchError({
          commandId: command.commandId,
          commandType: command.type,
          cause:
            "Workbench dispatchers require a durable queue claim through t3_workbench_dispatch.",
        });
    },
    Effect.catchTags({
      SqlError: (cause) => Effect.die(cause),
    }),
  );
  const preservePreparation = Effect.fn("WorkbenchQueueGuard.preservePreparation")(function* (
    threadId: ThreadId,
    runId: string,
    projectId: string,
    recoverySignal?: string,
  ) {
    const rows = yield* sql<{
      work_item_id: string;
      request_id: string;
    }>`SELECT e.work_item_id,w.request_id FROM workbench_queue_entries e JOIN workbench_work_items w ON w.id=e.work_item_id
      JOIN workbench_queue_bindings b ON b.queue_id=e.queue_id AND b.task_id=w.task_id AND b.task_created_at=w.task_created_at
      WHERE e.state='occupied' AND w.request_id IS NOT NULL AND w.project_id=${projectId} AND b.project_id=w.project_id AND b.dispatcher_thread_id=w.dispatcher_thread_id`;
    const row = rows.find((row) => {
      const child = derive.delegatedTaskThread({ commandId: CommandId.make(row.request_id) });
      return child === threadId && derive.run({ threadId: child, ordinal: 1 }) === runId;
    });
    if (row && recoverySignal !== undefined)
      yield* sql`UPDATE workbench_queue_entries SET needs_dispatch=1,recovery_signal=${recoverySignal},work_state='unknown',blocked_reason='awaiting_dispatcher' WHERE work_item_id=${row.work_item_id} AND state='occupied'`;
    return row !== undefined;
  }, Effect.orDie);
  const markWakeRecovery = Effect.fn("WorkbenchQueueGuard.markWakeRecovery")(function* (
    threadId: ThreadId,
    messageId: string,
    recoverySignal: string,
  ) {
    yield* sql`UPDATE workbench_queue_entries SET recovery_signal=${recoverySignal},needs_dispatch=1,work_state='unknown',blocked_reason='awaiting_dispatcher'
      WHERE state='occupied' AND needs_dispatch=1 AND 'message:workbench-queue:' || claim_id || ':' || wake_generation=${messageId}
      AND EXISTS(SELECT 1 FROM workbench_work_items w WHERE w.id=work_item_id AND w.dispatcher_thread_id=${threadId})`;
  }, Effect.orDie);
  return { ownsThread, assertCommand, preservePreparation, markWakeRecovery };
});
export const layer = Layer.effect(WorkbenchQueueGuard, make);
