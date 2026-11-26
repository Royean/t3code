import {
  CommandId,
  NodeId,
  type ScheduledTaskId,
  type ScheduledTask,
  ScheduledTaskError,
  ThreadId,
  type WorkbenchWorkItem,
  type WorkbenchWorkItemsResult,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Receipts from "../orchestration-v2/CommandReceiptStore.ts";
import * as Events from "../orchestration-v2/EventStore.ts";
import * as Layer from "effect/Layer";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as ScheduledTasks from "./ScheduledTaskService.ts";
import { ledgerError } from "./executionLedger.ts";

export interface WorkItemRow {
  id: string;
  task_id: string;
  task_created_at: string;
  project_id: string;
  business_delivery_id: string;
  dispatcher_thread_id: string;
  source_execution_id: string | null;
  event_json: string;
  base_ref: string | null;
  request_id: string | null;
  request_json: string | null;
  parent_run_id: string | null;
  delegated_task_id: string | null;
  work_thread_id: string | null;
  message_id: string | null;
  accepted_run_id: string | null;
  run_id: string | null;
  branch: string | null;
  worktree_path: string | null;
  state: WorkbenchWorkItem["state"];
  error_code: string | null;
  created_at: string;
  updated_at: string;
}

export class WorkbenchWorkItems extends Context.Service<
  WorkbenchWorkItems,
  {
    readonly binding: (taskId: ScheduledTaskId) => Effect.Effect<ScheduledTask, ScheduledTaskError>;
    readonly observe: (row: WorkItemRow) => Effect.Effect<WorkbenchWorkItem, ScheduledTaskError>;
    readonly get: (
      taskId: ScheduledTaskId,
      businessDeliveryId: string,
    ) => Effect.Effect<WorkbenchWorkItemsResult, ScheduledTaskError>;
  }
>()("t3/scheduledTasks/WorkbenchWorkItems") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const receipts = yield* Receipts.CommandReceiptStoreV2;
  const events = yield* Events.EventStoreV2;
  const tasks = yield* ScheduledTasks.ScheduledTaskService;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const binding = Effect.fn("WorkbenchWorkItems.binding")(function* (taskId: ScheduledTaskId) {
    const task = (yield* tasks.list()).tasks.find((task) => task.id === taskId);
    if (
      task === undefined ||
      task.schedule.type !== "webhook" ||
      task.threadMode !== "continue" ||
      task.threadId === null
    )
      return yield* new ScheduledTaskError({
        message: "A fixed webhook dispatcher task is required.",
      });
    const parent = yield* threads.getThreadShell(task.threadId).pipe(Effect.mapError(ledgerError));
    if (
      parent === null ||
      parent.deletedAt !== null ||
      parent.archivedAt !== null ||
      parent.projectId !== task.projectId
    )
      return yield* new ScheduledTaskError({
        message: "The dispatcher no longer belongs to this project.",
      });
    return task;
  });
  const observe = Effect.fn("WorkbenchWorkItems.observe")(function* (row: WorkItemRow) {
    if (row.request_id !== null && row.work_thread_id === null) {
      const receipt = yield* receipts
        .getByCommandId(CommandId.make(row.request_id))
        .pipe(Effect.mapError(ledgerError));
      if (
        Option.isSome(receipt) &&
        receipt.value.status === "accepted" &&
        receipt.value.threadId === row.dispatcher_thread_id &&
        receipt.value.commandType === "delegated_task.request"
      ) {
        const stored = yield* events
          .readByCommandId({ commandId: CommandId.make(row.request_id) })
          .pipe(Stream.runCollect, Effect.mapError(ledgerError));
        const taskEvent = stored.find(
          (stored) =>
            stored.event.type === "subagent.updated" &&
            stored.event.payload.origin === "app_owned" &&
            stored.event.payload.threadId === row.dispatcher_thread_id,
        )?.event;
        if (taskEvent?.type === "subagent.updated" && taskEvent.payload.childThreadId !== null) {
          const childId = taskEvent.payload.childThreadId;
          const runEvent = stored.find(
            (stored) => stored.event.type === "run.created" && stored.event.threadId === childId,
          )?.event;
          yield* sql`UPDATE workbench_work_items SET delegated_task_id = ${taskEvent.payload.id}, work_thread_id = ${childId}, parent_run_id = ${taskEvent.payload.runId},
            accepted_run_id = ${runEvent?.type === "run.created" ? runEvent.payload.id : null}, state = 'unknown', error_code = NULL WHERE id = ${row.id}`.pipe(
            Effect.mapError(ledgerError),
          );
          const [current] =
            yield* sql<WorkItemRow>`SELECT * FROM workbench_work_items WHERE id = ${row.id}`.pipe(
              Effect.mapError(ledgerError),
            );
          row = current!;
        }
      }
    }
    // Reconcile the formal source message/run through its existing receipt-aware observer.
    const source = yield* tasks.getExecutions({
      id: row.task_id as ScheduledTaskId,
      businessDeliveryId: row.business_delivery_id,
    });
    const sourceRunId =
      source.effectiveExecutions.find((execution) => execution.id === row.source_execution_id)
        ?.runId ?? null;
    if (row.work_thread_id !== null && row.delegated_task_id !== null) {
      const child = yield* threads
        .getThreadRecords(ThreadId.make(row.work_thread_id), [
          "runs",
          "messages",
          "runtimeRequests",
          "nodes",
        ])
        .pipe(Effect.mapError(ledgerError));
      const parent = yield* threads
        .getThreadRecords(ThreadId.make(row.dispatcher_thread_id), ["subagents"], {
          subagentIds: [NodeId.make(row.delegated_task_id)],
        })
        .pipe(Effect.mapError(ledgerError));
      const task = parent.subagents.find(
        (task) =>
          task.id === row.delegated_task_id &&
          task.origin === "app_owned" &&
          task.childThreadId === row.work_thread_id &&
          task.runId === row.parent_run_id,
      );
      if (
        task === undefined ||
        parent.thread.projectId !== row.project_id ||
        child.thread.projectId !== row.project_id ||
        child.thread.lineage.parentThreadId !== row.dispatcher_thread_id ||
        child.thread.lineage.relationshipToParent !== "subagent"
      )
        return yield* new ScheduledTaskError({
          message: "Work thread ownership does not match the dispatcher.",
        });
      const runs = child.runs
        .filter(
          (run) =>
            run.delegatedTaskId === row.delegated_task_id ||
            (run.delegatedTaskId === undefined &&
              run.id === row.accepted_run_id &&
              child.thread.forkedFrom?.type === "node" &&
              child.thread.forkedFrom.nodeId === row.delegated_task_id),
        )
        .toSorted((a, b) => b.ordinal - a.ordinal);
      const run = runs[0];
      const accepted = runs.at(-1);
      const nodeIds = new Set(
        child.nodes.filter((node) => node.runId === run?.id).map((node) => node.id),
      );
      const blocked = child.runtimeRequests.some(
        (request) =>
          run !== undefined &&
          !["completed", "failed", "cancelled", "interrupted", "rolled_back"].includes(
            run.status,
          ) &&
          request.status === "pending" &&
          ["command", "file-change", "file-read", "permission", "mcp-elicitation"].includes(
            request.kind,
          ) &&
          nodeIds.has(request.nodeId),
      );
      const now = DateTime.formatIso(yield* DateTime.now);
      yield* sql`UPDATE workbench_work_items SET run_id = ${run?.id ?? null},
        accepted_run_id = COALESCE(accepted_run_id, ${accepted?.id ?? null}), message_id = ${run?.userMessageId ?? null},
        branch = ${child.thread.branch}, worktree_path = ${child.thread.worktreePath},
        state = ${blocked ? "blocked" : run === undefined || run.status === "preparing" || child.thread.worktreePath === null ? "unknown" : "linked"}, error_code = NULL, updated_at = ${now}
        WHERE id = ${row.id}`.pipe(Effect.mapError(ledgerError));
      const [current] =
        yield* sql<WorkItemRow>`SELECT * FROM workbench_work_items WHERE id = ${row.id}`.pipe(
          Effect.mapError(ledgerError),
        );
      row = current!;
    }
    return {
      id: row.id,
      businessDeliveryId: row.business_delivery_id,
      dispatcherThreadId: row.dispatcher_thread_id,
      sourceExecutionId: row.source_execution_id,
      sourceRunId,
      requestId: row.request_id,
      parentRunId: row.parent_run_id,
      delegatedTaskId: row.delegated_task_id,
      workThreadId: row.work_thread_id,
      messageId: row.message_id,
      acceptedRunId: row.accepted_run_id,
      runId: row.run_id,
      branch: row.branch,
      worktreePath: row.worktree_path,
      state: row.state,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      errorCode: row.error_code,
    } satisfies WorkbenchWorkItem;
  });
  const get = Effect.fn("WorkbenchWorkItems.get")(function* (
    taskId: ScheduledTaskId,
    businessDeliveryId: string,
  ) {
    const task = yield* binding(taskId);
    const [metadata] = yield* sql<{
      tracking_id: string;
    }>`SELECT tracking_id FROM scheduled_task_execution_metadata WHERE singleton = 1`.pipe(
      Effect.mapError(ledgerError),
    );
    const rows =
      yield* sql<WorkItemRow>`SELECT * FROM workbench_work_items WHERE task_id = ${task.id} AND task_created_at = ${task.createdAt} AND project_id = ${task.projectId} AND dispatcher_thread_id = ${task.threadId} AND business_delivery_id = ${businessDeliveryId} ORDER BY rowid`.pipe(
        Effect.mapError(ledgerError),
      );
    return {
      trackingId: metadata!.tracking_id,
      taskId: task.id,
      taskCreatedAt: task.createdAt,
      projectId: task.projectId,
      workItems: yield* Effect.forEach(rows, observe),
    };
  });
  return { binding, observe, get };
});
export const layer = Layer.effect(WorkbenchWorkItems, make).pipe(
  Layer.provide(Receipts.layer),
  Layer.provide(Events.layer),
);
