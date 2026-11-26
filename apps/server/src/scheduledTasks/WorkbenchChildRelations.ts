import {
  CommandId,
  type NodeId,
  type RunId,
  ThreadId,
  MessageId,
  ScheduledTaskError,
} from "@t3tools/contracts";
import { derive } from "@t3tools/provider-core/server/IdAllocator";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import * as Receipts from "../orchestration-v2/CommandReceiptStore.ts";
import * as Projections from "../orchestration-v2/ProjectionStore.ts";
import { ledgerError } from "./executionLedger.ts";

import type { BatchRow } from "./workbenchLifecycle.ts";

interface ChildIntent {
  readonly id: string;
  readonly requestId: string;
  readonly dispatcherThreadId: string;
  readonly projectId: string;
}

interface ChildRelation extends ChildIntent {
  readonly workThreadId: ThreadId;
  readonly delegatedTaskId: NodeId;
  readonly parentRunId: RunId;
  readonly acceptedRunId: RunId;
}

export class WorkbenchChildRelations extends Context.Service<
  WorkbenchChildRelations,
  {
    readonly resolve: (
      intent: ChildIntent,
    ) => Effect.Effect<Option.Option<ChildRelation>, ScheduledTaskError>;
    readonly resolveBatch: (
      intent: ChildIntent,
      batch: BatchRow,
    ) => Effect.Effect<
      Option.Option<ChildRelation & { messageId: MessageId; runId: RunId }>,
      ScheduledTaskError
    >;
    readonly find: (
      childThreadId?: ThreadId,
    ) => Effect.Effect<ReadonlyArray<ChildRelation>, ScheduledTaskError>;
  }
>()("t3/scheduledTasks/WorkbenchChildRelations") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const receipts = yield* Receipts.CommandReceiptStoreV2;
  const projections = yield* Projections.ProjectionStoreV2;
  const resolve = Effect.fn("WorkbenchChildRelations.resolve")(function* (intent: ChildIntent) {
    const commandId = CommandId.make(intent.requestId);
    const receipt = yield* receipts.getByCommandId(commandId);
    if (
      Option.isNone(receipt) ||
      receipt.value.status !== "accepted" ||
      receipt.value.threadId !== intent.dispatcherThreadId ||
      receipt.value.commandType !== "delegated_task.request"
    )
      return Option.none<ChildRelation>();
    // The same derivation creates these records in the command transaction. Receipts and
    // projections survive command-event pruning and do not depend on the original provider session.
    const childId = derive.delegatedTaskThread({ commandId });
    const taskId = derive.delegatedTaskNode({ commandId });
    const messageId = derive.delegatedTaskMessage({ commandId });
    const acceptedRunId = derive.run({ threadId: childId, ordinal: 1 });
    const parent = yield* projections.getThreadRecords(receipt.value.threadId, ["subagents"], {
      subagentIds: [taskId],
    });
    const task = parent.subagents.find(
      (task) => task.id === taskId && task.origin === "app_owned" && task.childThreadId === childId,
    );
    if (
      task === undefined ||
      task.runId === null ||
      parent.thread.projectId !== intent.projectId ||
      parent.thread.archivedAt !== null ||
      parent.thread.deletedAt !== null
    )
      return Option.none<ChildRelation>();
    const child = yield* projections.getThreadRecords(childId, ["runs", "messages"], {
      runIds: [acceptedRunId],
      messageIds: [messageId],
    });
    if (
      child.thread.projectId !== intent.projectId ||
      child.thread.lineage.parentThreadId !== intent.dispatcherThreadId ||
      child.thread.lineage.relationshipToParent !== "subagent" ||
      child.thread.forkedFrom?.type !== "node" ||
      child.thread.forkedFrom.nodeId !== taskId ||
      !child.messages.some((message) => message.id === messageId) ||
      !child.runs.some(
        (run) =>
          run.id === acceptedRunId &&
          run.userMessageId === messageId &&
          (run.delegatedTaskId === undefined || run.delegatedTaskId === taskId),
      )
    )
      return Option.none<ChildRelation>();
    return Option.some({
      ...intent,
      workThreadId: childId,
      delegatedTaskId: taskId,
      parentRunId: task.runId,
      acceptedRunId,
    });
  }, Effect.mapError(ledgerError));
  const resolveBatch = Effect.fn("WorkbenchChildRelations.resolveBatch")(function* (
    intent: ChildIntent,
    batch: BatchRow,
  ) {
    const initial = yield* resolve(intent);
    if (Option.isNone(initial) || batch.command_id === null)
      return Option.none<ChildRelation & { messageId: MessageId; runId: RunId }>();
    const first = batch.command_id === intent.requestId;
    const messageId = first
      ? derive.delegatedTaskMessage({ commandId: CommandId.make(intent.requestId) })
      : batch.message_id === null
        ? null
        : MessageId.make(batch.message_id);
    if (messageId === null)
      return Option.none<ChildRelation & { messageId: MessageId; runId: RunId }>();
    if (!first) {
      const receipt = yield* receipts.getByCommandId(CommandId.make(batch.command_id));
      if (
        Option.isNone(receipt) ||
        receipt.value.status !== "accepted" ||
        receipt.value.threadId !== initial.value.workThreadId ||
        receipt.value.commandType !== "message.dispatch"
      )
        return Option.none<ChildRelation & { messageId: MessageId; runId: RunId }>();
    }
    const child = yield* projections.getThreadRecords(
      initial.value.workThreadId,
      ["messages", "runs"],
      { messageIds: [messageId] },
    );
    const message = child.messages.find(
      (candidate) =>
        candidate.id === messageId && candidate.threadId === initial.value.workThreadId,
    );
    const run = child.runs.find((candidate) => candidate.id === message?.runId);
    const taskId = run?.delegatedTaskId ?? initial.value.delegatedTaskId;
    if (
      run === undefined ||
      taskId === undefined ||
      child.thread.projectId !== intent.projectId ||
      child.thread.lineage.parentThreadId !== intent.dispatcherThreadId
    )
      return Option.none<ChildRelation & { messageId: MessageId; runId: RunId }>();
    const parent = yield* projections.getThreadRecords(
      ThreadId.make(intent.dispatcherThreadId),
      ["subagents"],
      { subagentIds: [taskId] },
    );
    const task = parent.subagents.find(
      (candidate) =>
        candidate.id === taskId &&
        candidate.origin === "app_owned" &&
        candidate.childThreadId === initial.value.workThreadId,
    );
    if (
      task?.runId == null ||
      parent.thread.projectId !== intent.projectId ||
      (!first &&
        run.delegatedTaskId === undefined &&
        task.result !== null &&
        (task.completedAt === null ||
          message === undefined ||
          DateTime.toEpochMillis(message.createdAt) > DateTime.toEpochMillis(task.completedAt)))
    )
      return Option.none<ChildRelation & { messageId: MessageId; runId: RunId }>();
    const accepted =
      batch.accepted_run_id === null
        ? (child.runs.find((candidate) => candidate.userMessageId === messageId)?.id ?? run.id)
        : batch.accepted_run_id;
    return Option.some({
      ...initial.value,
      delegatedTaskId: task.id,
      parentRunId: task.runId,
      acceptedRunId: accepted as RunId,
      messageId,
      runId: run.id,
    });
  }, Effect.mapError(ledgerError));
  const find = Effect.fn("WorkbenchChildRelations.find")(function* (childThreadId?: ThreadId) {
    const rows = yield* sql<{
      id: string;
      request_id: string;
      dispatcher_thread_id: string;
      project_id: string;
    }>`SELECT id, request_id, dispatcher_thread_id, project_id FROM workbench_work_items
       WHERE request_id IS NOT NULL AND ${childThreadId === undefined ? sql`1 = 1` : sql`(work_thread_id = ${childThreadId} OR work_thread_id IS NULL)`}`.pipe(
      Effect.mapError(ledgerError),
    );
    const resolved = yield* Effect.forEach(
      rows.filter(
        (row) =>
          childThreadId === undefined ||
          derive.delegatedTaskThread({ commandId: CommandId.make(row.request_id) }) ===
            childThreadId,
      ),
      (row) =>
        resolve({
          id: row.id,
          requestId: row.request_id,
          dispatcherThreadId: row.dispatcher_thread_id,
          projectId: row.project_id,
        }),
    );
    const initial = resolved.filter(Option.isSome).map((relation) => relation.value);
    const result = [...initial];
    for (const relation of initial) {
      const batches =
        yield* sql<BatchRow>`SELECT * FROM workbench_work_batches WHERE work_item_id=${relation.id} AND command_id IS NOT NULL`;
      for (const batch of batches) {
        const current = yield* resolveBatch(relation, batch);
        if (
          Option.isSome(current) &&
          !result.some(
            (other) =>
              other.id === current.value.id &&
              other.delegatedTaskId === current.value.delegatedTaskId,
          )
        )
          result.push(current.value);
      }
    }
    return result;
  }, Effect.mapError(ledgerError));
  return { resolve, resolveBatch, find };
});

export const layer = Layer.effect(WorkbenchChildRelations, make).pipe(
  Layer.provide(Receipts.layer),
  Layer.provide(Projections.layer),
);
