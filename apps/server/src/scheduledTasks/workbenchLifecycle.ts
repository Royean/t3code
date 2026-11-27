// @effect-diagnostics-next-line nodeBuiltinImport:off -- Stable matter identities and exact webhook bytes need SHA-256.
import * as NodeCrypto from "node:crypto";
import {
  ScheduledTaskError,
  WorkbenchQueueError,
  type WorkbenchDeliveryIntentInput,
  type WorkbenchWorkBatch,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

import { MAX_WORKBENCH_BATCH_BYTES, renderWorkbenchBatchBrief } from "./workbenchBrief.ts";
import { CommandId } from "@t3tools/contracts";
import { derive } from "@t3tools/provider-core/server/IdAllocator";

export const digest = (value: string | Uint8Array) =>
  NodeCrypto.createHash("sha256").update(value).digest("hex");
const decodeEvent = Schema.decodeUnknownEffect(Schema.fromJsonString(Schema.Unknown));
const encodeMatter = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

export const prepareDeliveryIntent = Effect.fn("WorkbenchQueue.prepareDeliveryIntent")(function* (
  queueId: string,
  input: WorkbenchDeliveryIntentInput,
) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const [binding] =
        yield* sql`SELECT 1 FROM workbench_queues q JOIN workbench_queue_bindings b ON b.queue_id=q.id
      JOIN scheduled_task_execution_metadata m ON m.singleton=1
      WHERE q.id=${queueId} AND q.controller_id=${input.controllerId} AND q.project_id=${input.projectId}
      AND b.task_id=${input.taskId} AND b.task_created_at=${input.taskCreatedAt} AND b.project_id=q.project_id
      AND b.dispatcher_thread_id=${input.dispatcherThreadId} AND m.tracking_id=${input.expectedTrackingId}`;
      if (!binding) return yield* new WorkbenchQueueError({ reason: "invalid_binding" });
      const matterKey = yield* encodeMatter(
        input.matter === null
          ? ["delivery", input.businessDeliveryId]
          : [input.matter.provider, input.matter.namespace, input.matter.kind, input.matter.key],
      );
      const matterJson =
        input.matter === null
          ? null
          : yield* encodeMatter({
              provider: input.matter.provider,
              kind: input.matter.kind,
              key: matterKey,
              label: input.matter.label,
            });
      const [prior] =
        yield* sql`SELECT * FROM workbench_delivery_intents WHERE intent_id=${input.intentId}
      OR (task_id=${input.taskId} AND task_created_at=${input.taskCreatedAt} AND business_delivery_id=${input.businessDeliveryId})`;
      if (
        prior &&
        (prior.intent_id !== input.intentId ||
          prior.queue_id !== queueId ||
          prior.controller_id !== input.controllerId ||
          prior.project_id !== input.projectId ||
          prior.task_id !== input.taskId ||
          prior.task_created_at !== input.taskCreatedAt ||
          prior.dispatcher_thread_id !== input.dispatcherThreadId ||
          prior.subscription_scope_id !== input.subscriptionScopeId ||
          prior.business_delivery_id !== input.businessDeliveryId ||
          prior.body_sha256 !== input.bodySha256 ||
          prior.matter_key !== matterKey ||
          prior.matter_json !== matterJson)
      )
        return yield* new WorkbenchQueueError({ reason: "conflict" });
      const [matter] = yield* sql`SELECT * FROM workbench_matters WHERE queue_id=${queueId}
      AND subscription_scope_id=${input.subscriptionScopeId} AND matter_key=${matterKey}`;
      if (
        matter &&
        (matter.task_id !== input.taskId ||
          matter.task_created_at !== input.taskCreatedAt ||
          matter.dispatcher_thread_id !== input.dispatcherThreadId)
      )
        return yield* new WorkbenchQueueError({ reason: "conflict" });
      yield* sql`INSERT OR IGNORE INTO workbench_delivery_intents
      (intent_id,queue_id,controller_id,project_id,task_id,task_created_at,dispatcher_thread_id,subscription_scope_id,business_delivery_id,body_sha256,matter_key,matter_json,created_at)
      VALUES(${input.intentId},${queueId},${input.controllerId},${input.projectId},${input.taskId},${input.taskCreatedAt},${input.dispatcherThreadId},
      ${input.subscriptionScopeId},${input.businessDeliveryId},${input.bodySha256},${matterKey},${matterJson},${DateTime.formatIso(yield* DateTime.now)})`;
      return {
        trackingId: input.expectedTrackingId,
        queueId,
        controllerId: input.controllerId,
        projectId: input.projectId,
        taskId: input.taskId,
        taskCreatedAt: input.taskCreatedAt,
        dispatcherThreadId: input.dispatcherThreadId,
        intentId: input.intentId,
        subscriptionScopeId: input.subscriptionScopeId,
        businessDeliveryId: input.businessDeliveryId,
        bodySha256: input.bodySha256,
      };
    }),
  );
});

export const admitMatterEvent = Effect.fn("ScheduledTaskService.admitMatterEvent")(
  function* (input: {
    taskId: string;
    taskCreatedAt: string;
    projectId: string;
    dispatcherThreadId: string;
    businessDeliveryId: string;
    attemptId: string;
    executionId: string;
    bodyDigest: string | null;
    eventJson: string;
    receivedAt: string;
  }) {
    const sql = yield* SqlClient.SqlClient;
    const [intent] = yield* sql<{
      intent_id: string;
      queue_id: string;
      controller_id: string;
      project_id: string;
      task_id: string;
      task_created_at: string;
      dispatcher_thread_id: string;
      subscription_scope_id: string;
      business_delivery_id: string;
      body_sha256: string;
      matter_key: string;
      matter_json: string | null;
    }>`SELECT * FROM workbench_delivery_intents WHERE task_id=${input.taskId} AND task_created_at=${input.taskCreatedAt}
    AND business_delivery_id=${input.businessDeliveryId}`;
    if (!intent) return false;
    const [binding] =
      yield* sql`SELECT 1 FROM workbench_queue_bindings b JOIN workbench_queues q ON q.id=b.queue_id
    WHERE b.queue_id=${intent.queue_id} AND q.controller_id=${intent.controller_id} AND q.project_id=${input.projectId}
    AND b.task_id=${input.taskId} AND b.task_created_at=${input.taskCreatedAt} AND b.project_id=${input.projectId}
    AND b.dispatcher_thread_id=${input.dispatcherThreadId}`;
    if (
      !binding ||
      intent.project_id !== input.projectId ||
      intent.dispatcher_thread_id !== input.dispatcherThreadId ||
      intent.body_sha256 !== input.bodyDigest
    )
      return yield* new ScheduledTaskError({
        message: "Prepared business delivery does not match this request.",
      });
    const [member] =
      yield* sql`SELECT 1 FROM workbench_work_item_events WHERE task_id=${input.taskId}
    AND task_created_at=${input.taskCreatedAt} AND business_delivery_id=${input.businessDeliveryId}`;
    if (member) return true;
    const [historical] =
      yield* sql`SELECT 1 FROM scheduled_task_execution_aliases WHERE task_id=${input.taskId}
    AND task_created_at=${input.taskCreatedAt} AND business_delivery_id=${input.businessDeliveryId} AND execution_id IS NOT NULL`;
    if (historical) return true;
    let [matter] = yield* sql<{
      id: string;
      task_id: string;
      task_created_at: string;
      dispatcher_thread_id: string;
      active_work_item_id: string | null;
      latest_cycle_number: number;
    }>`
    SELECT * FROM workbench_matters WHERE queue_id=${intent.queue_id} AND subscription_scope_id=${intent.subscription_scope_id} AND matter_key=${intent.matter_key}`;
    if (
      matter &&
      (matter.task_id !== input.taskId ||
        matter.task_created_at !== input.taskCreatedAt ||
        matter.dispatcher_thread_id !== input.dispatcherThreadId)
    )
      return yield* new ScheduledTaskError({ message: "Business matter target changed." });
    if (!matter) {
      matter = {
        id: `matter:${intent.intent_id}`,
        task_id: input.taskId,
        task_created_at: input.taskCreatedAt,
        dispatcher_thread_id: input.dispatcherThreadId,
        active_work_item_id: null,
        latest_cycle_number: 0,
      };
      yield* sql`INSERT INTO workbench_matters(id,queue_id,subscription_scope_id,matter_key,matter_json,task_id,task_created_at,dispatcher_thread_id)
      VALUES(${matter.id},${intent.queue_id},${intent.subscription_scope_id},${intent.matter_key},${intent.matter_json},${input.taskId},${input.taskCreatedAt},${input.dispatcherThreadId})`;
    }
    let [cycle] =
      matter.active_work_item_id === null
        ? []
        : yield* sql<{
            id: string;
            business_state: string;
            latest_event_sequence: number;
          }>`SELECT * FROM workbench_work_items WHERE id=${matter.active_work_item_id}`;
    if (!cycle || cycle.business_state === "completed") {
      cycle = {
        id: `work-item:${input.attemptId}`,
        business_state: "active",
        latest_event_sequence: 0,
      };
      yield* sql`INSERT INTO workbench_work_items
      (id,task_id,task_created_at,project_id,business_delivery_id,dispatcher_thread_id,source_execution_id,event_json,created_at,updated_at,matter_id,cycle_number,previous_work_item_id)
      VALUES(${cycle.id},${input.taskId},${input.taskCreatedAt},${input.projectId},${input.businessDeliveryId},${input.dispatcherThreadId},${input.executionId},${input.eventJson},
      ${input.receivedAt},${input.receivedAt},${matter.id},${matter.latest_cycle_number + 1},${matter.active_work_item_id})`;
      yield* sql`UPDATE workbench_matters SET active_work_item_id=${cycle.id},latest_cycle_number=latest_cycle_number+1 WHERE id=${matter.id}`;
    }
    const sequence = cycle.latest_event_sequence + 1;
    yield* sql`INSERT INTO workbench_work_item_events
    (task_id,task_created_at,business_delivery_id,work_item_id,sequence,source_execution_id,intent_id,event_json,received_at)
    VALUES(${input.taskId},${input.taskCreatedAt},${input.businessDeliveryId},${cycle.id},${sequence},${input.executionId},${intent.intent_id},${input.eventJson},${input.receivedAt})`;
    yield* sql`UPDATE workbench_work_items SET latest_event_sequence=${sequence},version=version+1,business_state='active',updated_at=${input.receivedAt} WHERE id=${cycle.id}`;
    const [entry] = yield* sql<{
      state: string;
      needs_dispatch: number;
    }>`SELECT state,needs_dispatch FROM workbench_queue_entries WHERE work_item_id=${cycle.id}`;
    if (entry?.state === "finished") {
      yield* sql`UPDATE workbench_queue_entries SET state='queued',claim_id=NULL,needs_dispatch=1,wake_sent=0,wake_generation=wake_generation+1,
      recovery_signal=NULL,handled_recovery_signal=NULL,work_state='unknown',blocked_reason='queue_full',
      enqueue_sequence=(SELECT COALESCE(MAX(enqueue_sequence),0)+1 FROM workbench_queue_entries),updated_at=${input.receivedAt} WHERE work_item_id=${cycle.id}`;
      yield* sql`UPDATE sqlite_sequence SET seq=(SELECT MAX(enqueue_sequence) FROM workbench_queue_entries) WHERE name='workbench_queue_entries'`;
    } else if (entry && entry.needs_dispatch === 0) {
      yield* sql`UPDATE workbench_queue_entries SET needs_dispatch=1,wake_sent=0,wake_generation=wake_generation+1,recovery_signal=NULL,handled_recovery_signal=NULL,
      blocked_reason='awaiting_dispatcher',updated_at=${input.receivedAt} WHERE work_item_id=${cycle.id}`;
    }
    return true;
  },
);

export interface BatchRow {
  id: string;
  work_item_id: string;
  from_sequence: number;
  through_sequence: number;
  command_id: string | null;
  message_id: string | null;
  accepted_run_id: string | null;
  run_id: string | null;
  delegated_task_id: string | null;
  parent_run_id: string | null;
  state: WorkbenchWorkBatch["state"];
  outcome: WorkbenchWorkBatch["outcome"];
  work_state: "unknown" | "working" | "waiting_for_children" | "result_available";
  event_json: string;
  prompt_text: string | null;
  created_at: string;
  updated_at: string;
}

export const batchView = (row: BatchRow): WorkbenchWorkBatch => ({
  id: row.id,
  fromSequence: row.from_sequence,
  throughSequence: row.through_sequence,
  commandId: row.command_id,
  messageId: row.message_id,
  acceptedRunId: row.accepted_run_id,
  runId: row.run_id,
  delegatedTaskId: row.delegated_task_id,
  parentRunId: row.parent_run_id,
  state: row.state,
  outcome: row.outcome,
});

interface WorkspaceBinding {
  work_thread_id: string | null;
  request_id: string | null;
  worktree_path: string | null;
  business_state: string;
  latest_event_sequence: number;
  processed_through_sequence: number;
  handoff_ref: string | null;
  preserved_ref: string | null;
  recovery_ref: string | null;
}

export const workspaceBindings = Effect.fn("WorkbenchWorkItems.workspaceBindings")(function* (
  paths: ReadonlyArray<string>,
  threadIds: ReadonlyArray<string>,
) {
  const sql = yield* SqlClient.SqlClient;
  const encodedPaths = yield* encodeMatter(paths),
    encodedThreads = yield* encodeMatter(threadIds);
  const rows =
    yield* sql<WorkspaceBinding>`SELECT * FROM workbench_work_items WHERE (matter_id IS NOT NULL OR EXISTS(SELECT 1 FROM workbench_attention_items a WHERE a.work_item_id=workbench_work_items.id AND a.phase!='resolved')) AND
    (worktree_path IN (SELECT value FROM json_each(${encodedPaths})) OR work_thread_id IN (SELECT value FROM json_each(${encodedThreads}))
    OR (work_thread_id IS NULL AND request_id IS NOT NULL))`;
  return rows.filter(
    (row) =>
      paths.includes(row.worktree_path ?? "") ||
      threadIds.includes(row.work_thread_id ?? "") ||
      (row.request_id !== null &&
        threadIds.includes(
          derive.delegatedTaskThread({ commandId: CommandId.make(row.request_id) }),
        )),
  );
});

export const retainedBusinessThreads = Effect.fn("WorkbenchWorkItems.retainedBusinessThreads")(
  function* () {
    const sql = yield* SqlClient.SqlClient;
    const rows =
      yield* sql<WorkspaceBinding>`SELECT * FROM workbench_work_items WHERE (matter_id IS NOT NULL OR EXISTS(SELECT 1 FROM workbench_attention_items a WHERE a.work_item_id=workbench_work_items.id AND a.phase!='resolved')) AND
    (business_state!='completed' OR handoff_ref IS NULL OR preserved_ref IS NULL OR recovery_ref IS NOT NULL OR latest_event_sequence!=processed_through_sequence)
    OR EXISTS(SELECT 1 FROM workbench_repair_generations g WHERE g.work_item_id=workbench_work_items.id AND g.thread_id IS NOT NULL)`;
    return rows.flatMap((row) =>
      row.work_thread_id !== null
        ? [row.work_thread_id]
        : row.request_id === null
          ? []
          : [derive.delegatedTaskThread({ commandId: CommandId.make(row.request_id) })],
    );
  },
);

export const reserveBatch = Effect.fn("WorkbenchDispatch.reserveBatch")(function* (
  workItemId: string,
) {
  const sql = yield* SqlClient.SqlClient;
  return yield* sql.withTransaction(
    Effect.gen(function* () {
      const [unconfirmed] =
        yield* sql<BatchRow>`SELECT * FROM workbench_work_batches WHERE work_item_id=${workItemId}
      AND state IN ('pending','dispatching','unknown') ORDER BY from_sequence LIMIT 1`;
      if (unconfirmed) return unconfirmed;
      const events = yield* sql<{
        sequence: number;
        encoded_bytes: number;
      }>`SELECT sequence,length(CAST(event_json AS BLOB)) AS encoded_bytes FROM workbench_work_item_events WHERE work_item_id=${workItemId}
      AND batch_id IS NULL ORDER BY sequence LIMIT 20`;
      if (events.length === 0) return null;
      const [cycle] = yield* sql<{
        work_thread_id: string | null;
        previous_work_item_id: string | null;
      }>`SELECT work_thread_id,previous_work_item_id FROM workbench_work_items WHERE id=${workItemId}`;
      const [previous] =
        cycle?.previous_work_item_id == null
          ? []
          : yield* sql<{
              id: string;
              handoff_ref: string | null;
            }>`SELECT id,handoff_ref FROM workbench_work_items WHERE id=${cycle.previous_work_item_id}`;
      const decoded: Array<{ sequence: number; event: unknown }> = [];
      let encoded = "[]";
      for (const event of events) {
        if (event.encoded_bytes > MAX_WORKBENCH_BATCH_BYTES) break;
        const [source] = yield* sql<{
          event_json: string;
        }>`SELECT event_json FROM workbench_work_item_events WHERE work_item_id=${workItemId} AND sequence=${event.sequence}`;
        const value = { sequence: event.sequence, event: yield* decodeEvent(source!.event_json) };
        const candidate = yield* encodeMatter([...decoded, value]);
        const prompt = renderWorkbenchBatchBrief({
          eventJson: candidate,
          followup: cycle?.work_thread_id != null,
          ...(previous === undefined
            ? {}
            : { previous: { id: previous.id, handoffRef: previous.handoff_ref } }),
        });
        if (new TextEncoder().encode(prompt).byteLength > MAX_WORKBENCH_BATCH_BYTES) break;
        decoded.push(value);
        encoded = candidate;
      }
      const now = DateTime.formatIso(yield* DateTime.now);
      if (decoded.length === 0) {
        yield* sql`UPDATE workbench_work_items SET state='blocked',error_code='batch_too_large',version=version+CASE WHEN error_code='batch_too_large' THEN 0 ELSE 1 END,updated_at=${now} WHERE id=${workItemId}`;
        yield* sql`UPDATE workbench_queue_entries SET needs_dispatch=0,work_state='unknown',blocked_reason='state_unknown' WHERE work_item_id=${workItemId} AND state='occupied'`;
        return null;
      }
      const first = decoded[0]!.sequence,
        last = decoded.at(-1)!.sequence,
        id = `work-batch:${workItemId}:${first}`;
      yield* sql`INSERT INTO workbench_work_batches(id,work_item_id,from_sequence,through_sequence,event_json,created_at,updated_at)
      VALUES(${id},${workItemId},${first},${last},${encoded},${now},${now})`;
      yield* sql`UPDATE workbench_work_item_events SET batch_id=${id} WHERE work_item_id=${workItemId} AND sequence>=${first} AND sequence<=${last}`;
      return (yield* sql<BatchRow>`SELECT * FROM workbench_work_batches WHERE id=${id}`)[0]!;
    }),
  );
});
