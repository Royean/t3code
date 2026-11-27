import {
  ProjectId,
  WorkbenchQueueError,
  type WorkbenchAttentionRecordInput,
  type WorkbenchAttentionRequeueInput,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { digest } from "./workbenchLifecycle.ts";
import { decodeRepairPolicy, freezeRepairPolicy } from "./workbenchRepairPolicy.ts";
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
type Generation = {
  id: string;
  work_item_id: string;
  queue_id: string;
  task_id: string;
  task_created_at: string;
  project_id: string;
  dispatcher_thread_id: string;
  thread_id: string | null;
  generation: number;
  context_json: string | null;
};

export const attachRepairAttention = Effect.fn("WorkbenchRepairs.attachAttention")(function* (
  attentionId: string,
  workItemId: string,
) {
  const sql = yield* SqlClient.SqlClient;
  const [g] =
    yield* sql<Generation>`SELECT * FROM workbench_repair_generations WHERE work_item_id=${workItemId} AND superseded_at IS NULL`;
  if (!g) return;
  yield* sql`UPDATE workbench_attention_items SET repair_generation_id=${g.id} WHERE id=${attentionId}`;
  yield* sql`UPDATE workbench_repair_generations SET phase='awaiting_user',attention_id=${attentionId},version=version+CASE WHEN attention_id=${attentionId} THEN 0 ELSE 1 END WHERE id=${g.id}`;
});

export const recordRepairAttention = Effect.fn("WorkbenchRepairs.recordHuman")(function* (
  queueId: string,
  attentionId: string,
  input: WorkbenchAttentionRecordInput,
  principalId: string | undefined,
) {
  const sql = yield* SqlClient.SqlClient;
  const [a] = yield* sql<{
    repair_generation_id: string | null;
  }>`SELECT repair_generation_id FROM workbench_attention_items WHERE id=${attentionId} AND queue_id=${queueId}`;
  if (!a?.repair_generation_id) return;
  if (
    !principalId ||
    !input.claimedActor ||
    input.expectedRepairGenerationId !== a.repair_generation_id
  )
    return yield* new WorkbenchQueueError({ reason: "stale_state" });
  const [g] =
    yield* sql`SELECT 1 FROM workbench_repair_generations WHERE id=${a.repair_generation_id} AND superseded_at IS NULL`;
  if (!g) return yield* new WorkbenchQueueError({ reason: "stale_state" });
  const [recorded] =
    yield* sql`UPDATE workbench_attention_operations SET claimed_actor_json=${yield* encodeJson(input.claimedActor)},native_principal_json=${yield* encodeJson({ kind: "environment_session", id: principalId })},repair_generation_id=${a.repair_generation_id}
    WHERE queue_id=${queueId} AND client_request_id=${input.clientRequestId} AND attention_id=${attentionId} AND kind='record' AND state='applied' RETURNING client_request_id`;
  if (!recorded) return yield* new WorkbenchQueueError({ reason: "stale_state" });
});

export const requeueRepairGeneration = Effect.fn("WorkbenchRepairs.requeueHuman")(function* (
  queueId: string,
  attentionId: string,
  input: WorkbenchAttentionRequeueInput,
  principalId: string | undefined,
) {
  const sql = yield* SqlClient.SqlClient;
  const [a] = yield* sql<{
    repair_generation_id: string | null;
  }>`SELECT repair_generation_id FROM workbench_attention_items WHERE id=${attentionId} AND queue_id=${queueId}`;
  if (!a?.repair_generation_id) return;
  if (
    !principalId ||
    !input.claimedActor ||
    input.expectedRepairGenerationId !== a.repair_generation_id
  )
    return yield* new WorkbenchQueueError({ reason: "stale_state" });
  const [explicit] =
    yield* sql`SELECT 1 FROM workbench_attention_operations o JOIN workbench_attention_items a ON a.id=o.attention_id JOIN workbench_queue_entries e ON e.work_item_id=a.work_item_id
    WHERE o.queue_id=${queueId} AND o.client_request_id=${input.clientRequestId} AND o.attention_id=${attentionId} AND o.kind='requeue' AND o.state='pending' AND a.phase='requeue_pending' AND a.checkpoint_status='ready' AND a.result_text IS NOT NULL AND e.state='paused'`;
  if (!explicit) return yield* new WorkbenchQueueError({ reason: "stale_state" });
  const actor = yield* encodeJson(input.claimedActor),
    principal = yield* encodeJson({ kind: "environment_session", id: principalId });
  const [record] = yield* sql<{
    client_request_id: string;
  }>`SELECT client_request_id FROM workbench_attention_operations WHERE queue_id=${queueId} AND attention_id=${attentionId} AND kind='record' AND state='applied' AND repair_generation_id=${a.repair_generation_id} AND claimed_actor_json=${actor} AND native_principal_json IS NOT NULL ORDER BY created_at DESC,rowid DESC LIMIT 1`;
  const [g] =
    yield* sql<Generation>`SELECT * FROM workbench_repair_generations WHERE id=${a.repair_generation_id} AND superseded_at IS NULL`;
  if (!record || !g) return yield* new WorkbenchQueueError({ reason: "stale_state" });
  const [b] = yield* sql<{
    repair_policy_json: string | null;
  }>`SELECT repair_policy_json FROM workbench_queue_bindings WHERE queue_id=${queueId} AND task_id=${g.task_id} AND task_created_at=${g.task_created_at} AND project_id=${g.project_id} AND dispatcher_thread_id=${g.dispatcher_thread_id}`;
  if (!b) return yield* new WorkbenchQueueError({ reason: "invalid_binding" });
  const frozen =
    b.repair_policy_json === null
      ? null
      : yield* freezeRepairPolicy(
          ProjectId.make(g.project_id),
          yield* decodeRepairPolicy(b.repair_policy_json),
        );
  const stamp = DateTime.formatIso(yield* DateTime.now),
    id = `repair-generation:${digest(g.work_item_id)}:${String(g.generation + 1).padStart(8, "0")}`;
  yield* sql`UPDATE workbench_repair_generations SET phase='superseded',superseded_at=${stamp},version=version+1,updated_at=${stamp} WHERE id=${g.id} AND superseded_at IS NULL`;
  yield* sql`INSERT INTO workbench_repair_generations ${sql.insert({ id, work_item_id: g.work_item_id, queue_id: g.queue_id, task_id: g.task_id, task_created_at: g.task_created_at, project_id: g.project_id, dispatcher_thread_id: g.dispatcher_thread_id, thread_id: g.thread_id, generation: g.generation + 1, phase: "watching", policy_json: frozen === null ? null : yield* encodeJson(frozen), context_json: g.context_json, created_from_attention_id: attentionId, record_request_id: record.client_request_id, requeue_request_id: input.clientRequestId, claimed_actor_json: actor, native_principal_json: principal, created_at: stamp, updated_at: stamp })}`;
  yield* sql`UPDATE workbench_attention_operations SET claimed_actor_json=${actor},native_principal_json=${principal},repair_generation_id=${g.id},processed_record_request_id=${record.client_request_id} WHERE queue_id=${queueId} AND client_request_id=${input.clientRequestId} AND attention_id=${attentionId} AND kind='requeue'`;
});
