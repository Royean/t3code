import { ProjectId } from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { derive } from "@t3tools/provider-core/server/IdAllocator";
import { CommandId } from "@t3tools/contracts";
import { digest } from "./workbenchLifecycle.ts";
import { decodeRepairPolicy, freezeRepairPolicy } from "./workbenchRepairPolicy.ts";
const encode = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

export const ensureRepairGeneration = Effect.fn("WorkbenchRepairs.ensureGeneration")(function* (
  workItemId: string,
) {
  const sql = yield* SqlClient.SqlClient;
  const [existing] =
    yield* sql`SELECT id FROM workbench_repair_generations WHERE work_item_id=${workItemId} AND superseded_at IS NULL`;
  if (existing) return;
  const [w] = yield* sql<{
    id: string;
    queue_id: string;
    task_id: string;
    task_created_at: string;
    project_id: string;
    dispatcher_thread_id: string;
    work_thread_id: string | null;
    request_id: string | null;
    repair_policy_json: string | null;
  }>`SELECT w.*,b.queue_id,b.repair_policy_json FROM workbench_work_items w JOIN workbench_queue_bindings b ON b.task_id=w.task_id AND b.task_created_at=w.task_created_at AND b.project_id=w.project_id AND b.dispatcher_thread_id=w.dispatcher_thread_id WHERE w.id=${workItemId}`;
  if (!w) return;
  const frozen =
    w.repair_policy_json === null
      ? Option.some(null)
      : yield* decodeRepairPolicy(w.repair_policy_json).pipe(
          Effect.flatMap((p) => freezeRepairPolicy(ProjectId.make(w.project_id), p)),
          Effect.option,
        );
  const stamp = DateTime.formatIso(yield* DateTime.now),
    threadId =
      w.work_thread_id ??
      (w.request_id === null
        ? null
        : derive.delegatedTaskThread({ commandId: CommandId.make(w.request_id) }));
  yield* sql`INSERT OR IGNORE INTO workbench_repair_generations ${sql.insert({ id: `repair-generation:${digest(w.id)}:00000001`, work_item_id: w.id, queue_id: w.queue_id, task_id: w.task_id, task_created_at: w.task_created_at, project_id: w.project_id, dispatcher_thread_id: w.dispatcher_thread_id, thread_id: threadId, generation: 1, phase: "watching", reason: Option.isNone(frozen) ? "policy_changed" : null, policy_json: Option.isNone(frozen) || frozen.value === null ? null : yield* encode(frozen.value), created_at: stamp, updated_at: stamp })}`;
});
