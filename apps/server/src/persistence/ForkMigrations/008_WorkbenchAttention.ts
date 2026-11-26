import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  for (const [table, name, definition] of [
    ["workbench_work_items", "attention_epoch", "INTEGER NOT NULL DEFAULT 0"],
    ["workbench_work_items", "attention_hold", "INTEGER NOT NULL DEFAULT 0"],
    ["orchestration_v2_effect_outbox", "attention_epoch", "INTEGER"],
  ]) {
    const columns = yield* sql<{ name: string }>`PRAGMA table_info(${sql(table!)})`;
    if (!columns.some((column) => column.name === name))
      yield* sql.unsafe(`ALTER TABLE ${table} ADD COLUMN ${name} ${definition}`);
  }
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_attention_items (
    id TEXT PRIMARY KEY,queue_id TEXT NOT NULL,work_item_id TEXT NOT NULL REFERENCES workbench_work_items(id),
    version INTEGER NOT NULL DEFAULT 1,phase TEXT NOT NULL,reason_json TEXT NOT NULL,pre_pause_json TEXT NOT NULL DEFAULT '{}',observation_only INTEGER NOT NULL DEFAULT 0,runtime_request_id TEXT,
    result_text TEXT,recorded_at TEXT,priority INTEGER NOT NULL,blocked_reason TEXT,
    checkpoint_status TEXT NOT NULL DEFAULT 'pending',captured_at TEXT,source_run_id TEXT,
    point_id TEXT,resume_point_id TEXT,resume_run_id TEXT,pause_command_id TEXT NOT NULL,
    pause_claim_id TEXT,pause_wake_generation INTEGER NOT NULL,epoch INTEGER NOT NULL,
    created_at TEXT NOT NULL,updated_at TEXT NOT NULL
  )`;
  yield* sql`CREATE UNIQUE INDEX IF NOT EXISTS idx_workbench_attention_active ON workbench_attention_items(work_item_id) WHERE phase!='resolved'`;
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_attention_points (
    id TEXT PRIMARY KEY,attention_id TEXT NOT NULL REFERENCES workbench_attention_items(id),
    work_item_id TEXT NOT NULL,stage TEXT NOT NULL,manifest_json TEXT NOT NULL,
    checkpoint_ref TEXT NOT NULL,checkpoint_sha TEXT NOT NULL,created_at TEXT NOT NULL
  )`;
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_attention_operations (
    queue_id TEXT NOT NULL,client_request_id TEXT NOT NULL,attention_id TEXT NOT NULL,
    kind TEXT NOT NULL,payload_sha TEXT NOT NULL,state TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,payload_json TEXT NOT NULL DEFAULT '{}',
    PRIMARY KEY(queue_id,client_request_id)
  )`;
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_attention_resume_intents (
    id TEXT PRIMARY KEY,attention_id TEXT NOT NULL,work_item_id TEXT NOT NULL,queue_id TEXT NOT NULL,
    claim_id TEXT,attention_epoch INTEGER NOT NULL,thread_id TEXT NOT NULL,project_id TEXT NOT NULL,
    command_id TEXT NOT NULL UNIQUE,message_id TEXT NOT NULL UNIQUE,run_id TEXT,source_run_id TEXT,
    provider_instance_id TEXT NOT NULL,provider_thread_id TEXT NOT NULL,provider_session_id TEXT NOT NULL,native_driver TEXT NOT NULL,native_thread_id TEXT NOT NULL,
    worktree_path TEXT NOT NULL,worktree_real_path TEXT NOT NULL,branch TEXT NOT NULL,git_common_dir TEXT NOT NULL,
    batch_id TEXT,request_json TEXT NOT NULL,state TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL
  )`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_workbench_attention_queue ON workbench_attention_items(queue_id,id)`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_workbench_attention_resume_run ON workbench_attention_resume_intents(thread_id,run_id)`;
});
