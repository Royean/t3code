import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  for (const [table, name, definition] of [
    ["workbench_queue_bindings", "repair_policy_json", "TEXT"],
    ["workbench_attention_items", "repair_generation_id", "TEXT"],
    ["workbench_attention_operations", "claimed_actor_json", "TEXT"],
    ["workbench_attention_operations", "native_principal_json", "TEXT"],
    ["workbench_attention_operations", "repair_generation_id", "TEXT"],
    ["workbench_attention_operations", "processed_record_request_id", "TEXT"],
  ]) {
    const columns = yield* sql<{ name: string }>`PRAGMA table_info(${sql(table!)})`;
    if (!columns.some((column) => column.name === name))
      yield* sql.unsafe(`ALTER TABLE ${table} ADD COLUMN ${name} ${definition}`);
  }
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_repair_generations (
    id TEXT PRIMARY KEY,work_item_id TEXT NOT NULL REFERENCES workbench_work_items(id),queue_id TEXT NOT NULL,
    task_id TEXT NOT NULL,task_created_at TEXT NOT NULL,project_id TEXT NOT NULL,dispatcher_thread_id TEXT NOT NULL,thread_id TEXT,
    generation INTEGER NOT NULL CHECK(generation>=1),version INTEGER NOT NULL DEFAULT 1,
    limit_count INTEGER NOT NULL DEFAULT 3 CHECK(limit_count=3),used INTEGER NOT NULL DEFAULT 0 CHECK(used BETWEEN 0 AND 3),
    phase TEXT NOT NULL,reason TEXT,policy_json TEXT,context_json TEXT,source_run_id TEXT,source_sequence INTEGER NOT NULL DEFAULT 0,
    attention_id TEXT,created_from_attention_id TEXT,record_request_id TEXT,requeue_request_id TEXT,
    claimed_actor_json TEXT,native_principal_json TEXT,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,superseded_at TEXT,
    UNIQUE(work_item_id,generation)
  )`;
  yield* sql`CREATE UNIQUE INDEX IF NOT EXISTS idx_workbench_repair_current ON workbench_repair_generations(work_item_id) WHERE superseded_at IS NULL`;
  yield* sql`CREATE UNIQUE INDEX IF NOT EXISTS idx_workbench_repair_requeue ON workbench_repair_generations(queue_id,requeue_request_id) WHERE requeue_request_id IS NOT NULL`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_workbench_repair_queue ON workbench_repair_generations(queue_id,id)`;
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_repair_attempts (
    id TEXT PRIMARY KEY,generation_id TEXT NOT NULL REFERENCES workbench_repair_generations(id),work_item_id TEXT NOT NULL,queue_id TEXT NOT NULL,
    ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 1 AND 3),charged INTEGER NOT NULL DEFAULT 0 CHECK(charged IN(0,1)),state TEXT NOT NULL,
    source_run_id TEXT NOT NULL,failure_evidence_json TEXT NOT NULL,thread_id TEXT NOT NULL,project_id TEXT NOT NULL,
    claim_id TEXT NOT NULL,wake_generation INTEGER NOT NULL,attention_epoch INTEGER NOT NULL,
    command_id TEXT NOT NULL UNIQUE,message_id TEXT NOT NULL UNIQUE,run_id TEXT UNIQUE,native_attempt_id TEXT,
    provider_instance_id TEXT NOT NULL,provider_thread_id TEXT NOT NULL,provider_session_id TEXT NOT NULL,
    native_driver TEXT NOT NULL,native_thread_id TEXT NOT NULL,worktree_path TEXT NOT NULL,worktree_real_path TEXT NOT NULL,
    branch TEXT NOT NULL,git_common_dir TEXT NOT NULL,model_selection_json TEXT NOT NULL,runtime_mode TEXT NOT NULL,interaction_mode TEXT NOT NULL,
    request_json TEXT NOT NULL,accepted_sequence INTEGER,created_at TEXT NOT NULL,updated_at TEXT NOT NULL,accepted_at TEXT,completed_at TEXT,
    UNIQUE(generation_id,ordinal),UNIQUE(generation_id,source_run_id)
  )`;
  yield* sql`CREATE UNIQUE INDEX IF NOT EXISTS idx_workbench_repair_live_attempt ON workbench_repair_attempts(generation_id) WHERE state IN ('reserved','accepted','running','unknown')`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_workbench_repair_attempt_thread ON workbench_repair_attempts(thread_id,run_id)`;
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_repair_evidence (
    id TEXT PRIMARY KEY,generation_id TEXT NOT NULL,repair_attempt_id TEXT,check_id TEXT NOT NULL,check_name TEXT NOT NULL,definition_hash TEXT NOT NULL,
    thread_id TEXT NOT NULL,item_id TEXT NOT NULL,run_id TEXT NOT NULL,run_attempt_id TEXT NOT NULL,node_id TEXT NOT NULL,
    provider_thread_id TEXT NOT NULL,provider_turn_id TEXT NOT NULL,native_item_json TEXT NOT NULL,sequence INTEGER NOT NULL,exit_code INTEGER NOT NULL,
    command_text TEXT NOT NULL,cwd TEXT NOT NULL,output_sha256 TEXT NOT NULL,payload_sha256 TEXT NOT NULL,completed_at TEXT NOT NULL,created_at TEXT NOT NULL,
    UNIQUE(generation_id,item_id,sequence,check_id)
  )`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_workbench_repair_evidence_run ON workbench_repair_evidence(generation_id,run_id,sequence)`;
});
