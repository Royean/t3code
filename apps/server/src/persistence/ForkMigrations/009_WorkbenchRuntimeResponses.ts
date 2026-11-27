import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_runtime_request_sources (
    id TEXT PRIMARY KEY,version INTEGER NOT NULL,fingerprint TEXT NOT NULL,queue_id TEXT NOT NULL,work_item_id TEXT NOT NULL,
    cycle_number INTEGER NOT NULL,batch_id TEXT,thread_id TEXT NOT NULL,source_run_id TEXT NOT NULL,source_attempt_id TEXT,node_id TEXT NOT NULL,
    provider_thread_id TEXT NOT NULL,provider_turn_id TEXT,runtime_request_id TEXT NOT NULL,provider_session_id TEXT,
    request_json TEXT NOT NULL,turn_item_id TEXT NOT NULL,turn_item_json TEXT NOT NULL,capability TEXT NOT NULL,
    provider_instance_id TEXT NOT NULL,model_selection_json TEXT NOT NULL,runtime_mode TEXT NOT NULL,interaction_mode TEXT NOT NULL,native_driver TEXT,native_thread_id TEXT,worktree_path TEXT,worktree_real_path TEXT,branch TEXT,git_common_dir TEXT,
    status TEXT NOT NULL,summary_json TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL
  )`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_workbench_runtime_sources_queue ON workbench_runtime_request_sources(queue_id,id)`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_workbench_runtime_sources_thread ON workbench_runtime_request_sources(thread_id)`;
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_runtime_responses (
    id TEXT PRIMARY KEY,source_id TEXT NOT NULL REFERENCES workbench_runtime_request_sources(id),source_version INTEGER NOT NULL,source_json TEXT NOT NULL,
    queue_id TEXT NOT NULL,work_item_id TEXT NOT NULL,attention_epoch INTEGER NOT NULL,claim_id TEXT,command_id TEXT NOT NULL UNIQUE,request_json TEXT NOT NULL,
    async_message_id TEXT,run_id TEXT,delegated_task_id TEXT,state TEXT NOT NULL,evidence_level TEXT NOT NULL DEFAULT 'none',delivery_json TEXT NOT NULL DEFAULT '{}',
    effect_id TEXT,effect_state TEXT,submitted_sequence INTEGER,reason TEXT,payload_sha TEXT NOT NULL,actor_json TEXT NOT NULL,native_principal_json TEXT NOT NULL,
    created_at TEXT NOT NULL,updated_at TEXT NOT NULL,runtime_accepted_at TEXT,delivered_at TEXT,UNIQUE(source_id,source_version)
  )`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_workbench_runtime_responses_queue ON workbench_runtime_responses(queue_id,state)`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_workbench_runtime_responses_effect ON workbench_runtime_responses(effect_id)`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_workbench_runtime_responses_message ON workbench_runtime_responses(async_message_id)`;
});
