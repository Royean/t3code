import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  // No backfill: old aliases do not prove which payload belonged to a business delivery.
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_work_items (
    id TEXT PRIMARY KEY, task_id TEXT NOT NULL, task_created_at TEXT NOT NULL,
    project_id TEXT NOT NULL, business_delivery_id TEXT NOT NULL, dispatcher_thread_id TEXT NOT NULL,
    source_execution_id TEXT REFERENCES scheduled_task_executions(id),
    event_json TEXT NOT NULL, base_ref TEXT, request_id TEXT UNIQUE, request_json TEXT, parent_run_id TEXT,
    delegated_task_id TEXT, work_thread_id TEXT, message_id TEXT, accepted_run_id TEXT, run_id TEXT,
    branch TEXT, worktree_path TEXT, state TEXT NOT NULL DEFAULT 'pending', error_code TEXT,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    UNIQUE(task_id, task_created_at, business_delivery_id)
  )`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_workbench_work_items_pending ON workbench_work_items(task_id, task_created_at, state, created_at)`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_workbench_work_items_child ON workbench_work_items(work_thread_id)`;
});
