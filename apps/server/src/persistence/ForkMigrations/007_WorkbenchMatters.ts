import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const columns = new Set(
    (yield* sql<{ name: string }>`PRAGMA table_info(workbench_work_items)`).map((row) => row.name),
  );
  for (const [name, definition] of [
    ["matter_id", "TEXT"],
    ["cycle_number", "INTEGER NOT NULL DEFAULT 1"],
    ["previous_work_item_id", "TEXT"],
    ["business_state", "TEXT NOT NULL DEFAULT 'active'"],
    ["latest_event_sequence", "INTEGER NOT NULL DEFAULT 0"],
    ["dispatched_through_sequence", "INTEGER NOT NULL DEFAULT 0"],
    ["processed_through_sequence", "INTEGER NOT NULL DEFAULT 0"],
    ["version", "INTEGER NOT NULL DEFAULT 0"],
    ["completed_at", "TEXT"],
    ["handoff_ref", "TEXT"],
    ["preserved_ref", "TEXT"],
    ["recovery_ref", "TEXT"],
  ]) {
    if (!columns.has(name!))
      yield* sql.unsafe(`ALTER TABLE workbench_work_items ADD COLUMN ${name} ${definition}`);
  }
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_delivery_intents (
    intent_id TEXT PRIMARY KEY, queue_id TEXT NOT NULL, controller_id TEXT NOT NULL,
    project_id TEXT NOT NULL, task_id TEXT NOT NULL, task_created_at TEXT NOT NULL,
    dispatcher_thread_id TEXT NOT NULL, subscription_scope_id TEXT NOT NULL,
    business_delivery_id TEXT NOT NULL, body_sha256 TEXT NOT NULL, matter_key TEXT NOT NULL,
    matter_json TEXT, created_at TEXT NOT NULL,
    UNIQUE(task_id,task_created_at,business_delivery_id)
  )`;
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_matters (
    id TEXT PRIMARY KEY, queue_id TEXT NOT NULL, subscription_scope_id TEXT NOT NULL,
    matter_key TEXT NOT NULL, matter_json TEXT, task_id TEXT NOT NULL, task_created_at TEXT NOT NULL,
    dispatcher_thread_id TEXT NOT NULL, active_work_item_id TEXT, latest_cycle_number INTEGER NOT NULL DEFAULT 0,
    UNIQUE(queue_id,subscription_scope_id,matter_key)
  )`;
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_work_item_events (
    task_id TEXT NOT NULL, task_created_at TEXT NOT NULL, business_delivery_id TEXT NOT NULL,
    work_item_id TEXT NOT NULL REFERENCES workbench_work_items(id), sequence INTEGER NOT NULL,
    source_execution_id TEXT NOT NULL, intent_id TEXT NOT NULL, event_json TEXT NOT NULL,
    batch_id TEXT, received_at TEXT NOT NULL,
    PRIMARY KEY(task_id,task_created_at,business_delivery_id), UNIQUE(work_item_id,sequence)
  )`;
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_work_batches (
    id TEXT PRIMARY KEY, work_item_id TEXT NOT NULL REFERENCES workbench_work_items(id),
    from_sequence INTEGER NOT NULL, through_sequence INTEGER NOT NULL,
    command_id TEXT, message_id TEXT, accepted_run_id TEXT, run_id TEXT,
    delegated_task_id TEXT, parent_run_id TEXT, state TEXT NOT NULL DEFAULT 'pending', outcome TEXT, work_state TEXT NOT NULL DEFAULT 'unknown',
    event_json TEXT NOT NULL, prompt_text TEXT, created_at TEXT NOT NULL, updated_at TEXT NOT NULL,
    UNIQUE(work_item_id,from_sequence), UNIQUE(command_id)
  )`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_workbench_matter_workspace ON workbench_work_items(work_thread_id,business_state)`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_workbench_batch_runs ON workbench_work_batches(work_item_id,through_sequence)`;
});
