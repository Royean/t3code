import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_queues (
    id TEXT PRIMARY KEY, controller_id TEXT NOT NULL, project_id TEXT NOT NULL,
    UNIQUE(controller_id, project_id)
  )`;
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_queue_bindings (
    task_id TEXT PRIMARY KEY, task_created_at TEXT NOT NULL, queue_id TEXT NOT NULL REFERENCES workbench_queues(id),
    project_id TEXT NOT NULL, dispatcher_thread_id TEXT NOT NULL
  )`;
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_queue_entries (
    enqueue_sequence INTEGER PRIMARY KEY AUTOINCREMENT,
    work_item_id TEXT NOT NULL UNIQUE REFERENCES workbench_work_items(id),
    queue_id TEXT NOT NULL REFERENCES workbench_queues(id), priority INTEGER NOT NULL DEFAULT 0,
    state TEXT NOT NULL DEFAULT 'queued', claim_id TEXT UNIQUE, needs_dispatch INTEGER NOT NULL DEFAULT 1,
    work_state TEXT NOT NULL DEFAULT 'unknown', blocked_reason TEXT, wake_sent INTEGER NOT NULL DEFAULT 0,
    wake_generation INTEGER NOT NULL DEFAULT 0, recovery_signal TEXT, handled_recovery_signal TEXT,
    created_at TEXT NOT NULL, updated_at TEXT NOT NULL
  )`;
  yield* sql`CREATE TRIGGER IF NOT EXISTS workbench_queue_dispatcher_owner BEFORE INSERT ON workbench_queue_bindings
    WHEN EXISTS(SELECT 1 FROM workbench_queue_bindings WHERE dispatcher_thread_id=NEW.dispatcher_thread_id AND queue_id!=NEW.queue_id)
    BEGIN SELECT RAISE(ABORT,'dispatcher_already_bound'); END`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_workbench_queue_dispatcher ON workbench_queue_bindings(dispatcher_thread_id,queue_id)`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_workbench_queue_active ON workbench_queue_entries(state,work_item_id)`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_workbench_queue_history ON workbench_queue_entries(queue_id,state,updated_at DESC,enqueue_sequence DESC)`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_workbench_work_items_dispatcher ON workbench_work_items(dispatcher_thread_id)`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_workbench_queue_order ON workbench_queue_entries(queue_id,state,priority DESC,enqueue_sequence)`;
});
