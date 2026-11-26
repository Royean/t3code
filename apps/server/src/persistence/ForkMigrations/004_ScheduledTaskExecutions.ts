import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS scheduled_task_execution_metadata (
    singleton INTEGER PRIMARY KEY CHECK (singleton = 1), tracking_id TEXT NOT NULL
  )`;
  yield* sql`INSERT OR IGNORE INTO scheduled_task_execution_metadata VALUES (1, lower(hex(randomblob(16))))`;
  yield* sql`CREATE TABLE IF NOT EXISTS scheduled_task_executions (
    id TEXT PRIMARY KEY, task_id TEXT NOT NULL, task_created_at TEXT NOT NULL,
    project_id TEXT, owner_delivery_id TEXT NOT NULL, command_id TEXT UNIQUE,
    message_id TEXT, thread_id TEXT, accepted_run_id TEXT, run_id TEXT,
    state TEXT NOT NULL, updated_at TEXT NOT NULL, error TEXT
  )`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_execution_owner ON scheduled_task_executions(owner_delivery_id, task_id, task_created_at)`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_execution_state ON scheduled_task_executions(state)`;
  yield* sql`CREATE TABLE IF NOT EXISTS scheduled_task_execution_aliases (
    attempt_id TEXT PRIMARY KEY, task_id TEXT NOT NULL, task_created_at TEXT NOT NULL,
    business_delivery_id TEXT, delivery_id TEXT NOT NULL, outcome TEXT NOT NULL,
    execution_id TEXT REFERENCES scheduled_task_executions(id), received_at TEXT NOT NULL
  )`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_execution_alias_business ON scheduled_task_execution_aliases(task_id, task_created_at, business_delivery_id)`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_execution_alias_delivery ON scheduled_task_execution_aliases(task_id, task_created_at, delivery_id)`;
  for (const table of ["scheduled_task_webhook_replay_keys", "scheduled_task_webhook_pending"]) {
    const columns = yield* sql<{ name: string }>`PRAGMA table_info(${sql(table)})`;
    if (!columns.some((column) => column.name === "execution_id"))
      yield* sql`ALTER TABLE ${sql(table)} ADD COLUMN execution_id TEXT`;
  }
});
