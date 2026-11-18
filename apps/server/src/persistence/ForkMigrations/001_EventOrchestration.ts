import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE event_orchestration_registry (
    id INTEGER PRIMARY KEY CHECK (id = 1), snapshot_json TEXT NOT NULL
  )`;
  yield* sql`CREATE TABLE event_orchestration_jobs (
    job_id TEXT PRIMARY KEY, source TEXT NOT NULL, delivery_id TEXT NOT NULL,
    coordinator_thread_id TEXT NOT NULL UNIQUE,
    status TEXT NOT NULL, state_json TEXT NOT NULL, config_json TEXT NOT NULL,
    UNIQUE(source, delivery_id)
  )`;
  yield* sql`CREATE INDEX event_orchestration_jobs_status_idx ON event_orchestration_jobs(status)`;
  yield* sql`CREATE TABLE event_orchestration_tasks (
    job_id TEXT NOT NULL REFERENCES event_orchestration_jobs(job_id),
    task_id TEXT NOT NULL, state_json TEXT NOT NULL, PRIMARY KEY(job_id, task_id)
  )`;
  yield* sql`CREATE TABLE event_orchestration_attempts (
    attempt_id TEXT PRIMARY KEY,
    job_id TEXT NOT NULL, task_id TEXT NOT NULL, ordinal INTEGER NOT NULL CHECK(ordinal BETWEEN 1 AND 3),
    thread_id TEXT NOT NULL UNIQUE, status TEXT NOT NULL, state_json TEXT NOT NULL,
    FOREIGN KEY(job_id, task_id) REFERENCES event_orchestration_tasks(job_id, task_id),
    UNIQUE(job_id, task_id, ordinal)
  )`;
  yield* sql`CREATE UNIQUE INDEX event_orchestration_attempts_active_idx
    ON event_orchestration_attempts(job_id, task_id)
    WHERE status IN ('queued', 'running', 'waiting', 'stalled', 'unknown', 'awaiting_stop')`;
  yield* sql`CREATE TABLE event_orchestration_changes (
    cursor INTEGER PRIMARY KEY AUTOINCREMENT, job_id TEXT NOT NULL,
    task_id TEXT, attempt_id TEXT, kind TEXT NOT NULL, status TEXT NOT NULL, at TEXT NOT NULL
  )`;
});
