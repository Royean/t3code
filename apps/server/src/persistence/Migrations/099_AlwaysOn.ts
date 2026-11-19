import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;

  yield* sql`
    CREATE TABLE always_on_agent (
      agent_id TEXT PRIMARY KEY,
      project_id TEXT NOT NULL,
      title TEXT NOT NULL,
      enabled INTEGER NOT NULL,
      user_session_id TEXT,
      lease_expires_at TEXT,
      lease_run_id TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
  `;
  yield* sql`
    CREATE TABLE always_on_session (
      session_id TEXT PRIMARY KEY,
      agent_id TEXT NOT NULL,
      kind TEXT NOT NULL,
      status TEXT NOT NULL,
      created_at TEXT NOT NULL
    )
  `;
  yield* sql`
    CREATE TABLE always_on_inbox (
      event_id TEXT PRIMARY KEY,
      agent_id TEXT NOT NULL,
      source TEXT NOT NULL,
      source_event_id TEXT NOT NULL,
      kind TEXT NOT NULL,
      session_policy TEXT NOT NULL,
      related_session_id TEXT,
      actions_json TEXT NOT NULL,
      status TEXT NOT NULL,
      run_id TEXT,
      created_at TEXT NOT NULL,
      UNIQUE (agent_id, source, source_event_id)
    )
  `;
  yield* sql`
    CREATE INDEX idx_always_on_inbox_pending
    ON always_on_inbox (agent_id, status, created_at)
  `;
  yield* sql`
    CREATE TABLE always_on_run (
      run_id TEXT PRIMARY KEY,
      agent_id TEXT NOT NULL,
      session_id TEXT NOT NULL,
      status TEXT NOT NULL,
      outcome TEXT,
      uncertain INTEGER NOT NULL DEFAULT 0,
      cancel_requested INTEGER NOT NULL DEFAULT 0,
      context_json TEXT NOT NULL,
      event_ids_json TEXT NOT NULL,
      last_progress_at TEXT,
      started_at TEXT NOT NULL,
      finished_at TEXT
    )
  `;
  yield* sql`
    CREATE INDEX idx_always_on_run_agent ON always_on_run (agent_id, started_at)
  `;
  yield* sql`
    CREATE TABLE always_on_effect (
      agent_id TEXT NOT NULL,
      action_key TEXT NOT NULL,
      run_id TEXT NOT NULL,
      status TEXT NOT NULL,
      PRIMARY KEY (agent_id, action_key)
    )
  `;
  yield* sql`
    CREATE TABLE always_on_memory (
      memory_id TEXT PRIMARY KEY,
      agent_id TEXT NOT NULL,
      kind TEXT NOT NULL,
      title TEXT NOT NULL,
      body TEXT,
      pointer TEXT,
      task_key TEXT,
      created_at TEXT NOT NULL,
      updated_at TEXT NOT NULL
    )
  `;
  yield* sql`
    CREATE INDEX idx_always_on_memory_owner
    ON always_on_memory (agent_id, kind, updated_at)
  `;
  yield* sql`
    CREATE UNIQUE INDEX idx_always_on_progress_key
    ON always_on_memory (agent_id, task_key)
    WHERE task_key IS NOT NULL
  `;
  yield* sql`
    CREATE TABLE always_on_timer (
      timer_id TEXT PRIMARY KEY,
      agent_id TEXT NOT NULL,
      kind TEXT NOT NULL,
      due_at TEXT NOT NULL,
      every_ms INTEGER,
      status TEXT NOT NULL,
      fire_count INTEGER NOT NULL DEFAULT 0,
      action_key TEXT NOT NULL,
      UNIQUE (agent_id, action_key)
    )
  `;
  yield* sql`
    CREATE TABLE always_on_approval (
      approval_id TEXT PRIMARY KEY,
      agent_id TEXT NOT NULL,
      run_id TEXT NOT NULL,
      session_id TEXT NOT NULL,
      summary TEXT NOT NULL,
      on_approve_json TEXT NOT NULL,
      status TEXT NOT NULL,
      created_at TEXT NOT NULL,
      resolved_at TEXT
    )
  `;
  yield* sql`
    CREATE TABLE always_on_outbox (
      outbox_id TEXT PRIMARY KEY,
      agent_id TEXT NOT NULL,
      run_id TEXT NOT NULL,
      session_id TEXT NOT NULL,
      dedupe_key TEXT NOT NULL,
      body TEXT NOT NULL,
      status TEXT NOT NULL,
      attempts INTEGER NOT NULL DEFAULT 0,
      next_attempt_at TEXT,
      created_at TEXT NOT NULL,
      delivered_at TEXT,
      UNIQUE (agent_id, dedupe_key)
    )
  `;
  yield* sql`
    CREATE TABLE always_on_delivery (
      delivery_id TEXT PRIMARY KEY,
      outbox_id TEXT NOT NULL UNIQUE,
      agent_id TEXT NOT NULL,
      session_id TEXT NOT NULL,
      body TEXT NOT NULL,
      delivered_at TEXT NOT NULL
    )
  `;
});
