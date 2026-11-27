import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_release_preparations (
    id TEXT PRIMARY KEY,queue_id TEXT NOT NULL,controller_id TEXT NOT NULL,project_id TEXT NOT NULL,tracking_id TEXT NOT NULL,
    work_item_id TEXT NOT NULL,input_json TEXT NOT NULL,profile_json TEXT NOT NULL,snapshot_json TEXT NOT NULL,
    preparation_json TEXT NOT NULL,state TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL
  )`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_workbench_release_work ON workbench_release_preparations(queue_id,work_item_id,id)`;
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_release_stages (
    preparation_id TEXT NOT NULL REFERENCES workbench_release_preparations(id),ordinal INTEGER NOT NULL,role TEXT NOT NULL,
    state TEXT NOT NULL,receipt_json TEXT,exit_code INTEGER,output_digest TEXT,started_at TEXT NOT NULL,completed_at TEXT,
    PRIMARY KEY(preparation_id,ordinal)
  )`;
});
