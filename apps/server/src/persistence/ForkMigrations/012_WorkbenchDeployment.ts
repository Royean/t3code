import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_release_adapter_preflights(preparation_id TEXT PRIMARY KEY REFERENCES workbench_release_preparations(id),input_json TEXT NOT NULL,state TEXT NOT NULL,receipt_json TEXT)`;
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_deployments(id TEXT PRIMARY KEY,queue_id TEXT NOT NULL,controller_id TEXT NOT NULL,project_id TEXT NOT NULL,tracking_id TEXT NOT NULL,input_json TEXT NOT NULL,profile_json TEXT NOT NULL,commit_json TEXT,claim_intent INTEGER NOT NULL DEFAULT 0,settled_state TEXT,deployment_json TEXT NOT NULL,state TEXT NOT NULL,created_at TEXT NOT NULL,updated_at TEXT NOT NULL)`;
  yield* sql`CREATE INDEX IF NOT EXISTS idx_workbench_deployments_active ON workbench_deployments(state)`;
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_deployment_actions(deployment_id TEXT NOT NULL REFERENCES workbench_deployments(id),kind TEXT NOT NULL,operation_id TEXT NOT NULL UNIQUE,input_digest TEXT NOT NULL,state TEXT NOT NULL,receipt_json TEXT,PRIMARY KEY(deployment_id,kind))`;
});
