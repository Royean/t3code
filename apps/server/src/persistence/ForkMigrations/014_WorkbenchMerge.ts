import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_release_obligations(work_item_id TEXT PRIMARY KEY REFERENCES workbench_work_items(id),task_created_at TEXT NOT NULL,cycle_number INTEGER NOT NULL,preparation_id TEXT NOT NULL,created_at TEXT NOT NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_merge_runtime(deployment_id TEXT PRIMARY KEY REFERENCES workbench_deployments(id),permit_json TEXT,mutation_intent INTEGER NOT NULL DEFAULT 0,mutation_outcome_json TEXT,verification_intent_json TEXT)`;
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_merge_executions(execution_id TEXT PRIMARY KEY,deployment_id TEXT NOT NULL REFERENCES workbench_deployments(id),attempt INTEGER NOT NULL,role TEXT NOT NULL,state TEXT NOT NULL,input_json TEXT NOT NULL,receipt_json TEXT)`;
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_merge_audits(id TEXT PRIMARY KEY,deployment_id TEXT NOT NULL REFERENCES workbench_deployments(id),kind TEXT NOT NULL,digest TEXT NOT NULL,body_json TEXT NOT NULL,created_at TEXT NOT NULL)`;
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_delivery_receipts(deployment_id TEXT PRIMARY KEY REFERENCES workbench_deployments(id),work_item_id TEXT NOT NULL UNIQUE REFERENCES workbench_work_items(id),receipt_json TEXT NOT NULL,created_at TEXT NOT NULL)`;
});
