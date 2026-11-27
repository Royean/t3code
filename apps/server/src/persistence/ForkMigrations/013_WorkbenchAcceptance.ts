import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_acceptance_runtime(deployment_id TEXT PRIMARY KEY REFERENCES workbench_deployments(id),runner_epoch TEXT,last_mono TEXT,last_wall INTEGER,intent_json TEXT,recovery_json TEXT)`;
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_acceptance_samples(deployment_id TEXT NOT NULL REFERENCES workbench_deployments(id),ordinal INTEGER NOT NULL,sample_id TEXT NOT NULL UNIQUE,input_digest TEXT NOT NULL,sample_digest TEXT NOT NULL,sample_json TEXT NOT NULL,audit_json TEXT NOT NULL,PRIMARY KEY(deployment_id,ordinal))`;
  yield* sql`CREATE TABLE IF NOT EXISTS workbench_acceptance_executions(execution_id TEXT PRIMARY KEY,deployment_id TEXT NOT NULL REFERENCES workbench_deployments(id),sample_id TEXT NOT NULL,role TEXT NOT NULL,state TEXT NOT NULL,input_json TEXT NOT NULL,receipt_json TEXT)`;
});
