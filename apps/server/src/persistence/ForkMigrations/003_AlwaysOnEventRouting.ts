import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";

export default Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`
    CREATE TABLE always_on_event_route (
      thread_id TEXT PRIMARY KEY,
      event_json TEXT NOT NULL,
      launch_json TEXT,
      link_session_id TEXT,
      completed INTEGER NOT NULL DEFAULT 0
    )
  `;
});
