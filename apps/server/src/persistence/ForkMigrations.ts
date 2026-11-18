import * as Effect from "effect/Effect";
import * as Migrator from "effect/sql/Migrator";
import * as SqlClient from "effect/sql/SqlClient";

import EventOrchestration from "./ForkMigrations/001_EventOrchestration.ts";

const TABLE = "t3_fork_migrations";
const run = Migrator.make({});

export function reconciledForkHistory(
  history: ReadonlyArray<{ readonly id: number; readonly name: string }>,
) {
  return history.filter(({ id, name }) => id !== 96 || name !== "EventOrchestration");
}

const legacyMigration = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const tables = yield* sql`SELECT name FROM sqlite_master
    WHERE type = 'table' AND name = 'effect_sql_migrations'`;
  if (tables.length === 0) return false;
  const rows = yield* sql`SELECT 1 FROM effect_sql_migrations
    WHERE migration_id = 96 AND name = 'EventOrchestration'`;
  return rows.length > 0;
});

export const hasPendingForkMigrations = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  if (yield* legacyMigration) return true;
  const tables = yield* sql`SELECT name FROM sqlite_master
    WHERE type = 'table' AND name = ${TABLE}`;
  if (tables.length === 0) return true;
  const rows = yield* sql`SELECT 1 FROM ${sql(TABLE)}
    WHERE migration_id = 1 AND name = 'EventOrchestration'`;
  return rows.length === 0;
});

export const runForkMigrations = () =>
  run({
    table: TABLE,
    loader: Migrator.fromRecord({ "1_EventOrchestration": EventOrchestration }),
  });

// Released forks used upstream's id 96. Move only that exact record, preserving its tables and data.
export const reconcileLegacyForkMigration = Effect.fn("reconcileLegacyForkMigration")(function* () {
  if (!(yield* legacyMigration)) return;
  const sql = yield* SqlClient.SqlClient;
  yield* sql.withTransaction(
    Effect.gen(function* () {
      if (!(yield* legacyMigration)) return;
      yield* run({
        table: TABLE,
        loader: Migrator.fromRecord({ "1_EventOrchestration": Effect.void }),
      });
      yield* sql`DELETE FROM effect_sql_migrations
        WHERE migration_id = 96 AND name = 'EventOrchestration'`;
    }),
  );
});
