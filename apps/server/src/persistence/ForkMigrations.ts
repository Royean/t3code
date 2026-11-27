import WorkbenchAcceptance from "./ForkMigrations/013_WorkbenchAcceptance.ts";
import WorkbenchMerge from "./ForkMigrations/014_WorkbenchMerge.ts";
import WorkbenchDeployment from "./ForkMigrations/012_WorkbenchDeployment.ts";
import * as Effect from "effect/Effect";
import * as Migrator from "effect/sql/Migrator";
import * as SqlClient from "effect/sql/SqlClient";

import AlwaysOn from "./ForkMigrations/002_AlwaysOn.ts";
import EventOrchestration from "./ForkMigrations/001_EventOrchestration.ts";
import AlwaysOnEventRouting from "./ForkMigrations/003_AlwaysOnEventRouting.ts";

import ScheduledTaskExecutions from "./ForkMigrations/004_ScheduledTaskExecutions.ts";

import WorkbenchWorkItems from "./ForkMigrations/005_WorkbenchWorkItems.ts";

import WorkbenchQueue from "./ForkMigrations/006_WorkbenchQueue.ts";

import WorkbenchMatters from "./ForkMigrations/007_WorkbenchMatters.ts";
import WorkbenchAttention from "./ForkMigrations/008_WorkbenchAttention.ts";

import WorkbenchRuntimeResponses from "./ForkMigrations/009_WorkbenchRuntimeResponses.ts";
import WorkbenchRepairBudgets from "./ForkMigrations/010_WorkbenchRepairBudgets.ts";

import WorkbenchReleaseEvidence from "./ForkMigrations/011_WorkbenchReleaseEvidence.ts";

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
  const rows = yield* sql<{ readonly migration_id: number; readonly name: string }>`
    SELECT migration_id, name FROM ${sql(TABLE)}
  `;
  const applied = new Set(rows.map((row) => `${row.migration_id}:${row.name}`));
  return (
    !applied.has("1:EventOrchestration") ||
    !applied.has("2:AlwaysOn") ||
    !applied.has("3:AlwaysOnEventRouting") ||
    !applied.has("4:ScheduledTaskExecutions") ||
    !applied.has("5:WorkbenchWorkItems") ||
    !applied.has("6:WorkbenchQueue") ||
    !applied.has("7:WorkbenchMatters") ||
    !applied.has("8:WorkbenchAttention") ||
    !applied.has("9:WorkbenchRuntimeResponses") ||
    !applied.has("10:WorkbenchRepairBudgets") ||
    !applied.has("11:WorkbenchReleaseEvidence") ||
    !applied.has("12:WorkbenchDeployment") ||
    !applied.has("13:WorkbenchAcceptance") ||
    !applied.has("14:WorkbenchMerge")
  );
});

export const runForkMigrations = () =>
  run({
    table: TABLE,
    loader: Migrator.fromRecord({
      "1_EventOrchestration": EventOrchestration,
      "2_AlwaysOn": AlwaysOn,
      "3_AlwaysOnEventRouting": AlwaysOnEventRouting,
      "4_ScheduledTaskExecutions": ScheduledTaskExecutions,
      "5_WorkbenchWorkItems": WorkbenchWorkItems,
      "6_WorkbenchQueue": WorkbenchQueue,
      "7_WorkbenchMatters": WorkbenchMatters,
      "8_WorkbenchAttention": WorkbenchAttention,
      "9_WorkbenchRuntimeResponses": WorkbenchRuntimeResponses,
      "10_WorkbenchRepairBudgets": WorkbenchRepairBudgets,
      "11_WorkbenchReleaseEvidence": WorkbenchReleaseEvidence,
      "12_WorkbenchDeployment": WorkbenchDeployment,
      "13_WorkbenchAcceptance": WorkbenchAcceptance,
      "14_WorkbenchMerge": WorkbenchMerge,
    }),
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
