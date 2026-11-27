import { assert, describe, it } from "@effect/vitest";
import * as NodeSqliteClient from "@t3tools/shared/nodeSqliteClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";

import { migrationManifest, runMigrations } from "./Migrations.ts";
import EventOrchestrationMigration from "./ForkMigrations/001_EventOrchestration.ts";
import { hasPendingForkMigrations } from "./ForkMigrations.ts";

const v2Migrations = migrationManifest.filter(([id]) => id >= 55);
const newerMigrationId = Math.max(...migrationManifest.map(([id]) => id)) + 1;

const history = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<{ readonly migration_id: number; readonly name: string }>`
    SELECT migration_id, name FROM effect_sql_migrations ORDER BY migration_id
  `;
  return rows.map((row) => [row.migration_id, row.name] as const);
});

const v2TableCount = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql`
    SELECT name FROM sqlite_master WHERE type = 'table' AND name LIKE 'orchestration_v2_%'
  `;
  return rows.length;
});

describe("runMigrations", () => {
  it.effect(
    "upgrades released fork migration 96 without losing event tasks or skipping upstream",
    () =>
      Effect.gen(function* () {
        const sql = yield* SqlClient.SqlClient;
        yield* runMigrations({ toMigrationInclusive: 95 });
        yield* EventOrchestrationMigration;
        yield* sql`INSERT INTO effect_sql_migrations (migration_id, name)
        VALUES (96, 'EventOrchestration')`;
        yield* sql`INSERT INTO event_orchestration_registry (id, snapshot_json)
        VALUES (1, '{"preserved":true}')`;

        assert.isTrue(yield* hasPendingForkMigrations);
        assert.deepStrictEqual(
          yield* runMigrations(),
          migrationManifest.filter(([id]) => id >= 96),
        );
        assert.deepStrictEqual(yield* history, migrationManifest);
        const registry = yield* sql`SELECT snapshot_json FROM event_orchestration_registry`;
        assert.deepStrictEqual(registry, [{ snapshot_json: '{"preserved":true}' }]);
        const forkHistory =
          yield* sql`SELECT migration_id, name FROM t3_fork_migrations ORDER BY migration_id`;
        assert.deepStrictEqual(forkHistory, [
          { migration_id: 1, name: "EventOrchestration" },
          { migration_id: 2, name: "AlwaysOn" },
          { migration_id: 3, name: "AlwaysOnEventRouting" },
          { migration_id: 4, name: "ScheduledTaskExecutions" },
          { migration_id: 5, name: "WorkbenchWorkItems" },
          { migration_id: 6, name: "WorkbenchQueue" },
          { migration_id: 7, name: "WorkbenchMatters" },
          { migration_id: 8, name: "WorkbenchAttention" },
          { migration_id: 9, name: "WorkbenchRuntimeResponses" },
          { migration_id: 10, name: "WorkbenchRepairBudgets" },
          { migration_id: 11, name: "WorkbenchReleaseEvidence" },
          { migration_id: 12, name: "WorkbenchDeployment" },
        ]);
        const alwaysOn = yield* sql`SELECT name FROM sqlite_master WHERE name = 'always_on_agent'`;
        assert.deepStrictEqual(alwaysOn, [{ name: "always_on_agent" }]);
        assert.isFalse(yield* hasPendingForkMigrations);
        assert.deepStrictEqual(yield* runMigrations(), []);
      }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("adds fork tables to an upstream database without changing its migration ledger", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 98 });
      assert.deepStrictEqual(yield* history, migrationManifest);
      assert.isTrue(yield* hasPendingForkMigrations);
      assert.deepStrictEqual(yield* runMigrations(), []);
      yield* sql`INSERT INTO event_orchestration_registry (id, snapshot_json) VALUES (1, '{}')`;
      const alwaysOn = yield* sql`SELECT name FROM sqlite_master WHERE name = 'always_on_agent'`;
      assert.deepStrictEqual(alwaysOn, [{ name: "always_on_agent" }]);
      assert.deepStrictEqual(yield* history, migrationManifest);
      assert.deepStrictEqual(
        yield* sql`SELECT migration_id FROM effect_sql_migrations WHERE name = 'AlwaysOn'`,
        [],
      );
      assert.isFalse(yield* hasPendingForkMigrations);
      yield* sql`INSERT INTO workbench_work_items(id,task_id,task_created_at,project_id,business_delivery_id,dispatcher_thread_id,event_json,created_at,updated_at)
        VALUES('legacy-work','task','2026-11-26T00:00:00.000Z','project','delivery','dispatcher','{"kept":true}','2026-11-26T00:00:00.000Z','2026-11-26T00:00:00.000Z')`;
      yield* sql`DELETE FROM t3_fork_migrations WHERE migration_id IN (7,8,9,10,11,12)`;
      assert.isTrue(yield* hasPendingForkMigrations);
      assert.deepStrictEqual(yield* runMigrations(), []);
      assert.deepStrictEqual(
        yield* sql`SELECT event_json,matter_id,latest_event_sequence,cycle_number FROM workbench_work_items WHERE id='legacy-work'`,
        [
          {
            event_json: '{"kept":true}',
            matter_id: null,
            latest_event_sequence: 0,
            cycle_number: 1,
          },
        ],
      );
      assert.isFalse(yield* hasPendingForkMigrations);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("adds always-on beside an existing event-orchestration migration", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      const tracking = yield* sql`SELECT tracking_id FROM scheduled_task_execution_metadata`;
      yield* sql`UPDATE event_orchestration_registry SET snapshot_json = '{"kept":true}'
        WHERE id = 1`;
      yield* sql`INSERT INTO event_orchestration_registry (id, snapshot_json)
        SELECT 1, '{"kept":true}' WHERE NOT EXISTS (
          SELECT 1 FROM event_orchestration_registry WHERE id = 1
        )`;
      yield* sql`DELETE FROM t3_fork_migrations WHERE migration_id >= 2`;
      for (const name of [
        "always_on_event_route",
        "always_on_delivery",
        "always_on_outbox",
        "always_on_approval",
        "always_on_timer",
        "always_on_memory",
        "always_on_effect",
        "always_on_run",
        "always_on_inbox",
        "always_on_session",
        "always_on_agent",
      ]) {
        yield* sql`DROP TABLE ${sql(name)}`;
      }
      assert.isTrue(yield* hasPendingForkMigrations);
      assert.deepStrictEqual(yield* runMigrations(), []);
      assert.deepStrictEqual(
        yield* sql`SELECT tracking_id FROM scheduled_task_execution_metadata`,
        tracking,
      );
      assert.deepStrictEqual(yield* sql`SELECT snapshot_json FROM event_orchestration_registry`, [
        { snapshot_json: '{"kept":true}' },
      ]);
      assert.deepStrictEqual(
        yield* sql`SELECT migration_id, name FROM t3_fork_migrations ORDER BY migration_id`,
        [
          { migration_id: 1, name: "EventOrchestration" },
          { migration_id: 2, name: "AlwaysOn" },
          { migration_id: 3, name: "AlwaysOnEventRouting" },
          { migration_id: 4, name: "ScheduledTaskExecutions" },
          { migration_id: 5, name: "WorkbenchWorkItems" },
          { migration_id: 6, name: "WorkbenchQueue" },
          { migration_id: 7, name: "WorkbenchMatters" },
          { migration_id: 8, name: "WorkbenchAttention" },
          { migration_id: 9, name: "WorkbenchRuntimeResponses" },
          { migration_id: 10, name: "WorkbenchRepairBudgets" },
          { migration_id: 11, name: "WorkbenchReleaseEvidence" },
          { migration_id: 12, name: "WorkbenchDeployment" },
        ],
      );
      assert.deepStrictEqual(
        yield* sql`SELECT name FROM sqlite_master WHERE name = 'always_on_agent'`,
        [{ name: "always_on_agent" }],
      );
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("refuses to start when another migration took a V2 id", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 54 });
      yield* sql`
        INSERT INTO effect_sql_migrations (migration_id, name) VALUES (55, 'FutureMainMigration')
      `;
      const before = yield* history;

      assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 54 }), []);
      const exit = yield* Effect.exit(runMigrations());

      assert.isTrue(Exit.isFailure(exit));
      assert.include(String(exit), "55_OrchestrationV2 (recorded: FutureMainMigration)");
      assert.deepStrictEqual(yield* history, before);
      assert.strictEqual(yield* v2TableCount, 0);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("refuses to start when a later id would skip V2 migrations", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 54 });
      yield* sql`
        INSERT INTO effect_sql_migrations (migration_id, name)
        VALUES (${newerMigrationId}, 'FutureMainMigration')
      `;
      const before = yield* history;

      const exit = yield* Effect.exit(runMigrations());

      assert.isTrue(Exit.isFailure(exit));
      for (const [id, name] of v2Migrations) {
        assert.include(String(exit), `${id}_${name} (recorded: nothing)`);
      }
      assert.deepStrictEqual(yield* history, before);
      assert.strictEqual(yield* v2TableCount, 0);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("upgrades a database at exactly 54, then rechecks it without a write lock", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 54 });
      assert.deepStrictEqual(yield* runMigrations(), v2Migrations);
      assert.deepStrictEqual(yield* history, migrationManifest);
      assert.isAbove(yield* v2TableCount, 0);

      // query_only rejects BEGIN IMMEDIATE, which CLI startup would otherwise take on the live database.
      yield* sql`PRAGMA query_only = ON`;
      assert.deepStrictEqual(yield* runMigrations(), []);
      assert.deepStrictEqual(yield* history, migrationManifest);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("drops the unused correlation index and keeps correlation values", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 54 });
      yield* sql`
        INSERT INTO orchestration_events (
          event_id, aggregate_kind, stream_id, stream_version, event_type, occurred_at,
          command_id, causation_event_id, correlation_id, actor_kind, payload_json, metadata_json
        )
        VALUES (
          'event-1', 'thread', 'thread-1', 0, 'thread.created', '2026-01-01T00:00:00.000Z',
          'command-1', NULL, 'correlation-1', 'client', '{}', '{}'
        )
      `;

      yield* runMigrations();

      const indexes = yield* sql<{ readonly name: string }>`
        SELECT name FROM sqlite_master
        WHERE type = 'index' AND tbl_name = 'orchestration_events' AND sql IS NOT NULL
      `;
      const indexNames = indexes.map((row) => row.name);
      assert.notInclude(indexNames, "idx_orch_events_correlation_id");
      for (const kept of [
        "idx_orch_events_stream_version",
        "idx_orch_events_stream_sequence",
        "idx_orch_events_command_id",
        "idx_orchestration_events_agent_stream_sequence",
      ]) {
        assert.include(indexNames, kept);
      }
      const events = yield* sql<{ readonly correlation_id: string | null }>`
        SELECT correlation_id FROM orchestration_events WHERE event_id = 'event-1'
      `;
      assert.deepStrictEqual(events, [{ correlation_id: "correlation-1" }]);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect.each([30, 54])("still starts with a site-local migration at id %s", (id) =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 54 });
      yield* sql`
        UPDATE effect_sql_migrations SET name = 'SiteLocalMigration' WHERE migration_id = ${id}
      `;

      assert.deepStrictEqual(yield* runMigrations(), v2Migrations);
      assert.isAbove(yield* v2TableCount, 0);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("only checks V2 migrations within the requested limit", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 55 });
      yield* sql`
        INSERT INTO effect_sql_migrations (migration_id, name)
        VALUES (56, 'SiteLocalMigration'), (${newerMigrationId}, 'NewerV2Migration')
      `;
      const before = yield* history;

      assert.deepStrictEqual(yield* runMigrations({ toMigrationInclusive: 55 }), []);
      const exit = yield* Effect.exit(runMigrations({ toMigrationInclusive: 56 }));
      assert.isTrue(Exit.isFailure(exit));
      assert.include(
        String(exit),
        "56_RemoveRedundantProjectionIndexes (recorded: SiteLocalMigration)",
      );
      assert.notInclude(String(exit), "57_ScheduledTaskWebhooks");
      assert.deepStrictEqual(yield* history, before);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("keeps the migrations that finished before a later one failed", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 81 });
      yield* sql`CREATE TABLE orchestration_v2_projection_turn_item_counts (thread_id TEXT)`;

      const exit = yield* Effect.exit(runMigrations());

      assert.isTrue(Exit.isFailure(exit));
      assert.deepStrictEqual(
        yield* history,
        migrationManifest.filter(([id]) => id <= 84),
      );
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("reapplies peer link migrations whose ledger rows are missing", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations({ toMigrationInclusive: 72 });
      yield* sql`DELETE FROM effect_sql_migrations WHERE migration_id BETWEEN 70 AND 72`;

      assert.deepStrictEqual(
        yield* runMigrations(),
        migrationManifest.filter(([id]) => id >= 70),
      );
      assert.deepStrictEqual(yield* history, migrationManifest);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );

  it.effect("still starts after a newer build recorded a later V2 migration", () =>
    Effect.gen(function* () {
      const sql = yield* SqlClient.SqlClient;
      yield* runMigrations();
      yield* sql`
        INSERT INTO effect_sql_migrations (migration_id, name)
        VALUES (${newerMigrationId}, 'NewerV2Migration')
      `;

      assert.deepStrictEqual(yield* runMigrations(), []);
    }).pipe(Effect.provide(NodeSqliteClient.layer({ filename: ":memory:" }))),
  );
});

// The npm launcher reads the highest migration id from these file names.
it.effect("registers every migration file", () =>
  Effect.gen(function* () {
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const dir = path.join(import.meta.dirname, "Migrations");
    const files = (yield* fs.readDirectory(dir)).flatMap((name) => {
      const match = /^(\d+)_(\w+)\.ts$/.exec(name);
      return match ? [[Number(match[1]), match[2]] as const] : [];
    });
    assert.deepStrictEqual(
      files.toSorted(([a], [b]) => a - b),
      migrationManifest,
    );
  }).pipe(Effect.provide(NodeServices.layer)),
);
