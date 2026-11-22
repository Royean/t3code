// @effect-diagnostics-next-line nodeBuiltinImport:off -- Stable alert identifiers are hashed before serialization.
import * as NodeCrypto from "node:crypto";
import type * as NodeSqlite from "node:sqlite";
import { RelayAgentActivityState, RelayDeviceRegistrationRequest } from "@t3tools/contracts/relay";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import type { SelfHostedConfig } from "./config.ts";

const decodeRegistration = Schema.decodeUnknownSync(
  Schema.fromJsonString(RelayDeviceRegistrationRequest),
);
const decodeState = Schema.decodeUnknownSync(Schema.fromJsonString(RelayAgentActivityState));
const decodePush = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.String)),
);

function alertAllowed(
  phase: RelayAgentActivityState["phase"],
  preferences: RelayDeviceRegistrationRequest["preferences"],
) {
  if (!preferences.notificationsEnabled) return false;
  switch (phase) {
    case "waiting_for_approval":
      return preferences.notifyOnApproval;
    case "waiting_for_input":
      return preferences.notifyOnInput;
    case "completed":
      return preferences.notifyOnCompletion;
    case "failed":
      return preferences.notifyOnFailure;
    default:
      return false;
  }
}

const ACTIVE_LIFETIME_MS = 7_200_000;

function isActive(state: RelayAgentActivityState) {
  return state.phase !== "completed" && state.phase !== "failed" && state.phase !== "stale";
}

function shorten(value: string) {
  return Array.from(value.replace(/\s+/g, " ").trim()).slice(0, 56).join("");
}

function messageData(
  device: RelayDeviceRegistrationRequest,
  accountId: string,
  states: readonly RelayAgentActivityState[],
  alert: RelayAgentActivityState | null,
) {
  const shown = device.preferences.liveActivitiesEnabled ? states : [];
  const first = shown[0];
  const urgent = shown.filter(
    ({ phase }) => phase === "waiting_for_approval" || phase === "waiting_for_input",
  ).length;
  return {
    t3_kind: "agent_activity",
    device_id: device.deviceId,
    user_id: accountId,
    active: String(shown.length > 0),
    activity_expires_at: String(first ? Date.parse(first.updatedAt) + ACTIVE_LIFETIME_MS : 0),
    activity_title: shown.length > 0 ? `${shown.length} active` : "",
    activity_phase: first?.phase ?? "",
    activity_active_count: String(shown.length),
    activity_attention_count: String(urgent),
    activity_chip: shown.length > 0 ? (urgent > 0 ? "Review" : "Active") : "",
    activity_path: first?.deepLink ?? "/",
    activity_body: first ? shorten(first.headline + " · " + first.threadTitle) : "",
    ...Object.fromEntries(
      shown
        .slice(0, 3)
        .map((state, index) => [
          `activity_line_${index}`,
          [state.headline, state.threadTitle, state.projectTitle].map(shorten).join("\t"),
        ]),
    ),
    ...(alert
      ? {
          alert_id: NodeCrypto.createHash("sha256")
            .update(
              JSON.stringify([alert.environmentId, alert.threadId, alert.phase, alert.updatedAt]),
            )
            .digest("hex"),
          alert_group: NodeCrypto.createHash("sha256").update(alert.deepLink).digest("hex"),
          alert_title: shorten(alert.threadTitle),
          alert_body: shorten(alert.headline + " · " + alert.projectTitle),
          alert_path: alert.deepLink,
        }
      : {}),
  };
}

export const make = Effect.fn("SelfHostedAndroidActivity.make")(function* (
  database: NodeSqlite.DatabaseSync,
  config: SelfHostedConfig,
) {
  const accountId = config.account.accountId;
  let changed = yield* Deferred.make<void>();
  const notify = Effect.gen(function* () {
    const next = yield* Deferred.make<void>();
    const previous = changed;
    changed = next;
    yield* Deferred.succeed(previous, undefined);
  });
  yield* Effect.sync(() =>
    database.exec(`
      CREATE TABLE IF NOT EXISTS android_notification_devices (
        installation_id TEXT PRIMARY KEY,
        family_id TEXT NOT NULL,
        token_hash TEXT UNIQUE NOT NULL,
        registration TEXT NOT NULL
      );
      CREATE TABLE IF NOT EXISTS activity_states (
        environment_id TEXT NOT NULL,
        thread_id TEXT NOT NULL,
        state TEXT NOT NULL,
        expires_at INTEGER NOT NULL,
        PRIMARY KEY (environment_id, thread_id)
      );
      CREATE TABLE IF NOT EXISTS android_notification_queue (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        installation_id TEXT NOT NULL,
        family_id TEXT NOT NULL,
        payload TEXT NOT NULL,
        created_at INTEGER NOT NULL
      );
      CREATE INDEX IF NOT EXISTS android_notification_queue_device
        ON android_notification_queue (installation_id, family_id, id);
    `),
  );
  const activeStates = (now: number) =>
    database
      .prepare(
        "SELECT state FROM activity_states WHERE expires_at > ? ORDER BY expires_at DESC LIMIT 100",
      )
      .all(now)
      .map((row) => decodeState(row.state))
      .filter(isActive);
  const enqueue = (
    device: RelayDeviceRegistrationRequest,
    familyId: string,
    states: readonly RelayAgentActivityState[],
    alert: RelayAgentActivityState | null,
    now: number,
  ) => {
    if (!device.preferences.notificationsEnabled && !device.preferences.liveActivitiesEnabled)
      return;
    if (!alert) {
      database
        .prepare(`DELETE FROM android_notification_queue
        WHERE installation_id = ? AND family_id = ? AND json_extract(payload, '$.alert_id') IS NULL`)
        .run(device.deviceId, familyId);
    }
    database
      .prepare(`INSERT INTO android_notification_queue
      (installation_id, family_id, payload, created_at) VALUES (?, ?, ?, ?)`)
      .run(
        device.deviceId,
        familyId,
        JSON.stringify(messageData(device, accountId, states, alert)),
        now,
      );
  };

  const cleanup = (now: number) => {
    database
      .prepare("DELETE FROM android_notification_queue WHERE created_at <= ?")
      .run(now - 300_000);
    database
      .prepare(`DELETE FROM android_notification_devices WHERE NOT EXISTS (
      SELECT 1 FROM sessions WHERE sessions.family_id = android_notification_devices.family_id
        AND account_id = ? AND refresh_expires_at > ? AND consumed_at IS NULL AND revoked_at IS NULL
    )`)
      .run(accountId, now);
  };
  return {
    packageName: config.androidPackageName ?? "com.royean.t3code",
    notify,
    waitForChange: () => Deferred.await(changed),
    register: (
      familyId: string,
      tokenHash: string,
      device: RelayDeviceRegistrationRequest,
      now: number,
    ) => {
      cleanup(now);
      const previous = database
        .prepare(`SELECT registration, family_id FROM android_notification_devices
        WHERE installation_id = ?`)
        .get(device.deviceId);
      // Never store a third-party token supplied by an older client.
      const { pushToken: _pushToken, ...registration } = device;
      const serialized = JSON.stringify(registration);
      database
        .prepare(`INSERT INTO android_notification_devices (installation_id, family_id, token_hash, registration)
        VALUES (?, ?, ?, ?) ON CONFLICT(installation_id) DO UPDATE
        SET family_id = excluded.family_id, token_hash = excluded.token_hash, registration = excluded.registration`)
        .run(device.deviceId, familyId, tokenHash, serialized);
      if (previous && (previous.registration !== serialized || previous.family_id !== familyId)) {
        database
          .prepare("DELETE FROM android_notification_queue WHERE installation_id = ?")
          .run(device.deviceId);
      }
      enqueue(device, familyId, activeStates(now), null, now);
    },
    read: (tokenHash: string, after: number, now: number, replay: boolean) => {
      const device = database
        .prepare(`SELECT installation_id, family_id, registration FROM android_notification_devices
        WHERE token_hash = ? AND EXISTS (SELECT 1 FROM sessions
          WHERE sessions.family_id = android_notification_devices.family_id AND account_id = ?
            AND refresh_expires_at > ? AND consumed_at IS NULL AND revoked_at IS NULL)`)
        .get(tokenHash, accountId, now);
      if (!device) return null;
      const installationId = String(device.installation_id);
      const familyId = String(device.family_id);
      database
        .prepare(`DELETE FROM android_notification_queue WHERE installation_id = ? AND family_id = ?
        AND (id <= ? OR created_at <= ?)`)
        .run(installationId, familyId, after, now - 300_000);
      if (replay)
        enqueue(decodeRegistration(device.registration), familyId, activeStates(now), null, now);
      return database
        .prepare(`SELECT id, payload, created_at FROM android_notification_queue
        WHERE installation_id = ? AND family_id = ? ORDER BY id LIMIT 16`)
        .all(installationId, familyId)
        .map((row) => ({
          id: Number(row.id),
          data: {
            ...decodePush(row.payload),
            updated_at: String(row.created_at),
          },
        }));
    },
    publish: (
      environmentId: string,
      threadId: string,
      state: RelayAgentActivityState | null,
      now: number,
    ) => {
      database.exec("BEGIN IMMEDIATE");
      try {
        cleanup(now);
        database.prepare("DELETE FROM activity_states WHERE expires_at <= ?").run(now);
        const previous = database
          .prepare("SELECT state FROM activity_states WHERE environment_id = ? AND thread_id = ?")
          .get(environmentId, threadId);
        const serialized = state ? JSON.stringify(state) : null;
        if (state && previous?.state !== serialized) {
          database
            .prepare(`INSERT INTO activity_states (environment_id, thread_id, state, expires_at)
            VALUES (?, ?, ?, ?) ON CONFLICT(environment_id, thread_id) DO UPDATE
            SET state = excluded.state, expires_at = excluded.expires_at`)
            .run(
              environmentId,
              threadId,
              serialized,
              now + (isActive(state) ? ACTIVE_LIFETIME_MS : 600_000),
            );
        } else if (previous && !state) {
          database
            .prepare("DELETE FROM activity_states WHERE environment_id = ? AND thread_id = ?")
            .run(environmentId, threadId);
        }
        if ((previous?.state ?? null) !== serialized) {
          const states = activeStates(now);
          const devices = database
            .prepare(`SELECT registration, family_id FROM android_notification_devices
            WHERE EXISTS (SELECT 1 FROM sessions WHERE sessions.family_id = android_notification_devices.family_id
              AND account_id = ? AND refresh_expires_at > ? AND consumed_at IS NULL AND revoked_at IS NULL)`)
            .all(accountId, now);
          for (const row of devices) {
            const device = decodeRegistration(row.registration);
            const alert =
              state &&
              now - Date.parse(state.updatedAt) <= 600_000 &&
              alertAllowed(state.phase, device.preferences)
                ? state
                : null;
            enqueue(device, String(row.family_id), states, alert, now);
          }
        }
        database.exec("COMMIT");
      } catch (cause) {
        database.exec("ROLLBACK");
        throw cause;
      }
    },
  };
});
