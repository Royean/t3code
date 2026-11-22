// @effect-diagnostics-next-line nodeBuiltinImport:off -- Native crypto helpers run inside the synchronous SQLite transaction boundary.
import * as NodeCrypto from "node:crypto";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The native SQLite resource must open an owned, permission-restricted file without following symlinks.
import * as NodeFS from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The native SQLite resource validates its containing directory during acquisition.
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import {
  AuthAccessTokenResult,
  AuthAccessTokenType,
  AuthEnvironmentBootstrapTokenType,
  AuthStandardClientScopes,
  AuthTokenExchangeGrantType,
} from "@t3tools/contracts";
import type {
  SelfHostedAndroidNotifications,
  SelfHostedConnection,
  SelfHostedDevices,
  SelfHostedEnvironmentSession,
  SelfHostedEnvironments,
  SelfHostedLoginInput,
  SelfHostedSession,
} from "@t3tools/contracts/self-hosted-connect";
import type {
  RelayAgentActivityState,
  RelayDeviceRegistrationRequest,
} from "@t3tools/contracts/relay";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Schedule from "effect/Schedule";
import * as Semaphore from "effect/Semaphore";
import type { SelfHostedConfig } from "./config.ts";
import { verifyPassword } from "./password.ts";
import * as AndroidActivity from "./AndroidActivity.ts";
import * as WebhookQueue from "./WebhookQueue.ts";

export class Unauthorized extends Schema.TaggedError<Unauthorized>()("Unauthorized", {}) {
  override get message() {
    return "The credentials are invalid or the session has expired.";
  }
}
export class RateLimited extends Schema.TaggedError<RateLimited>()("RateLimited", {
  retryAfterSeconds: Schema.Int,
}) {
  override get message() {
    return "Too many sign-in attempts. Try again later.";
  }
}
export class EnvironmentNotFound extends Schema.TaggedError<EnvironmentNotFound>()(
  "EnvironmentNotFound",
  {},
) {
  override get message() {
    return "The environment is not configured.";
  }
}
export class EnvironmentUnavailable extends Schema.TaggedError<EnvironmentUnavailable>()(
  "EnvironmentUnavailable",
  { cause: Schema.Defect() },
) {
  override get message() {
    return "The environment is unavailable. Try again later.";
  }
}
export class UpstreamTimeout extends Schema.TaggedError<UpstreamTimeout>()("UpstreamTimeout", {}) {
  override get message() {
    return "The environment did not respond in time.";
  }
}
export class StorageError extends Schema.TaggedError<StorageError>()("StorageError", {
  operation: Schema.Literals(["open", "read", "write"]),
  cause: Schema.Defect(),
}) {
  override get message() {
    return "The connection service could not access session storage.";
  }
}
export class PasswordError extends Schema.TaggedError<PasswordError>()("PasswordError", {
  cause: Schema.Defect(),
}) {
  override get message() {
    return "The connection service could not verify credentials.";
  }
}

type SessionFailure = Unauthorized | StorageError;
export class SelfHostedConnect extends Context.Service<
  SelfHostedConnect,
  {
    readonly login: (
      input: SelfHostedLoginInput,
      remoteAddress: string,
    ) => Effect.Effect<SelfHostedSession, SessionFailure | RateLimited | PasswordError>;
    readonly refresh: (refreshToken: string) => Effect.Effect<SelfHostedSession, SessionFailure>;
    readonly logout: (refreshToken: string) => Effect.Effect<void, StorageError>;
    readonly listDevices: (accessToken: string) => Effect.Effect<SelfHostedDevices, SessionFailure>;
    readonly revokeDevice: (
      accessToken: string,
      deviceId: string,
    ) => Effect.Effect<void, SessionFailure>;
    readonly registerAndroid: (
      accessToken: string,
      input: RelayDeviceRegistrationRequest,
      deliveryTokenHash: string,
    ) => Effect.Effect<void, SessionFailure | EnvironmentUnavailable>;
    readonly readAndroid: (
      deliveryToken: string,
      after: number,
      replay?: boolean,
    ) => Effect.Effect<SelfHostedAndroidNotifications, SessionFailure>;
    readonly publishActivity: (
      key: string,
      environmentId: string,
      threadId: string,
      state: RelayAgentActivityState | null,
    ) => Effect.Effect<void, SessionFailure | EnvironmentUnavailable>;
    readonly queueWebhook: (
      input: WebhookQueue.WebhookDelivery,
    ) => Effect.Effect<
      { readonly deliveryId: string },
      EnvironmentNotFound | StorageError | WebhookQueue.WebhookQueueFull
    >;
    readonly listEnvironments: (
      accessToken: string,
    ) => Effect.Effect<SelfHostedEnvironments, SessionFailure>;
    readonly connect: (
      accessToken: string,
      environmentId: string,
    ) => Effect.Effect<
      SelfHostedConnection,
      SessionFailure | EnvironmentNotFound | EnvironmentUnavailable | UpstreamTimeout
    >;
    readonly connectAccount: (
      accessToken: string,
      environmentId: string,
    ) => Effect.Effect<
      SelfHostedEnvironmentSession,
      SessionFailure | EnvironmentNotFound | EnvironmentUnavailable | UpstreamTimeout
    >;
  }
>()("@t3tools/self-hosted/SelfHostedConnect") {}

const tokenHash = (token: string) => NodeCrypto.createHash("sha256").update(token).digest("hex");
const newToken = () => NodeCrypto.randomBytes(32).toString("base64url");
const isoTimestamp = (millis: number) => DateTime.formatIso(DateTime.makeUnsafe(millis));
const pairingRequestBody = (label: string) =>
  JSON.stringify({ label, scopes: AuthStandardClientScopes });
const TOKEN_PATTERN = /^[A-Za-z0-9_-]{43}$/;
const PairingResponse = Schema.Struct({
  credential: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(8_192)),
  expiresAt: Schema.String.check(Schema.isMaxLength(64)),
});
const decodePairingResponse = Schema.decodeUnknownSync(PairingResponse);
const decodeAccessToken = Schema.decodeUnknownSync(AuthAccessTokenResult);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));

async function readUpstreamResponse<A>(response: Response, decode: (value: unknown) => A) {
  if (!response.ok || !response.body) {
    await response.body?.cancel();
    throw new Error("Upstream rejected pairing.");
  }
  const reader = response.body.getReader();
  const chunks: Uint8Array[] = [];
  let size = 0;
  try {
    while (true) {
      const chunk = await reader.read();
      if (chunk.done) break;
      size += chunk.value.byteLength;
      if (size > 16_384) throw new Error("Upstream response exceeds the size limit.");
      chunks.push(chunk.value);
    }
    return decode(JSON.parse(Buffer.concat(chunks).toString("utf8")));
  } finally {
    await reader.cancel();
  }
}

const make = Effect.fn("SelfHostedConnect.make")(function* (
  config: SelfHostedConfig,
  fetchRequest: typeof globalThis.fetch,
) {
  const database = yield* Effect.acquireRelease(
    Effect.try({
      try: () => {
        if (config.databasePath !== ":memory:") {
          const directory = NodePath.dirname(config.databasePath);
          NodeFS.mkdirSync(directory, { recursive: true, mode: 0o700 });
          if ((NodeFS.statSync(directory).mode & 0o077) !== 0) {
            throw new Error("Session storage directory must have mode 0700.");
          }
          const fd = NodeFS.openSync(
            config.databasePath,
            NodeFS.constants.O_CREAT | NodeFS.constants.O_RDWR | NodeFS.constants.O_NOFOLLOW,
            0o600,
          );
          NodeFS.closeSync(fd);
          NodeFS.chmodSync(config.databasePath, 0o600);
        }
        const db = new NodeSqlite.DatabaseSync(config.databasePath);
        db.exec(`
          PRAGMA journal_mode = WAL;
          PRAGMA busy_timeout = 5000;
          CREATE TABLE IF NOT EXISTS sessions (
            id TEXT PRIMARY KEY,
            family_id TEXT NOT NULL,
            account_id TEXT NOT NULL,
            access_hash TEXT UNIQUE NOT NULL,
            refresh_hash TEXT UNIQUE NOT NULL,
            access_expires_at INTEGER NOT NULL,
            refresh_expires_at INTEGER NOT NULL,
            consumed_at INTEGER,
            revoked_at INTEGER
          );
          CREATE INDEX IF NOT EXISTS sessions_family ON sessions(family_id);
          CREATE TABLE IF NOT EXISTS device_labels (
            family_id TEXT PRIMARY KEY,
            label TEXT NOT NULL,
            expires_at INTEGER NOT NULL
          );
          CREATE TABLE IF NOT EXISTS environment_sessions (
            family_id TEXT NOT NULL,
            environment_id TEXT NOT NULL,
            session_id TEXT NOT NULL,
            PRIMARY KEY(environment_id, session_id)
          );
        `);
        return db;
      },
      catch: (cause) => new StorageError({ operation: "open", cause }),
    }),
    (db) => Effect.sync(() => db.close()),
  );
  const storage = <A>(operation: StorageError["operation"], work: () => A) =>
    Effect.try({ try: work, catch: (cause) => new StorageError({ operation, cause }) });
  const timeout = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.timeoutOrElse({
        duration: config.requestTimeoutMs ?? 5_000,
        orElse: () => Effect.fail(new UpstreamTimeout()),
      }),
    );
  const activity = yield* AndroidActivity.make(database, config);
  const webhookQueue = yield* WebhookQueue.make(database, config, fetchRequest);
  const loginBudgets = new Map<string, { count: number; resetsAt: number }>();
  let concurrentPasswordChecks = 0;
  const revokeLock = yield* Semaphore.make(1);
  const revokeEnvironmentSession = Effect.fn("SelfHostedConnect.revokeEnvironmentSession")(
    function* (environmentId: string, sessionId: string) {
      const environment = config.environments.find(
        (entry) => entry.environmentId === environmentId,
      );
      if (!environment) return;
      const revoked = yield* timeout(
        Effect.tryPromise({
          try: async (signal) => {
            const response = await fetchRequest(
              new URL("/api/auth/clients/revoke", environment.pairingOrigin),
              {
                signal,
                method: "POST",
                redirect: "error",
                headers: {
                  Authorization: `Bearer ${environment.credential}`,
                  "Content-Type": "application/json",
                },
                body: encodeJson({ sessionId }),
              },
            );
            await response.body?.cancel();
            return response.ok;
          },
          catch: (cause) => new EnvironmentUnavailable({ cause }),
        }),
      ).pipe(Effect.orElseSucceed(() => false));
      if (revoked) {
        yield* storage("write", () => {
          database
            .prepare("DELETE FROM environment_sessions WHERE environment_id = ? AND session_id = ?")
            .run(environmentId, sessionId);
        });
      }
    },
  );
  const flushRevokedEnvironmentSessions = Effect.fn("SelfHostedConnect.flushRevoked")(function* (
    familyId: string | null,
  ) {
    const now = yield* Clock.currentTimeMillis;
    const sessions = yield* storage("read", () =>
      database
        .prepare(`
        SELECT environment_id, session_id FROM environment_sessions
        WHERE (? IS NULL OR environment_sessions.family_id = ?) AND NOT EXISTS (
          SELECT 1 FROM sessions
          WHERE sessions.family_id = environment_sessions.family_id AND account_id = ?
            AND refresh_expires_at > ? AND revoked_at IS NULL AND consumed_at IS NULL
        ) LIMIT 16`)
        .all(familyId, familyId, config.account.accountId, now),
    );
    yield* Effect.forEach(
      sessions,
      (session) =>
        revokeEnvironmentSession(String(session.environment_id), String(session.session_id)),
      { concurrency: 4, discard: true },
    );
  }, revokeLock.withPermits(1));
  yield* flushRevokedEnvironmentSessions(null).pipe(
    Effect.catch(() => Effect.logWarning("Could not reconcile environment session revocations.")),
    Effect.repeat(Schedule.spaced("10 seconds")),
    Effect.forkScoped,
  );

  const issueSession = (
    now: number,
    familyId: string,
    refreshExpiresAt: number,
    deviceLabel?: string,
  ): SelfHostedSession => {
    const accessToken = newToken();
    const refreshToken = newToken();
    const accessExpiresAt = Math.min(
      now + (config.accessTokenTtlSeconds ?? 900) * 1_000,
      refreshExpiresAt,
    );
    database
      .prepare(`INSERT INTO sessions
      (id, family_id, account_id, access_hash, refresh_hash, access_expires_at, refresh_expires_at)
      VALUES (?, ?, ?, ?, ?, ?, ?)`)
      .run(
        NodeCrypto.randomUUID(),
        familyId,
        config.account.accountId,
        tokenHash(accessToken),
        tokenHash(refreshToken),
        accessExpiresAt,
        refreshExpiresAt,
      );
    if (deviceLabel) {
      database
        .prepare("INSERT INTO device_labels (family_id, label, expires_at) VALUES (?, ?, ?)")
        .run(familyId, deviceLabel, refreshExpiresAt);
    }
    return {
      accountId: config.account.accountId,
      accessToken,
      refreshToken,
      expiresAt: isoTimestamp(accessExpiresAt),
    };
  };

  const authorize = Effect.fn("SelfHostedConnect.authorize")(function* (accessToken: string) {
    if (!TOKEN_PATTERN.test(accessToken)) return yield* new Unauthorized();
    const now = yield* Clock.currentTimeMillis;
    const session = yield* storage("read", () =>
      database
        .prepare(`SELECT family_id FROM sessions
      WHERE access_hash = ? AND account_id = ? AND access_expires_at > ? AND revoked_at IS NULL AND consumed_at IS NULL`)
        .get(tokenHash(accessToken), config.account.accountId, now),
    );
    if (!session) return yield* new Unauthorized();
    return { deviceId: String(session.family_id), now };
  });

  const login: SelfHostedConnect["Service"]["login"] = Effect.fn("SelfHostedConnect.login")(
    function* (input, remoteAddress) {
      const now = yield* Clock.currentTimeMillis;
      for (const [key, budget] of loginBudgets) {
        if (budget.resetsAt <= now) loginBudgets.delete(key);
      }
      // The socket address is bounded; forwarded headers never choose a login budget.
      const address = remoteAddress.slice(0, 128);
      let budget = loginBudgets.get(address);
      if (!budget) {
        if (loginBudgets.size >= 4_096) return yield* new RateLimited({ retryAfterSeconds: 60 });
        budget = { count: 0, resetsAt: now + 60_000 };
        loginBudgets.set(address, budget);
      }
      budget.count++;
      if (budget.count > 5 || concurrentPasswordChecks >= 4) {
        return yield* new RateLimited({
          retryAfterSeconds: Math.max(1, Math.ceil((budget.resetsAt - now) / 1_000)),
        });
      }
      concurrentPasswordChecks++;
      const valid = yield* Effect.tryPromise({
        try: () => verifyPassword(input.password, config.account.passwordHash),
        catch: (cause) => new PasswordError({ cause }),
      }).pipe(
        Effect.ensuring(
          Effect.sync(() => {
            concurrentPasswordChecks--;
          }),
        ),
      );
      if (!valid || input.username !== config.account.username) return yield* new Unauthorized();
      return yield* storage("write", () => {
        database.prepare("DELETE FROM sessions WHERE refresh_expires_at <= ?").run(now);
        database.prepare("DELETE FROM device_labels WHERE expires_at <= ?").run(now);
        return issueSession(
          now,
          NodeCrypto.randomUUID(),
          now + (config.refreshTokenTtlSeconds ?? 2_592_000) * 1_000,
          input.deviceLabel?.trim(),
        );
      });
    },
  );

  const refresh: SelfHostedConnect["Service"]["refresh"] = Effect.fn("SelfHostedConnect.refresh")(
    function* (refreshToken) {
      if (!TOKEN_PATTERN.test(refreshToken)) return yield* new Unauthorized();
      const now = yield* Clock.currentTimeMillis;
      const result = yield* storage("write", () => {
        database.exec("BEGIN IMMEDIATE");
        try {
          const session = database
            .prepare("SELECT * FROM sessions WHERE refresh_hash = ? AND account_id = ?")
            .get(tokenHash(refreshToken), config.account.accountId);
          if (
            !session ||
            Number(session.refresh_expires_at) <= now ||
            session.revoked_at !== null
          ) {
            return undefined;
          }
          if (session.consumed_at !== null) {
            database
              .prepare("UPDATE sessions SET revoked_at = ? WHERE family_id = ?")
              .run(now, String(session.family_id));
            return undefined;
          }
          database
            .prepare("UPDATE sessions SET consumed_at = ? WHERE id = ?")
            .run(now, String(session.id));
          return issueSession(now, String(session.family_id), Number(session.refresh_expires_at));
        } catch (cause) {
          database.exec("ROLLBACK");
          throw cause;
        } finally {
          if (database.isTransaction) database.exec("COMMIT");
        }
      });
      if (!result) return yield* new Unauthorized();
      return result;
    },
  );

  const logout: SelfHostedConnect["Service"]["logout"] = Effect.fn("SelfHostedConnect.logout")(
    function* (refreshToken) {
      const now = yield* Clock.currentTimeMillis;
      const familyId = yield* storage("write", () => {
        const session = database
          .prepare("SELECT family_id FROM sessions WHERE refresh_hash = ? AND account_id = ?")
          .get(tokenHash(refreshToken), config.account.accountId);
        database
          .prepare(`UPDATE sessions SET revoked_at = ? WHERE family_id IN
        (SELECT family_id FROM sessions WHERE refresh_hash = ? AND account_id = ?)`)
          .run(now, tokenHash(refreshToken), config.account.accountId);
        return typeof session?.family_id === "string" ? session.family_id : null;
      });
      if (familyId) {
        yield* activity.notify;
        yield* flushRevokedEnvironmentSessions(familyId);
      }
    },
  );

  const listDevices: SelfHostedConnect["Service"]["listDevices"] = Effect.fn(
    "SelfHostedConnect.listDevices",
  )(function* (accessToken) {
    const { deviceId, now } = yield* authorize(accessToken);
    const rows = yield* storage("read", () =>
      database
        .prepare(`SELECT sessions.family_id, device_labels.label, sessions.refresh_expires_at
          FROM sessions LEFT JOIN device_labels USING (family_id)
          WHERE account_id = ? AND refresh_expires_at > ?
            AND revoked_at IS NULL AND consumed_at IS NULL
          ORDER BY refresh_expires_at DESC, sessions.family_id`)
        .all(config.account.accountId, now),
    );
    return {
      devices: rows.map((row) => ({
        id: String(row.family_id),
        label: typeof row.label === "string" ? row.label : null,
        expiresAt: isoTimestamp(Number(row.refresh_expires_at)),
        current: row.family_id === deviceId,
      })),
    };
  });

  const revokeDevice: SelfHostedConnect["Service"]["revokeDevice"] = Effect.fn(
    "SelfHostedConnect.revokeDevice",
  )(function* (accessToken, deviceId) {
    const { now } = yield* authorize(accessToken);
    yield* storage("write", () => {
      database
        .prepare("UPDATE sessions SET revoked_at = ? WHERE family_id = ? AND account_id = ?")
        .run(now, deviceId, config.account.accountId);
    });
    yield* activity.notify;
    yield* flushRevokedEnvironmentSessions(deviceId);
  });

  const registerAndroid: SelfHostedConnect["Service"]["registerAndroid"] = Effect.fn(
    "SelfHostedConnect.registerAndroid",
  )(function* (accessToken, input, deliveryTokenHash) {
    const { now, deviceId } = yield* authorize(accessToken);
    if (!/^[a-f0-9]{64}$/.test(deliveryTokenHash)) {
      return yield* new Unauthorized();
    }
    if (input.platform !== "android" || input.bundleId !== activity.packageName) {
      return yield* new EnvironmentUnavailable({
        cause: new Error("The Android app package is not configured for notifications."),
      });
    }
    yield* storage("write", () => activity.register(deviceId, deliveryTokenHash, input, now));
  });

  const readAndroid: SelfHostedConnect["Service"]["readAndroid"] = Effect.fn(
    "SelfHostedConnect.readAndroid",
  )(function* (deliveryToken, after, replay) {
    if (!TOKEN_PATTERN.test(deliveryToken)) return yield* new Unauthorized();
    const hash = tokenHash(deliveryToken);
    const read = Effect.fn("SelfHostedConnect.readAndroidBatch")(function* (snapshot: boolean) {
      const now = yield* Clock.currentTimeMillis;
      const messages = yield* storage("write", () => activity.read(hash, after, now, snapshot));
      return messages ?? (yield* new Unauthorized());
    });
    const changed = activity.waitForChange();
    const messages = yield* read(replay === true);
    if (messages.length > 0) return { messages };
    yield* Effect.timeoutOrElse(changed, { duration: "20 seconds", orElse: () => Effect.void });
    return { messages: yield* read(false) };
  });

  const publishActivity: SelfHostedConnect["Service"]["publishActivity"] = Effect.fn(
    "SelfHostedConnect.publishActivity",
  )(function* (key, environmentId, threadId, state) {
    const expected = config.environments.find(
      (entry) => entry.environmentId === environmentId,
    )?.activityKey;
    if (
      !expected ||
      !TOKEN_PATTERN.test(key) ||
      !NodeCrypto.timingSafeEqual(Buffer.from(tokenHash(key)), Buffer.from(tokenHash(expected))) ||
      (state !== null && (state.environmentId !== environmentId || state.threadId !== threadId))
    ) {
      return yield* new Unauthorized();
    }
    const now = yield* Clock.currentTimeMillis;
    const deepLink = `/threads/${encodeURIComponent(environmentId)}/${encodeURIComponent(threadId)}`;
    if (deepLink.length > 512) return yield* new Unauthorized();
    const normalized = state
      ? {
          ...state,
          deepLink,
        }
      : null;
    yield* storage("write", () => activity.publish(environmentId, threadId, normalized, now));
    yield* activity.notify;
  });

  const queueWebhook: SelfHostedConnect["Service"]["queueWebhook"] = Effect.fn(
    "SelfHostedConnect.queueWebhook",
  )(function* (input) {
    if (!webhookQueue.accepts(input)) return yield* new EnvironmentNotFound();
    const now = yield* Clock.currentTimeMillis;
    const deliveryId = yield* storage("write", () => webhookQueue.enqueue(input, now));
    if (!deliveryId) return yield* new WebhookQueue.WebhookQueueFull();
    return { deliveryId };
  });

  const listEnvironments: SelfHostedConnect["Service"]["listEnvironments"] = Effect.fn(
    "SelfHostedConnect.listEnvironments",
  )(function* (accessToken) {
    yield* authorize(accessToken);
    const environments = yield* Effect.forEach(
      config.environments,
      (environment) =>
        Effect.gen(function* () {
          const available = yield* timeout(
            Effect.tryPromise({
              try: async (signal) => {
                const response = await fetchRequest(
                  new URL("/.well-known/t3/environment", environment.pairingOrigin),
                  {
                    signal,
                    redirect: "error",
                    headers: { Accept: "application/json" },
                  },
                );
                await response.body?.cancel();
                return response.ok;
              },
              catch: (cause) => new EnvironmentUnavailable({ cause }),
            }),
          ).pipe(Effect.orElseSucceed(() => false));
          return {
            environmentId: environment.environmentId,
            label: environment.label,
            httpBaseUrl: environment.origin,
            wsBaseUrl: environment.origin.replace(/^https:/, "wss:"),
            available,
          };
        }),
      { concurrency: 8 },
    );
    return { environments };
  });

  const connect: SelfHostedConnect["Service"]["connect"] = Effect.fn("SelfHostedConnect.connect")(
    function* (accessToken, environmentId) {
      yield* authorize(accessToken);
      const environment = config.environments.find((item) => item.environmentId === environmentId);
      if (!environment) return yield* new EnvironmentNotFound();
      const pairing = yield* timeout(
        Effect.tryPromise({
          try: async (signal) => {
            const response = await fetchRequest(
              new URL("/api/auth/pairing-token", environment.pairingOrigin),
              {
                method: "POST",
                signal,
                redirect: "error",
                headers: {
                  Authorization: `Bearer ${environment.credential}`,
                  "Content-Type": "application/json",
                },
                body: pairingRequestBody(environment.label),
              },
            );
            return readUpstreamResponse(response, decodePairingResponse);
          },
          catch: (cause) => new EnvironmentUnavailable({ cause }),
        }),
      );
      const expiresAt = Date.parse(pairing.expiresAt);
      if (!Number.isFinite(expiresAt) || expiresAt <= (yield* Clock.currentTimeMillis)) {
        return yield* new EnvironmentUnavailable({
          cause: new Error("Upstream pairing expiry is invalid."),
        });
      }
      return {
        environmentId: environment.environmentId,
        pairingUrl: `${environment.origin}/pair#token=${encodeURIComponent(pairing.credential)}`,
        expiresAt: isoTimestamp(expiresAt),
      };
    },
  );
  const connectAccount: SelfHostedConnect["Service"]["connectAccount"] = Effect.fn(
    "SelfHostedConnect.connectAccount",
  )(function* (accessToken, environmentId) {
    const { deviceId } = yield* authorize(accessToken);
    const environment = config.environments.find((entry) => entry.environmentId === environmentId);
    if (!environment) return yield* new EnvironmentNotFound();
    const pairing = yield* connect(accessToken, environmentId);
    const credential = new URLSearchParams(new URL(pairing.pairingUrl).hash.slice(1)).get("token")!;
    const grant = yield* timeout(
      Effect.tryPromise({
        try: async (signal) => {
          const response = await fetchRequest(new URL("/oauth/token", environment.pairingOrigin), {
            method: "POST",
            redirect: "error",
            signal,
            body: new URLSearchParams({
              grant_type: AuthTokenExchangeGrantType,
              subject_token: credential,
              subject_token_type: AuthEnvironmentBootstrapTokenType,
              requested_token_type: AuthAccessTokenType,
            }),
          });
          return readUpstreamResponse(response, decodeAccessToken);
        },
        catch: (cause) => new EnvironmentUnavailable({ cause }),
      }),
    );
    if (!grant.session_id || grant.token_type !== "Bearer") {
      return yield* new EnvironmentUnavailable({
        cause: new Error("Update the environment before connecting it to the account."),
      });
    }
    const sessionId = grant.session_id;
    const now = yield* Clock.currentTimeMillis;
    const active = yield* storage("write", () => {
      database
        .prepare("INSERT INTO environment_sessions VALUES (?, ?, ?)")
        .run(deviceId, environmentId, sessionId);
      return (
        database
          .prepare(`SELECT 1 FROM sessions WHERE family_id = ? AND account_id = ?
        AND refresh_expires_at > ? AND revoked_at IS NULL AND consumed_at IS NULL`)
          .get(deviceId, config.account.accountId, now) !== undefined
      );
    }).pipe(
      Effect.tapError(() => revokeEnvironmentSession(environmentId, sessionId).pipe(Effect.ignore)),
    );
    if (!active) {
      yield* flushRevokedEnvironmentSessions(deviceId);
      return yield* new Unauthorized();
    }
    return { environmentId, httpBaseUrl: environment.origin, accessToken: grant.access_token };
  });
  return SelfHostedConnect.of({
    login,
    refresh,
    logout,
    listDevices,
    revokeDevice,
    registerAndroid,
    readAndroid,
    publishActivity,
    queueWebhook,
    listEnvironments,
    connect,
    connectAccount,
  });
});

export const layer = (
  config: SelfHostedConfig,
  options?: { readonly fetch?: typeof globalThis.fetch },
) => Layer.effect(SelfHostedConnect, make(config, options?.fetch ?? globalThis.fetch));
