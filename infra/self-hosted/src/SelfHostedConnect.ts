// @effect-diagnostics-next-line nodeBuiltinImport:off -- Native crypto helpers run inside the synchronous SQLite transaction boundary.
import * as NodeCrypto from "node:crypto";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The native SQLite resource must open an owned, permission-restricted file without following symlinks.
import * as NodeFS from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- The native SQLite resource validates its containing directory during acquisition.
import * as NodePath from "node:path";
import * as NodeSqlite from "node:sqlite";
import { AuthStandardClientScopes } from "@t3tools/contracts";
import type {
  SelfHostedConnection,
  SelfHostedEnvironments,
  SelfHostedLoginInput,
  SelfHostedSession,
} from "@t3tools/contracts/self-hosted-connect";
import * as Clock from "effect/Clock";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import type { SelfHostedConfig } from "./config.ts";
import { verifyPassword } from "./password.ts";

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

async function readPairingResponse(response: Response) {
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
    return decodePairingResponse(JSON.parse(Buffer.concat(chunks).toString("utf8")));
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
        `);
        return db;
      },
      catch: (cause) => new StorageError({ operation: "open", cause }),
    }),
    (db) => Effect.sync(() => db.close()),
  );
  const storage = <A>(operation: StorageError["operation"], work: () => A) =>
    Effect.try({ try: work, catch: (cause) => new StorageError({ operation, cause }) });
  const loginBudgets = new Map<string, { count: number; resetsAt: number }>();
  let concurrentPasswordChecks = 0;

  const issueSession = (
    now: number,
    familyId: string,
    refreshExpiresAt: number,
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
    const exists = yield* storage("read", () =>
      database
        .prepare(`SELECT id FROM sessions
      WHERE access_hash = ? AND account_id = ? AND access_expires_at > ? AND revoked_at IS NULL AND consumed_at IS NULL`)
        .get(tokenHash(accessToken), config.account.accountId, now),
    );
    if (!exists) return yield* new Unauthorized();
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
        return issueSession(
          now,
          NodeCrypto.randomUUID(),
          now + (config.refreshTokenTtlSeconds ?? 2_592_000) * 1_000,
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
      yield* storage("write", () => {
        database
          .prepare(`UPDATE sessions SET revoked_at = ? WHERE family_id IN
        (SELECT family_id FROM sessions WHERE refresh_hash = ? AND account_id = ?)`)
          .run(now, tokenHash(refreshToken), config.account.accountId);
      });
    },
  );

  const timeout = <A, E, R>(effect: Effect.Effect<A, E, R>) =>
    effect.pipe(
      Effect.timeoutOrElse({
        duration: config.requestTimeoutMs ?? 5_000,
        orElse: () => Effect.fail(new UpstreamTimeout()),
      }),
    );
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
            return readPairingResponse(response);
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
  return SelfHostedConnect.of({ login, refresh, logout, listEnvironments, connect });
});

export const layer = (
  config: SelfHostedConfig,
  options?: { readonly fetch?: typeof globalThis.fetch },
) => Layer.effect(SelfHostedConnect, make(config, options?.fetch ?? globalThis.fetch));
