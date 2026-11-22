// @effect-diagnostics-next-line nodeBuiltinImport:off -- Delivery IDs and authentication are created at the SQLite boundary.
import * as NodeCrypto from "node:crypto";
import type * as NodeSqlite from "node:sqlite";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schedule from "effect/Schedule";
import * as Schema from "effect/Schema";

import type { SelfHostedConfig } from "./config.ts";

export class WebhookQueueFull extends Schema.TaggedError<
  WebhookQueueFull,
  { readonly brand: unique symbol }
>()("WebhookQueueFull", {}) {}
class WebhookForwardError extends Schema.TaggedError<
  WebhookForwardError,
  { readonly brand: unique symbol }
>()("WebhookForwardError", { cause: Schema.Defect() }) {}

export interface WebhookDelivery {
  readonly environmentId: string;
  readonly hookId: string;
  readonly token: string;
  readonly signature: string;
  readonly method: string;
  readonly query: string;
  readonly headers: Readonly<Record<string, string>>;
  readonly body: Uint8Array;
}

const decodeHeaders = Schema.decodeUnknownSync(
  Schema.fromJsonString(Schema.Record(Schema.String, Schema.String)),
);
const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const RETENTION = 24 * 60 * 60 * 1_000;

export const make = Effect.fn("SelfHostedWebhooks.make")(function* (
  database: NodeSqlite.DatabaseSync,
  config: SelfHostedConfig,
  fetchRequest: typeof globalThis.fetch,
) {
  const environments = new Map(
    config.environments
      .filter((entry) => entry.activityKey)
      .map((entry) => [entry.environmentId, entry]),
  );
  yield* Effect.sync(() =>
    database.exec(`
      CREATE TABLE IF NOT EXISTS held_webhooks (
        id TEXT PRIMARY KEY,
        environment_id TEXT NOT NULL,
        hook_id TEXT NOT NULL,
        token TEXT NOT NULL,
        method TEXT NOT NULL,
        query TEXT NOT NULL,
        headers TEXT NOT NULL,
        body BLOB NOT NULL,
        received_at INTEGER NOT NULL,
        next_attempt_at INTEGER NOT NULL
      );
    `),
  );
  const drain = Effect.fn("SelfHostedWebhooks.drain")(function* () {
    const now = yield* Clock.currentTimeMillis;
    const pending = yield* Effect.sync(() => {
      database.prepare("DELETE FROM held_webhooks WHERE received_at < ?").run(now - RETENTION);
      return database
        .prepare(`SELECT * FROM held_webhooks WHERE next_attempt_at <= ?
        ORDER BY received_at, id LIMIT 16`)
        .all(now);
    });
    yield* Effect.forEach(
      pending,
      (row) =>
        Effect.gen(function* () {
          const id = String(row.id);
          const environmentId = String(row.environment_id);
          const hookId = String(row.hook_id);
          const environment = environments.get(environmentId);
          const delivered = environment?.activityKey
            ? yield* Effect.tryPromise({
                try: async (signal) => {
                  const url = new URL(
                    `/api/hooks/${encodeURIComponent(hookId)}/${encodeURIComponent(String(row.token))}`,
                    environment.pairingOrigin,
                  );
                  url.search = String(row.query);
                  const receivedAt = DateTime.formatIso(
                    DateTime.makeUnsafe(Number(row.received_at)),
                  );
                  const proof = NodeCrypto.createHmac("sha256", environment.activityKey!)
                    .update(
                      encodeJson([
                        "delivery",
                        environmentId,
                        hookId,
                        String(row.token),
                        id,
                        receivedAt,
                      ]),
                    )
                    .digest("base64url");
                  const response = await fetchRequest(url, {
                    method: String(row.method),
                    headers: {
                      ...decodeHeaders(row.headers),
                      "x-t3-self-hosted-delivery": proof,
                      "x-t3-relay-delivery-id": id,
                      "x-t3-relay-received-at": receivedAt,
                    },
                    ...(row.method === "GET" ? {} : { body: Buffer.from(row.body as Uint8Array) }),
                    signal,
                    redirect: "error",
                  });
                  await response.body?.cancel();
                  return response.status < 500 && response.status !== 429;
                },
                catch: (cause) => new WebhookForwardError({ cause }),
              }).pipe(
                Effect.timeoutOrElse({
                  duration: "6 seconds",
                  orElse: () => Effect.succeed(false),
                }),
                Effect.orElseSucceed(() => false),
              )
            : true;
          yield* Effect.sync(() => {
            if (delivered) {
              database.prepare("DELETE FROM held_webhooks WHERE id = ?").run(id);
            } else {
              database
                .prepare("UPDATE held_webhooks SET next_attempt_at = ? WHERE id = ?")
                .run(now + 30_000, id);
            }
          });
        }),
      { concurrency: 4, discard: true },
    );
  });
  if (environments.size > 0) {
    yield* drain().pipe(
      Effect.catchCause(() =>
        Effect.logWarning("Could not process the self-hosted webhook queue."),
      ),
      Effect.repeat(Schedule.spaced("2 seconds")),
      Effect.forkScoped,
    );
  }
  return {
    accepts: (delivery: WebhookDelivery) => {
      const key = environments.get(delivery.environmentId)?.activityKey;
      if (!key || !/^[A-Za-z0-9_-]{43}$/.test(delivery.signature)) return false;
      const expected = NodeCrypto.createHmac("sha256", key)
        .update(
          JSON.stringify(["ingress", delivery.environmentId, delivery.hookId, delivery.token]),
        )
        .digest("base64url");
      return NodeCrypto.timingSafeEqual(Buffer.from(expected), Buffer.from(delivery.signature));
    },
    enqueue: (delivery: WebhookDelivery, now: number) => {
      const queue = database
        .prepare("SELECT count(*) AS count, sum(length(body)) AS bytes FROM held_webhooks")
        .get();
      if (
        Number(queue?.count) >= 256 ||
        Number(queue?.bytes) + delivery.body.byteLength > 33_554_432
      )
        return null;
      const id = NodeCrypto.randomUUID();
      database
        .prepare(`INSERT INTO held_webhooks
        (id, environment_id, hook_id, token, method, query, headers, body, received_at, next_attempt_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`)
        .run(
          id,
          delivery.environmentId,
          delivery.hookId,
          delivery.token,
          delivery.method,
          delivery.query,
          JSON.stringify(delivery.headers),
          delivery.body,
          now,
          now,
        );
      return id;
    },
  };
});
