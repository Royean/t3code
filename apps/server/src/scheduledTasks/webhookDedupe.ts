// @effect-diagnostics-next-line nodeBuiltinImport:off -- Keys need a deterministic digest, not random bytes.
import * as NodeCrypto from "node:crypto";
import { ScheduledTask, ScheduledTaskError } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { renderWebhookPrompt, type WebhookRequest } from "./webhookTemplate.ts";

function key(template: string | undefined, request: WebhookRequest): string {
  if (template === undefined) return "";
  const rendered = renderWebhookPrompt(template, request, "", true);
  return rendered.missing.length > 0 || rendered.prompt === ""
    ? ""
    : NodeCrypto.createHash("sha256").update(rendered.prompt).digest("hex");
}

export const releaseWebhookReplay = Effect.fn("ScheduledTaskService.releaseWebhookReplay")(
  function* (deliveryId: string) {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`DELETE FROM scheduled_task_webhook_replay_keys WHERE delivery_id = ${deliveryId}`;
  },
);

const findReplayKey = Effect.fn("ScheduledTaskService.findReplayKey")(function* (
  taskId: string,
  replayKey: string,
  now: number,
) {
  const sql = yield* SqlClient.SqlClient;
  if (replayKey === "") return undefined;
  return (yield* sql<{
    delivery_id: string;
    execution_id: string | null;
  }>`SELECT delivery_id, execution_id FROM scheduled_task_webhook_replay_keys
    WHERE task_id = ${taskId} AND replay_key = ${replayKey} AND expires_at > ${now}`)[0];
});
const claimReplayKey = Effect.fn("ScheduledTaskService.claimReplayKey")(function* (
  taskId: string,
  replayKey: string,
  deliveryId: string,
  now: number,
  executionId: string | null,
) {
  if (replayKey === "") return;
  const sql = yield* SqlClient.SqlClient;
  yield* sql`DELETE FROM scheduled_task_webhook_replay_keys WHERE expires_at <= ${now}`;
  yield* sql`INSERT INTO scheduled_task_webhook_replay_keys (task_id, replay_key, delivery_id, expires_at, execution_id)
    VALUES (${taskId}, ${replayKey}, ${deliveryId}, ${now + 7 * 24 * 60 * 60 * 1000}, ${executionId})`;
});

export const acceptWebhookDedupe = Effect.fn("ScheduledTaskService.acceptWebhookDedupe")(function* (
  task: ScheduledTask,
  request: WebhookRequest,
  deliveryId: string,
  now: number,
  reserve: Effect.Effect<void, "first_rejected" | "rejected">,
  reserveCoalescedLog: Effect.Effect<boolean>,
  checkCapacity: Effect.Effect<void, "queue_full" | ScheduledTaskError>,
  replaceDeliveryRequest: (ownerId: string) => Effect.Effect<void, ScheduledTaskError>,
  executionId: string,
  track: (
    outcome: string,
    executionId: string | null,
    create?: boolean,
  ) => Effect.Effect<void, ScheduledTaskError>,
) {
  const sql = yield* SqlClient.SqlClient;
  const dedupe = task.schedule.type === "webhook" ? task.schedule.dedupe : undefined;
  if (dedupe === undefined) return "run" as const;
  const replayKey = key(dedupe.replayKey, request);
  const replay = yield* findReplayKey(task.id, replayKey, now);
  if (replay !== undefined) {
    yield* track("duplicate", replay.execution_id);
    return "duplicate" as const;
  }
  const coalesceKey = key(dedupe.coalesceKey, request);
  if (coalesceKey === "") return "run" as const;
  const pending = yield* sql<{
    delivery_id: string;
    execution_id: string | null;
  }>`SELECT delivery_id, execution_id FROM scheduled_task_webhook_pending
    WHERE task_id = ${task.id} AND coalesce_key = ${coalesceKey} AND state = 'pending' AND due_at > ${now}`;
  const logCoalesced = pending.length > 0 ? yield* reserveCoalescedLog : true;
  if (pending.length === 0) {
    yield* checkCapacity;
    yield* reserve;
  }
  // Coalesced requests retain their validation slot, bounding writes without dropping accepted payloads.
  const ownerExecutionId = pending.length > 0 ? pending[0]!.execution_id : executionId;
  yield* track(
    pending.length > 0 ? "coalesced" : "accepted",
    ownerExecutionId,
    pending.length === 0,
  );
  yield* claimReplayKey(
    task.id,
    replayKey,
    pending[0]?.delivery_id ?? deliveryId,
    now,
    ownerExecutionId,
  );
  const requestJson = yield* encodePendingRequest({
    method: request.method,
    path: request.path,
    query: request.query,
    headers: request.headers,
    bodyText: request.bodyText,
  });
  if (pending.length > 0) {
    yield* sql`UPDATE scheduled_task_webhook_pending SET request_json = ${requestJson},
      delivery_ids_json = json_insert(delivery_ids_json, '$[#]', ${deliveryId}) WHERE delivery_id = ${pending[0]!.delivery_id}`;
    yield* replaceDeliveryRequest(pending[0]!.delivery_id);
    return logCoalesced ? ("coalesced" as const) : ("coalesced_unlogged" as const);
  }
  // Expired windows are immutable, even if the scheduler has not dispatched them yet.
  yield* sql`UPDATE scheduled_task_webhook_pending SET state = 'dispatching'
    WHERE task_id = ${task.id} AND coalesce_key = ${coalesceKey} AND state = 'pending' AND due_at <= ${now}`;
  yield* sql`INSERT INTO scheduled_task_webhook_pending (delivery_id, task_id, coalesce_key, due_at, task_json, request_json, delivery_ids_json, execution_id)
    VALUES (${deliveryId}, ${task.id}, ${coalesceKey}, ${now + (dedupe.coalesceWindowSeconds ?? 120) * 1000}, ${yield* encodePendingTask(task)}, ${requestJson}, json_array(${deliveryId}), ${executionId})`;
  return "pending" as const;
});

export const acceptImmediateWebhookDedupe = Effect.fn(
  "ScheduledTaskService.acceptImmediateWebhookDedupe",
)(function* (
  task: ScheduledTask,
  request: WebhookRequest,
  deliveryId: string,
  now: number,
  reserve: Effect.Effect<void, "first_rejected" | "rejected" | "queue_full" | ScheduledTaskError>,
  executionId: string,
  track: (
    outcome: string,
    executionId: string | null,
    create?: boolean,
  ) => Effect.Effect<void, ScheduledTaskError>,
) {
  const replayKey = key(
    task.schedule.type === "webhook" ? task.schedule.dedupe?.replayKey : undefined,
    request,
  );
  const replay = yield* findReplayKey(task.id, replayKey, now);
  if (replay !== undefined) {
    yield* track("duplicate", replay.execution_id);
    return "duplicate" as const;
  }
  yield* reserve;
  yield* track("accepted", executionId, true);
  yield* claimReplayKey(task.id, replayKey, deliveryId, now, executionId);
  return "run" as const;
});

const PendingTaskJson = Schema.fromJsonString(ScheduledTask);
export const encodePendingTask = Schema.encodeEffect(PendingTaskJson);
export const decodePendingTask = Schema.decodeUnknownEffect(PendingTaskJson);
const PendingRequestJson = Schema.fromJsonString(
  Schema.Struct({
    method: Schema.String,
    path: Schema.String,
    query: Schema.String,
    headers: Schema.Record(Schema.String, Schema.String),
    bodyText: Schema.String,
  }),
);
const encodePendingRequest = Schema.encodeEffect(PendingRequestJson);
export const decodePendingRequest = Schema.decodeUnknownEffect(PendingRequestJson);
export const dedupeError = (cause: unknown) =>
  new ScheduledTaskError({ message: "Could not persist webhook dedupe state.", cause });
