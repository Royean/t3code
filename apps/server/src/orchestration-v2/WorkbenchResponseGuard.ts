import {
  OrchestrationV2Command,
  type OrchestrationV2ServerCommand,
  type OrchestrationV2StoredEvent,
  type ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { Admission, OriginalResume } from "./WorkbenchAttentionGuard.ts";
import type { PendingOrchestrationEffectV2 } from "./EffectOutbox.ts";

export class WorkbenchResponseGuardError extends Schema.TaggedError<WorkbenchResponseGuardError>()(
  "WorkbenchResponseGuardError",
  {
    reason: Schema.Literals([
      "intent-missing",
      "source-changed",
      "request-not-pending",
      "not-resumable",
      "claim-stale",
      "command-mismatch",
      "context-mismatch",
      "unavailable",
    ]),
    responseId: Schema.optional(Schema.String),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

const Source = Schema.Struct({
  id: Schema.String,
  version: Schema.Number,
  fingerprint: Schema.String,
  queue_id: Schema.String,
  work_item_id: Schema.String,
  cycle_number: Schema.Number,
  batch_id: Schema.NullOr(Schema.String),
  thread_id: Schema.String,
  source_run_id: Schema.String,
  source_attempt_id: Schema.NullOr(Schema.String),
  node_id: Schema.String,
  provider_thread_id: Schema.String,
  provider_turn_id: Schema.NullOr(Schema.String),
  runtime_request_id: Schema.String,
  provider_session_id: Schema.NullOr(Schema.String),
  request_json: Schema.String,
  turn_item_id: Schema.String,
  turn_item_json: Schema.String,
  capability: Schema.String,
  provider_instance_id: Schema.String,
  model_selection_json: Schema.String,
  runtime_mode: Schema.String,
  interaction_mode: Schema.String,
  native_driver: Schema.NullOr(Schema.String),
  native_thread_id: Schema.NullOr(Schema.String),
  worktree_path: Schema.NullOr(Schema.String),
  worktree_real_path: Schema.NullOr(Schema.String),
  branch: Schema.NullOr(Schema.String),
  git_common_dir: Schema.NullOr(Schema.String),
});
type Source = typeof Source.Type;
type ResponseRow = {
  id: string;
  source_id: string;
  source_version: number;
  source_json: string;
  queue_id: string;
  work_item_id: string;
  attention_epoch: number;
  claim_id: string | null;
  command_id: string;
  request_json: string;
  async_message_id: string | null;
  run_id: string | null;
  delegated_task_id: string | null;
  state: string;
  evidence_level: string;
  effect_id: string | null;
  effect_state: string | null;
};
export interface ResponseProof {
  readonly response: ResponseRow;
  readonly source: Source;
  readonly projectId: string;
}
type RespondCommand = Extract<OrchestrationV2ServerCommand, { type: "runtime-request.respond" }>;
const encodeCommand = Schema.encodeEffect(Schema.fromJsonString(OrchestrationV2Command));
const decodeSource = Schema.decodeEffect(Schema.fromJsonString(Source));
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const isGuardError = Schema.is(WorkbenchResponseGuardError);
const fail = (reason: WorkbenchResponseGuardError["reason"], responseId?: string) =>
  new WorkbenchResponseGuardError({ reason, ...(responseId === undefined ? {} : { responseId }) });
const normalizeFailure = (cause: unknown) =>
  isGuardError(cause) ? cause : new WorkbenchResponseGuardError({ reason: "unavailable", cause });

/** The original T3 UI retains its authorization, but cannot answer a held or different work cycle. */
const assertNativeResponse = Effect.fn("WorkbenchResponseGuard.assertNativeResponse")(function* (
  input: { readonly threadId: string; readonly requestId: string },
  status: "pending" | "resolved",
) {
  const sql = yield* SqlClient.SqlClient;
  const [allowed] = yield* sql`SELECT 1 FROM workbench_work_items w
    JOIN workbench_queue_bindings b ON b.task_id=w.task_id AND b.task_created_at=w.task_created_at
      AND b.dispatcher_thread_id=w.dispatcher_thread_id AND b.project_id=w.project_id
    JOIN workbench_queue_entries e ON e.queue_id=b.queue_id AND e.work_item_id=w.id
    JOIN orchestration_v2_projection_runtime_requests q ON q.thread_id=w.work_thread_id AND q.runtime_request_id=${input.requestId}
    JOIN orchestration_v2_projection_nodes n ON n.thread_id=q.thread_id AND n.node_id=q.node_id
    WHERE w.work_thread_id=${input.threadId} AND w.attention_hold=0
      AND e.state='occupied' AND e.claim_id IS NOT NULL AND q.status=${status}
      AND (n.run_id=w.run_id OR n.run_id=w.accepted_run_id OR EXISTS(
        SELECT 1 FROM workbench_work_batches batch WHERE batch.work_item_id=w.id
          AND (batch.run_id=n.run_id OR batch.accepted_run_id=n.run_id)) OR EXISTS(
        SELECT 1 FROM workbench_runtime_responses response WHERE response.work_item_id=w.id
          AND response.run_id=n.run_id AND json_extract(response.source_json,'$.cycle_number')=w.cycle_number) OR EXISTS(
        SELECT 1 FROM workbench_repair_attempts a JOIN workbench_repair_generations g ON g.id=a.generation_id
          WHERE a.work_item_id=w.id AND a.thread_id=w.work_thread_id AND a.run_id=n.run_id
            AND a.charged=1 AND a.state IN ('accepted','running') AND a.ordinal<=g.used
            AND g.work_item_id=w.id AND g.superseded_at IS NULL AND g.thread_id=w.work_thread_id
            AND g.task_id=w.task_id AND g.task_created_at=w.task_created_at AND g.project_id=w.project_id
            AND g.dispatcher_thread_id=w.dispatcher_thread_id AND g.queue_id=e.queue_id
            AND a.queue_id=e.queue_id AND a.project_id=w.project_id AND a.claim_id=e.claim_id
            AND a.attention_epoch=w.attention_epoch AND a.wake_generation=e.wake_generation
            AND (g.phase='repairing' OR (g.phase='awaiting_user' AND g.reason IN ('user_input','approval')))) OR EXISTS(
        SELECT 1 FROM workbench_repair_native_answers answer JOIN workbench_repair_generations g ON g.id=answer.generation_id
          WHERE answer.work_item_id=w.id AND answer.thread_id=w.work_thread_id AND answer.run_id=n.run_id
            AND g.work_item_id=w.id AND g.superseded_at IS NULL AND g.queue_id=e.queue_id
            AND answer.queue_id=e.queue_id AND answer.claim_id=e.claim_id
            AND answer.attention_epoch=w.attention_epoch AND answer.wake_generation=e.wake_generation))`;
  if (allowed === undefined) return yield* fail("claim-stale");
});

/** Frozen private provenance survives later source refreshes and request resolution. */
const proof = Effect.fn("WorkbenchResponseGuard.proof")(function* (
  row: ResponseRow,
  threadId: string,
) {
  const sql = yield* SqlClient.SqlClient;
  const source = yield* decodeSource(row.source_json);
  if (
    source.id !== row.source_id ||
    source.version !== row.source_version ||
    source.thread_id !== threadId ||
    source.queue_id !== row.queue_id ||
    source.work_item_id !== row.work_item_id
  )
    return yield* fail("context-mismatch", row.id);
  const [binding] = yield* sql<{ project_id: string }>`SELECT w.project_id
    FROM workbench_work_items w
    JOIN workbench_queue_bindings b ON b.queue_id=${row.queue_id} AND b.task_id=w.task_id
      AND b.task_created_at=w.task_created_at AND b.dispatcher_thread_id=w.dispatcher_thread_id AND b.project_id=w.project_id
    JOIN workbench_queue_entries e ON e.work_item_id=w.id AND e.queue_id=b.queue_id
    WHERE w.id=${row.work_item_id} AND w.work_thread_id=${threadId} AND w.cycle_number=${source.cycle_number}
      AND w.attention_epoch=${row.attention_epoch} AND e.state='occupied'
      AND e.claim_id=${row.claim_id} AND ${row.claim_id} IS NOT NULL`;
  if (
    binding === undefined ||
    !["queued", "submitted", "runtime_accepted", "delivered", "delivery_unconfirmed"].includes(
      row.state,
    )
  )
    return yield* fail("claim-stale", row.id);
  const [context] = yield* sql`SELECT 1 FROM orchestration_v2_projection_threads t
    JOIN orchestration_v2_projection_nodes n ON n.thread_id=t.thread_id AND n.node_id=${source.node_id}
    JOIN orchestration_v2_projection_runs r ON r.thread_id=t.thread_id AND r.run_id=${source.source_run_id}
    JOIN orchestration_v2_projection_provider_threads p ON p.thread_id=t.thread_id AND p.provider_thread_id=${source.provider_thread_id}
    WHERE t.thread_id=${threadId} AND t.project_id=${binding.project_id} AND t.deleted_at IS NULL
      AND json_extract(t.payload_json,'$.providerInstanceId')=${source.provider_instance_id}
      AND json_extract(t.payload_json,'$.runtimeMode')=${source.runtime_mode}
      AND json_extract(t.payload_json,'$.interactionMode')=${source.interaction_mode}
      AND json(json_extract(t.payload_json,'$.modelSelection'))=json(${source.model_selection_json})
      AND n.run_id=${source.source_run_id} AND n.provider_thread_id=${source.provider_thread_id}
      AND (${source.capability !== "message"} OR json_extract(p.payload_json,'$.nativeThreadRef.strength')='strong')
      AND (${source.native_thread_id} IS NULL OR json_extract(p.payload_json,'$.nativeThreadRef.nativeId')=${source.native_thread_id})
      AND (${source.native_driver} IS NULL OR json_extract(p.payload_json,'$.nativeThreadRef.driver')=${source.native_driver})`;
  if (context === undefined) return yield* fail("context-mismatch", row.id);
  return { response: row, source, projectId: binding.project_id } satisfies ResponseProof;
});

/** Fresh command admission; repeat inside EventSink's SQLite write transaction. */
export const assertResponseCommand = Effect.fn("WorkbenchResponseGuard.assertResponseCommand")(
  function* (command: RespondCommand) {
    const sql = yield* SqlClient.SqlClient;
    const [owned] =
      yield* sql`SELECT 1 FROM workbench_work_items WHERE work_thread_id=${command.threadId}`;
    if (owned === undefined) return;
    const [row] =
      yield* sql<ResponseRow>`SELECT * FROM workbench_runtime_responses WHERE command_id=${command.commandId}`;
    if (row === undefined) {
      if (command.commandId.startsWith("workbench-response:")) return yield* fail("intent-missing");
      yield* assertNativeResponse(command, "pending");
      return;
    }
    if ((yield* encodeCommand(command)) !== row.request_json)
      return yield* fail("command-mismatch", row.id);
    const result = yield* proof(row, command.threadId);
    const s = result.source;
    const [current] = yield* sql`SELECT 1 FROM workbench_runtime_request_sources
      WHERE id=${s.id} AND version=${s.version} AND fingerprint=${s.fingerprint}`;
    if (current === undefined) return yield* fail("source-changed", row.id);
    const [request] = yield* sql<{
      status: string;
      payload_json: string;
    }>`SELECT status,payload_json
      FROM orchestration_v2_projection_runtime_requests WHERE thread_id=${command.threadId}
        AND runtime_request_id=${command.requestId} AND node_id=${s.node_id}`;
    if (request?.status !== "pending") return yield* fail("request-not-pending", row.id);
    if (request.payload_json !== s.request_json || command.requestId !== s.runtime_request_id)
      return yield* fail("source-changed", row.id);
    const [item] = yield* sql`SELECT 1 FROM orchestration_v2_projection_turn_items
      WHERE thread_id=${command.threadId} AND turn_item_id=${s.turn_item_id} AND node_id=${s.node_id}
        AND payload_json=${s.turn_item_json}`;
    if (item === undefined) return yield* fail("source-changed", row.id);
    if (!["live", "message"].includes(s.capability)) return yield* fail("not-resumable", row.id);
    if (
      s.capability === "message" &&
      (s.native_driver === null ||
        s.native_thread_id === null ||
        s.worktree_path === null ||
        s.worktree_real_path === null ||
        s.branch === null ||
        s.git_common_dir === null)
    )
      return yield* fail("not-resumable", row.id);
    return result;
  },
  Effect.mapError(normalizeFailure),
);

export const responseAdmission = Effect.fn("WorkbenchResponseGuard.responseAdmission")(function* (
  input: Admission,
) {
  const sql = yield* SqlClient.SqlClient;
  const [row] = yield* sql<ResponseRow>`SELECT x.* FROM workbench_runtime_responses x
      WHERE json_extract(x.source_json,'$.thread_id')=${input.threadId} AND (
        (${input.commandId ?? null} IS NOT NULL AND x.command_id=${input.commandId ?? null}) OR
        (${input.messageId ?? null} IS NOT NULL AND x.async_message_id=${input.messageId ?? null}) OR
        (${input.runId ?? null} IS NOT NULL AND x.run_id=${input.runId ?? null})) LIMIT 1`;
  if (row === undefined) return;
  return yield* proof(row, input.threadId);
}, Effect.mapError(normalizeFailure));

export const originalResponse = Effect.fn("WorkbenchResponseGuard.originalResponse")(function* (
  input: Admission,
) {
  const result = yield* responseAdmission(input);
  if (result === undefined || result.source.capability !== "message") return;
  const { source: s, response: r } = result;
  if (
    s.native_driver === null ||
    s.native_thread_id === null ||
    s.worktree_path === null ||
    s.worktree_real_path === null ||
    s.branch === null ||
    s.git_common_dir === null
  )
    return yield* fail("not-resumable", r.id);
  return {
    id: r.id,
    workItemId: r.work_item_id,
    attentionEpoch: r.attention_epoch,
    projectId: result.projectId,
    providerInstanceId: s.provider_instance_id,
    providerThreadId: s.provider_thread_id,
    nativeDriver: s.native_driver,
    nativeThreadId: s.native_thread_id,
    worktreePath: s.worktree_path,
    worktreeRealPath: s.worktree_real_path,
    branch: s.branch,
    gitCommonDir: s.git_common_dir,
    requestJson: r.request_json,
  } satisfies OriginalResume;
}, Effect.mapError(normalizeFailure));

/** Resolution precedes the outbox. Only this exact persisted answer may reach the live callback. */
export const assertResponseExecution = Effect.fn("WorkbenchResponseGuard.assertResponseExecution")(
  function* (input: {
    readonly threadId: ThreadId;
    readonly commandId?: string;
    readonly responseId?: string;
    readonly requestId: string;
    readonly providerSessionId: string;
    readonly decision?: string;
    readonly feedback?: string;
    readonly answers?: unknown;
  }) {
    const sql = yield* SqlClient.SqlClient;
    const [owned] =
      yield* sql`SELECT 1 FROM workbench_work_items WHERE work_thread_id=${input.threadId}`;
    if (owned === undefined) return;
    const [row] = yield* sql<ResponseRow>`SELECT * FROM workbench_runtime_responses
      WHERE command_id=${input.commandId ?? null} AND id=${input.responseId ?? null}`;
    if (row === undefined) {
      if (input.commandId?.startsWith("workbench-response:") || input.responseId !== undefined)
        return yield* fail("intent-missing");
      yield* assertNativeResponse(input, "resolved");
      const answer = yield* encodeJson(input.answers ?? null);
      const [matching] = yield* sql`SELECT 1 FROM orchestration_v2_projection_runtime_requests
        WHERE thread_id=${input.threadId} AND runtime_request_id=${input.requestId}
          AND json_extract(payload_json,'$.responseCapability.type')='live'
          AND json_extract(payload_json,'$.responseCapability.providerSessionId')=${input.providerSessionId}
          AND json_extract(payload_json,'$.decision') IS ${input.decision ?? null}
          AND json_extract(payload_json,'$.feedback') IS ${input.feedback ?? null}
          AND json(${answer})=json(COALESCE(json_extract(payload_json,'$.answers'),'null'))`;
      if (matching === undefined) return yield* fail("command-mismatch");
      return;
    }
    const { source } = yield* proof(row, input.threadId);
    const [acceptedEffect] = yield* sql`SELECT 1 FROM workbench_runtime_responses x
      JOIN orchestration_command_receipts c ON c.command_id=x.command_id AND c.aggregate_kind='thread' AND c.aggregate_id=${input.threadId}
      JOIN orchestration_v2_effect_outbox e ON e.effect_id=x.effect_id AND e.command_id=x.command_id AND e.thread_id=c.aggregate_id
      WHERE x.id=${row.id} AND x.state='submitted' AND x.submitted_sequence IS NOT NULL
        AND c.status='accepted' AND c.command_type='runtime-request.respond'
        AND e.status='running' AND e.effect_type='runtime-request.respond'
        AND e.attention_epoch=x.attention_epoch AND json_extract(e.payload_json,'$.responseId')=x.id`;
    if (acceptedEffect === undefined) return yield* fail("command-mismatch", row.id);
    const [matching] = yield* sql`SELECT 1 FROM orchestration_v2_projection_runtime_requests
      WHERE thread_id=${input.threadId} AND runtime_request_id=${input.requestId} AND status='resolved'
        AND node_id=${source.node_id} AND json_extract(payload_json,'$.responseCapability.type')='live'
        AND json_extract(payload_json,'$.responseCapability.providerSessionId')=${input.providerSessionId}
        AND json_extract(${row.request_json},'$.requestId')=${input.requestId}
        AND json_extract(${source.request_json},'$.responseCapability.providerSessionId')=${input.providerSessionId}
        AND json_extract(${row.request_json},'$.decision') IS ${input.decision ?? null}
        AND json_extract(${row.request_json},'$.feedback') IS ${input.feedback ?? null}
        AND json_extract(payload_json,'$.decision') IS ${input.decision ?? null}
        AND json_extract(payload_json,'$.feedback') IS ${input.feedback ?? null}
        AND COALESCE(json_extract(payload_json,'$.answers'),'null')=COALESCE(json_extract(${row.request_json},'$.answers'),'null')`;
    if (matching === undefined) return yield* fail("command-mismatch", row.id);
    const answersJson = yield* encodeJson(input.answers ?? null);
    const [answers] =
      yield* sql`SELECT 1 WHERE json(${answersJson})=json(COALESCE(json_extract(${row.request_json},'$.answers'),'null'))`;
    if (answers === undefined) return yield* fail("command-mismatch", row.id);
  },
  Effect.mapError(normalizeFailure),
);

/** Caller holds EventSink's command transaction; use events from this command, never a latest run. */
export const recordResponseCommit = Effect.fn("WorkbenchResponseGuard.recordResponseCommit")(
  function* (input: {
    readonly proof: ResponseProof;
    readonly events: ReadonlyArray<OrchestrationV2StoredEvent>;
    readonly effects: ReadonlyArray<PendingOrchestrationEffectV2>;
    readonly sequence: number;
  }) {
    const sql = yield* SqlClient.SqlClient;
    const r = input.proof.response;
    const message = input.events.find(
      ({ event }) => event.type === "message.updated" && event.payload.id === r.async_message_id,
    );
    const runId = message?.event.type === "message.updated" ? message.event.payload.runId : null;
    const run = input.events.findLast(
      ({ event }) =>
        (event.type === "run.created" || event.type === "run.updated") &&
        event.payload.id === runId,
    );
    const delegatedTaskId =
      run?.event.type === "run.updated" || run?.event.type === "run.created"
        ? run.event.payload.delegatedTaskId
        : null;
    const effect = input.effects.find(
      (e) =>
        e.commandId === r.command_id &&
        (e.request.type === "runtime-request.respond" ||
          ("runId" in e.request && e.request.runId === runId)),
    );
    const now = DateTime.formatIso(yield* DateTime.now);
    const dismissed = input.proof.source.capability === "message" && runId === null;
    yield* sql`UPDATE workbench_runtime_responses SET state=${dismissed ? "delivery_unconfirmed" : "submitted"},
      evidence_level='command_receipt',submitted_sequence=${input.sequence},run_id=${runId ?? null},
      delegated_task_id=${delegatedTaskId ?? null},effect_id=${effect?.id ?? null},
      async_message_id=${message === undefined ? null : r.async_message_id},
      reason=${dismissed ? "local-dismissal" : null},updated_at=${now}
      WHERE id=${r.id}`;
  },
  Effect.mapError(normalizeFailure),
);

/** Native UI answers retain their accepted source without impersonating a Workbench human response. */
export const recordNativeRepairAnswer = Effect.fn(
  "WorkbenchResponseGuard.recordNativeRepairAnswer",
)(function* (input: {
  readonly command: RespondCommand;
  readonly events: ReadonlyArray<OrchestrationV2StoredEvent>;
  readonly acceptedAt: DateTime.Utc;
}) {
  const sql = yield* SqlClient.SqlClient;
  const [row] = yield* sql<{
    generation_id: string;
    work_item_id: string;
    queue_id: string;
    claim_id: string;
    wake_generation: number;
    attention_epoch: number;
    source_run_id: string;
    repair_attempt_id: string | null;
  }>`SELECT g.id AS generation_id,w.id AS work_item_id,e.queue_id,e.claim_id,e.wake_generation,w.attention_epoch,
        n.run_id AS source_run_id,COALESCE(a.id,prior.repair_attempt_id) AS repair_attempt_id
      FROM workbench_work_items w JOIN workbench_repair_generations g ON g.work_item_id=w.id AND g.superseded_at IS NULL
      JOIN workbench_queue_entries e ON e.work_item_id=w.id AND e.queue_id=g.queue_id
      JOIN orchestration_v2_projection_runtime_requests q ON q.thread_id=w.work_thread_id AND q.runtime_request_id=${input.command.requestId} AND q.status='resolved'
      JOIN orchestration_v2_projection_nodes n ON n.thread_id=q.thread_id AND n.node_id=q.node_id
      LEFT JOIN workbench_repair_attempts a ON a.generation_id=g.id AND a.run_id=n.run_id AND a.charged=1 AND a.state IN ('accepted','running')
      LEFT JOIN workbench_repair_native_answers prior ON prior.generation_id=g.id AND prior.run_id=n.run_id
      WHERE w.work_thread_id=${input.command.threadId} AND w.attention_hold=0 AND e.state='occupied' AND e.claim_id IS NOT NULL`;
  if (row === undefined) return;
  const message = input.events.find(
    ({ event }) =>
      event.type === "message.updated" &&
      event.payload.id === `async-answer:${input.command.requestId}`,
  );
  const messageId = message?.event.type === "message.updated" ? message.event.payload.id : null;
  const runId = message?.event.type === "message.updated" ? message.event.payload.runId : null;
  yield* sql`INSERT OR IGNORE INTO workbench_repair_native_answers ${sql.insert({
    command_id: input.command.commandId,
    ...row,
    thread_id: input.command.threadId,
    request_id: input.command.requestId,
    message_id: messageId,
    run_id: runId,
    created_at: DateTime.formatIso(input.acceptedAt),
  })}`;
  yield* sql`UPDATE workbench_repair_generations SET phase=${row.repair_attempt_id === null ? "watching" : "repairing"},reason=NULL,attention_id=NULL,version=version+1
      WHERE id=${row.generation_id} AND (phase='succeeded' OR (phase='awaiting_user' AND reason IN ('user_input','approval')))`;
}, Effect.mapError(normalizeFailure));

/** Persist evidence before outbox retention can discard the transport record. No external IO. */
export const refreshResponseEvidence = Effect.fn("WorkbenchResponseGuard.refreshResponseEvidence")(
  function* (input: {
    readonly responseId?: string;
    readonly effectId?: string;
    readonly threadId?: ThreadId;
  }) {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<ResponseRow>`SELECT r.* FROM workbench_runtime_responses r
      WHERE (${input.responseId ?? null} IS NULL OR r.id=${input.responseId ?? null})
        AND (${input.effectId ?? null} IS NULL OR r.effect_id=${input.effectId ?? null})
        AND (${input.threadId ?? null} IS NULL OR json_extract(r.source_json,'$.thread_id')=${input.threadId ?? null})
        AND r.state NOT IN ('recorded','queued','rejected')`;
    for (const row of rows) {
      const s = yield* decodeSource(row.source_json);
      const [effect] =
        row.effect_id === null
          ? []
          : yield* sql<{ status: string; last_error: string | null; completed_at: string | null }>`
        SELECT status,last_error,completed_at FROM orchestration_v2_effect_outbox WHERE effect_id=${row.effect_id}`;
      const now = DateTime.formatIso(yield* DateTime.now);
      if (row.state === "delivered") {
        if (effect !== undefined)
          yield* sql`UPDATE workbench_runtime_responses SET effect_state=${effect.status},
          delivery_json=json_set(delivery_json,'$.effectState',${effect.status},'$.error',${effect.last_error}),updated_at=${now}
          WHERE id=${row.id}`;
        continue;
      }
      // run.running and attempt.nativeThreadId are written before startRootRun; only a native turn is proof.
      const [ack] =
        row.run_id === null
          ? []
          : yield* sql<{
              native_turn_ref: string;
            }>`SELECT json_extract(p.payload_json,'$.nativeTurnRef') AS native_turn_ref
        FROM orchestration_v2_projection_provider_turns p
        JOIN orchestration_v2_projection_run_attempts a ON a.attempt_id=p.run_attempt_id AND a.thread_id=p.thread_id
        JOIN orchestration_v2_projection_runs r ON r.run_id=a.run_id AND r.thread_id=p.thread_id
        JOIN orchestration_v2_projection_provider_threads pt ON pt.provider_thread_id=p.provider_thread_id AND pt.thread_id=p.thread_id
        WHERE p.thread_id=${s.thread_id} AND a.run_id=${row.run_id} AND p.provider_thread_id=${s.provider_thread_id}
          AND p.run_attempt_id=json_extract(r.payload_json,'$.activeAttemptId')
          AND json_extract(p.payload_json,'$.nativeTurnRef') IS NOT NULL
          AND json_extract(p.payload_json,'$.nativeTurnRef.strength')='strong'
          AND json_extract(a.payload_json,'$.nativeThreadId')=${s.native_thread_id}
          AND json_extract(pt.payload_json,'$.nativeThreadRef.driver')=${s.native_driver}
          AND json_extract(pt.payload_json,'$.nativeThreadRef.nativeId')=${s.native_thread_id}
          AND json_extract(p.payload_json,'$.nativeTurnRef.driver')=${s.native_driver}
          AND (json_extract(p.payload_json,'$.nativeThreadRef.nativeId') IS NULL OR json_extract(p.payload_json,'$.nativeThreadRef.nativeId')=${s.native_thread_id})
          AND p.status IN ('running','completed') LIMIT 1`;
      const state = ack
        ? "delivered"
        : effect?.status === "succeeded"
          ? s.capability === "live"
            ? "runtime_accepted"
            : "delivery_unconfirmed"
          : effect && ["failed", "cancelled"].includes(effect.status)
            ? "unknown"
            : row.state;
      const evidence = ack
        ? "provider_ack"
        : effect?.status === "succeeded"
          ? s.capability === "live"
            ? "runtime_handoff"
            : "unconfirmed"
          : state === "unknown"
            ? "unconfirmed"
            : row.evidence_level;
      const detail = yield* encodeJson({
        effectId: row.effect_id,
        effectState: effect?.status ?? row.effect_state,
        error: effect?.last_error ?? null,
        nativeTurnRef: ack?.native_turn_ref ?? null,
      });
      yield* sql`UPDATE workbench_runtime_responses SET state=${state},evidence_level=${evidence},
        effect_state=COALESCE(${effect?.status ?? null},effect_state),delivery_json=${detail},
        runtime_accepted_at=CASE WHEN ${s.capability === "live" && effect?.status === "succeeded"} THEN COALESCE(runtime_accepted_at,${effect?.completed_at ?? now}) ELSE runtime_accepted_at END,
        delivered_at=CASE WHEN ${ack !== undefined} THEN COALESCE(delivered_at,${now}) ELSE delivered_at END,
        reason=CASE WHEN ${state === "unknown"} THEN 'delivery-unknown' ELSE reason END,updated_at=${now}
        WHERE id=${row.id}`;
    }
  },
  Effect.mapError(normalizeFailure),
);
