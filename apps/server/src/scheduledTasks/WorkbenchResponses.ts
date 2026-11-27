import {
  CommandId,
  OrchestrationV2Command,
  RuntimeRequestId,
  ThreadId,
  WorkbenchQueueError,
  WorkbenchRuntimeRequestSummary,
  WorkbenchRuntimeResponseSummary,
  type WorkbenchAttentionQuery,
  type WorkbenchAttentionScope,
  WorkbenchRuntimeResponseInput,
  type WorkbenchRuntimeRequestsSnapshot,
  type WorkbenchRuntimeResponseReceipt,
  type OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Schedule from "effect/Schedule";
import * as SqlClient from "effect/sql/SqlClient";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as ResponseGuard from "../orchestration-v2/WorkbenchResponseGuard.ts";
import * as Engine from "../orchestration-v2/Orchestrator.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Git from "../vcs/GitVcsDriver.ts";
import * as Queue from "./WorkbenchQueue.ts";
import * as Items from "./WorkbenchWorkItems.ts";
import { digest } from "./workbenchLifecycle.ts";

type Scope = typeof WorkbenchAttentionScope.Type;
type Snapshot = Omit<WorkbenchRuntimeRequestsSnapshot, "environmentId">;
type Receipt = Omit<WorkbenchRuntimeResponseReceipt, "environmentId">;
type Source = {
  id: string;
  version: number;
  fingerprint: string;
  queue_id: string;
  work_item_id: string;
  cycle_number: number;
  batch_id: string | null;
  thread_id: string;
  source_run_id: string;
  source_attempt_id: string | null;
  node_id: string;
  provider_thread_id: string;
  provider_turn_id: string | null;
  runtime_request_id: string;
  provider_session_id: string | null;
  request_json: string;
  turn_item_id: string;
  turn_item_json: string;
  capability: string;
  provider_instance_id: string;
  model_selection_json: string;
  runtime_mode: string;
  interaction_mode: string;
  native_driver: string | null;
  native_thread_id: string | null;
  worktree_path: string | null;
  worktree_real_path: string | null;
  branch: string | null;
  git_common_dir: string | null;
  status: string;
  summary_json: string;
  created_at: string;
  updated_at: string;
};
type Response = {
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
  state: WorkbenchRuntimeResponseSummary["status"];
  evidence_level: WorkbenchRuntimeResponseSummary["delivery"]["evidence"];
  reason: WorkbenchRuntimeResponseSummary["blockedReason"];
  payload_sha: string;
  actor_json: string;
  native_principal_json: string;
  created_at: string;
  updated_at: string;
  delivered_at: string | null;
  runtime_accepted_at: string | null;
};
const isQueueError = Schema.is(WorkbenchQueueError);
const decodeRawSummary = Schema.decodeUnknownEffect(WorkbenchRuntimeRequestSummary);
const isPublicResponseReason = Schema.is(WorkbenchRuntimeResponseSummary.fields.blockedReason);
class Failure extends WorkbenchQueueError {
  static from(cause: unknown) {
    return isQueueError(cause)
      ? cause
      : Object.defineProperty(new Failure({ reason: "unavailable" }), "cause", { value: cause });
  }
}
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const decodeSummary = Schema.decodeEffect(Schema.fromJsonString(WorkbenchRuntimeRequestSummary));
const decodeActor = Schema.decodeEffect(
  Schema.fromJsonString(WorkbenchRuntimeResponseInput.fields.claimedActor),
);
const decodePrincipal = Schema.decodeEffect(
  Schema.fromJsonString(WorkbenchRuntimeResponseSummary.fields.nativePrincipal),
);
const encodeInput = Schema.encodeEffect(Schema.fromJsonString(WorkbenchRuntimeResponseInput));
const encodeCommand = Schema.encodeEffect(Schema.fromJsonString(OrchestrationV2Command));
const decodeCommand = Schema.decodeEffect(Schema.fromJsonString(OrchestrationV2Command));
const isResponseGuardError = Schema.is(ResponseGuard.WorkbenchResponseGuardError);
const responseRefusal = (cause: unknown) => {
  let current = cause;
  const seen = new Set<object>();
  for (let i = 0; i < 16 && typeof current === "object" && current !== null; i++) {
    if (seen.has(current)) break;
    seen.add(current);
    if (isResponseGuardError(current)) return current.reason;
    current = "cause" in current ? current.cause : undefined;
  }
  return undefined;
};
const summaryValid = Schema.is(WorkbenchRuntimeRequestSummary);
const bytes = (value: string) => new TextEncoder().encode(value).byteLength;

export class WorkbenchResponses extends Context.Service<
  WorkbenchResponses,
  {
    readonly list: (
      queueId: string,
      input: typeof WorkbenchAttentionQuery.Type,
    ) => Effect.Effect<Snapshot, WorkbenchQueueError>;
    readonly respond: (
      queueId: string,
      input: WorkbenchRuntimeResponseInput,
      principalId: string,
    ) => Effect.Effect<Receipt, WorkbenchQueueError>;
    readonly get: (
      queueId: string,
      responseId: string,
      input: Scope,
    ) => Effect.Effect<Receipt, WorkbenchQueueError>;
    readonly reconcile: () => Effect.Effect<void, WorkbenchQueueError>;
  }
>()("t3/scheduledTasks/WorkbenchResponses") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient,
    queue = yield* Queue.WorkbenchQueue,
    items = yield* Items.WorkbenchWorkItems;
  const threads = yield* Threads.ThreadManagementService,
    engine = yield* Engine.OrchestratorV2;
  const fs = yield* FileSystem.FileSystem,
    git = yield* Git.GitVcsDriver;
  const locks = yield* KeyedLock.make<string>();
  const now = DateTime.now.pipe(Effect.map(DateTime.formatIso));
  const authorize = Effect.fn("WorkbenchResponses.authorize")(function* (
    queueId: string,
    input: Scope,
  ) {
    const [row] =
      yield* sql`SELECT 1 FROM workbench_queues q,scheduled_task_execution_metadata m WHERE q.id=${queueId} AND q.controller_id=${input.controllerId} AND q.project_id=${input.projectId} AND m.singleton=1 AND m.tracking_id=${input.expectedTrackingId}`;
    if (!row) return yield* new WorkbenchQueueError({ reason: "invalid_binding" });
  });
  const identity = (queueId: string, input: Scope) => ({
    queueId,
    controllerId: input.controllerId,
    projectId: input.projectId,
    trackingId: input.expectedTrackingId,
  });
  const capture = Effect.fn("WorkbenchResponses.capture")(function* (
    queueId: string,
    w: Items.WorkItemRow,
    p: OrchestrationV2ThreadProjection,
    requestId: RuntimeRequestId,
    input: Scope,
  ) {
    const r = p.runtimeRequests.find((row) => row.id === requestId),
      n = p.nodes.find((row) => row.id === r?.nodeId),
      item = p.turnItems
        .filter(
          (row) =>
            (row.type === "user_input_request" || row.type === "approval_request") &&
            row.nodeId === r?.nodeId &&
            row.requestId === requestId,
        )
        .sort((a, b) => a.ordinal - b.ordinal)[0];
    if (!r || !n || n.runId === null || n.providerThreadId === null) return;
    const run = p.runs.find((row) => row.id === n.runId),
      provider = p.providerThreads.find((row) => row.id === n.providerThreadId);
    if (!run || !provider) return;
    const [batch] = yield* sql<{
      id: string;
    }>`SELECT id FROM workbench_work_batches WHERE work_item_id=${w.id} AND (run_id=${run.id} OR accepted_run_id=${run.id})
      UNION SELECT json_extract(source_json,'$.batch_id') AS id FROM workbench_runtime_responses WHERE work_item_id=${w.id} AND run_id=${run.id}
      UNION SELECT (SELECT b.id FROM workbench_work_batches b WHERE b.work_item_id=a.work_item_id AND (b.run_id=a.source_run_id OR b.accepted_run_id=a.source_run_id) LIMIT 1) AS id
      FROM workbench_repair_attempts a JOIN workbench_repair_generations g ON g.id=a.generation_id JOIN workbench_queue_entries e ON e.work_item_id=a.work_item_id AND e.queue_id=a.queue_id
      WHERE a.work_item_id=${w.id} AND a.run_id=${run.id} AND a.charged=1 AND a.state IN ('accepted','running') AND a.ordinal<=g.used AND g.superseded_at IS NULL
      AND g.work_item_id=a.work_item_id AND g.queue_id=a.queue_id AND (g.phase='repairing' OR (g.phase='awaiting_user' AND g.reason IN ('user_input','approval')))
      AND g.task_id=${w.task_id} AND g.task_created_at=${w.task_created_at} AND g.project_id=${w.project_id} AND g.dispatcher_thread_id=${w.dispatcher_thread_id}
      AND e.state='occupied' AND e.claim_id=a.claim_id AND e.wake_generation=a.wake_generation AND a.attention_epoch=${w.attention_epoch} LIMIT 1`;
    if (!batch && ![w.run_id, w.accepted_run_id].includes(run.id)) return;
    const [raw] = yield* sql<{
      payload_json: string;
    }>`SELECT payload_json FROM orchestration_v2_projection_runtime_requests WHERE thread_id=${p.thread.id} AND runtime_request_id=${r.id}`;
    const [rawItem] = item
      ? yield* sql<{
          payload_json: string;
        }>`SELECT payload_json FROM orchestration_v2_projection_turn_items WHERE thread_id=${p.thread.id} AND turn_item_id=${item.id}`
      : [];
    if (!raw) return;
    const questions =
      item?.type === "user_input_request"
        ? item.questions.map((q) => ({
            id: q.id,
            header: q.header,
            question: q.question,
            options: q.options.map((o) => ({
              label: o.label,
              description: o.description,
              value: o.value ?? o.label,
            })),
            multiSelect: q.multiSelect ?? false,
            allowCustomAnswer: q.allowCustomAnswer ?? true,
            required: q.required ?? true,
          }))
        : [];
    const approval =
      item?.type === "approval_request"
        ? {
            requestKind: item.requestKind,
            prompt: item.prompt ?? null,
            reason: item.reason ?? null,
            appName: item.appName ?? null,
            fullAccessOverride: item.fullAccessOverride ?? null,
            options: (item.options ?? []).map((o) => ({
              decision: o.decision,
              label: o.label,
              warning: o.warning ?? null,
            })),
          }
        : null;
    const capability =
      r.responseCapability.type === "live"
        ? {
            type: "live" as const,
            supportsFeedback: r.responseCapability.supportsFeedback ?? false,
          }
        : r.responseCapability;
    const stamp = yield* now;
    const id = digest(
      yield* encodeJson({
        queueId,
        workItemId: w.id,
        cycle: w.cycle_number,
        threadId: p.thread.id,
        requestId: r.id,
        nodeId: r.nodeId,
        runId: run.id,
        providerThreadId: provider.id,
        providerTurnId: r.providerTurnId,
        batchId: batch?.id ?? null,
      }),
    );
    const [prior] =
      yield* sql<Source>`SELECT * FROM workbench_runtime_request_sources WHERE id=${id}`;
    let blocked: WorkbenchRuntimeRequestSummary["blockedReason"] =
      r.status !== "pending"
        ? "not_pending"
        : r.responseCapability.type === "not_resumable"
          ? "not_resumable"
          : !item || (!questions.length && !approval?.options.length)
            ? "details_unavailable"
            : null;
    const proposed = {
      sourceIdentity: id,
      version: prior?.version ?? 1,
      workItemId: w.id,
      taskId: w.task_id,
      taskCreatedAt: w.task_created_at,
      dispatcherThreadId: w.dispatcher_thread_id,
      workThreadId: p.thread.id,
      requestId: r.id,
      sourceRunId: run.id,
      batchId: batch?.id ?? null,
      kind: r.kind === "user_input" ? "user_input" : "approval",
      requestStatus: r.status,
      responseCapability: capability,
      questions,
      approval,
      canRespond: blocked === null,
      blockedReason: blocked as WorkbenchRuntimeRequestSummary["blockedReason"],
      createdAt: DateTime.formatIso(r.createdAt),
      updatedAt: stamp,
    };
    if (
      bytes(raw.payload_json) + bytes(rawItem?.payload_json ?? "{}") > 96 * 1024 ||
      !summaryValid(proposed)
    ) {
      blocked = "details_unavailable";
      proposed.questions = [];
      proposed.approval = null;
      proposed.responseCapability =
        r.responseCapability.type === "not_resumable"
          ? {
              type: "not_resumable",
              reason: "This request must be handled in its original thread.",
            }
          : capability;
      proposed.canRespond = false;
      proposed.blockedReason = blocked;
    }
    const realPath =
      w.worktree_path !== null
        ? yield* fs.realPath(w.worktree_path).pipe(Effect.option)
        : undefined;
    const common =
      w.worktree_path !== null
        ? yield* git
            .execute({
              operation: "WorkbenchResponses.capture",
              cwd: w.worktree_path,
              args: ["rev-parse", "--path-format=absolute", "--git-common-dir"],
            })
            .pipe(
              Effect.map((v) => v.stdout.trim()),
              Effect.option,
            )
        : undefined;
    const proof = {
      queue_id: queueId,
      work_item_id: w.id,
      cycle_number: w.cycle_number,
      batch_id: batch?.id ?? null,
      thread_id: p.thread.id,
      source_run_id: run.id,
      source_attempt_id:
        r.providerTurnId === null
          ? run.activeAttemptId
          : (p.providerTurns.find(
              (turn) => turn.id === r.providerTurnId && turn.providerThreadId === provider.id,
            )?.runAttemptId ?? null),
      node_id: n.id,
      provider_thread_id: provider.id,
      provider_turn_id: r.providerTurnId,
      runtime_request_id: r.id,
      provider_session_id:
        r.responseCapability.type === "live"
          ? r.responseCapability.providerSessionId
          : provider.providerSessionId,
      request_json: raw.payload_json,
      turn_item_id: item?.id ?? "",
      turn_item_json: rawItem?.payload_json ?? "{}",
      capability: r.responseCapability.type,
      provider_instance_id: p.thread.providerInstanceId,
      model_selection_json: yield* encodeJson(p.thread.modelSelection),
      runtime_mode: p.thread.runtimeMode,
      interaction_mode: p.thread.interactionMode,
      native_driver: provider.nativeThreadRef?.driver ?? null,
      native_thread_id: provider.nativeThreadRef?.nativeId ?? null,
      worktree_path: w.worktree_path,
      worktree_real_path: realPath ? (realPath._tag === "Some" ? realPath.value : null) : null,
      branch: w.branch,
      git_common_dir: common ? (common._tag === "Some" ? common.value : null) : null,
      status: r.status,
    };
    if (
      r.responseCapability.type === "message" &&
      (proof.native_thread_id === null ||
        provider.nativeThreadRef?.strength !== "strong" ||
        proof.worktree_real_path === null ||
        proof.git_common_dir === null ||
        p.thread.worktreePath !== w.worktree_path ||
        p.thread.branch !== w.branch)
    ) {
      proposed.canRespond = false;
      proposed.blockedReason = "state_unknown";
    }
    const fingerprint = digest(yield* encodeJson(proof));
    proposed.version = prior ? prior.version + (prior.fingerprint === fingerprint ? 0 : 1) : 1;
    const summary = yield* decodeRawSummary(proposed);
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* authorize(queueId, input);
        const [owned] =
          yield* sql`SELECT 1 FROM workbench_work_items w JOIN workbench_queue_bindings b ON b.task_id=w.task_id AND b.task_created_at=w.task_created_at AND b.dispatcher_thread_id=w.dispatcher_thread_id AND b.project_id=w.project_id
          WHERE b.queue_id=${queueId} AND w.id=${w.id} AND w.cycle_number=${w.cycle_number} AND w.work_thread_id=${p.thread.id} AND w.task_id=${w.task_id} AND w.task_created_at=${w.task_created_at}`;
        if (!owned) return;
        const [current] = yield* sql<{
          version: number;
          fingerprint: string;
        }>`SELECT version,fingerprint FROM workbench_runtime_request_sources WHERE id=${id}`;
        if (
          current &&
          (current.version !== prior?.version || current.fingerprint !== prior?.fingerprint)
        )
          return;
        const row = {
          id,
          version: proposed.version,
          fingerprint,
          ...proof,
          summary_json: yield* encodeJson(summary),
          created_at: prior?.created_at ?? stamp,
          updated_at: stamp,
        };
        yield* sql`INSERT INTO workbench_runtime_request_sources ${sql.insert(row)} ON CONFLICT(id) DO UPDATE SET ${sql.update({ ...row, created_at: row.created_at })}`;
      }),
    );
  });
  const list = Effect.fn("WorkbenchResponses.list")(function* (
    queueId: string,
    input: typeof WorkbenchAttentionQuery.Type,
  ) {
    yield* authorize(queueId, input);
    const work =
      yield* sql<Items.WorkItemRow>`SELECT w.* FROM workbench_work_items w JOIN workbench_queue_bindings b ON b.task_id=w.task_id AND b.task_created_at=w.task_created_at AND b.dispatcher_thread_id=w.dispatcher_thread_id AND b.project_id=w.project_id WHERE b.queue_id=${queueId} AND w.work_thread_id IS NOT NULL`;
    for (const w of work) {
      const binding = yield* items.binding(w.task_id as never).pipe(Effect.option);
      if (
        binding._tag === "None" ||
        binding.value.createdAt !== w.task_created_at ||
        binding.value.threadId !== w.dispatcher_thread_id ||
        binding.value.projectId !== input.projectId
      )
        continue;
      const projection = yield* threads.getThreadProjection(ThreadId.make(w.work_thread_id!));
      if (projection.thread.projectId !== w.project_id) continue;
      for (const r of projection.runtimeRequests)
        if (
          r.kind === "user_input" ||
          ["command", "file-read", "file-change", "permission", "mcp-elicitation"].includes(r.kind)
        )
          yield* capture(queueId, w, projection, r.id, input);
    }
    yield* authorize(queueId, input);
    const rows =
      yield* sql<Source>`SELECT * FROM workbench_runtime_request_sources WHERE queue_id=${queueId} AND id>${input.cursor ?? ""} ORDER BY id LIMIT 101`;
    const requests: WorkbenchRuntimeRequestSummary[] = [];
    let total = 0;
    for (const row of rows) {
      if (requests.length === 100 || total + bytes(row.summary_json) > 2 * 1024 * 1024) break;
      const value = yield* decodeSummary(row.summary_json);
      const recorded =
        yield* sql`SELECT 1 FROM workbench_runtime_responses WHERE source_id=${row.id} AND (state!='rejected' OR source_version>=${row.version})`;
      const [actual] = yield* sql<{
        status: WorkbenchRuntimeRequestSummary["requestStatus"];
        payload_json: string;
      }>`SELECT status,payload_json FROM orchestration_v2_projection_runtime_requests WHERE thread_id=${row.thread_id} AND runtime_request_id=${row.runtime_request_id}`;
      requests.push(
        recorded.length
          ? { ...value, canRespond: false, blockedReason: "response_recorded" }
          : actual === undefined
            ? { ...value, canRespond: false, blockedReason: "state_unknown" }
            : actual.status !== "pending"
              ? {
                  ...value,
                  requestStatus: actual.status,
                  canRespond: false,
                  blockedReason: "not_pending",
                }
              : actual.payload_json !== row.request_json
                ? { ...value, canRespond: false, blockedReason: "state_unknown" }
                : value,
      );
      total += bytes(row.summary_json);
    }
    yield* authorize(queueId, input);
    return {
      ...identity(queueId, input),
      requests,
      nextCursor: rows.length > requests.length ? (requests.at(-1)?.sourceIdentity ?? null) : null,
    };
  }, Effect.mapError(Failure.from));
  const get = Effect.fn("WorkbenchResponses.get")(function* (
    queueId: string,
    responseId: string,
    input: Scope,
  ) {
    yield* authorize(queueId, input);
    const [owned] =
      yield* sql<Response>`SELECT * FROM workbench_runtime_responses WHERE queue_id=${queueId} AND id=${responseId}`;
    if (!owned) return yield* new WorkbenchQueueError({ reason: "not_found" });
    yield* ResponseGuard.refreshResponseEvidence({ responseId }).pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
    );
    const [r] =
      yield* sql<Response>`SELECT * FROM workbench_runtime_responses WHERE queue_id=${queueId} AND id=${responseId}`;
    if (!r) return yield* new WorkbenchQueueError({ reason: "not_found" });
    const [s] =
      yield* sql<Source>`SELECT * FROM workbench_runtime_request_sources WHERE id=${r.source_id}`;
    if (!s) return yield* new WorkbenchQueueError({ reason: "unavailable" });
    yield* authorize(queueId, input);
    return {
      ...identity(queueId, input),
      response: {
        id: r.id,
        sourceIdentity: r.source_id,
        sourceVersion: r.source_version,
        workItemId: r.work_item_id,
        requestId: RuntimeRequestId.make(s.runtime_request_id),
        status: r.state,
        blockedReason: isPublicResponseReason(r.reason)
          ? r.reason
          : r.reason === null
            ? null
            : "state_unknown",
        recordedAt: r.created_at,
        updatedAt: r.updated_at,
        delivery: { evidence: r.evidence_level, confirmedAt: r.delivered_at },
        claimedActor: yield* decodeActor(r.actor_json),
        nativePrincipal: yield* decodePrincipal(r.native_principal_json),
        answerMessageId: r.async_message_id,
        answerRunId: r.run_id as never,
      },
    } satisfies Receipt;
  }, Effect.mapError(Failure.from));
  const forward = Effect.fn("WorkbenchResponses.forward")(
    function* (id: string) {
      yield* locks.withLock(
        id,
        Effect.gen(function* () {
          const [r] =
            yield* sql<Response>`SELECT * FROM workbench_runtime_responses WHERE id=${id}`;
          if (!r || !["recorded", "queued"].includes(r.state)) return;
          const [s] =
            yield* sql<Source>`SELECT * FROM workbench_runtime_request_sources WHERE id=${r.source_id}`;
          if (!s) return;
          const command = yield* decodeCommand(r.request_json);
          const q = yield* sql<{
            controller_id: string;
            project_id: string;
          }>`SELECT controller_id,project_id FROM workbench_queues WHERE id=${r.queue_id}`;
          if (!q[0]) return;
          yield* queue.reconcile(r.queue_id, {
            controllerId: q[0].controller_id,
            projectId: q[0].project_id as never,
          });
          const claimed = yield* sql.withTransaction(
            Effect.gen(function* () {
              const [entry] = yield* sql<{
                state: string;
                claim_id: string | null;
                attention_epoch: number;
              }>`SELECT e.state,e.claim_id,w.attention_epoch FROM workbench_queue_entries e JOIN workbench_work_items w ON w.id=e.work_item_id WHERE e.work_item_id=${r.work_item_id} AND e.queue_id=${r.queue_id}`;
              if (!entry || entry.state !== "occupied" || entry.claim_id === null) {
                yield* sql`UPDATE workbench_runtime_responses SET state='queued',reason='queue_full',updated_at=${yield* now} WHERE id=${r.id} AND state IN ('recorded','queued')`;
                return false;
              }
              yield* sql`UPDATE workbench_runtime_responses SET state='queued',claim_id=${entry.claim_id},reason=NULL,updated_at=${yield* now} WHERE id=${r.id} AND state IN ('recorded','queued')`;
              return true;
            }),
          );
          if (!claimed) return;
          const result = yield* engine.dispatch(command).pipe(Effect.result);
          if (result._tag === "Failure") {
            const refusal = responseRefusal(result.failure);
            const rejected =
              refusal !== undefined && !["unavailable", "claim-stale"].includes(refusal);
            const reason =
              refusal === "request-not-pending"
                ? "not_pending"
                : refusal === "not-resumable"
                  ? "not_resumable"
                  : refusal === "context-mismatch"
                    ? "provider_context_unavailable"
                    : rejected
                      ? "source_changed"
                      : "state_unknown";
            yield* sql`UPDATE workbench_runtime_responses SET state=${rejected ? "rejected" : "unknown"},reason=${reason},updated_at=${yield* now} WHERE id=${r.id} AND state IN ('recorded','queued') AND NOT EXISTS(SELECT 1 FROM orchestration_command_receipts c WHERE c.command_id=${r.command_id} AND c.status='accepted')`;
            if (rejected)
              yield* sql`UPDATE workbench_runtime_request_sources SET version=version+1 WHERE id=${r.source_id} AND version=${r.source_version}`;
          }
        }),
      );
    },
    (effect, id) =>
      effect.pipe(
        Effect.tapCause(() =>
          Effect.gen(function* () {
            yield* sql`UPDATE workbench_runtime_responses SET state='unknown',reason='state_unknown',updated_at=${yield* now} WHERE id=${id} AND state IN ('recorded','queued')`;
          }),
        ),
      ),
    Effect.mapError(Failure.from),
  );
  const respond = Effect.fn("WorkbenchResponses.respond")(function* (
    queueId: string,
    input: WorkbenchRuntimeResponseInput,
    principalId: string,
  ) {
    yield* authorize(queueId, input);
    const payload = yield* encodeInput({
      ...input,
      response:
        input.response.type === "answers"
          ? {
              ...input.response,
              answers: Object.fromEntries(
                Object.entries(input.response.answers).sort(([a], [b]) => a.localeCompare(b)),
              ),
            }
          : input.response,
    });
    if (bytes(payload) > 128 * 1024)
      return yield* new WorkbenchQueueError({ reason: "stale_state" });
    const sha = digest(payload),
      [prior] =
        yield* sql<Response>`SELECT * FROM workbench_runtime_responses WHERE id=${input.responseId}`;
    if (prior) {
      if (prior.queue_id !== queueId || prior.payload_sha !== sha)
        return yield* new WorkbenchQueueError({ reason: "stale_state" });
      return yield* get(queueId, input.responseId, input);
    }
    const [s] =
      yield* sql<Source>`SELECT * FROM workbench_runtime_request_sources WHERE id=${input.sourceIdentity} AND queue_id=${queueId}`;
    if (!s || s.version !== input.expectedVersion)
      return yield* new WorkbenchQueueError({ reason: "stale_state" });
    const summary = yield* decodeSummary(s.summary_json);
    if (!summary.canRespond || s.status !== "pending")
      return yield* new WorkbenchQueueError({ reason: "stale_state" });
    if (input.response.type === "answers") {
      if (summary.kind !== "user_input")
        return yield* new WorkbenchQueueError({ reason: "stale_state" });
      for (const key of Object.keys(input.response.answers))
        if (!summary.questions.some((q) => q.id === key))
          return yield* new WorkbenchQueueError({ reason: "stale_state" });
      for (const q of summary.questions) {
        const answers = input.response.answers[q.id] ?? [];
        if (
          (q.required && !answers.some((v) => v.trim())) ||
          (!q.multiSelect && answers.length > 1) ||
          (!q.allowCustomAnswer && answers.some((a) => !q.options.some((o) => o.value === a)))
        )
          return yield* new WorkbenchQueueError({ reason: "stale_state" });
      }
    } else {
      if (
        summary.kind !== "approval" ||
        !summary.approval?.options.some(
          (o) => input.response.type === "decision" && o.decision === input.response.decision,
        )
      )
        return yield* new WorkbenchQueueError({ reason: "stale_state" });
      if (
        input.response.feedback !== undefined &&
        (summary.responseCapability.type !== "live" ||
          !summary.responseCapability.supportsFeedback ||
          input.response.decision !== "decline" ||
          !input.response.feedback.trim())
      )
        return yield* new WorkbenchQueueError({ reason: "stale_state" });
    }
    const command = {
      type: "runtime-request.respond" as const,
      commandId: CommandId.make(`workbench-response:${digest(`${queueId}:${input.responseId}`)}`),
      threadId: ThreadId.make(s.thread_id),
      requestId: RuntimeRequestId.make(s.runtime_request_id),
      ...(input.response.type === "answers"
        ? { answers: input.response.answers }
        : {
            decision: input.response.decision,
            ...(input.response.feedback !== undefined ? { feedback: input.response.feedback } : {}),
          }),
    };
    const commandJson = yield* encodeCommand(command),
      stamp = yield* now;
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* authorize(queueId, input);
        const [current] =
          yield* sql<Source>`SELECT * FROM workbench_runtime_request_sources WHERE id=${s.id} AND version=${s.version} AND fingerprint=${s.fingerprint}`;
        const [pending] =
          yield* sql`SELECT 1 FROM orchestration_v2_projection_runtime_requests WHERE thread_id=${s.thread_id} AND runtime_request_id=${s.runtime_request_id} AND status='pending' AND payload_json=${s.request_json}`;
        const [existing] =
          yield* sql<Response>`SELECT * FROM workbench_runtime_responses WHERE id=${input.responseId} OR (source_id=${s.id} AND (state!='rejected' OR source_version>=${s.version}))`;
        if (existing) {
          if (
            existing.id === input.responseId &&
            existing.queue_id === queueId &&
            existing.payload_sha === sha
          )
            return;
          return yield* new WorkbenchQueueError({ reason: "stale_state" });
        }
        const [item] =
          yield* sql`SELECT 1 FROM orchestration_v2_projection_turn_items WHERE thread_id=${s.thread_id} AND turn_item_id=${s.turn_item_id} AND payload_json=${s.turn_item_json}`;
        if (!current || !pending || !item)
          return yield* new WorkbenchQueueError({ reason: "stale_state" });
        const [w] = yield* sql<{
          attention_epoch: number;
        }>`SELECT attention_epoch FROM workbench_work_items WHERE id=${s.work_item_id}`;
        yield* sql`INSERT INTO workbench_runtime_responses ${sql.insert({ id: input.responseId, source_id: s.id, source_version: s.version, source_json: yield* encodeJson(s), queue_id: queueId, work_item_id: s.work_item_id, attention_epoch: w!.attention_epoch, claim_id: null, command_id: command.commandId, request_json: commandJson, async_message_id: s.capability === "message" ? `async-answer:${s.runtime_request_id}` : null, state: "recorded", payload_sha: sha, actor_json: yield* encodeJson(input.claimedActor), native_principal_json: yield* encodeJson({ kind: "environment_session", id: principalId }), created_at: stamp, updated_at: stamp })}`;
        yield* sql`UPDATE workbench_work_items SET version=version+1 WHERE id=${s.work_item_id}`;
        yield* sql`UPDATE workbench_queue_entries SET state=CASE WHEN state='occupied' THEN state ELSE 'queued' END,claim_id=CASE WHEN state='occupied' THEN claim_id ELSE NULL END,wake_generation=CASE WHEN state='occupied' THEN wake_generation ELSE wake_generation+1 END,wake_sent=1,needs_dispatch=0,blocked_reason='attention',updated_at=${stamp} WHERE work_item_id=${s.work_item_id}`;
      }),
    );
    yield* forward(input.responseId).pipe(Effect.ignoreCause());
    return yield* get(queueId, input.responseId, input).pipe(
      Effect.mapError((cause) =>
        Object.defineProperty(new Failure({ reason: "unavailable" }), "cause", { value: cause }),
      ),
    );
  }, Effect.mapError(Failure.from));
  const reconcile = Effect.fn("WorkbenchResponses.reconcile")(function* () {
    const rows = yield* sql<{
      id: string;
    }>`SELECT id FROM workbench_runtime_responses WHERE state IN ('recorded','queued') ORDER BY created_at LIMIT 100`;
    for (const row of rows) yield* forward(row.id).pipe(Effect.ignoreCause());
    const evidence = yield* sql<{
      id: string;
    }>`SELECT id FROM workbench_runtime_responses WHERE state NOT IN ('recorded','queued','rejected','delivered') ORDER BY updated_at LIMIT 100`;
    for (const row of evidence)
      yield* ResponseGuard.refreshResponseEvidence({ responseId: row.id }).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
      );
  }, Effect.mapError(Failure.from));
  yield* reconcile().pipe(
    Effect.ignoreCause(),
    Effect.repeat(Schedule.spaced("2 seconds")),
    Effect.forkScoped,
  );
  return WorkbenchResponses.of({ list, respond, get, reconcile });
});
export const layer = Layer.effect(WorkbenchResponses, make);
