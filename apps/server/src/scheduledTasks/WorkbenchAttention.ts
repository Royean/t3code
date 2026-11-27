import {
  CheckpointRef,
  CommandId,
  MessageId,
  RunId,
  ThreadId,
  WorkbenchQueueError,
  WorkbenchAttentionQuery,
  WorkbenchAttentionScope,
  WorkbenchAttentionSummary,
  OrchestrationV2ThreadProjectionJson,
  OrchestrationV2Command,
  ModelSelection,
  RuntimeMode,
  ProviderInteractionMode,
  type WorkbenchAttentionPauseInput,
  type WorkbenchAttentionRecordInput,
  WorkbenchAttentionRequeueInput,
  type WorkbenchAttentionMutationResult,
  type WorkbenchAttentionSnapshot,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/sql/SqlClient";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as Checkpoints from "../checkpointing/CheckpointStore.ts";
import {
  attachRepairAttention,
  recordRepairAttention,
  requeueRepairGeneration,
} from "./workbenchRepairHuman.ts";
import * as AttentionGuard from "../orchestration-v2/WorkbenchAttentionGuard.ts";
import * as Engine from "../orchestration-v2/Orchestrator.ts";
import * as Outbox from "../orchestration-v2/EffectOutbox.ts";
import * as Execution from "../orchestration-v2/RunExecutionService.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as Sessions from "../orchestration-v2/ProviderSessionManager.ts";
import * as Git from "../vcs/GitVcsDriver.ts";
import * as Queue from "./WorkbenchQueue.ts";
import * as Items from "./WorkbenchWorkItems.ts";
import { digest } from "./workbenchLifecycle.ts";
import { renderAttentionResumeBrief } from "./workbenchAttentionBrief.ts";

type AttentionScope = typeof WorkbenchAttentionScope.Type;
type Mutation = Omit<WorkbenchAttentionMutationResult, "environmentId">;
type Summary = WorkbenchAttentionSummary;
type Phase = Summary["phase"];
type Reason = NonNullable<Summary["blockedReason"]>;
type AttentionRow = {
  id: string;
  queue_id: string;
  work_item_id: string;
  repair_generation_id: string | null;
  version: number;
  phase: Phase;
  reason_json: string;
  pre_pause_json: string;
  observation_only: number;
  runtime_request_id: string | null;
  result_text: string | null;
  recorded_at: string | null;
  priority: number;
  blocked_reason: Reason | null;
  checkpoint_status: Summary["checkpoint"]["status"];
  captured_at: string | null;
  source_run_id: string | null;
  point_id: string | null;
  resume_point_id: string | null;
  resume_run_id: string | null;
  pause_command_id: string;
  pause_claim_id: string | null;
  pause_wake_generation: number;
  epoch: number;
  created_at: string;
  updated_at: string;
};
type Operation = {
  client_request_id: string;
  attention_id: string;
  kind: "pause" | "record" | "requeue";
  payload_sha: string;
  payload_json: string;
  state: "pending" | "applied" | "blocked" | "unknown";
};
const Manifest = Schema.Struct({
  workItemId: Schema.String,
  threadId: ThreadId,
  projectId: Schema.String,
  path: Schema.String,
  realPath: Schema.String,
  branch: Schema.String,
  gitCommonDir: Schema.String,
  headSha: Schema.String,
  checkpointRef: Schema.String,
  checkpointSha: Schema.String,
  providerInstanceId: Schema.String,
  providerThreadId: Schema.String,
  providerSessionId: Schema.String,
  nativeDriver: Schema.String,
  nativeThreadId: Schema.String,
  sourceRunId: RunId,
  modelSelection: ModelSelection,
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode,
  progress: Schema.Unknown,
});
type Manifest = typeof Manifest.Type;
const encodeManifest = Schema.encodeEffect(Schema.fromJsonString(Manifest));
const decodeManifest = Schema.decodeEffect(Schema.fromJsonString(Manifest));
const encodeProjection = Schema.encodeEffect(OrchestrationV2ThreadProjectionJson);
const decodeProjection = Schema.decodeEffect(
  Schema.fromJsonString(OrchestrationV2ThreadProjectionJson),
);
const decodeRequeue = Schema.decodeEffect(Schema.fromJsonString(WorkbenchAttentionRequeueInput));
const encodeCommand = Schema.encodeEffect(Schema.fromJsonString(OrchestrationV2Command));
const decodeCommand = Schema.decodeEffect(Schema.fromJsonString(OrchestrationV2Command));
const decodeReason = Schema.decodeEffect(
  Schema.fromJsonString(WorkbenchAttentionSummary.fields.reason),
);
class Blocked extends Schema.TaggedError<Blocked>()("WorkbenchAttentionBlocked", {
  reason: Schema.Literals([
    "state_unknown",
    "not_quiescent",
    "checkpoint_failed",
    "workspace_unavailable",
    "workspace_changed",
    "provider_context_unavailable",
    "pending_runtime_request",
    "stale_state",
  ]),
}) {}
const isBlocked = Schema.is(Blocked);
const isQueueError = Schema.is(WorkbenchQueueError);
const isAttentionGuardError = Schema.is(AttentionGuard.WorkbenchAttentionGuardError);
const pendingRuntimeRequest = (cause: unknown) => {
  const seen = new Set<object>();
  let current = cause;
  for (let depth = 0; depth < 16 && typeof current === "object" && current !== null; depth++) {
    if (seen.has(current)) break;
    seen.add(current);
    if (
      isAttentionGuardError(current) &&
      current.reason === "runtime-request-pending" &&
      current.runtimeRequestId !== undefined
    )
      return current;
    current = "cause" in current ? current.cause : undefined;
  }
  return undefined;
};
const decodeJson = Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown));
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const encodeReason = Schema.encodeEffect(
  Schema.fromJsonString(WorkbenchAttentionSummary.fields.reason),
);
class Failure extends WorkbenchQueueError {
  static from(cause: unknown) {
    return Object.defineProperty(new Failure({ reason: "unavailable" }), "cause", { value: cause });
  }
}
const stale = () => new WorkbenchQueueError({ reason: "stale_state" });

export class WorkbenchAttention extends Context.Service<
  WorkbenchAttention,
  {
    readonly get: (
      queueId: string,
      scope: typeof WorkbenchAttentionQuery.Type,
    ) => Effect.Effect<Omit<WorkbenchAttentionSnapshot, "environmentId">, WorkbenchQueueError>;
    readonly pause: (
      queueId: string,
      workItemId: string,
      input: WorkbenchAttentionPauseInput,
    ) => Effect.Effect<Mutation, WorkbenchQueueError>;
    readonly pauseSystem: (
      workItemId: string,
      generationId: string,
    ) => Effect.Effect<void, WorkbenchQueueError>;
    readonly record: (
      queueId: string,
      id: string,
      input: WorkbenchAttentionRecordInput,
      principalId?: string,
    ) => Effect.Effect<Mutation, WorkbenchQueueError>;
    readonly requeue: (
      queueId: string,
      id: string,
      input: WorkbenchAttentionRequeueInput,
      principalId?: string,
    ) => Effect.Effect<Mutation, WorkbenchQueueError>;
    readonly reconcile: (
      queueId: string,
      scope: AttentionScope,
    ) => Effect.Effect<void, WorkbenchQueueError>;
    readonly resumeClaim: (
      workItemId: string,
      claimId: string,
    ) => Effect.Effect<void, WorkbenchQueueError>;
  }
>()("t3/scheduledTasks/WorkbenchAttention") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient,
    queue = yield* Queue.WorkbenchQueue,
    items = yield* Items.WorkbenchWorkItems;
  const engine = yield* Engine.OrchestratorV2,
    threads = yield* Threads.ThreadManagementService;
  const guard = yield* AttentionGuard.WorkbenchAttentionGuard,
    execution = yield* Execution.RunExecutionServiceV2;
  const checkpoints = yield* Checkpoints.CheckpointStore,
    git = yield* Git.GitVcsDriver;
  const outbox = yield* Outbox.EffectOutboxV2,
    fs = yield* FileSystem.FileSystem,
    path = yield* Path.Path,
    scope = yield* Scope.Scope;
  const sessions = yield* Effect.serviceOption(Sessions.ProviderSessionManagerV2);
  const locks = yield* KeyedLock.make<string>();
  const active = new Set<string>();
  const now = DateTime.now.pipe(Effect.map(DateTime.formatIso));
  const authorize = Effect.fn("WorkbenchAttention.authorize")(function* (
    queueId: string,
    input: AttentionScope,
  ) {
    const [row] =
      yield* sql`SELECT 1 FROM workbench_queues q,scheduled_task_execution_metadata m WHERE q.id=${queueId} AND q.controller_id=${input.controllerId} AND q.project_id=${input.projectId} AND m.singleton=1 AND m.tracking_id=${input.expectedTrackingId}`;
    if (!row) return yield* new WorkbenchQueueError({ reason: "invalid_binding" });
  });
  const workItem = Effect.fn("WorkbenchAttention.workItem")(function* (
    queueId: string,
    id: string,
  ) {
    const [row] =
      yield* sql<Items.WorkItemRow>`SELECT w.* FROM workbench_work_items w JOIN workbench_queue_entries e ON e.work_item_id=w.id
   JOIN workbench_queue_bindings b ON b.queue_id=e.queue_id AND b.task_id=w.task_id AND b.task_created_at=w.task_created_at AND b.dispatcher_thread_id=w.dispatcher_thread_id AND b.project_id=w.project_id
   WHERE e.queue_id=${queueId} AND w.id=${id}`;
    if (!row) return yield* new WorkbenchQueueError({ reason: "not_found" });
    const task = yield* items.binding(row.task_id as never);
    if (
      task.createdAt !== row.task_created_at ||
      task.projectId !== row.project_id ||
      task.threadId !== row.dispatcher_thread_id
    )
      return yield* new WorkbenchQueueError({ reason: "invalid_binding" });
    return row;
  });
  const read = Effect.fn("WorkbenchAttention.read")(function* (queueId: string, id: string) {
    const [row] =
      yield* sql<AttentionRow>`SELECT * FROM workbench_attention_items WHERE queue_id=${queueId} AND id=${id}`;
    if (!row) return yield* new WorkbenchQueueError({ reason: "not_found" });
    return row;
  });
  const summary = Effect.fn("WorkbenchAttention.summary")(function* (row: AttentionRow) {
    const w = yield* workItem(row.queue_id, row.work_item_id);
    const [entry] = yield* sql<{
      state: string;
    }>`SELECT state FROM workbench_queue_entries WHERE work_item_id=${w.id}`;
    return {
      id: row.id,
      workItemId: w.id,
      repairGenerationId: row.repair_generation_id,
      taskId: w.task_id as never,
      taskCreatedAt: w.task_created_at,
      version: row.version,
      phase: row.phase,
      reason: yield* decodeReason(row.reason_json),
      priority: row.priority,
      slotState:
        entry?.state === "occupied"
          ? ("occupied" as const)
          : entry?.state === "queued"
            ? ("queued" as const)
            : ("released" as const),
      checkpoint: { status: row.checkpoint_status, capturedAt: row.captured_at },
      resolution: { summary: row.result_text?.slice(0, 1024) ?? null, recordedAt: row.recorded_at },
      canRequeue:
        row.phase === "awaiting_user" &&
        row.checkpoint_status === "ready" &&
        row.result_text !== null &&
        entry?.state === "paused",
      blockedReason: row.blocked_reason,
      dispatcherThreadId: ThreadId.make(w.dispatcher_thread_id),
      workThreadId: w.work_thread_id === null ? null : ThreadId.make(w.work_thread_id),
      sourceRunId: row.source_run_id === null ? null : RunId.make(row.source_run_id),
      resumeRunId: row.resume_run_id === null ? null : RunId.make(row.resume_run_id),
      createdAt: row.created_at,
      updatedAt: row.updated_at,
    } satisfies Summary;
  });
  const operation = Effect.fn("WorkbenchAttention.operation")(function* (
    queueId: string,
    input: { clientRequestId: string },
    kind: Operation["kind"],
    target: string,
  ) {
    const sha = digest(yield* encodeJson({ kind, target, input }));
    const [prior] =
      yield* sql<Operation>`SELECT * FROM workbench_attention_operations WHERE queue_id=${queueId} AND client_request_id=${input.clientRequestId}`;
    if (prior && (prior.payload_sha !== sha || prior.kind !== kind)) return yield* stale();
    return { sha, prior };
  });
  const result = Effect.fn("WorkbenchAttention.result")(function* (
    queueId: string,
    input: AttentionScope,
    clientRequestId: string,
  ) {
    yield* authorize(queueId, input);
    const [op] =
      yield* sql<Operation>`SELECT * FROM workbench_attention_operations WHERE queue_id=${queueId} AND client_request_id=${clientRequestId}`;
    if (!op) return yield* new WorkbenchQueueError({ reason: "not_found" });
    return {
      trackingId: input.expectedTrackingId,
      queueId,
      controllerId: input.controllerId,
      projectId: input.projectId,
      attention: yield* summary(yield* read(queueId, op.attention_id)),
      operation: { clientRequestId, kind: op.kind, state: op.state },
      queue: yield* queue.get(queueId, input),
    } satisfies Mutation;
  });
  const block = Effect.fn("WorkbenchAttention.block")(function* (a: AttentionRow, reason: Reason) {
    const stamp = yield* now;
    yield* sql`UPDATE workbench_attention_items SET phase='blocked',blocked_reason=${reason},version=version+1,updated_at=${stamp},checkpoint_status=CASE WHEN ${reason}='checkpoint_failed' THEN 'error' ELSE checkpoint_status END WHERE id=${a.id} AND epoch=${a.epoch} AND phase!='resolved'`;
    yield* sql`UPDATE workbench_attention_operations SET state='blocked',updated_at=${stamp} WHERE attention_id=${a.id} AND state='pending'`;
  });
  const preservePendingRequest = Effect.fn("WorkbenchAttention.preservePendingRequest")(function* (
    id: string,
    rejected: AttentionGuard.WorkbenchAttentionGuardError,
  ) {
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const [a] =
          yield* sql<AttentionRow>`SELECT * FROM workbench_attention_items WHERE id=${id}`;
        if (
          !a ||
          a.observation_only !== 0 ||
          !["pausing", "blocked"].includes(a.phase) ||
          a.resume_point_id !== null
        )
          return false;
        const accepted =
          yield* sql`SELECT 1 FROM orchestration_command_receipts WHERE command_id=${a.pause_command_id} AND status='accepted'`;
        // A replayed accepted Stop cannot be reinterpreted as a pause that never stopped execution.
        if (accepted.length) return false;
        const [request] = yield* sql<{
          kind: string;
          status: string;
          run_id: string | null;
          payload_json: string;
        }>`SELECT r.kind,r.status,n.run_id,r.payload_json FROM orchestration_v2_projection_runtime_requests r
          LEFT JOIN orchestration_v2_projection_nodes n ON n.node_id=r.node_id AND n.thread_id=r.thread_id
          WHERE r.thread_id=${rejected.threadId} AND r.runtime_request_id=${rejected.runtimeRequestId!}`;
        if (!request || !["pending", "resolved"].includes(request.status)) return false;
        const [retained] =
          yield* sql`UPDATE workbench_work_items SET attention_hold=0,version=version+1,
          recovery_ref=CASE WHEN recovery_ref=${a.id} THEN NULL ELSE recovery_ref END
          WHERE id=${a.work_item_id} AND work_thread_id=${rejected.threadId} AND attention_epoch=${a.epoch} AND attention_hold=1
          AND EXISTS(SELECT 1 FROM workbench_queue_entries e WHERE e.work_item_id=${a.work_item_id} AND e.queue_id=${a.queue_id}
            AND e.state='occupied' AND e.claim_id=${a.pause_claim_id}) RETURNING id`;
        if (!retained) return false;
        const stamp = yield* now;
        const reason = {
          code: request.kind === "user_input" ? ("user_input" as const) : ("approval" as const),
          summary:
            request.kind === "user_input"
              ? "The original worker requires user input."
              : "The original worker requires approval.",
          requiredAction:
            "Open the original thread to handle this pending request. The pause was not applied and no ready recovery point was saved.",
        };
        yield* sql`UPDATE workbench_attention_items SET observation_only=1,runtime_request_id=${rejected.runtimeRequestId!},
          phase='blocked',reason_json=${yield* encodeReason(reason)},blocked_reason='pending_runtime_request',checkpoint_status='pending',
          source_run_id=${request.run_id},version=version+1,updated_at=${stamp} WHERE id=${a.id}`;
        const operations =
          yield* sql<Operation>`SELECT * FROM workbench_attention_operations WHERE attention_id=${a.id} AND kind='pause'`;
        for (const op of operations) {
          const evidence = yield* encodeJson({
            pause: yield* decodeJson(op.payload_json),
            pauseReason: yield* decodeReason(a.reason_json),
            runtimeRequestId: rejected.runtimeRequestId!,
            runtimeRequest: yield* decodeJson(request.payload_json),
          });
          yield* sql`UPDATE workbench_attention_operations SET state='blocked',payload_json=${evidence},updated_at=${stamp}
            WHERE queue_id=${a.queue_id} AND client_request_id=${op.client_request_id}`;
        }
        return true;
      }),
    );
  });
  const discover = Effect.fn("WorkbenchAttention.discover")(function* (
    queueId: string,
    input: AttentionScope,
  ) {
    yield* authorize(queueId, input);
    const observations =
      yield* sql<AttentionRow>`SELECT * FROM workbench_attention_items WHERE queue_id=${queueId} AND observation_only=1 AND phase='blocked'`;
    for (const observation of observations) {
      const w = yield* workItem(queueId, observation.work_item_id);
      if (w.work_thread_id === null) continue;
      const p = yield* threads.getThreadRecords(ThreadId.make(w.work_thread_id), [
        "runtimeRequests",
      ]);
      if (
        p.runtimeRequests.some(
          (request) =>
            request.id === observation.runtime_request_id && request.status === "resolved",
        )
      )
        yield* sql`UPDATE workbench_attention_items SET phase='resolved',version=version+1,blocked_reason=NULL,updated_at=${yield* now} WHERE id=${observation.id} AND observation_only=1 AND phase='blocked' AND EXISTS(SELECT 1 FROM orchestration_v2_projection_runtime_requests r WHERE r.thread_id=${w.work_thread_id} AND r.runtime_request_id=${observation.runtime_request_id} AND r.status='resolved')`;
    }
    const snapshot = yield* queue.get(queueId, input);
    for (const entry of snapshot.items) {
      if (entry.queueState !== "occupied" || entry.workThreadId === null) continue;
      const w = yield* workItem(queueId, entry.id);
      const projection = yield* threads.getThreadProjection(ThreadId.make(entry.workThreadId));
      const [existing] =
        yield* sql<AttentionRow>`SELECT * FROM workbench_attention_items WHERE work_item_id=${entry.id} AND phase!='resolved'`;
      if (existing) {
        if (existing.observation_only === 1) {
          const current = projection.runtimeRequests.find(
            (request) => request.id === existing.runtime_request_id,
          );
          if (current?.status === "resolved")
            yield* sql`UPDATE workbench_attention_items SET phase='resolved',version=version+1,blocked_reason=NULL,updated_at=${yield* now} WHERE id=${existing.id} AND observation_only=1 AND phase='blocked' AND EXISTS(SELECT 1 FROM orchestration_v2_projection_runtime_requests r WHERE r.thread_id=${entry.workThreadId} AND r.runtime_request_id=${existing.runtime_request_id} AND r.status='resolved')`;
        }
        continue;
      }
      const pending = projection.runtimeRequests.find(
        (request) =>
          request.status === "pending" &&
          (request.kind === "user_input" ||
            ["command", "file-change", "file-read", "permission", "mcp-elicitation"].includes(
              request.kind,
            )) &&
          projection.nodes.some(
            (node) =>
              node.id === request.nodeId &&
              projection.runs.some(
                (run) =>
                  run.id === node.runId &&
                  ["preparing", "starting", "running", "waiting"].includes(run.status),
              ),
          ),
      );
      if (!pending) continue;
      const source = projection.nodes.find((node) => node.id === pending.nodeId)?.runId ?? null;
      const id = `attention:request:${digest(`${queueId}:${w.id}:${pending.id}`)}`;
      const reason = {
        code: pending.kind === "user_input" ? ("user_input" as const) : ("approval" as const),
        summary:
          pending.kind === "user_input"
            ? "The original worker requires user input."
            : "The original worker requires approval.",
        requiredAction:
          "Open the original thread to handle this pending request. This observation has not paused the worker or saved a ready recovery point.",
      };
      const evidence = yield* encodeJson(yield* encodeProjection(projection)),
        stamp = yield* now;
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* authorize(queueId, input);
          yield* sql`INSERT OR IGNORE INTO workbench_attention_items(id,queue_id,work_item_id,phase,reason_json,pre_pause_json,observation_only,runtime_request_id,priority,blocked_reason,source_run_id,pause_command_id,pause_claim_id,pause_wake_generation,epoch,created_at,updated_at)
          SELECT ${id},${queueId},${w.id},'blocked',${yield* encodeReason(reason)},${evidence},1,${pending.id},${entry.priority},'pending_runtime_request',${source},${`command:${id}:pause`},e.claim_id,e.wake_generation,w.attention_epoch,${stamp},${stamp}
          FROM workbench_queue_entries e JOIN workbench_work_items w ON w.id=e.work_item_id WHERE e.work_item_id=${w.id} AND e.queue_id=${queueId} AND e.state='occupied' AND w.attention_hold=0
          AND EXISTS(SELECT 1 FROM orchestration_v2_projection_runtime_requests r WHERE r.thread_id=${entry.workThreadId} AND r.runtime_request_id=${pending.id} AND r.status='pending')`;
        }),
      );
    }
  });
  const point = Effect.fn("WorkbenchAttention.point")(function* (
    a: AttentionRow,
    w: Items.WorkItemRow,
    id: string,
    stage: string,
    original?: Manifest,
  ) {
    const [prior] = yield* sql<{
      manifest_json: string;
      checkpoint_sha: string;
    }>`SELECT * FROM workbench_attention_points WHERE id=${id}`;
    if (prior?.checkpoint_sha) {
      const saved = yield* decodeManifest(prior.manifest_json);
      if (
        !(yield* checkpoints.hasCheckpointRef({
          cwd: saved.path,
          checkpointRef: CheckpointRef.make(saved.checkpointRef),
        }))
      )
        return yield* new Blocked({ reason: "checkpoint_failed" });
      const ref = yield* git.resolveCommit({ cwd: saved.path, revision: saved.checkpointRef });
      if (ref.commitSha !== saved.checkpointSha)
        return yield* new Blocked({ reason: "checkpoint_failed" });
      return saved;
    }
    if (
      w.work_thread_id === null ||
      w.worktree_path === null ||
      w.branch === null ||
      !(yield* fs.exists(w.worktree_path))
    )
      return yield* new Blocked({ reason: "workspace_unavailable" });
    const projection = yield* threads.getThreadProjection(ThreadId.make(w.work_thread_id));
    if (
      projection.thread.projectId !== w.project_id ||
      projection.thread.worktreePath !== w.worktree_path ||
      projection.thread.branch !== w.branch ||
      projection.thread.deletedAt !== null ||
      projection.thread.archivedAt !== null
    )
      return yield* new Blocked({ reason: "workspace_changed" });
    const realPath = yield* fs.realPath(w.worktree_path);
    const branch = (yield* git.execute({
      operation: "WorkbenchAttention.verifyBranch",
      cwd: w.worktree_path,
      args: ["symbolic-ref", "--quiet", "--short", "HEAD"],
    })).stdout.trim();
    const common = (yield* git.execute({
      operation: "WorkbenchAttention.verifyRepository",
      cwd: w.worktree_path,
      args: ["rev-parse", "--git-common-dir"],
    })).stdout.trim();
    const gitCommonDir = yield* fs.realPath(path.resolve(w.worktree_path, common));
    const provider =
      projection.providerThreads.find((p) => p.id === projection.thread.activeProviderThreadId) ??
      projection.providerThreads.at(-1);
    const source = projection.runs.findLast((r) => r.status !== "queued");
    if (branch !== w.branch) return yield* new Blocked({ reason: "workspace_changed" });
    if (
      provider?.nativeThreadRef?.nativeId == null ||
      provider.nativeThreadRef.strength !== "strong" ||
      provider.providerSessionId === null ||
      source === undefined
    )
      return yield* new Blocked({ reason: "provider_context_unavailable" });
    if (
      original &&
      (realPath !== original.realPath ||
        gitCommonDir !== original.gitCommonDir ||
        provider.id !== original.providerThreadId ||
        provider.providerInstanceId !== original.providerInstanceId ||
        provider.nativeThreadRef.nativeId !== original.nativeThreadId ||
        provider.nativeThreadRef.driver !== original.nativeDriver)
    )
      return yield* new Blocked({ reason: "provider_context_unavailable" });
    if (
      original &&
      ((yield* encodeJson(projection.thread.modelSelection)) !==
        (yield* encodeJson(original.modelSelection)) ||
        projection.thread.runtimeMode !== original.runtimeMode ||
        projection.thread.interactionMode !== original.interactionMode)
    )
      return yield* new Blocked({ reason: "provider_context_unavailable" });
    const checkpointRef = `refs/t3/workbench-attention/${digest(id)}`;
    const head = yield* git.resolveCommit({ cwd: w.worktree_path, revision: "HEAD" });
    if (original && head.commitSha !== original.headSha) {
      const ancestry = yield* git.execute({
        operation: "WorkbenchAttention.verifyRepairHead",
        cwd: w.worktree_path,
        args: ["merge-base", "--is-ancestor", original.headSha, head.commitSha],
        allowNonZeroExit: true,
      });
      if (ancestry.exitCode !== 0) return yield* new Blocked({ reason: "workspace_changed" });
    }
    const initial: Manifest = {
      workItemId: w.id,
      threadId: ThreadId.make(w.work_thread_id),
      projectId: w.project_id,
      path: w.worktree_path,
      realPath,
      branch,
      gitCommonDir,
      headSha: head.commitSha,
      checkpointRef,
      checkpointSha: "",
      providerInstanceId: provider.providerInstanceId,
      providerThreadId: provider.id,
      providerSessionId: provider.providerSessionId,
      nativeDriver: provider.nativeThreadRef.driver,
      nativeThreadId: provider.nativeThreadRef.nativeId,
      sourceRunId: source.id,
      modelSelection: projection.thread.modelSelection,
      runtimeMode: projection.thread.runtimeMode,
      interactionMode: projection.thread.interactionMode,
      progress: {
        repair:
          yield* sql`SELECT g.id AS generationId,g.used,g.reason,(SELECT json_group_array(e.id) FROM workbench_repair_evidence e WHERE e.generation_id=g.id) AS evidenceIds
          FROM workbench_repair_generations g WHERE g.id=${a.repair_generation_id}`,
        beforePause: yield* decodeJson(a.pre_pause_json),
        afterQuiescence: yield* encodeProjection(projection),
        workItem: {
          id: w.id,
          cycleNumber: w.cycle_number,
          version: w.version,
          latestEventSequence: w.latest_event_sequence,
          dispatchedThroughSequence: w.dispatched_through_sequence,
          processedThroughSequence: w.processed_through_sequence,
        },
        reason: yield* decodeReason(a.reason_json),
      },
    };
    const fixed = prior ? yield* decodeManifest(prior.manifest_json) : initial;
    if (!prior)
      yield* sql`INSERT INTO workbench_attention_points(id,attention_id,work_item_id,stage,manifest_json,checkpoint_ref,checkpoint_sha,created_at) VALUES(${id},${a.id},${w.id},${stage},${yield* encodeManifest(fixed)},${checkpointRef},'',${yield* now})`;
    if (
      !(yield* checkpoints.hasCheckpointRef({
        cwd: fixed.path,
        checkpointRef: CheckpointRef.make(fixed.checkpointRef),
      }))
    )
      yield* checkpoints.captureCheckpoint({
        cwd: fixed.path,
        checkpointRef: CheckpointRef.make(fixed.checkpointRef),
      });
    if (
      !(yield* checkpoints.hasCheckpointRef({
        cwd: fixed.path,
        checkpointRef: CheckpointRef.make(fixed.checkpointRef),
      }))
    )
      return yield* new Blocked({ reason: "checkpoint_failed" });
    const checkpoint = yield* git.resolveCommit({ cwd: fixed.path, revision: fixed.checkpointRef });
    const ready = { ...fixed, checkpointSha: checkpoint.commitSha };
    yield* sql`UPDATE workbench_attention_points SET manifest_json=${yield* encodeManifest(ready)},checkpoint_sha=${checkpoint.commitSha} WHERE id=${id} AND checkpoint_sha=''`;
    return ready;
  });
  const assertQuiescent = Effect.fn("WorkbenchAttention.assertQuiescent")(function* (
    threadId: ThreadId,
  ) {
    const p = yield* threads.getThreadProjection(threadId);
    if (
      p.runs.some((r) => ["preparing", "starting", "running", "waiting"].includes(r.status)) ||
      p.runs.some((r) => r.status === "queued" && r.queueHeld !== true) ||
      p.providerTurns.some((t) => t.status === "running") ||
      p.turnItems.some(
        (t) =>
          ["command_execution", "dynamic_tool", "subagent"].includes(t.type) &&
          ["pending", "running", "waiting"].includes(t.status),
      ) ||
      p.subagents.some((s) => ["pending", "running", "waiting"].includes(s.status)) ||
      p.providerThreads.some((t) => (t.pendingBackgroundTasks?.length ?? 0) > 0)
    )
      return yield* new Blocked({ reason: "not_quiescent" });
    for (const provider of p.providerThreads) {
      if (provider.providerSessionId === null) continue;
      const runtime = Option.isSome(sessions)
        ? yield* sessions.value.get(provider.providerSessionId)
        : Option.none();
      if (Option.isSome(runtime)) {
        const pending = yield* (
          runtime.value.hasPendingBackgroundWorkForThread?.(provider, { includeSubagents: true }) ??
            runtime.value.hasPendingBackgroundWork ??
            Effect.fail(new Blocked({ reason: "not_quiescent" }))
        );
        if (pending) return yield* new Blocked({ reason: "not_quiescent" });
      } else if (provider.status === "active" || provider.status === "error")
        return yield* new Blocked({ reason: "state_unknown" });
    }
    if (p.runtimeRequests.some((r) => r.status === "pending"))
      return yield* new Blocked({ reason: "pending_runtime_request" });
  });
  const prepareRequeue = Effect.fn("WorkbenchAttention.prepareRequeue")(function* (
    a: AttentionRow,
    w: Items.WorkItemRow,
  ) {
    const [entry] = yield* sql<{
      state: string;
    }>`SELECT state FROM workbench_queue_entries WHERE work_item_id=${w.id}`;
    if (entry?.state !== "paused") return;
    const [op] =
      yield* sql<Operation>`SELECT * FROM workbench_attention_operations WHERE attention_id=${a.id} AND kind='requeue' AND state='pending' ORDER BY created_at DESC LIMIT 1`;
    if (!op || a.point_id === null || a.result_text === null)
      return yield* new Blocked({ reason: "stale_state" });
    const input = yield* decodeRequeue(op.payload_json);
    yield* authorize(a.queue_id, input);
    const [saved] = yield* sql<{
      manifest_json: string;
    }>`SELECT manifest_json FROM workbench_attention_points WHERE id=${a.point_id}`;
    if (!saved) return yield* new Blocked({ reason: "checkpoint_failed" });
    const original = yield* decodeManifest(saved.manifest_json);
    yield* assertQuiescent(original.threadId);
    const pointId = `point:${a.id}:resume:${digest(op.client_request_id)}`;
    const fixed = yield* point(a, w, pointId, "resume", original);
    const intentId = `resume:${digest(`${a.id}:${op.client_request_id}`)}`,
      commandId = CommandId.make(`command:${intentId}`),
      messageId = MessageId.make(`message:${intentId}`);
    const command = {
      type: "message.dispatch" as const,
      commandId,
      threadId: fixed.threadId,
      messageId,
      senderThreadId: ThreadId.make(w.dispatcher_thread_id),
      text: renderAttentionResumeBrief(a.result_text),
      attachments: [],
      dispatchMode: { type: "start_immediately" as const },
      createdBy: "agent" as const,
      creationSource: "mcp" as const,
    };
    const requestJson = yield* encodeCommand(command);
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* authorize(a.queue_id, input);
        const stamp = yield* now;
        const [current] = yield* sql<{
          attention_epoch: number;
        }>`UPDATE workbench_work_items SET attention_epoch=attention_epoch+1,version=version+1 WHERE id=${w.id} AND attention_epoch=${a.epoch} AND attention_hold=1 RETURNING attention_epoch`;
        if (!current) return yield* new Blocked({ reason: "stale_state" });
        const [changed] =
          yield* sql`UPDATE workbench_attention_items SET epoch=${current.attention_epoch},resume_point_id=${pointId},version=version+1,updated_at=${stamp}
    WHERE id=${a.id} AND version=${a.version} AND phase='requeue_pending' RETURNING id`;
        if (!changed) return yield* new Blocked({ reason: "stale_state" });
        const [queued] =
          yield* sql`UPDATE workbench_queue_entries SET state='queued',claim_id=NULL,needs_dispatch=1,wake_sent=0,wake_generation=wake_generation+1,work_state='unknown',blocked_reason='queue_full',
    enqueue_sequence=(SELECT COALESCE(MAX(enqueue_sequence),0)+1 FROM workbench_queue_entries),updated_at=${stamp} WHERE work_item_id=${w.id} AND state='paused' RETURNING work_item_id`;
        if (!queued) return yield* new Blocked({ reason: "stale_state" });
        yield* sql`UPDATE sqlite_sequence SET seq=(SELECT MAX(enqueue_sequence) FROM workbench_queue_entries) WHERE name='workbench_queue_entries'`;
        const [batch] = yield* sql<{
          id: string;
        }>`SELECT id FROM workbench_work_batches WHERE work_item_id=${w.id} AND state!='completed' ORDER BY from_sequence LIMIT 1`;
        yield* sql`INSERT INTO workbench_attention_resume_intents(id,attention_id,work_item_id,queue_id,attention_epoch,thread_id,project_id,command_id,message_id,source_run_id,provider_instance_id,provider_thread_id,provider_session_id,native_driver,native_thread_id,worktree_path,worktree_real_path,branch,git_common_dir,batch_id,request_json,state,created_at,updated_at)
    VALUES(${intentId},${a.id},${w.id},${a.queue_id},${current.attention_epoch},${fixed.threadId},${fixed.projectId},${commandId},${messageId},${fixed.sourceRunId},${fixed.providerInstanceId},${fixed.providerThreadId},${fixed.providerSessionId},${fixed.nativeDriver},${fixed.nativeThreadId},${fixed.path},${fixed.realPath},${fixed.branch},${fixed.gitCommonDir},${batch?.id ?? null},${requestJson},'pending',${stamp},${stamp})`;
        yield* sql`UPDATE workbench_attention_operations SET state='applied',updated_at=${stamp} WHERE queue_id=${a.queue_id} AND client_request_id=${op.client_request_id}`;
      }),
    );
  });
  const assertStopObservable = Effect.fn("WorkbenchAttention.assertStopObservable")(function* (
    a: AttentionRow,
    threadId: ThreadId,
  ) {
    const before = yield* decodeProjection(a.pre_pause_json);
    const current = yield* threads.getThreadProjection(threadId);
    const starts = yield* sql<{
      run_id: string;
      attempt_count: number;
    }>`SELECT json_extract(payload_json,'$.runId') AS run_id,MAX(attempt_count) AS attempt_count FROM orchestration_v2_effect_outbox WHERE thread_id=${threadId} AND effect_type='provider-turn.start' GROUP BY json_extract(payload_json,'$.runId')`;
    const sources = [
      ...new Map(
        [...before.runs, ...current.runs]
          .filter(
            (run) =>
              ["running", "waiting"].includes(run.status) ||
              (run.status === "starting" &&
                !starts.some((start) => start.run_id === run.id && start.attempt_count === 0)),
          )
          .map((run) => [run.id, run]),
      ).values(),
    ];
    for (const source of sources) {
      const provider =
        current.providerThreads.find((row) => row.id === source.providerThreadId) ??
        before.providerThreads.find((row) => row.id === source.providerThreadId);
      const runtime =
        provider?.providerSessionId != null && Option.isSome(sessions)
          ? yield* sessions.value.get(provider.providerSessionId)
          : Option.none();
      if (Option.isSome(runtime)) continue;
      const ended = current.runs.find((run) => run.id === source.id);
      const nativeCompleted =
        ended?.status === "completed" &&
        current.providerTurns.some(
          (turn) =>
            turn.runAttemptId === ended.activeAttemptId &&
            turn.providerThreadId === source.providerThreadId &&
            turn.status === "completed" &&
            turn.nativeTurnRef?.nativeId != null &&
            turn.nativeTurnRef.strength === "strong",
        );
      // Stop can synthesize interrupted/idle when its runtime is absent. That write is not an exit acknowledgement.
      if (!nativeCompleted) return yield* new Blocked({ reason: "state_unknown" });
    }
  });
  const reconcileOne = Effect.fn("WorkbenchAttention.reconcileOne")(function* (id: string) {
    yield* locks.withLock(
      id,
      Effect.gen(function* () {
        const [a] =
          yield* sql<AttentionRow>`SELECT * FROM workbench_attention_items WHERE id=${id}`;
        if (!a || a.phase === "resolved" || a.phase === "awaiting_user") return;
        if (a.observation_only === 1) return;
        const w = yield* workItem(a.queue_id, a.work_item_id);
        if (a.phase === "blocked" && a.resume_point_id === null) {
          const failed =
            yield* sql`SELECT 1 FROM workbench_attention_operations WHERE attention_id=${a.id} AND kind='requeue' AND state='blocked' LIMIT 1`;
          if (failed.length) return;
        }

        if (w.work_thread_id === null) return yield* new Blocked({ reason: "state_unknown" });
        if (a.phase === "requeue_pending") {
          yield* prepareRequeue(a, w);
          return;
        }
        if (a.phase === "resuming" || (a.phase === "blocked" && a.resume_point_id !== null)) {
          const [intent] = yield* sql<{
            id: string;
            run_id: string | null;
            message_id: string;
            provider_thread_id: string;
            native_thread_id: string;
            native_driver: string;
          }>`SELECT * FROM workbench_attention_resume_intents WHERE attention_id=${a.id} AND attention_epoch=${a.epoch} ORDER BY created_at DESC LIMIT 1`;
          if (!intent) return;
          const p = yield* threads.getThreadProjection(ThreadId.make(w.work_thread_id));
          const run = p.runs.find(
            (r) => r.id === intent.run_id || r.userMessageId === intent.message_id,
          );
          const provider = p.providerThreads.find((t) => t.id === run?.providerThreadId);
          const attempt = p.attempts.find((t) => t.id === run?.activeAttemptId);
          const providerTurn = p.providerTurns.find(
            (turn) =>
              turn.runAttemptId === run?.activeAttemptId &&
              turn.providerThreadId === provider?.id &&
              ["running", "completed"].includes(turn.status) &&
              (turn.nativeThreadRef === undefined ||
                (turn.nativeThreadRef.nativeId === intent.native_thread_id &&
                  turn.nativeThreadRef.driver === intent.native_driver)),
          );
          if (
            run &&
            ["running", "waiting", "completed"].includes(run.status) &&
            provider?.id === intent.provider_thread_id &&
            provider.nativeThreadRef?.nativeId === intent.native_thread_id &&
            provider.nativeThreadRef.driver === intent.native_driver &&
            attempt?.nativeThreadId === intent.native_thread_id &&
            providerTurn !== undefined
          ) {
            yield* sql.withTransaction(
              Effect.gen(function* () {
                const [changed] =
                  yield* sql`UPDATE workbench_attention_items SET phase='resolved',resume_run_id=${run.id},version=version+1,blocked_reason=NULL,updated_at=${yield* now} WHERE id=${a.id} AND epoch=${a.epoch} AND phase='resuming' RETURNING id`;
                if (changed) {
                  yield* sql`UPDATE workbench_work_items SET attention_hold=0,recovery_ref=NULL,version=version+1 WHERE id=${w.id} AND attention_epoch=${a.epoch}`;
                  yield* sql`UPDATE workbench_attention_resume_intents SET state=${run.status === "completed" ? "completed" : "running"},run_id=${run.id},updated_at=${yield* now} WHERE id=${intent.id}`;
                }
              }),
            );
          } else if (
            run &&
            ["failed", "cancelled", "interrupted", "rolled_back"].includes(run.status)
          ) {
            yield* guard.awaitAdmissions(ThreadId.make(w.work_thread_id));
            yield* execution.awaitThreadQuiescent(ThreadId.make(w.work_thread_id));
            yield* assertQuiescent(ThreadId.make(w.work_thread_id));
            yield* sql.withTransaction(
              Effect.gen(function* () {
                const [entry] = yield* sql<{
                  state: string;
                }>`SELECT state FROM workbench_queue_entries WHERE work_item_id=${w.id}`;
                if (entry?.state === "paused") return;
                yield* sql`UPDATE workbench_attention_resume_intents SET state='blocked',run_id=${run.id},updated_at=${yield* now} WHERE id=${intent.id}`;
                yield* sql`UPDATE workbench_queue_entries SET state='paused',claim_id=NULL,needs_dispatch=0,blocked_reason='attention' WHERE work_item_id=${w.id}`;
                yield* block(a, "provider_context_unavailable");
              }),
            );
          } else if (run) {
            yield* sql`UPDATE workbench_attention_resume_intents SET state='accepted',run_id=${run.id},updated_at=${yield* now} WHERE id=${intent.id}`;
            yield* sql`UPDATE workbench_attention_items SET phase='resuming',resume_run_id=${run.id},version=version+CASE WHEN phase='resuming' THEN 0 ELSE 1 END WHERE id=${a.id}`;
          }
          return;
        }
        const cancelled = yield* outbox.cancelUnsettled({
          threadId: ThreadId.make(w.work_thread_id),
          effectTypes: ["provider-turn.start", "provider-turn.steer", "provider-turn.restart"],
          reason: "Workbench Attention pause",
        });
        yield* outbox.signalCancellations(cancelled);
        yield* guard.awaitAdmissions(ThreadId.make(w.work_thread_id));
        yield* assertStopObservable(a, ThreadId.make(w.work_thread_id));
        yield* engine.dispatch({
          type: "thread.stop",
          commandId: CommandId.make(a.pause_command_id),
          threadId: ThreadId.make(w.work_thread_id),
          reason: "Workbench Attention pause",
        });
        yield* execution.awaitThreadQuiescent(ThreadId.make(w.work_thread_id));
        yield* assertQuiescent(ThreadId.make(w.work_thread_id));
        const manifest = yield* point(a, w, `point:${a.id}:paused`, "paused");
        yield* sql.withTransaction(
          Effect.gen(function* () {
            const [current] = yield* sql<{
              attention_epoch: number;
              attention_hold: number;
            }>`SELECT attention_epoch,attention_hold FROM workbench_work_items WHERE id=${w.id}`;
            if (current?.attention_epoch !== a.epoch || current.attention_hold !== 1)
              return yield* stale();
            const stamp = yield* now;
            yield* sql`UPDATE workbench_attention_items SET phase='awaiting_user',point_id=${`point:${a.id}:paused`},source_run_id=${manifest.sourceRunId},checkpoint_status='ready',captured_at=${stamp},blocked_reason=NULL,version=version+1,updated_at=${stamp} WHERE id=${a.id} AND epoch=${a.epoch}`;
            yield* sql`UPDATE workbench_queue_entries SET state='paused',claim_id=NULL,needs_dispatch=0,wake_sent=1,work_state='unknown',blocked_reason='attention',updated_at=${stamp} WHERE work_item_id=${w.id} AND state IN ('occupied','queued','finished','paused')`;
            yield* sql`UPDATE workbench_repair_attempts SET state='blocked',completed_at=${stamp},updated_at=${stamp} WHERE generation_id=${a.repair_generation_id} AND state IN ('reserved','accepted','running','unknown')`;
            yield* sql`UPDATE workbench_attention_operations SET state='applied',updated_at=${stamp} WHERE attention_id=${a.id} AND kind='pause'`;
          }),
        );
      }).pipe(
        Effect.catch((cause) =>
          Effect.gen(function* () {
            const request = pendingRuntimeRequest(cause);
            if (request && (yield* preservePendingRequest(id, request))) return;
            const [a] =
              yield* sql<AttentionRow>`SELECT * FROM workbench_attention_items WHERE id=${id}`;
            if (a) yield* block(a, isBlocked(cause) ? cause.reason : "state_unknown");
          }),
        ),
      ),
    );
  }, Effect.mapError(Failure.from));
  const schedule = Effect.fn("WorkbenchAttention.schedule")(function* (id: string) {
    if (active.has(id)) return;
    active.add(id);
    yield* reconcileOne(id).pipe(
      Effect.ensuring(Effect.sync(() => active.delete(id))),
      Effect.ignoreCause(),
      Effect.forkIn(scope),
    );
  });
  const pause = Effect.fn("WorkbenchAttention.pause")(
    function* (
      queueId: string,
      workItemId: string,
      input: Omit<WorkbenchAttentionPauseInput, "reason"> & { reason: Summary["reason"] },
    ) {
      yield* authorize(queueId, input);
      const checked = yield* operation(queueId, input, "pause", workItemId);
      if (checked.prior)
        return yield* result(queueId, input, input.clientRequestId).pipe(
          Effect.mapError(Failure.from),
        );
      yield* discover(queueId, input);
      const w = yield* workItem(queueId, workItemId);
      if (w.work_thread_id === null || w.business_state === "completed") return yield* stale();
      const before = yield* threads.getThreadProjection(ThreadId.make(w.work_thread_id));
      const beforePause = yield* encodeJson(yield* encodeProjection(before));
      const [observation] =
        yield* sql<AttentionRow>`SELECT * FROM workbench_attention_items WHERE work_item_id=${w.id} AND phase!='resolved'`;
      const id =
        observation?.observation_only === 1
          ? observation.id
          : `attention:${digest(`${queueId}:${workItemId}:${input.clientRequestId}`)}`;
      if (
        before.runtimeRequests.some((request) => request.status === "pending") ||
        observation?.observation_only === 1
      ) {
        if (observation && observation.observation_only !== 1) return yield* stale();
        yield* sql.withTransaction(
          Effect.gen(function* () {
            yield* authorize(queueId, input);
            const [fixed] =
              yield* sql`SELECT 1 FROM workbench_work_items WHERE id=${w.id} AND version=${input.expectedWorkItemVersion} AND (CASE WHEN matter_id IS NULL THEN 1 ELSE latest_event_sequence END)=${input.expectedLatestEventSequence} AND attention_hold=0`;
            if (!fixed) return yield* stale();
            const prior = yield* operation(queueId, input, "pause", workItemId);
            if (prior.prior) return;
            const stamp = yield* now;
            if (!observation) {
              const [entry] = yield* sql<{
                priority: number;
                claim_id: string | null;
                wake_generation: number;
              }>`SELECT * FROM workbench_queue_entries WHERE work_item_id=${w.id}`;
              if (!entry) return yield* stale();
              yield* sql`INSERT INTO workbench_attention_items(id,queue_id,work_item_id,phase,reason_json,pre_pause_json,observation_only,runtime_request_id,priority,blocked_reason,source_run_id,pause_command_id,pause_claim_id,pause_wake_generation,epoch,created_at,updated_at)
              VALUES(${id},${queueId},${w.id},'blocked',${yield* encodeReason(input.reason)},${beforePause},1,${before.runtimeRequests.find((request) => request.status === "pending")?.id ?? null},${entry.priority},'pending_runtime_request',${w.run_id},${`command:${id}:pause`},${entry.claim_id},${entry.wake_generation},${w.attention_epoch},${stamp},${stamp})`;
            }
            yield* sql`INSERT INTO workbench_attention_operations(queue_id,client_request_id,attention_id,kind,payload_sha,state,created_at,updated_at,payload_json) VALUES(${queueId},${input.clientRequestId},${id},'pause',${checked.sha},'blocked',${stamp},${stamp},${yield* encodeJson(input)})`;
            yield* attachRepairAttention(id, w.id);
          }),
        );
        return yield* result(queueId, input, input.clientRequestId).pipe(
          Effect.mapError(Failure.from),
        );
      }

      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* authorize(queueId, input);
          const prior = yield* operation(queueId, input, "pause", workItemId);
          if (prior.prior) return;
          const [entry] = yield* sql<{
            claim_id: string | null;
            wake_generation: number;
            priority: number;
          }>`SELECT * FROM workbench_queue_entries WHERE queue_id=${queueId} AND work_item_id=${w.id}`;
          const existing =
            yield* sql`SELECT 1 FROM workbench_attention_items WHERE work_item_id=${w.id} AND phase!='resolved'`;
          if (!entry || (existing.length && observation?.observation_only !== 1))
            return yield* stale();
          const [current] = yield* sql<{
            attention_epoch: number;
          }>`UPDATE workbench_work_items SET attention_hold=1,attention_epoch=attention_epoch+1,version=version+1,recovery_ref=${id}
    WHERE id=${w.id} AND version=${input.expectedWorkItemVersion} AND (CASE WHEN matter_id IS NULL THEN 1 ELSE latest_event_sequence END)=${input.expectedLatestEventSequence} RETURNING attention_epoch`;
          if (!current) return yield* stale();
          const stamp = yield* now;
          if (observation?.observation_only === 1) {
            const [upgraded] =
              yield* sql`UPDATE workbench_attention_items SET observation_only=0,phase='pausing',pre_pause_json=${beforePause},reason_json=${yield* encodeReason(input.reason)},epoch=${current.attention_epoch},pause_claim_id=${entry.claim_id},pause_wake_generation=${entry.wake_generation},version=version+1,blocked_reason=NULL,updated_at=${stamp} WHERE id=${id} AND observation_only=1 AND phase='blocked' RETURNING id`;
            if (!upgraded) return yield* stale();
          } else
            yield* sql`INSERT INTO workbench_attention_items(id,queue_id,work_item_id,phase,reason_json,pre_pause_json,priority,pause_command_id,pause_claim_id,pause_wake_generation,epoch,created_at,updated_at)
    VALUES(${id},${queueId},${w.id},'pausing',${yield* encodeReason(input.reason)},${beforePause},${entry.priority},${`command:${id}:pause`},${entry.claim_id},${entry.wake_generation},${current.attention_epoch},${stamp},${stamp})`;
          yield* sql`UPDATE workbench_queue_entries SET needs_dispatch=0,blocked_reason='attention',updated_at=${stamp} WHERE work_item_id=${w.id}`;
          yield* sql`INSERT INTO workbench_attention_operations(queue_id,client_request_id,attention_id,kind,payload_sha,state,created_at,updated_at) VALUES(${queueId},${input.clientRequestId},${id},'pause',${checked.sha},'pending',${stamp},${stamp})`;
          yield* attachRepairAttention(id, w.id);
        }),
      );
      const answer = yield* result(queueId, input, input.clientRequestId).pipe(
        Effect.mapError(Failure.from),
      );
      yield* schedule(id);
      return answer;
    },
    Effect.provideService(SqlClient.SqlClient, sql),
    Effect.mapError((cause) => (isQueueError(cause) ? cause : Failure.from(cause))),
  );
  const record = Effect.fn("WorkbenchAttention.record")(
    function* (
      queueId: string,
      id: string,
      input: WorkbenchAttentionRecordInput,
      principalId?: string,
    ) {
      yield* authorize(queueId, input);
      const checked = yield* operation(queueId, input, "record", id);
      if (checked.prior)
        return yield* result(queueId, input, input.clientRequestId).pipe(
          Effect.mapError(Failure.from),
        );
      yield* workItem(queueId, (yield* read(queueId, id)).work_item_id);
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* authorize(queueId, input);
          const prior = yield* operation(queueId, input, "record", id);
          if (prior.prior) return;
          const stamp = yield* now;
          const [changed] = yield* sql<{
            work_item_id: string;
          }>`UPDATE workbench_attention_items SET result_text=${input.result},recorded_at=${stamp},priority=${input.priority},
    phase=CASE WHEN phase='blocked' AND checkpoint_status='ready' AND EXISTS(SELECT 1 FROM workbench_queue_entries e WHERE e.work_item_id=workbench_attention_items.work_item_id AND e.state='paused') THEN 'awaiting_user' ELSE phase END,version=version+1,updated_at=${stamp}
    WHERE id=${id} AND queue_id=${queueId} AND version=${input.expectedVersion} AND phase IN ('pausing','awaiting_user','blocked') RETURNING work_item_id`;
          if (!changed) return yield* stale();
          yield* sql`UPDATE workbench_queue_entries SET priority=${input.priority},updated_at=${stamp} WHERE work_item_id=${changed.work_item_id}`;
          yield* sql`INSERT INTO workbench_attention_operations(queue_id,client_request_id,attention_id,kind,payload_sha,state,created_at,updated_at) VALUES(${queueId},${input.clientRequestId},${id},'record',${checked.sha},'applied',${stamp},${stamp})`;
          yield* recordRepairAttention(queueId, id, input, principalId);
        }),
      );
      return yield* result(queueId, input, input.clientRequestId).pipe(
        Effect.mapError(Failure.from),
      );
    },
    Effect.provideService(SqlClient.SqlClient, sql),
    Effect.mapError((cause) => (isQueueError(cause) ? cause : Failure.from(cause))),
  );
  const requeue = Effect.fn("WorkbenchAttention.requeue")(
    function* (
      queueId: string,
      id: string,
      input: WorkbenchAttentionRequeueInput,
      principalId?: string,
    ) {
      yield* authorize(queueId, input);
      const checked = yield* operation(queueId, input, "requeue", id);
      if (checked.prior) {
        yield* schedule(id);
        return yield* result(queueId, input, input.clientRequestId).pipe(
          Effect.mapError(Failure.from),
        );
      }
      yield* workItem(queueId, (yield* read(queueId, id)).work_item_id);
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* authorize(queueId, input);
          const prior = yield* operation(queueId, input, "requeue", id);
          if (prior.prior) return;
          const stamp = yield* now;
          const [changed] =
            yield* sql`UPDATE workbench_attention_items SET phase='requeue_pending',version=version+1,blocked_reason=NULL,updated_at=${stamp}
    WHERE id=${id} AND queue_id=${queueId} AND version=${input.expectedVersion} AND phase='awaiting_user' AND point_id IS NOT NULL AND result_text IS NOT NULL AND checkpoint_status='ready'
    AND EXISTS(SELECT 1 FROM workbench_queue_entries e WHERE e.work_item_id=workbench_attention_items.work_item_id AND e.state='paused') RETURNING id`;
          if (!changed) return yield* stale();
          yield* sql`INSERT INTO workbench_attention_operations(queue_id,client_request_id,attention_id,kind,payload_sha,state,created_at,updated_at,payload_json)
    VALUES(${queueId},${input.clientRequestId},${id},'requeue',${checked.sha},'pending',${stamp},${stamp},${yield* encodeJson(input)})`;
          yield* requeueRepairGeneration(queueId, id, input, principalId);
        }),
      );
      const answer = yield* result(queueId, input, input.clientRequestId).pipe(
        Effect.mapError(Failure.from),
      );
      yield* schedule(id);
      return answer;
    },
    Effect.provideService(SqlClient.SqlClient, sql),
    Effect.mapError((cause) => (isQueueError(cause) ? cause : Failure.from(cause))),
  );
  const resumeClaim = Effect.fn("WorkbenchAttention.resumeClaim")(
    function* (workItemId: string, claimId: string) {
      const [a] =
        yield* sql<AttentionRow>`SELECT * FROM workbench_attention_items WHERE work_item_id=${workItemId} AND phase IN ('requeue_pending','resuming')`;
      if (!a) return yield* stale();
      yield* locks.withLock(
        a.id,
        Effect.gen(function* () {
          const [intent] = yield* sql<{
            id: string;
            command_id: string;
            message_id: string;
            request_json: string;
          }>`SELECT * FROM workbench_attention_resume_intents WHERE attention_id=${a.id} AND attention_epoch=${a.epoch} ORDER BY created_at DESC LIMIT 1`;
          if (!intent) return yield* stale();
          yield* sql.withTransaction(
            Effect.gen(function* () {
              const [changed] =
                yield* sql`UPDATE workbench_attention_resume_intents SET claim_id=${claimId},updated_at=${yield* now} WHERE id=${intent.id} AND (claim_id IS NULL OR claim_id=${claimId})
     AND EXISTS(SELECT 1 FROM workbench_queue_entries e JOIN workbench_work_items w ON w.id=e.work_item_id WHERE e.work_item_id=${workItemId} AND e.queue_id=${a.queue_id} AND e.state='occupied' AND e.claim_id=${claimId} AND w.attention_epoch=${a.epoch} AND w.attention_hold=1) RETURNING id`;
              if (!changed) return yield* stale();
              yield* sql`UPDATE workbench_attention_items SET phase='resuming',version=version+CASE WHEN phase='resuming' THEN 0 ELSE 1 END,updated_at=${yield* now} WHERE id=${a.id}`;
            }),
          );
          const command = yield* decodeCommand(intent.request_json);
          if (command.type !== "message.dispatch") return yield* stale();
          yield* engine.dispatch(command).pipe(Effect.ignoreCause());
          const child = yield* threads.getThreadRecords(command.threadId as ThreadId, [
            "runs",
            "messages",
          ]);
          const run = child.runs.find((r) => r.userMessageId === intent.message_id);
          if (run) {
            yield* sql`UPDATE workbench_attention_resume_intents SET run_id=${run.id},state='accepted',updated_at=${yield* now} WHERE id=${intent.id}`;
            yield* sql`UPDATE workbench_attention_items SET resume_run_id=${run.id} WHERE id=${a.id}`;
          } else {
            yield* sql`UPDATE workbench_attention_resume_intents SET state='unknown',updated_at=${yield* now} WHERE id=${intent.id}`;
            yield* block(a, "state_unknown");
          }
        }),
      );
      yield* reconcileOne(a.id);
    },
    Effect.provideService(SqlClient.SqlClient, sql),
    Effect.mapError((cause) => (isQueueError(cause) ? cause : Failure.from(cause))),
  );
  const reconcile = Effect.fn("WorkbenchAttention.reconcile")(
    function* (queueId: string, input: AttentionScope) {
      yield* authorize(queueId, input);
      yield* discover(queueId, input);
      const rows = yield* sql<{
        id: string;
      }>`SELECT id FROM workbench_attention_items WHERE queue_id=${queueId} AND phase IN ('pausing','resuming','blocked','requeue_pending')`;
      for (const row of rows) yield* reconcileOne(row.id);
    },
    Effect.provideService(SqlClient.SqlClient, sql),
    Effect.mapError((cause) => (isQueueError(cause) ? cause : Failure.from(cause))),
  );
  const get = Effect.fn("WorkbenchAttention.get")(
    function* (queueId: string, input: typeof WorkbenchAttentionQuery.Type) {
      yield* authorize(queueId, input);
      yield* discover(queueId, input);

      const rows =
        yield* sql<AttentionRow>`SELECT * FROM workbench_attention_items WHERE queue_id=${queueId} AND id>${input.cursor ?? ""} ORDER BY id LIMIT 101`;
      const page = rows.slice(0, 100);
      const attentions = yield* Effect.forEach(page, summary);
      for (const row of page)
        if (["pausing", "resuming", "blocked", "requeue_pending"].includes(row.phase))
          yield* schedule(row.id);
      return {
        trackingId: input.expectedTrackingId,
        queueId,
        controllerId: input.controllerId,
        projectId: input.projectId,
        attentions,
        nextCursor: rows.length > 100 ? page.at(-1)!.id : null,
      };
    },
    Effect.provideService(SqlClient.SqlClient, sql),
    Effect.mapError((cause) => (isQueueError(cause) ? cause : Failure.from(cause))),
  );
  const pauseSystem = Effect.fn("WorkbenchAttention.pauseSystem")(
    function* (workItemId: string, generationId: string) {
      const [g] = yield* sql<{
        queue_id: string;
        controller_id: string;
        project_id: string;
        tracking_id: string;
        version: number;
        latest_sequence: number;
        reason: string | null;
        source_run_id: string | null;
      }>`SELECT g.queue_id,q.controller_id,g.project_id,m.tracking_id,w.version,CASE WHEN w.matter_id IS NULL THEN 1 ELSE w.latest_event_sequence END AS latest_sequence,g.reason,g.source_run_id
      FROM workbench_repair_generations g JOIN workbench_work_items w ON w.id=g.work_item_id JOIN workbench_queues q ON q.id=g.queue_id,scheduled_task_execution_metadata m
      WHERE g.id=${generationId} AND g.work_item_id=${workItemId} AND g.superseded_at IS NULL AND g.phase='awaiting_user' AND m.singleton=1`;
      if (!g) return yield* stale();
      const [pending] = yield* sql<{
        runtime_request_id: string;
      }>`SELECT runtime_request_id FROM orchestration_v2_projection_runtime_requests WHERE thread_id=(SELECT work_thread_id FROM workbench_work_items WHERE id=${workItemId}) AND status='pending' AND kind IN ('user_input','command','file-change','file-read','permission','mcp-elicitation') ORDER BY created_at LIMIT 1`;
      const clientRequestId = `repair-pause:${digest(yield* encodeJson([generationId, g.reason, pending?.runtime_request_id ?? g.source_run_id]))}`;
      const [prior] = yield* sql<{
        attention_id: string;
      }>`SELECT attention_id FROM workbench_attention_operations WHERE queue_id=${g.queue_id} AND client_request_id=${clientRequestId} AND kind='pause'`;
      if (prior) {
        yield* schedule(prior.attention_id);
        return;
      }
      yield* pause(g.queue_id, workItemId, {
        controllerId: g.controller_id,
        projectId: g.project_id as never,
        expectedTrackingId: g.tracking_id,
        clientRequestId,
        expectedWorkItemVersion: g.version,
        expectedLatestEventSequence: g.latest_sequence,
        reason: {
          code: g.reason === "budget_exhausted" ? "repair_budget_exhausted" : "repair_blocked",
          summary:
            g.reason === "budget_exhausted"
              ? "Three automatic repair attempts were used."
              : "The work item requires human attention before automatic repair can continue.",
          requiredAction:
            "Review the saved check evidence and original thread, handle the cause, record the result, then explicitly requeue the work item.",
        },
      });
    },
    Effect.provideService(SqlClient.SqlClient, sql),
    Effect.mapError((cause) => (isQueueError(cause) ? cause : Failure.from(cause))),
  );
  return WorkbenchAttention.of({
    get,
    pause,
    pauseSystem,
    record,
    requeue,
    reconcile,
    resumeClaim,
  });
});
export const layer = Layer.effect(WorkbenchAttention, make);
