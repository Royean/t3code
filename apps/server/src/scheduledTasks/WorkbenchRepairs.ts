import {
  CommandId,
  MessageId,
  ThreadId,
  ModelSelection,
  OrchestrationV2Command,
  OrchestrationV2TurnItemJson,
  ProjectId,
  ProviderInteractionMode,
  RuntimeMode,
  WorkbenchQueueError,
  WorkbenchRepairsSnapshot,
  type WorkbenchAttentionQuery,
  type WorkbenchRepairSummary,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as SqlClient from "effect/sql/SqlClient";
import * as Git from "../vcs/GitVcsDriver.ts";
import { digest } from "./workbenchLifecycle.ts";
import type { WorkItemRow } from "./WorkbenchWorkItems.ts";
import {
  decodeFrozenRepairPolicy,
  freezeRepairPolicy,
  publicRepairPolicy,
} from "./workbenchRepairPolicy.ts";
import { renderWorkbenchRepairBrief } from "./workbenchRepairBrief.ts";
import { ensureRepairGeneration } from "./workbenchRepairGeneration.ts";

type GenerationRow = {
  id: string;
  work_item_id: string;
  queue_id: string;
  task_id: string;
  task_created_at: string;
  project_id: string;
  dispatcher_thread_id: string;
  thread_id: string | null;
  generation: number;
  version: number;
  used: number;
  phase: string;
  reason: string | null;
  policy_json: string | null;
  context_json: string | null;
  source_run_id: string | null;
  source_sequence: number;
  attention_id: string | null;
  created_at: string;
  updated_at: string;
  superseded_at: string | null;
  created_from_attention_id: string | null;
};
type AttemptRow = {
  id: string;
  generation_id: string;
  ordinal: number;
  state: string;
  command_id: string;
  message_id: string;
  run_id: string | null;
  native_attempt_id: string | null;
  created_at: string;
  accepted_at: string | null;
  completed_at: string | null;
  request_json: string;
};
type EvidenceRow = {
  id: string;
  check_id: string;
  check_name: string;
  exit_code: number;
  item_id: string;
  run_id: string;
  run_attempt_id: string;
  sequence: number;
  completed_at: string;
  payload_sha256: string;
  repair_attempt_id: string | null;
};
const NativeRun = Schema.Struct({
  id: Schema.String,
  status: Schema.String,
  activeAttemptId: Schema.NullOr(Schema.String),
  providerThreadId: Schema.NullOr(Schema.String),
});
const NativeThread = Schema.Struct({
  projectId: Schema.String,
  worktreePath: Schema.NullOr(Schema.String),
  branch: Schema.NullOr(Schema.String),
  modelSelection: Schema.NullOr(ModelSelection),
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode,
});
const NativeProvider = Schema.Struct({
  id: Schema.String,
  providerInstanceId: Schema.String,
  providerSessionId: Schema.NullOr(Schema.String),
  nativeThreadRef: Schema.NullOr(
    Schema.Struct({ driver: Schema.String, nativeId: Schema.String, strength: Schema.String }),
  ),
});
const OriginalContext = Schema.Struct({
  threadId: Schema.String,
  projectId: Schema.String,
  providerInstanceId: Schema.String,
  providerThreadId: Schema.String,
  providerSessionId: Schema.String,
  nativeDriver: Schema.String,
  nativeThreadId: Schema.String,
  worktreePath: Schema.String,
  worktreeRealPath: Schema.String,
  branch: Schema.String,
  gitCommonDir: Schema.String,
  modelSelection: ModelSelection,
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode,
});
const decodeRun = Schema.decodeEffect(Schema.fromJsonString(NativeRun));
const decodeThread = Schema.decodeEffect(Schema.fromJsonString(NativeThread));
const decodeProvider = Schema.decodeEffect(Schema.fromJsonString(NativeProvider));
const decodeContext = Schema.decodeEffect(Schema.fromJsonString(OriginalContext));
const decodeItem = Schema.decodeEffect(Schema.fromJsonString(OrchestrationV2TurnItemJson));
const encodeCommand = Schema.encodeEffect(Schema.fromJsonString(OrchestrationV2Command));
const decodeSnapshot = Schema.decodeUnknownEffect(WorkbenchRepairsSnapshot);
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
class Failure extends WorkbenchQueueError {
  static from(cause: unknown) {
    return Object.defineProperty(new Failure({ reason: "unavailable" }), "cause", { value: cause });
  }
}

export type RepairDecision =
  | { readonly kind: "none" }
  | { readonly kind: "repair"; readonly attemptId: string }
  | { readonly kind: "attention"; readonly generationId: string; readonly reason: string };
export class WorkbenchRepairs extends Context.Service<
  WorkbenchRepairs,
  {
    readonly observe: (workItemId: string) => Effect.Effect<RepairDecision, WorkbenchQueueError>;
    readonly list: (
      queueId: string,
      scope: typeof WorkbenchAttentionQuery.Type,
    ) => Effect.Effect<Omit<WorkbenchRepairsSnapshot, "environmentId">, WorkbenchQueueError>;
  }
>()("t3/scheduledTasks/WorkbenchRepairs") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const fs = yield* Effect.serviceOption(FileSystem.FileSystem),
    path = yield* Effect.serviceOption(Path.Path),
    git = yield* Effect.serviceOption(Git.GitVcsDriver);
  const locks = yield* KeyedLock.make<string>();
  const context = Effect.fn("WorkbenchRepairs.originalContext")(function* (
    w: WorkItemRow,
    run: typeof NativeRun.Type,
  ) {
    if (
      !w.work_thread_id ||
      !w.worktree_path ||
      !w.branch ||
      !run.providerThreadId ||
      Option.isNone(fs) ||
      Option.isNone(path) ||
      Option.isNone(git)
    )
      return null;
    const [t] = yield* sql<{
      payload_json: string;
    }>`SELECT payload_json FROM orchestration_v2_projection_threads WHERE thread_id=${w.work_thread_id} AND deleted_at IS NULL`;
    const [p] = yield* sql<{
      payload_json: string;
    }>`SELECT payload_json FROM orchestration_v2_projection_provider_threads WHERE provider_thread_id=${run.providerThreadId} AND thread_id=${w.work_thread_id}`;
    if (!t || !p) return null;
    const thread = yield* decodeThread(t.payload_json),
      provider = yield* decodeProvider(p.payload_json);
    if (
      thread.projectId !== w.project_id ||
      thread.worktreePath !== w.worktree_path ||
      thread.branch !== w.branch ||
      !thread.modelSelection ||
      !provider.providerSessionId ||
      provider.nativeThreadRef?.strength !== "strong"
    )
      return null;
    const real = yield* fs.value.realPath(w.worktree_path);
    const actualBranch = (yield* git.value.execute({
      operation: "WorkbenchRepairs.branch",
      cwd: w.worktree_path,
      args: ["symbolic-ref", "--quiet", "--short", "HEAD"],
    })).stdout.trim();
    const common = (yield* git.value.execute({
      operation: "WorkbenchRepairs.repository",
      cwd: w.worktree_path,
      args: ["rev-parse", "--git-common-dir"],
    })).stdout.trim();
    if (actualBranch !== w.branch) return null;
    return {
      threadId: w.work_thread_id,
      projectId: w.project_id,
      providerInstanceId: provider.providerInstanceId,
      providerThreadId: provider.id,
      providerSessionId: provider.providerSessionId,
      nativeDriver: provider.nativeThreadRef.driver,
      nativeThreadId: provider.nativeThreadRef.nativeId,
      worktreePath: w.worktree_path,
      worktreeRealPath: real,
      branch: w.branch,
      gitCommonDir: yield* fs.value.realPath(path.value.resolve(w.worktree_path, common)),
      modelSelection: thread.modelSelection,
      runtimeMode: thread.runtimeMode,
      interactionMode: thread.interactionMode,
    };
  });
  const attention = Effect.fn("WorkbenchRepairs.attention")(function* (
    g: GenerationRow,
    reason: NonNullable<WorkbenchRepairSummary["reason"]>,
  ) {
    yield* sql`UPDATE workbench_repair_generations SET phase='awaiting_user',reason=${reason},version=version+CASE WHEN phase='awaiting_user' AND reason=${reason} THEN 0 ELSE 1 END,updated_at=${DateTime.formatIso(yield* DateTime.now)} WHERE id=${g.id} AND superseded_at IS NULL`;
    return { kind: "attention", generationId: g.id, reason } as const;
  });
  const observe = Effect.fn("WorkbenchRepairs.observe")(
    function* (workItemId: string) {
      const [w] = yield* sql<
        WorkItemRow & { queue_id: string; repair_policy_json: string | null }
      >`SELECT w.*,b.queue_id,b.repair_policy_json FROM workbench_work_items w
      JOIN workbench_queue_bindings b ON b.task_id=w.task_id AND b.task_created_at=w.task_created_at AND b.project_id=w.project_id AND b.dispatcher_thread_id=w.dispatcher_thread_id WHERE w.id=${workItemId}`;
      if (
        !w?.work_thread_id ||
        (!w.run_id && !w.accepted_run_id) ||
        w.business_state === "completed" ||
        w.attention_hold === 1
      )
        return { kind: "none" } as const;
      const [source] = yield* sql<{
        payload_json: string;
      }>`SELECT payload_json FROM orchestration_v2_projection_runs WHERE thread_id=${w.work_thread_id}
      AND (run_id=${w.run_id ?? w.accepted_run_id} OR run_id IN (SELECT run_id FROM workbench_repair_attempts WHERE work_item_id=${w.id}) OR run_id IN (SELECT run_id FROM workbench_work_batches WHERE work_item_id=${w.id}) OR run_id IN (SELECT run_id FROM workbench_attention_resume_intents WHERE work_item_id=${w.id}) OR run_id IN (SELECT run_id FROM workbench_runtime_responses WHERE work_item_id=${w.id} AND state!='rejected')) ORDER BY ordinal DESC LIMIT 1`;
      if (!source) return { kind: "none" } as const;
      const run = yield* decodeRun(source.payload_json);
      const pending = yield* sql<{
        kind: string;
      }>`SELECT kind FROM orchestration_v2_projection_runtime_requests WHERE thread_id=${w.work_thread_id} AND status='pending' AND kind IN ('user_input','command','file-change','file-read','permission','mcp-elicitation') LIMIT 1`;
      const terminal = ["completed", "failed", "cancelled", "interrupted", "rolled_back"].includes(
        run.status,
      );
      yield* ensureRepairGeneration(w.id);
      const stamp = DateTime.formatIso(yield* DateTime.now);
      let [existing] =
        yield* sql<GenerationRow>`SELECT * FROM workbench_repair_generations WHERE work_item_id=${w.id} AND superseded_at IS NULL`;
      if (
        existing?.phase === "awaiting_user" &&
        ["user_input", "approval"].includes(existing.reason ?? "") &&
        pending.length === 0
      ) {
        const [resolved] =
          yield* sql`SELECT 1 FROM workbench_attention_items a JOIN orchestration_v2_projection_runtime_requests r ON r.runtime_request_id=a.runtime_request_id AND r.thread_id=${w.work_thread_id}
        WHERE a.id=${existing.attention_id} AND a.observation_only=1 AND r.status='resolved'`;
        if (resolved) {
          yield* sql`UPDATE workbench_repair_generations SET phase=CASE WHEN EXISTS(SELECT 1 FROM workbench_repair_attempts x WHERE x.generation_id=workbench_repair_generations.id AND x.state IN ('reserved','accepted','running','unknown')) THEN 'repairing' ELSE 'watching' END,reason=NULL,attention_id=NULL,version=version+1,updated_at=${stamp} WHERE id=${existing.id} AND superseded_at IS NULL`;
          [existing] =
            yield* sql<GenerationRow>`SELECT * FROM workbench_repair_generations WHERE id=${existing.id}`;
        }
      }
      if (!terminal && pending.length === 0) return { kind: "none" } as const;
      const g = existing;
      if (!g) return yield* new WorkbenchQueueError({ reason: "stale_state" });
      if (g.phase === "awaiting_user")
        return {
          kind: "attention",
          generationId: g.id,
          reason: g.reason ?? "state_unknown",
        } as const;
      if (pending.length)
        return yield* attention(g, pending[0]!.kind === "user_input" ? "user_input" : "approval");
      if (run.status !== "completed") return yield* attention(g, "provider_failed");
      if (g.policy_json === null)
        return yield* attention(
          g,
          g.reason === "policy_changed" ? "policy_changed" : "policy_missing",
        );
      const policy = yield* decodeFrozenRepairPolicy(g.policy_json);
      const verified = yield* freezeRepairPolicy(ProjectId.make(w.project_id), {
        checks: policy.map(({ scriptId, definitionHash }) => ({ scriptId, definitionHash })),
      }).pipe(Effect.option);
      if (Option.isNone(verified) || verified.value === null)
        return yield* attention(g, "policy_changed");
      const live =
        yield* sql<AttemptRow>`SELECT * FROM workbench_repair_attempts WHERE generation_id=${g.id} AND state IN ('reserved','accepted','running','unknown')`;
      if (live[0]?.state === "reserved") {
        const [receipt] = yield* sql<{
          status: string;
        }>`SELECT status FROM orchestration_command_receipts WHERE command_id=${live[0].command_id}`;
        if (receipt?.status === "rejected") {
          yield* sql`UPDATE workbench_repair_attempts SET state='blocked',completed_at=${stamp},updated_at=${stamp} WHERE id=${live[0].id}`;
          return yield* attention(g, "state_unknown");
        }
        return { kind: "repair", attemptId: live[0].id } as const;
      }
      const busy =
        yield* sql`SELECT 1 FROM orchestration_v2_projection_runs WHERE thread_id=${w.work_thread_id} AND status IN ('preparing','starting','running','waiting','queued') LIMIT 1`;
      const children =
        yield* sql`SELECT 1 FROM orchestration_v2_projection_subagents WHERE thread_id=${w.work_thread_id} AND status IN ('pending','running','waiting') LIMIT 1`;
      if (busy.length || children.length) return { kind: "none" } as const;
      const [owned] = yield* sql<{
        attempt_id: string;
        native_attempt_id: string | null;
      }>`WITH RECURSIVE related(attempt_id,run_id) AS (
        SELECT id,run_id FROM workbench_repair_attempts WHERE generation_id=${g.id} AND state IN ('accepted','running') AND charged=1
        UNION SELECT related.attempt_id,r.run_id FROM related JOIN workbench_runtime_responses r ON json_extract(r.source_json,'$.source_run_id')=related.run_id
          WHERE r.work_item_id=${w.id} AND r.run_id IS NOT NULL AND r.state!='rejected'
      ) SELECT related.attempt_id,json_extract(root.payload_json,'$.activeAttemptId') AS native_attempt_id FROM related
      JOIN workbench_repair_attempts a ON a.id=related.attempt_id LEFT JOIN orchestration_v2_projection_runs root ON root.run_id=a.run_id AND root.thread_id=a.thread_id WHERE related.run_id=${run.id} LIMIT 1`;
      const active = live.find((a) => a.id === owned?.attempt_id);
      const actual = yield* context(w, run).pipe(Effect.option);
      if (Option.isNone(actual) || actual.value === null)
        return yield* attention(g, "context_unavailable");
      let original = g.context_json === null ? actual.value : yield* decodeContext(g.context_json);
      if ((yield* encodeJson(original)) !== (yield* encodeJson(actual.value))) {
        const sameConversation =
          (yield* encodeJson({
            ...original,
            providerSessionId: actual.value.providerSessionId,
          })) === (yield* encodeJson(actual.value));
        const [resumed] =
          yield* sql`SELECT 1 FROM workbench_attention_items a JOIN workbench_attention_resume_intents i ON i.attention_id=a.id WHERE a.id=${g.created_from_attention_id} AND a.phase='resolved' AND a.resume_run_id=${run.id} AND i.run_id=${run.id} AND i.state IN ('running','completed')`;
        if (!sameConversation || g.used !== 0 || !resumed)
          return yield* attention(g, "context_unavailable");
        original = actual.value;
      }
      const evidence: Array<{
        check: (typeof policy)[number];
        item: Extract<OrchestrationV2TurnItemJson, { type: "command_execution" }>;
        sequence: number;
        attemptId: string;
        payload: string;
      }> = [];
      const rows = yield* sql<{
        payload_json: string;
        sequence: number;
      }>`SELECT payload_json,sequence FROM orchestration_events WHERE stream_id=${w.work_thread_id} AND event_type='turn-item.updated' AND json_extract(payload_json,'$.runId')=${run.id} ORDER BY sequence`;
      for (const row of rows) {
        const decoded = yield* decodeItem(row.payload_json).pipe(Effect.option);
        if (Option.isNone(decoded)) continue;
        const item = decoded.value;
        if (
          item.type !== "command_execution" ||
          !["completed", "failed"].includes(item.status) ||
          !Number.isInteger(item.exitCode) ||
          !item.cwd ||
          !item.completedAt ||
          item.toolNonExecutionKind ||
          !item.nodeId ||
          !item.nativeItemRef ||
          !item.providerThreadId ||
          !item.providerTurnId ||
          Option.isNone(fs) ||
          Option.isNone(path) ||
          !path.value.isAbsolute(item.cwd)
        )
          continue;
        const observedCwd = yield* fs.value.realPath(item.cwd).pipe(Effect.option);
        if (Option.isNone(observedCwd) || observedCwd.value !== original.worktreeRealPath) continue;
        const check = policy.find((c) => c.acceptedInputs.includes(item.input));
        if (!check) continue;
        const [turn] = yield* sql<{
          attempt_id: string | null;
          native_thread: string | null;
        }>`SELECT json_extract(payload_json,'$.runAttemptId') AS attempt_id,json_extract(payload_json,'$.nativeThreadRef.nativeId') AS native_thread FROM orchestration_v2_projection_provider_turns WHERE provider_turn_id=${item.providerTurnId} AND provider_thread_id=${item.providerThreadId}`;
        const [node] =
          yield* sql`SELECT 1 FROM orchestration_v2_projection_nodes WHERE node_id=${item.nodeId} AND thread_id=${w.work_thread_id} AND run_id=${run.id}`;
        const [projected] = yield* sql<{
          payload_json: string;
        }>`SELECT payload_json FROM orchestration_v2_projection_turn_items WHERE turn_item_id=${item.id} AND thread_id=${w.work_thread_id} AND run_id=${run.id}`;
        if (
          !turn?.attempt_id ||
          turn.attempt_id !== run.activeAttemptId ||
          !node ||
          !projected ||
          item.providerThreadId !== original.providerThreadId
        )
          continue;
        const projectedItem = yield* decodeItem(projected.payload_json).pipe(Effect.option);
        if (
          Option.isNone(projectedItem) ||
          projectedItem.value.type !== "command_execution" ||
          projectedItem.value.exitCode !== item.exitCode ||
          projectedItem.value.input !== item.input ||
          projectedItem.value.cwd !== item.cwd ||
          projectedItem.value.status !== item.status
        )
          continue;
        const [attempt] =
          yield* sql`SELECT 1 FROM orchestration_v2_projection_run_attempts WHERE attempt_id=${turn.attempt_id} AND run_id=${run.id}`;
        if (!attempt) continue;
        const prior = evidence.findIndex((e) => e.check.scriptId === check.scriptId);
        if (prior !== -1) evidence.splice(prior, 1);
        evidence.push({
          check,
          item,
          sequence: row.sequence,
          attemptId: turn.attempt_id,
          payload: row.payload_json,
        });
      }
      const failed = evidence.filter((e) => e.item.exitCode !== 0),
        succeeded = evidence.length === policy.length && failed.length === 0;
      return yield* sql.withTransaction(
        Effect.gen(function* () {
          const [fence] =
            yield* sql`SELECT 1 FROM workbench_work_items w JOIN workbench_queue_entries e ON e.work_item_id=w.id JOIN workbench_repair_generations g ON g.work_item_id=w.id WHERE w.id=${w.id} AND w.attention_hold=0 AND w.attention_epoch=${w.attention_epoch} AND e.state='occupied' AND g.id=${g.id} AND g.superseded_at IS NULL AND g.version=${g.version}`;
          if (!fence) return { kind: "none" } as const;
          const ids: string[] = [];
          for (const e of evidence) {
            const id = `repair-evidence:${digest(yield* encodeJson([g.id, e.item.id, e.sequence, e.check.scriptId]))}`;
            ids.push(id);
            yield* sql`INSERT OR IGNORE INTO workbench_repair_evidence ${sql.insert({ id, generation_id: g.id, repair_attempt_id: active?.id ?? null, check_id: e.check.scriptId, check_name: e.check.name, definition_hash: e.check.definitionHash, thread_id: w.work_thread_id!, item_id: e.item.id, run_id: run.id, run_attempt_id: e.attemptId, node_id: e.item.nodeId!, provider_thread_id: e.item.providerThreadId!, provider_turn_id: e.item.providerTurnId!, native_item_json: yield* encodeJson(e.item.nativeItemRef), sequence: e.sequence, exit_code: e.item.exitCode!, command_text: e.item.input, cwd: e.item.cwd!, output_sha256: digest(e.item.output ?? ""), payload_sha256: digest(e.payload), completed_at: DateTime.formatIso(e.item.completedAt!), created_at: stamp })}`;
          }
          if (active)
            yield* sql`UPDATE workbench_repair_attempts SET state=${succeeded ? "succeeded" : failed.length ? "failed" : "blocked"},native_attempt_id=COALESCE(native_attempt_id,${owned?.native_attempt_id ?? null}),completed_at=${stamp},updated_at=${stamp} WHERE id=${active.id}`;
          yield* sql`UPDATE workbench_repair_generations SET source_run_id=${run.id},source_sequence=${evidence.at(-1)?.sequence ?? g.source_sequence},context_json=${yield* encodeJson(original)} WHERE id=${g.id}`;
          if (succeeded) {
            yield* sql`UPDATE workbench_repair_generations SET phase='succeeded',reason=NULL,version=version+CASE WHEN phase='succeeded' AND source_run_id=${run.id} THEN 0 ELSE 1 END,updated_at=${stamp} WHERE id=${g.id}`;
            return { kind: "none" } as const;
          }
          if (failed.length === 0) return yield* attention(g, "evidence_unavailable");
          if (g.used >= 3) return yield* attention(g, "budget_exhausted");
          const [entry] = yield* sql<{
            claim_id: string | null;
            wake_generation: number;
          }>`SELECT claim_id,wake_generation FROM workbench_queue_entries WHERE work_item_id=${w.id} AND state='occupied'`;
          if (!entry?.claim_id) return yield* attention(g, "state_unknown");
          const ordinal = g.used + 1,
            id = `repair-attempt:${g.id}:${ordinal}`,
            commandId = `command:workbench-repair:${id}`,
            messageId = `message:workbench-repair:${id}`;
          const text = renderWorkbenchRepairBrief({
            ordinal,
            checks: policy,
            evidence: failed.map((e) => ({
              id: ids[evidence.indexOf(e)]!,
              checkId: e.check.scriptId,
              exitCode: e.item.exitCode!,
            })),
          });
          const request = yield* encodeCommand({
            type: "message.dispatch",
            commandId: CommandId.make(commandId),
            threadId: ThreadId.make(w.work_thread_id!),
            messageId: MessageId.make(messageId),
            senderThreadId: ThreadId.make(w.dispatcher_thread_id),
            text,
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            createdBy: "agent",
            creationSource: "server",
          });
          yield* sql`INSERT OR IGNORE INTO workbench_repair_attempts ${sql.insert({ id, generation_id: g.id, work_item_id: w.id, queue_id: g.queue_id, ordinal, charged: 0, state: "reserved", source_run_id: run.id, failure_evidence_json: yield* encodeJson(ids), thread_id: w.work_thread_id, project_id: w.project_id, claim_id: entry.claim_id, wake_generation: entry.wake_generation, attention_epoch: w.attention_epoch, command_id: commandId, message_id: messageId, provider_instance_id: original.providerInstanceId, provider_thread_id: original.providerThreadId, provider_session_id: original.providerSessionId, native_driver: original.nativeDriver, native_thread_id: original.nativeThreadId, worktree_path: original.worktreePath, worktree_real_path: original.worktreeRealPath, branch: original.branch, git_common_dir: original.gitCommonDir, model_selection_json: yield* encodeJson(original.modelSelection), runtime_mode: original.runtimeMode, interaction_mode: original.interactionMode, request_json: request, created_at: stamp, updated_at: stamp })}`;
          const [reserved] =
            yield* sql<AttemptRow>`SELECT * FROM workbench_repair_attempts WHERE generation_id=${g.id} AND source_run_id=${run.id}`;
          if (!reserved) return yield* attention(g, "state_unknown");
          yield* sql`UPDATE workbench_repair_generations SET phase='repairing',reason='check_failed',version=version+1,updated_at=${stamp} WHERE id=${g.id}`;
          return { kind: "repair", attemptId: reserved.id } as const;
        }),
      );
    },
    (effect, workItemId) => locks.withLock(workItemId, effect),
    Effect.provideService(SqlClient.SqlClient, sql),
    Effect.mapError(Failure.from),
  );
  const list = Effect.fn("WorkbenchRepairs.list")(function* (
    queueId: string,
    scope: typeof WorkbenchAttentionQuery.Type,
  ) {
    const [owned] =
      yield* sql`SELECT 1 FROM workbench_queues q,scheduled_task_execution_metadata m WHERE q.id=${queueId} AND q.controller_id=${scope.controllerId} AND q.project_id=${scope.projectId} AND m.singleton=1 AND m.tracking_id=${scope.expectedTrackingId}`;
    if (!owned) return yield* new WorkbenchQueueError({ reason: "invalid_binding" });
    const rows =
      yield* sql<GenerationRow>`SELECT * FROM workbench_repair_generations WHERE queue_id=${queueId} AND id>${scope.cursor ?? ""} ORDER BY id LIMIT 101`;
    const repairs = yield* Effect.forEach(
      rows.slice(0, 100),
      Effect.fn("WorkbenchRepairs.summary")(function* (g) {
        const checks =
          yield* sql<EvidenceRow>`SELECT * FROM workbench_repair_evidence WHERE generation_id=${g.id} ORDER BY sequence`;
        const safe = (e: EvidenceRow) => ({
          id: e.id,
          checkId: e.check_id,
          name: e.check_name,
          exitCode: e.exit_code,
          itemId: e.item_id,
          runId: e.run_id,
          attemptId: e.run_attempt_id,
          sequence: e.sequence,
          completedAt: e.completed_at,
          digest: e.payload_sha256,
        });
        const attempts =
          yield* sql<AttemptRow>`SELECT * FROM workbench_repair_attempts WHERE generation_id=${g.id} ORDER BY ordinal`;
        return {
          id: g.id,
          workItemId: g.work_item_id,
          taskId: g.task_id,
          taskCreatedAt: g.task_created_at,
          dispatcherThreadId: g.dispatcher_thread_id,
          workThreadId: g.thread_id,
          generation: g.generation,
          version: g.version,
          limit: 3,
          used: g.used,
          phase: g.phase,
          reason: g.reason,
          sourceRunId: g.source_run_id,
          attentionId: g.attention_id,
          policy: publicRepairPolicy(
            g.policy_json === null ? null : yield* decodeFrozenRepairPolicy(g.policy_json),
          ),
          checks: checks.filter((c) => c.run_id === g.source_run_id).map(safe),
          attempts: attempts.map((a) => ({
            id: a.id,
            ordinal: a.ordinal,
            state: a.state,
            commandId: a.command_id,
            messageId: a.message_id,
            runId: a.run_id,
            nativeAttemptId: a.native_attempt_id,
            createdAt: a.created_at,
            acceptedAt: a.accepted_at,
            completedAt: a.completed_at,
            checks: checks.filter((c) => c.repair_attempt_id === a.id).map(safe),
          })),
          createdAt: g.created_at,
          updatedAt: g.updated_at,
        };
      }),
    );
    const snapshot = yield* decodeSnapshot({
      environmentId: "",
      trackingId: scope.expectedTrackingId,
      queueId,
      controllerId: scope.controllerId,
      projectId: scope.projectId,
      repairs,
      nextCursor: rows.length > 100 ? rows[99]!.id : null,
    });
    const { environmentId: _, ...result } = snapshot;
    return result;
  }, Effect.mapError(Failure.from));
  return WorkbenchRepairs.of({ observe, list });
});
export const layer = Layer.effect(WorkbenchRepairs, make);
