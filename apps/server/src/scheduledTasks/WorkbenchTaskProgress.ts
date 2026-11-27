import {
  RunId,
  ScheduledTaskId,
  ThreadId,
  WorkbenchQueueError,
  WorkbenchRepairPolicy,
  WorkbenchTaskProgressQuery,
  WorkbenchTaskProgressResult,
} from "@t3tools/contracts";
import { hostedThreadLink } from "@t3tools/shared/threadLinks";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Environment from "../environment/ServerEnvironment.ts";
import * as Git from "../vcs/GitVcsDriver.ts";
import type { WorkItemRow } from "./WorkbenchWorkItems.ts";

type Task = WorkbenchTaskProgressResult["task"];
type Row = Pick<
  WorkItemRow,
  | "id"
  | "task_id"
  | "task_created_at"
  | "project_id"
  | "dispatcher_thread_id"
  | "work_thread_id"
  | "worktree_path"
  | "branch"
  | "previous_work_item_id"
  | "cycle_number"
  | "version"
  | "business_state"
  | "attention_hold"
  | "matter_id"
  | "updated_at"
> & {
  queue_state: Task["queueState"];
  work_state: Task["workState"];
  blocked_reason: string | null;
  repair_policy_json: string | null;
  matter_key: string | null;
  matter_json: string | null;
  subscription_scope_id: string | null;
  thread_json: string | null;
  generation_id: string | null;
  repair_phase: string | null;
  used: number | null;
  source_run_id: string | null;
  frozen_policy_json: string | null;
  context_json: string | null;
  attention_manifest: string | null;
};
const OriginalWorkspace = Schema.Struct({
  projectId: Schema.optional(Schema.String),
  threadId: Schema.optional(Schema.String),
  path: Schema.optional(Schema.String),
  worktreePath: Schema.optional(Schema.String),
  realPath: Schema.optional(Schema.String),
  worktreeRealPath: Schema.optional(Schema.String),
  gitCommonDir: Schema.optional(Schema.String),
  branch: Schema.optional(Schema.String),
});
const decodeWorkspace = Schema.decodeEffect(Schema.fromJsonString(OriginalWorkspace));
const decodeParts = Schema.decodeEffect(Schema.fromJsonString(Schema.Array(Schema.String)));
const decodePolicy = Schema.decodeEffect(Schema.fromJsonString(WorkbenchRepairPolicy));
const decodeFrozen = Schema.decodeEffect(
  Schema.fromJsonString(Schema.Array(Schema.Struct({ scriptId: Schema.String }))),
);
const decodeResult = Schema.decodeUnknownEffect(WorkbenchTaskProgressResult);
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
// Observation refreshes do not change the state protected around the Git probes.
const stateFence = (row: Row) => ({ ...row, updated_at: null });
const decodeBlocked = Schema.decodeUnknownOption(
  WorkbenchTaskProgressResult.fields.task.fields.blockedReason,
);
const isQueueError = Schema.is(WorkbenchQueueError);
class Failure extends WorkbenchQueueError {
  static from(cause: unknown) {
    return isQueueError(cause)
      ? cause
      : Object.defineProperty(new Failure({ reason: "unavailable" }), "cause", { value: cause });
  }
}

export class WorkbenchTaskProgress extends Context.Service<
  WorkbenchTaskProgress,
  {
    readonly get: (
      queueId: string,
      workItemId: string,
      scope: typeof WorkbenchTaskProgressQuery.Type,
    ) => Effect.Effect<WorkbenchTaskProgressResult, WorkbenchQueueError>;
  }
>()("t3/scheduledTasks/WorkbenchTaskProgress") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const git = yield* Git.GitVcsDriver;
  const fs = yield* FileSystem.FileSystem;
  const path = yield* Path.Path;
  const environment = yield* Environment.ServerEnvironmentIdentity;
  const read = Effect.fn("WorkbenchTaskProgress.read")(function* (
    queueId: string,
    workItemId: string,
    scope: typeof WorkbenchTaskProgressQuery.Type,
  ) {
    const [row] =
      yield* sql<Row>`SELECT w.id,w.task_id,w.task_created_at,w.project_id,w.dispatcher_thread_id,w.work_thread_id,
      w.worktree_path,w.branch,w.previous_work_item_id,w.cycle_number,w.version,w.business_state,w.attention_hold,w.matter_id,w.updated_at,
      e.state AS queue_state,e.work_state,e.blocked_reason,b.repair_policy_json,
      m.matter_key,m.matter_json,m.subscription_scope_id,t.payload_json AS thread_json,
      g.id AS generation_id,g.phase AS repair_phase,g.used,g.source_run_id,g.policy_json AS frozen_policy_json,g.context_json,
      (SELECT manifest_json FROM workbench_attention_points WHERE work_item_id=w.id AND checkpoint_sha!='' ORDER BY created_at DESC,id DESC LIMIT 1) AS attention_manifest
      FROM workbench_work_items w JOIN workbench_queue_entries e ON e.work_item_id=w.id
      JOIN workbench_queues q ON q.id=e.queue_id
      JOIN workbench_queue_bindings b ON b.queue_id=q.id AND b.task_id=w.task_id AND b.task_created_at=w.task_created_at
        AND b.project_id=w.project_id AND b.dispatcher_thread_id=w.dispatcher_thread_id
      JOIN scheduled_tasks s ON s.task_id=b.task_id AND s.created_at=b.task_created_at AND s.project_id=b.project_id
        AND s.thread_id=b.dispatcher_thread_id AND s.thread_mode='continue' AND json_extract(s.schedule_json,'$.type')='webhook'
      JOIN orchestration_v2_projection_threads p ON p.thread_id=s.thread_id AND p.project_id=s.project_id
        AND p.deleted_at IS NULL AND p.archived_at IS NULL
      JOIN scheduled_task_execution_metadata metadata ON metadata.singleton=1 AND metadata.tracking_id=${scope.expectedTrackingId}
      LEFT JOIN workbench_matters m ON m.id=w.matter_id AND m.queue_id=q.id AND m.task_id=w.task_id
        AND m.task_created_at=w.task_created_at AND m.dispatcher_thread_id=w.dispatcher_thread_id
      LEFT JOIN orchestration_v2_projection_threads t ON t.thread_id=w.work_thread_id AND t.project_id=w.project_id AND t.deleted_at IS NULL
      LEFT JOIN workbench_repair_generations g ON g.work_item_id=w.id AND g.queue_id=q.id AND g.task_id=w.task_id
        AND g.task_created_at=w.task_created_at AND g.project_id=w.project_id AND g.dispatcher_thread_id=w.dispatcher_thread_id AND g.superseded_at IS NULL
      WHERE q.id=${queueId} AND q.controller_id=${scope.controllerId} AND q.project_id=${scope.projectId}
        AND w.project_id=q.project_id AND w.id=${workItemId}`;
    if (!row || (row.matter_id !== null && row.matter_key === null))
      return yield* new WorkbenchQueueError({ reason: "invalid_binding" });
    return row;
  });
  const matter = Effect.fn("WorkbenchTaskProgress.matter")(function* (row: Row) {
    if (row.matter_json === null) return null;
    if (!row.matter_key || !row.subscription_scope_id)
      return yield* new WorkbenchQueueError({ reason: "invalid_binding" });
    const parts = yield* decodeParts(row.matter_key);
    const [provider, namespace, kind, key] = parts;
    if (
      parts.length !== 4 ||
      !["github", "alert"].includes(provider!) ||
      !["issue", "pull_request", "alert"].includes(kind!)
    )
      return yield* new WorkbenchQueueError({ reason: "invalid_binding" });
    return {
      scopeKey: yield* encodeJson([row.subscription_scope_id, row.matter_key]),
      provider: provider as "github" | "alert",
      namespace: namespace!,
      kind: kind as "issue" | "pull_request" | "alert",
      key: key!,
    };
  });
  const observeHead = Effect.fn("WorkbenchTaskProgress.observeHead")(
    function* (row: Row) {
      if (!row.worktree_path || !row.branch || !row.thread_json) return null;
      const thread = yield* decodeWorkspace(row.thread_json);
      if (
        thread.projectId !== row.project_id ||
        thread.worktreePath !== row.worktree_path ||
        thread.branch !== row.branch
      )
        return null;
      const real = yield* fs.realPath(row.worktree_path);
      const run = (args: ReadonlyArray<string>) =>
        git.execute({
          operation: "WorkbenchTaskProgress.observeHead",
          cwd: row.worktree_path!,
          args,
          maxOutputBytes: 4096,
          timeoutMs: 10000,
        });
      const branch = yield* run(["symbolic-ref", "--quiet", "--short", "HEAD"]);
      const common = yield* run(["rev-parse", "--git-common-dir"]);
      const commonReal = yield* fs.realPath(path.resolve(row.worktree_path, common.stdout.trim()));
      if (branch.stdoutTruncated || common.stdoutTruncated || branch.stdout.trim() !== row.branch)
        return null;
      for (const encoded of [row.context_json, row.attention_manifest]) {
        if (encoded === null) continue;
        const proof = yield* decodeWorkspace(encoded);
        if (
          (proof.projectId !== undefined && proof.projectId !== row.project_id) ||
          (proof.threadId !== undefined && proof.threadId !== row.work_thread_id) ||
          (proof.branch !== undefined && proof.branch !== row.branch) ||
          (proof.path !== undefined && proof.path !== row.worktree_path) ||
          (proof.worktreePath !== undefined && proof.worktreePath !== row.worktree_path) ||
          (proof.realPath !== undefined && proof.realPath !== real) ||
          (proof.worktreeRealPath !== undefined && proof.worktreeRealPath !== real) ||
          (proof.gitCommonDir !== undefined && proof.gitCommonDir !== commonReal)
        )
          return null;
      }
      const head = yield* run(["rev-parse", "--verify", "HEAD^{commit}"]);
      const afterBranch = yield* run(["symbolic-ref", "--quiet", "--short", "HEAD"]);
      const afterCommon = yield* run(["rev-parse", "--git-common-dir"]);
      if (
        head.stdoutTruncated ||
        afterBranch.stdoutTruncated ||
        afterCommon.stdoutTruncated ||
        afterBranch.stdout.trim() !== row.branch ||
        real !== (yield* fs.realPath(row.worktree_path)) ||
        commonReal !==
          (yield* fs.realPath(path.resolve(row.worktree_path, afterCommon.stdout.trim())))
      )
        return null;
      const sha = head.stdout.trim();
      return /^(?:[a-f0-9]{40}|[a-f0-9]{64})$/.test(sha) ? sha : null;
    },
    Effect.orElseSucceed(() => null),
  );
  const checks = Effect.fn("WorkbenchTaskProgress.checks")(function* (row: Row) {
    const policy =
      row.frozen_policy_json === null
        ? row.repair_policy_json === null
          ? []
          : (yield* decodePolicy(row.repair_policy_json)).checks
        : yield* decodeFrozen(row.frozen_policy_json);
    if (policy.length > 10 || new Set(policy.map((check) => check.scriptId)).size !== policy.length)
      return yield* new WorkbenchQueueError({ reason: "unavailable" });
    const source = row.source_run_id;
    const [run] =
      source === null
        ? []
        : yield* sql`SELECT 1 FROM orchestration_v2_projection_runs WHERE run_id=${source} AND thread_id=${row.work_thread_id}`;
    const evidence =
      !run || row.generation_id === null
        ? []
        : yield* sql<{ check_id: string; exit_code: number }>`
      SELECT check_id,exit_code FROM (SELECT check_id,exit_code,ROW_NUMBER() OVER(PARTITION BY check_id ORDER BY sequence DESC,id DESC) AS latest
        FROM workbench_repair_evidence WHERE generation_id=${row.generation_id} AND run_id=${source} AND thread_id=${row.work_thread_id}) WHERE latest=1`;
    const current = evidence.filter((e) => policy.some((check) => check.scriptId === e.check_id));
    const passed = current.filter((e) => e.exit_code === 0).length;
    const failed = current.length - passed;
    const state: Task["checks"]["state"] =
      row.repair_phase === "awaiting_user"
        ? "awaiting_user"
        : policy.length === 0
          ? "not_configured"
          : source !== null && !run
            ? "unknown"
            : failed > 0
              ? "failed"
              : passed === policy.length && row.repair_phase === "succeeded"
                ? "passed"
                : row.repair_phase === "succeeded"
                  ? "unknown"
                  : "pending";
    return {
      state,
      passed,
      failed,
      used: row.used,
      sourceRunId: run && source !== null ? RunId.make(source) : null,
    };
  });
  const get = Effect.fn("WorkbenchTaskProgress.get")(
    function* (queueId: string, workItemId: string, scope: typeof WorkbenchTaskProgressQuery.Type) {
      const before = yield* read(queueId, workItemId, scope);
      const sha = before.work_thread_id === null ? null : yield* observeHead(before);
      const observedAt = DateTime.formatIso(yield* DateTime.now);
      const row = yield* read(queueId, workItemId, scope);
      if ((yield* encodeJson(stateFence(row))) !== (yield* encodeJson(stateFence(before))))
        return yield* new WorkbenchQueueError({ reason: "stale_state" });
      const evidence = yield* checks(row);
      const final = yield* read(queueId, workItemId, scope);
      if ((yield* encodeJson(stateFence(final))) !== (yield* encodeJson(stateFence(row))))
        return yield* new WorkbenchQueueError({ reason: "stale_state" });
      const environmentId = yield* environment.getEnvironmentId;
      const decodedBlock = decodeBlocked(row.blocked_reason);
      const blockedReason =
        row.attention_hold === 1 || row.repair_phase === "awaiting_user"
          ? ("attention" as const)
          : decodedBlock._tag === "Some"
            ? decodedBlock.value
            : ("state_unknown" as const);
      const nextStep: Task["nextStep"] =
        row.business_state === "completed"
          ? "completed"
          : row.queue_state === "paused" ||
              blockedReason === "attention" ||
              blockedReason === "approval"
            ? "await_user"
            : row.business_state === "awaiting_delivery"
              ? "await_delivery"
              : row.queue_state === "queued"
                ? "await_dispatch"
                : ["working", "waiting_for_children"].includes(row.work_state)
                  ? "work_in_progress"
                  : "unknown";
      return yield* decodeResult({
        environmentId,
        trackingId: scope.expectedTrackingId,
        queueId,
        controllerId: scope.controllerId,
        projectId: scope.projectId,
        task: {
          id: row.id,
          taskId: ScheduledTaskId.make(row.task_id),
          taskCreatedAt: row.task_created_at,
          dispatcherThreadId: ThreadId.make(row.dispatcher_thread_id),
          workThreadId: row.work_thread_id === null ? null : ThreadId.make(row.work_thread_id),
          previousTaskId: row.previous_work_item_id,
          cycleNumber: row.cycle_number,
          version: row.version,
          businessState: row.business_state,
          queueState: row.queue_state,
          workState: row.work_state,
          blockedReason,
          matter: yield* matter(row),
          branch:
            row.branch !== null && row.branch.length <= 500 && !/[\x00-\x1f\x7f]/.test(row.branch)
              ? row.branch
              : null,
          head: {
            state: row.work_thread_id === null ? "not_created" : sha === null ? "unknown" : "known",
            sha,
            observedAt,
            checksBound: false,
          },
          checks: evidence,
          nextStep,
          threadUrl:
            row.work_thread_id === null
              ? null
              : hostedThreadLink({ environmentId, threadId: ThreadId.make(row.work_thread_id) }),
          updatedAt: final.updated_at,
        },
      });
    },
    Effect.provideService(SqlClient.SqlClient, sql),
    Effect.mapError(Failure.from),
  );
  return WorkbenchTaskProgress.of({ get });
});

export const layer = Layer.effect(WorkbenchTaskProgress, make);
