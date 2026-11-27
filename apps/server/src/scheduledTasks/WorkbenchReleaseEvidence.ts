import * as Verification from "./releaseVerification.ts";
import * as ReleaseRefs from "./WorkbenchReleaseRefs.ts";
import { mergeObservationReason } from "./releaseMergeProof.ts";
import type { WorkbenchOnlineReceipt, WorkbenchMergeReason } from "@t3tools/contracts";
import * as Adapter from "./WorkbenchReleaseAdapter.ts";
import {
  WorkbenchReleaseHostProfile as HostProfileSchema,
  type WorkbenchReleaseEvidence as Evidence,
} from "@t3tools/contracts";
import {
  WorkbenchReleaseError,
  WorkbenchReleaseEvidence as EvidenceSchema,
  WorkbenchReleasePreparation as PreparationSchema,
  ScheduledTaskId,
  ThreadId,
  type WorkbenchAttentionScope,
  type WorkbenchReleaseCatalog,
  WorkbenchReleasePrepareInput,
  type WorkbenchReleaseRevalidateInput,
  type WorkbenchReleaseResult,
  type WorkbenchReleaseDiffResult,
  type WorkbenchReleasePreparation,
  type WorkbenchReleaseHostProfile,
  WorkbenchReleaseReason,
  type WorkbenchReleaseReceipt,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as Config from "../config.ts";
import * as Environment from "../environment/ServerEnvironment.ts";
import type * as Git from "../vcs/GitVcsDriver.ts";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Runner from "../processRunner.ts";
import {
  canonicalReleaseJson,
  readReleaseProfiles,
  publicReleaseProfile,
  releaseProfileScripts,
  releaseDigest,
  releaseHash,
  releaseBlobHash,
  releaseScriptHash,
  releaseCommandArgv,
  safeReleasePath,
} from "./workbenchReleaseProfile.ts";

export type ReleaseDeploymentMaterial = {
  evidence: Evidence;
  profile: WorkbenchReleaseHostProfile;
  artifact: Uint8Array;
  rollback: Uint8Array;
};
export class ReleaseMergeMaterialError extends Schema.TaggedError<ReleaseMergeMaterialError>()(
  "ReleaseMergeMaterialError",
  { reason: Schema.String, known: Schema.Boolean, cause: Schema.Defect() },
) {
  override get message() {
    return "Release merge material verification failed.";
  }
}
type Scope = typeof WorkbenchAttentionScope.Type;
type Task = {
  id: string;
  task_id: string;
  task_created_at: string;
  project_id: string;
  dispatcher_thread_id: string;
  work_thread_id: string;
  worktree_path: string;
  branch: string;
  cycle_number: number;
  version: number;
  latest_event_sequence: number;
  processed_through_sequence: number;
  business_state: string;
  attention_hold: number;
  thread_json: string;
  workspace_root: string;
};
type Source = {
  task: Task;
  real: string;
  common: string;
  head: string;
  tree: string;
  base: string;
  remoteDigest: string;
};
type Row = {
  id: string;
  queue_id: string;
  controller_id: string;
  project_id: string;
  tracking_id: string;
  work_item_id: string;
  input_json: string;
  profile_json: string;
  snapshot_json: string;
  preparation_json: string;
  state: string;
};
class Blocked extends Schema.TaggedError<Blocked>()("ReleasePreparationBlocked", {
  reason: Schema.String,
}) {}
const problem = (reason: WorkbenchReleaseReason) => new Blocked({ reason });
const DiagnosticStage = Schema.Literals([
  "catalog",
  "prepare",
  "get",
  "revalidate",
  "diff",
  "await_prepared",
  "profile",
  "source",
  "artifact",
  "target",
  "snapshot",
  "git",
  "build",
  "check",
  "health",
  "core",
  "rollback_verify",
  "cleanup",
]);
type DiagnosticStage = typeof DiagnosticStage.Type;
class ReleaseEvidenceOperationError extends Schema.TaggedError<ReleaseEvidenceOperationError>()(
  "ReleaseEvidenceOperationError",
  {
    stage: DiagnosticStage,
    reason: WorkbenchReleaseReason,
    cause: Schema.Defect(),
  },
) {
  override get message() {
    return `Release evidence operation failed (${this.stage}).`;
  }
}
class ReleaseExecutionExitError extends Schema.TaggedError<ReleaseExecutionExitError>()(
  "ReleaseExecutionExitError",
  {
    exitCode: Schema.NullOr(Schema.Int),
    cause: Schema.Defect(),
  },
) {
  override get message() {
    return "Release subprocess did not complete successfully.";
  }
}
const isOperationError = Schema.is(ReleaseEvidenceOperationError);
const isPreparationProblem = Schema.is(Schema.Union([Blocked, ReleaseEvidenceOperationError]));
const isReleaseError = Schema.is(WorkbenchReleaseError);
const causedProblem = (stage: DiagnosticStage, reason: WorkbenchReleaseReason, cause: unknown) =>
  isOperationError(cause) ? cause : new ReleaseEvidenceOperationError({ stage, reason, cause });
const reportFailure = Effect.fn("WorkbenchReleaseEvidence.reportFailure")(function* (
  stage: DiagnosticStage,
  reason: WorkbenchReleaseReason,
  cause: unknown,
) {
  yield* Effect.logWarning(
    "Release evidence operation failed",
    causedProblem(stage, reason, cause),
  );
});
// Public release errors omit private causes after the internal diagnostics record them.
const publicFailure = Effect.fn("WorkbenchReleaseEvidence.publicFailure")(function* (
  stage: DiagnosticStage,
  cause: unknown,
) {
  if (isReleaseError(cause)) return yield* cause;
  yield* reportFailure(
    stage,
    isOperationError(cause) ? cause.reason : "evidence_unavailable",
    cause,
  );
  return yield* new WorkbenchReleaseError({ reason: "unavailable" });
});
const decodeHostProfile = Schema.decodeEffect(Schema.fromJsonString(HostProfileSchema));
const decodePreparation = Schema.decodeEffect(Schema.fromJsonString(PreparationSchema));
const decodeEvidence = Schema.decodeUnknownEffect(EvidenceSchema);
const decodeJson = Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown));
const decodeThread = Schema.decodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      projectId: Schema.String,
      worktreePath: Schema.NullOr(Schema.String),
      branch: Schema.NullOr(Schema.String),
    }),
  ),
);
const decodeTarget = Schema.decodeUnknownEffect(
  Schema.Struct({
    serviceId: Schema.String,
    version: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(200)),
    sourceSha: Schema.String.check(Schema.isPattern(/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/)),
    artifactSha256: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
  }),
);
const decodePrepareInput = Schema.decodeEffect(Schema.fromJsonString(WorkbenchReleasePrepareInput));
const MAX_ARTIFACT = 64 * 1024 * 1024;
const MAX_DIFF = 1024 * 1024;

const isReleaseMergeMaterialError = Schema.is(ReleaseMergeMaterialError);

export class WorkbenchReleaseEvidence extends Context.Service<
  WorkbenchReleaseEvidence,
  {
    readonly catalog: (query: {
      projectId?: string | undefined;
    }) => Effect.Effect<WorkbenchReleaseCatalog, WorkbenchReleaseError>;
    readonly prepare: (
      queueId: string,
      input: WorkbenchReleasePrepareInput,
    ) => Effect.Effect<WorkbenchReleaseResult, WorkbenchReleaseError>;
    readonly get: (
      queueId: string,
      id: string,
      scope: Scope,
    ) => Effect.Effect<WorkbenchReleaseResult, WorkbenchReleaseError>;
    readonly revalidate: (
      queueId: string,
      id: string,
      input: WorkbenchReleaseRevalidateInput,
    ) => Effect.Effect<WorkbenchReleaseResult, WorkbenchReleaseError>;
    readonly diff: (
      queueId: string,
      id: string,
      scope: Scope,
    ) => Effect.Effect<WorkbenchReleaseDiffResult, WorkbenchReleaseError>;
    readonly admissionMaterial: (
      queueId: string,
      id: string,
      input: WorkbenchReleaseRevalidateInput,
    ) => Effect.Effect<ReleaseDeploymentMaterial, WorkbenchReleaseError>;
    readonly admittedMaterial: (
      queueId: string,
      id: string,
      input: WorkbenchReleaseRevalidateInput,
    ) => Effect.Effect<ReleaseDeploymentMaterial, WorkbenchReleaseError>;
    readonly runOnlineCheck: (
      queueId: string,
      id: string,
      input: WorkbenchReleaseRevalidateInput & Verification.OnlineCheckInput,
    ) => Effect.Effect<WorkbenchOnlineReceipt, Verification.VerificationError>;
    readonly mergeMaterial: (
      queueId: string,
      id: string,
      input: WorkbenchReleaseRevalidateInput,
      phase: "admission" | "finalization" | "recovery",
    ) => Effect.Effect<ReleaseDeploymentMaterial & { cwd: string }, ReleaseMergeMaterialError>;
    readonly preserveMergeSource: (
      queueId: string,
      id: string,
      input: WorkbenchReleaseRevalidateInput,
      ref: string,
    ) => Effect.Effect<void, ReleaseMergeMaterialError>;
    readonly awaitPrepared: (
      queueId: string,
      id: string,
      scope: Scope,
    ) => Effect.Effect<WorkbenchReleaseResult, WorkbenchReleaseError>;
  }
>()("t3/scheduledTasks/WorkbenchReleaseEvidence") {}

const make = Effect.gen(function* () {
  const environment = yield* Environment.ServerEnvironmentIdentity,
    sql = yield* SqlClient.SqlClient,
    fs = yield* FileSystem.FileSystem,
    path = yield* Path.Path,
    config = yield* Config.ServerConfig,
    runner = yield* Runner.ProcessRunner,
    adapter = yield* Adapter.WorkbenchReleaseAdapter;
  const verificationExecutor = yield* Verification.make;
  const releaseRefs = yield* Effect.serviceOption(ReleaseRefs.WorkbenchReleaseRefs);
  const serviceScope = yield* Effect.scope;
  const context = Context.make(FileSystem.FileSystem, fs).pipe(
    Context.add(Path.Path, path),
    Context.add(Config.ServerConfig, config),
    Context.add(SqlClient.SqlClient, sql),
  );
  const locks = yield* KeyedLock.make<string>();
  const jobs = new Map<string, Deferred.Deferred<void>>();
  const stamp = DateTime.now.pipe(Effect.map(DateTime.formatIso));
  const root = path.join(config.stateDir, "workbench-release-evidence");
  const directory = (id: string) => path.join(root, releaseHash(id));
  const executeGit = Effect.fn("WorkbenchReleaseEvidence.executeGit")(function* (
    input: Git.ExecuteGitInput,
  ) {
    const host = yield* HostProcess.Environment;
    const env: NodeJS.ProcessEnv = {
      PATH: host.PATH ?? "/usr/bin:/bin",
      HOME: host.HOME ?? config.stateDir,
      ...(host.SSH_AUTH_SOCK ? { SSH_AUTH_SOCK: host.SSH_AUTH_SOCK } : {}),
      ...(host.USER ? { USER: host.USER } : {}),
      ...(host.LOGNAME ? { LOGNAME: host.LOGNAME } : {}),
      LANGUAGE: "C",
      GIT_OPTIONAL_LOCKS: "0",
      GIT_CONFIG_NOSYSTEM: "1",
      GIT_TERMINAL_PROMPT: "0",
      GIT_ASKPASS: "",
      SSH_ASKPASS: "",
      SSH_ASKPASS_REQUIRE: "never",
      GIT_PAGER: "cat",
      PAGER: "cat",
      GIT_CEILING_DIRECTORIES: path.dirname(input.cwd),
      GIT_ALLOW_PROTOCOL: "file:ssh:http:https:git",
    };
    // Network observations run outside the project; only host/global authentication config applies.
    const hardened = [
      "--no-pager",
      "-c",
      "core.fsmonitor=false",
      "-c",
      "core.untrackedCache=false",
      "-c",
      "core.hooksPath=/dev/null",
      "-c",
      "core.pager=cat",
      "-c",
      "diff.external=",
      "-c",
      "diff.trustExitCode=false",
    ];
    const result = yield* runner
      .run({
        command: "/usr/bin/git",
        args: [...hardened, ...input.args],
        cwd: input.cwd,
        env,
        extendEnv: false,
        timeout: input.timeoutMs ?? 10000,
        maxOutputBytes: input.maxOutputBytes ?? MAX_DIFF,
        outputMode: "truncate",
      })
      .pipe(
        Effect.mapError((cause) =>
          causedProblem(
            "git",
            cause._tag === "ProcessTimeoutError"
              ? "timeout"
              : cause._tag === "ProcessOutputLimitError"
                ? "output_limit"
                : "evidence_unavailable",
            cause,
          ),
        ),
      );
    if (
      result.code === null ||
      (!input.allowNonZeroExit && result.code !== 0) ||
      result.stdoutInvalidUtf8 ||
      result.stderrInvalidUtf8
    )
      return yield* causedProblem(
        "git",
        "evidence_unavailable",
        new ReleaseExecutionExitError({ exitCode: result.code, cause: result }),
      );
    return {
      exitCode: result.code,
      stdout: result.stdout,
      stderr: result.stderr,
      stdoutTruncated: result.stdoutTruncated,
      stderrTruncated: result.stderrTruncated,
    };
  });
  const resolveCommit = Effect.fn("WorkbenchReleaseEvidence.resolveCommit")(function* (input: {
    cwd: string;
    revision: string;
  }) {
    const result = yield* executeGit({
      operation: "release.resolveCommit",
      cwd: input.cwd,
      args: ["rev-parse", "--verify", `${input.revision}^{commit}`],
      maxOutputBytes: 4096,
    });
    const commitSha = result.stdout.trim();
    if (result.stdoutTruncated || !/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/.test(commitSha))
      return yield* problem("evidence_unavailable");
    return { commitSha };
  });
  const listWorktreePaths = Effect.fn("WorkbenchReleaseEvidence.listWorktreePaths")(function* (
    cwd: string,
  ) {
    const result = yield* executeGit({
      operation: "release.worktrees",
      cwd,
      args: ["worktree", "list", "--porcelain", "-z"],
    });
    if (result.stdoutTruncated) return yield* problem("evidence_unavailable");
    return result.stdout.split("\0\0").flatMap((record) => {
      const fields = record.split("\0"),
        entry = fields.find((f) => f.startsWith("worktree "));
      return !entry || fields.some((f) => f.startsWith("prunable"))
        ? []
        : [path.resolve(cwd, entry.slice("worktree ".length))];
    });
  });
  const git = { execute: executeGit, resolveCommit, listWorktreePaths };
  const remoteDefinition = Effect.fn("WorkbenchReleaseEvidence.remoteDefinition")(function* (
    cwd: string,
    main: WorkbenchReleaseHostProfile["policy"]["main"],
  ) {
    const configured = yield* executeGit({
      operation: "release.remote",
      cwd,
      args: ["config", "--local", "--get-all", `remote.${main.remote}.url`],
      maxOutputBytes: 16384,
    });
    const urls = configured.stdout.replace(/\r?\n$/, "").split(/\r?\n/);
    if (
      configured.stdoutTruncated ||
      urls.length !== 1 ||
      !urls[0] ||
      urls[0].trim() !== urls[0] ||
      /[\x00-\x1f\x7f]/.test(urls[0])
    )
      return yield* problem("profile_invalid");
    const originalUrl = urls[0];
    let url = originalUrl;
    if (path.isAbsolute(url)) url = path.resolve(url);
    else if (
      /^(?:[A-Za-z0-9._-]+@)?[A-Za-z0-9][A-Za-z0-9._-]*:[^\x00-\x20]+$/.test(url) &&
      !url.includes("://")
    ) {
      if (url.startsWith("ext:") || url.includes("::")) return yield* problem("profile_invalid");
    } else {
      const parsed = yield* Effect.try({
        try: () => new URL(url),
        catch: (cause) => causedProblem("source", "profile_invalid", cause),
      });
      if (
        !["https:", "http:", "ssh:", "git:", "file:"].includes(parsed.protocol) ||
        parsed.password ||
        parsed.search ||
        parsed.hash ||
        (parsed.protocol !== "ssh:" && parsed.username)
      )
        return yield* problem("profile_invalid");
    }
    return {
      url,
      remoteDigest: releaseDigest({ remote: main.remote, url: originalUrl, ref: main.ref }),
    };
  });
  const remoteObservation = Effect.fn("WorkbenchReleaseEvidence.remoteObservation")(function* (
    cwd: string,
    main: WorkbenchReleaseHostProfile["policy"]["main"],
  ) {
    const definition = yield* remoteDefinition(cwd, main);
    const observer = path.join(root, "git-observer");
    yield* fs.makeDirectory(observer, { recursive: true, mode: 0o700 });
    if (yield* fs.exists(path.join(observer, ".git")))
      return yield* problem("evidence_unavailable");
    const result = yield* executeGit({
      operation: "release.remoteObservation",
      cwd: observer,
      args: [
        "ls-remote",
        "--refs",
        "--exit-code",
        "--upload-pack=git-upload-pack",
        definition.url,
        main.ref,
      ],
      maxOutputBytes: 16384,
    });
    return { ...result, remoteDigest: definition.remoteDigest };
  });
  const authorize = Effect.fn("WorkbenchReleaseEvidence.authorize")(function* (
    queueId: string,
    scope: Scope,
  ) {
    const [binding] =
      yield* sql`SELECT 1 FROM workbench_queues q,scheduled_task_execution_metadata m WHERE q.id=${queueId} AND q.controller_id=${scope.controllerId} AND q.project_id=${scope.projectId} AND m.singleton=1 AND m.tracking_id=${scope.expectedTrackingId}`;
    if (!binding) return yield* new WorkbenchReleaseError({ reason: "invalid_binding" });
  });
  const read = Effect.fn("WorkbenchReleaseEvidence.read")(function* (
    queueId: string,
    id: string,
    scope: Scope,
  ) {
    yield* authorize(queueId, scope);
    const [row] =
      yield* sql<Row>`SELECT * FROM workbench_release_preparations WHERE id=${id} AND queue_id=${queueId} AND controller_id=${scope.controllerId} AND project_id=${scope.projectId} AND tracking_id=${scope.expectedTrackingId}`;
    if (!row) return yield* new WorkbenchReleaseError({ reason: "not_found" });
    return row;
  });
  const envelope = Effect.fn("WorkbenchReleaseEvidence.envelope")(function* (
    queueId: string,
    scope: Scope,
    p: WorkbenchReleasePreparation,
  ) {
    return {
      environmentId: yield* environment.getEnvironmentId,
      trackingId: scope.expectedTrackingId,
      queueId,
      controllerId: scope.controllerId,
      projectId: scope.projectId,
      preparation: p,
    };
  });
  const save = Effect.fn("WorkbenchReleaseEvidence.save")(function* (
    p: WorkbenchReleasePreparation,
  ) {
    yield* sql`UPDATE workbench_release_preparations SET preparation_json=${canonicalReleaseJson(p)},state=${p.state},updated_at=${p.updatedAt} WHERE id=${p.id}`;
  });
  const profile = Effect.fn("WorkbenchReleaseEvidence.profile")(function* (
    id: string,
    hash: string,
    projectId: string,
  ) {
    const registry = yield* readReleaseProfiles().pipe(
      Effect.mapError((cause) => causedProblem("profile", "profile_invalid", cause)),
    );
    const found = registry.profiles.find(
      (p) => p.id === id && p.projectId === projectId && p.enabled,
    );
    if (!found) return yield* problem("profile_missing");
    if (releaseDigest(found) !== hash) return yield* problem("profile_changed");
    yield* releaseProfileScripts(found).pipe(
      Effect.mapError((cause) => causedProblem("profile", "profile_changed", cause)),
    );
    return found;
  });
  const task = Effect.fn("WorkbenchReleaseEvidence.task")(function* (
    queueId: string,
    input: WorkbenchReleasePrepareInput,
  ) {
    yield* authorize(queueId, input);
    const [row] =
      yield* sql<Task>`SELECT w.id,w.task_id,w.task_created_at,w.project_id,w.dispatcher_thread_id,w.work_thread_id,w.worktree_path,w.branch,w.cycle_number,w.version,w.latest_event_sequence,w.processed_through_sequence,w.business_state,w.attention_hold,t.payload_json AS thread_json,p.workspace_root
      FROM workbench_work_items w JOIN workbench_queue_bindings b ON b.task_id=w.task_id AND b.task_created_at=w.task_created_at AND b.queue_id=${queueId} AND b.project_id=w.project_id AND b.dispatcher_thread_id=w.dispatcher_thread_id
      JOIN scheduled_tasks st ON st.task_id=w.task_id AND st.created_at=w.task_created_at AND st.project_id=w.project_id AND st.thread_id=w.dispatcher_thread_id
      JOIN orchestration_v2_projection_threads t ON t.thread_id=w.work_thread_id AND t.project_id=w.project_id AND t.deleted_at IS NULL
      JOIN projection_projects p ON p.project_id=w.project_id AND p.deleted_at IS NULL
      WHERE w.id=${input.workItemId} AND w.project_id=${input.projectId}`;
    if (!row) return yield* problem("binding_changed");
    if (
      row.version !== input.expectedWorkItemVersion ||
      row.latest_event_sequence !== row.processed_through_sequence ||
      row.business_state !== "awaiting_delivery" ||
      row.attention_hold !== 0 ||
      !row.worktree_path ||
      !row.branch
    )
      return yield* problem("task_not_ready");
    const active = yield* sql`WITH RECURSIVE related(thread_id) AS (
      SELECT ${row.work_thread_id} UNION SELECT s.child_thread_id FROM orchestration_v2_projection_subagents s JOIN related r ON s.thread_id=r.thread_id WHERE s.child_thread_id IS NOT NULL
    ) SELECT 1 FROM orchestration_v2_projection_runs WHERE thread_id IN (SELECT thread_id FROM related) AND status IN ('preparing','starting','running','waiting','queued')
      UNION SELECT 1 FROM orchestration_v2_projection_subagents WHERE thread_id IN (SELECT thread_id FROM related) AND (status IN ('pending','running','waiting') OR completion_delivery_state IN ('pending','claimed'))
      UNION SELECT 1 FROM orchestration_v2_projection_provider_threads WHERE thread_id IN (SELECT thread_id FROM related) AND json_array_length(json_extract(payload_json,'$.pendingBackgroundTasks'))>0
      UNION SELECT 1 FROM orchestration_v2_projection_runtime_requests WHERE thread_id IN (SELECT thread_id FROM related) AND status='pending'
      UNION SELECT 1 FROM workbench_work_batches WHERE work_item_id=${row.id} AND state!='completed'
      UNION SELECT 1 FROM workbench_runtime_responses WHERE work_item_id=${row.id} AND state IN ('recorded','queued','submitted','unknown') LIMIT 1`;
    if (active.length) return yield* problem("work_active");
    const t = yield* decodeThread(row.thread_json);
    if (
      t.projectId !== row.project_id ||
      t.worktreePath !== row.worktree_path ||
      t.branch !== row.branch
    )
      return yield* problem("binding_changed");
    return row;
  });
  const localFence = Effect.fn("WorkbenchReleaseEvidence.localFence")(function* (
    queueId: string,
    input: WorkbenchReleasePrepareInput,
    original: Source,
    main: WorkbenchReleaseHostProfile["policy"]["main"],
  ) {
    const w = original.task;
    const run = (args: ReadonlyArray<string>) =>
      git.execute({
        operation: "WorkbenchReleaseEvidence.localFence",
        cwd: w.worktree_path,
        args,
        timeoutMs: 10000,
        maxOutputBytes: MAX_DIFF + 1,
      });
    const before = yield* task(queueId, input);
    if (canonicalReleaseJson(before) !== canonicalReleaseJson(w))
      return yield* problem("source_changed");
    if ((yield* remoteDefinition(w.worktree_path, main)).remoteDigest !== original.remoteDigest)
      return yield* problem("source_changed");
    const branch = yield* run(["symbolic-ref", "--quiet", "--short", "HEAD"]);
    const head = (yield* git.resolveCommit({ cwd: w.worktree_path, revision: "HEAD" })).commitSha;
    const tree = yield* run(["rev-parse", "--verify", "HEAD^{tree}"]);
    const clean = yield* run(["status", "--porcelain=v1", "--untracked-files=all"]);
    if (clean.stdoutTruncated || clean.stdout.trim()) return yield* problem("source_dirty");
    const common = yield* run(["rev-parse", "--git-common-dir"]);
    if (
      branch.stdoutTruncated ||
      tree.stdoutTruncated ||
      common.stdoutTruncated ||
      branch.stdout.trim() !== w.branch ||
      head !== original.head ||
      tree.stdout.trim() !== original.tree ||
      (yield* fs.realPath(w.worktree_path)) !== original.real ||
      (yield* fs.realPath(path.resolve(w.worktree_path, common.stdout.trim()))) !==
        original.common ||
      (yield* git.resolveCommit({ cwd: w.worktree_path, revision: "HEAD" })).commitSha !==
        original.head
    )
      return yield* problem("source_changed");
    if (canonicalReleaseJson(yield* task(queueId, input)) !== canonicalReleaseJson(w))
      return yield* problem("source_changed");
  });
  const saveFenced = Effect.fn("WorkbenchReleaseEvidence.saveFenced")(function* (
    queueId: string,
    input: WorkbenchReleasePrepareInput,
    original: Source,
    p: WorkbenchReleasePreparation,
  ) {
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const checked = yield* task(queueId, input).pipe(
          Effect.tapError((cause) =>
            reportFailure(
              "source",
              isPreparationProblem(cause)
                ? (cause.reason as WorkbenchReleaseReason)
                : "evidence_unavailable",
              cause,
            ),
          ),
          Effect.match({
            onSuccess: (value) =>
              canonicalReleaseJson(value) === canonicalReleaseJson(original.task)
                ? null
                : ("source_changed" as const),
            onFailure: (e) =>
              isPreparationProblem(e)
                ? (e.reason as WorkbenchReleaseReason)
                : ("evidence_unavailable" as const),
          }),
        );
        const result: WorkbenchReleasePreparation = checked
          ? {
              ...p,
              state: checked === "evidence_unavailable" ? "unknown" : "invalidated",
              reasons: [checked],
              validation: {
                ...p.validation,
                state: checked === "evidence_unavailable" ? "unknown" : "invalidated",
              },
            }
          : p;
        yield* save(result);
        return result;
      }),
    );
  });
  const source = Effect.fn("WorkbenchReleaseEvidence.source")(function* (
    queueId: string,
    input: WorkbenchReleasePrepareInput,
    p: WorkbenchReleaseHostProfile,
  ) {
    const w = yield* task(queueId, input);
    const run = (args: ReadonlyArray<string>) =>
      git.execute({
        operation: "WorkbenchReleaseEvidence.source",
        cwd: w.worktree_path,
        args,
        timeoutMs: 10000,
        maxOutputBytes: MAX_DIFF + 1,
      });
    const real = yield* fs.realPath(w.worktree_path);
    const common = yield* fs.realPath(
      path.resolve(real, (yield* run(["rev-parse", "--git-common-dir"])).stdout.trim()),
    );
    const projectCommon = yield* git.execute({
      operation: "WorkbenchReleaseEvidence.project",
      cwd: w.workspace_root,
      args: ["rev-parse", "--git-common-dir"],
    });
    if (
      common !==
        (yield* fs.realPath(path.resolve(w.workspace_root, projectCommon.stdout.trim()))) ||
      !(yield* git.listWorktreePaths(w.workspace_root)).includes(real)
    )
      return yield* problem("binding_changed");
    const branch = yield* run(["symbolic-ref", "--quiet", "--short", "HEAD"]);
    if (branch.stdoutTruncated || branch.stdout.trim() !== w.branch)
      return yield* problem("source_changed");
    const status = yield* run(["status", "--porcelain=v1", "--untracked-files=all"]);
    if (status.stdoutTruncated || status.stdout.trim()) return yield* problem("source_dirty");
    const head = (yield* git.resolveCommit({ cwd: real, revision: "HEAD" })).commitSha;
    const tree = (yield* run(["rev-parse", "--verify", `${head}^{tree}`])).stdout.trim();
    const remote = yield* remoteObservation(real, p.policy.main);
    const line = remote.stdout.trim().split(/\r?\n/);
    const match = /^([a-f0-9]{40}|[a-f0-9]{64})\s+(.+)$/.exec(line[0] ?? "");
    if (remote.stdoutTruncated || line.length !== 1 || !match || match[2] !== p.policy.main.ref)
      return yield* problem("evidence_unavailable");
    const base = match[1]!;
    const integrated = yield* git.execute({
      operation: "WorkbenchReleaseEvidence.ancestry",
      cwd: real,
      args: ["merge-base", "--is-ancestor", base, head],
      allowNonZeroExit: true,
    });
    if (integrated.exitCode !== 0) return yield* problem("base_not_integrated");
    const observed = { task: w, real, common, head, tree, base, remoteDigest: remote.remoteDigest };
    yield* localFence(queueId, input, observed, p.policy.main);
    return observed satisfies Source;
  });
  const regular = Effect.fn("WorkbenchReleaseEvidence.regular")(function* (
    parent: string,
    relative: string,
    reason: WorkbenchReleaseReason,
  ) {
    const invalid = reason === "artifact_missing" ? "artifact_invalid" : reason;
    if (!safeReleasePath(relative)) return yield* problem(invalid);
    let current = parent;
    for (const part of relative.split("/")) {
      current = path.join(current, part);
      const info = yield* fs
        .stat(current)
        .pipe(Effect.mapError((cause) => causedProblem("artifact", reason, cause)));
      if (info.type === "SymbolicLink") return yield* problem(invalid);
      if ((yield* fs.realPath(current)) !== current) return yield* problem(invalid);
    }
    const info = yield* fs
      .stat(current)
      .pipe(Effect.mapError((cause) => causedProblem("artifact", reason, cause)));
    if (info.type !== "File" || info.size > MAX_ARTIFACT) return yield* problem(invalid);
    const bytes = yield* fs
      .readFile(current)
      .pipe(Effect.mapError((cause) => causedProblem("artifact", reason, cause)));
    if (bytes.byteLength !== Number(info.size) || bytes.byteLength > MAX_ARTIFACT)
      return yield* problem(reason);
    return bytes;
  });
  const target = Effect.fn("WorkbenchReleaseEvidence.target")(function* (
    p: WorkbenchReleaseHostProfile,
  ) {
    const result = yield* Effect.tryPromise({
      try: async (signal) => {
        // @effect-diagnostics-next-line globalFetchInEffect:off -- This pinned endpoint adapter bounds bytes, disables redirects and forwards cancellation.
        const response = await fetch(new URL(p.policy.target.versionPath, p.policy.target.origin), {
          signal,
          redirect: "error",
          credentials: "omit",
          headers: { accept: "application/json" },
        });
        if (!response.ok || !response.body) throw new Error("Target unavailable");
        const reader = response.body.getReader();
        let size = 0;
        const chunks: Uint8Array[] = [];
        try {
          for (;;) {
            const next = await reader.read();
            if (next.done) break;
            size += next.value.byteLength;
            if (size > 16384) throw new Error("Target too large");
            chunks.push(next.value);
          }
        } finally {
          await reader.cancel();
        }
        const bytes = new Uint8Array(size);
        let offset = 0;
        for (const c of chunks) {
          bytes.set(c, offset);
          offset += c.byteLength;
        }
        return new TextDecoder("utf-8", { fatal: true }).decode(bytes);
      },
      catch: (cause) => causedProblem("target", "target_unavailable", cause),
    }).pipe(
      Effect.timeout("10 seconds"),
      Effect.mapError((cause) => causedProblem("target", "target_unavailable", cause)),
    );
    const decoded = yield* decodeTarget(yield* decodeJson(result)).pipe(
      Effect.mapError((cause) => causedProblem("target", "target_unavailable", cause)),
    );
    if (decoded.serviceId !== p.policy.target.serviceId) return yield* problem("target_changed");
    return {
      ...p.policy.target,
      basis: {
        version: decoded.version,
        sourceSha: decoded.sourceSha,
        artifactSha256: decoded.artifactSha256,
      },
      observedAt: yield* stamp,
    };
  });
  const catalog = Effect.fn("WorkbenchReleaseEvidence.catalog")(
    function* (query: { projectId?: string | undefined }) {
      const registry = yield* readReleaseProfiles();
      const profiles = yield* Effect.forEach(
        registry.profiles.filter(
          (p) => p.enabled && (!query.projectId || p.projectId === query.projectId),
        ),
        publicReleaseProfile,
      );
      return {
        environmentId: yield* environment.getEnvironmentId,
        configured: registry.configured,
        profiles,
      };
    },
    Effect.provide(context),
    Effect.catch((cause) => publicFailure("catalog", cause)),
  );
  const get = Effect.fn("WorkbenchReleaseEvidence.get")(
    function* (queueId: string, id: string, scope: Scope) {
      const row = yield* read(queueId, id, scope);
      let p = yield* decodePreparation(row.preparation_json);
      if ((p.state === "preparing" || p.state === "queued") && !jobs.has(id)) {
        const now = yield* stamp;
        p = {
          ...p,
          state: "unknown",
          reasons: ["preparation_interrupted"],
          version: p.version + 1,
          updatedAt: now,
          validation: { state: "unknown", version: p.validation.version + 1, observedAt: now },
        };
        yield* save(p);
      }
      return yield* envelope(queueId, scope, p);
    },
    Effect.catch((cause) => publicFailure("get", cause)),
  );
  const revalidate = Effect.fn("WorkbenchReleaseEvidence.revalidate")(
    function* (queueId: string, id: string, input: WorkbenchReleaseRevalidateInput) {
      return yield* locks.withLock(
        id,
        Effect.gen(function* () {
          const row = yield* read(queueId, id, input),
            p = yield* decodePreparation(row.preparation_json);
          if (!p.evidence || p.evidence.digest !== input.expectedEvidenceDigest)
            return yield* new WorkbenchReleaseError({ reason: "conflict" });
          if (p.state === "invalidated") return yield* envelope(queueId, input, p);
          const evidence = p.evidence;
          const original = (yield* decodeJson(row.snapshot_json)) as Source;
          const prepInput = yield* decodePrepareInput(row.input_json);
          const checked = yield* Effect.gen(function* () {
            const registered = yield* profile(
              prepInput.profileId,
              prepInput.profileDigest,
              prepInput.projectId,
            );
            const observed = yield* source(queueId, prepInput, registered);
            if (observed.base !== original.base) return yield* problem("base_changed");
            if (canonicalReleaseJson(observed) !== canonicalReleaseJson(original))
              return yield* problem("source_changed");
            const bytes = yield* regular(directory(id), "artifact", "artifact_changed");
            const rollback = yield* regular(directory(id), "rollback", "rollback_mismatch");
            const diffBytes = yield* regular(directory(id), "diff", "artifact_changed");
            if (diffBytes.byteLength > MAX_DIFF || releaseHash(diffBytes) !== evidence.diff.digest)
              return yield* problem("artifact_changed");
            if (
              releaseHash(bytes) !== evidence.artifact.sha256 ||
              releaseHash(rollback) !== evidence.rollback.artifact.sha256
            )
              return yield* problem("artifact_changed");
            const current = yield* target(registered);
            if (canonicalReleaseJson(current.basis) !== canonicalReleaseJson(evidence.target.basis))
              return yield* problem("target_changed");
            const baseline = yield* regular(
              observed.real,
              registered.policy.rollback.artifactRelativePath,
              "rollback_missing",
            );
            if (releaseHash(baseline) !== evidence.rollback.artifact.sha256)
              return yield* problem("rollback_mismatch");
            const { digest, ...unsigned } = evidence;
            if (releaseDigest(unsigned) !== digest) return yield* problem("evidence_unavailable");
            if (registered.policy.deployment) {
              if (!evidence.deployment) return yield* problem("evidence_unavailable");
              yield* adapter
                .verify(
                  registered,
                  evidence.deployment,
                  Adapter.deploymentPreflightInput(
                    evidence.deployment.preflight.id,
                    evidence.deployment.plan,
                    evidence.source.commitSha,
                    { sha256: evidence.artifact.sha256, bytes: evidence.artifact.bytes },
                    {
                      basis: { serviceId: evidence.target.serviceId, ...evidence.target.basis },
                      revision: evidence.deployment.baselineRevision,
                      artifact: {
                        sha256: evidence.rollback.artifact.sha256,
                        bytes: evidence.rollback.artifact.bytes,
                      },
                    },
                  ),
                )
                .pipe(
                  Effect.mapError((cause) =>
                    causedProblem(
                      "target",
                      [
                        "adapter_unavailable",
                        "coordinator_unavailable",
                        "target_unavailable",
                      ].includes(cause.reason)
                        ? "target_unavailable"
                        : cause.reason === "target_changed"
                          ? "target_changed"
                          : "preflight_failed",
                      cause,
                    ),
                  ),
                );
            }
            if (evidence.verification)
              yield* verificationExecutor
                .validate(directory(id), evidence, registered)
                .pipe(
                  Effect.mapError((cause) => causedProblem("artifact", "artifact_changed", cause)),
                );
            const finalProfile = yield* profile(
              prepInput.profileId,
              prepInput.profileDigest,
              prepInput.projectId,
            );
            const finalSource = yield* source(queueId, prepInput, finalProfile);
            if (finalSource.base !== original.base) return yield* problem("base_changed");
            if (canonicalReleaseJson(finalSource) !== canonicalReleaseJson(original))
              return yield* problem("source_changed");
            const finalTarget = yield* target(finalProfile);
            if (
              canonicalReleaseJson(finalTarget.basis) !==
              canonicalReleaseJson(evidence.target.basis)
            )
              return yield* problem("target_changed");
            yield* profile(prepInput.profileId, prepInput.profileDigest, prepInput.projectId);
            if (
              releaseHash(yield* regular(directory(id), "artifact", "artifact_changed")) !==
                evidence.artifact.sha256 ||
              releaseHash(yield* regular(directory(id), "rollback", "rollback_mismatch")) !==
                evidence.rollback.artifact.sha256 ||
              releaseHash(yield* regular(directory(id), "diff", "artifact_changed")) !==
                evidence.diff.digest
            )
              return yield* problem("artifact_changed");
            yield* localFence(queueId, prepInput, original, finalProfile.policy.main);
          }).pipe(
            Effect.tapError((cause) =>
              reportFailure(
                "revalidate",
                isPreparationProblem(cause)
                  ? (cause.reason as WorkbenchReleaseReason)
                  : "evidence_unavailable",
                cause,
              ),
            ),
            Effect.match({
              onSuccess: () => null,
              onFailure: (error) =>
                isPreparationProblem(error)
                  ? (error.reason as WorkbenchReleaseReason)
                  : ("evidence_unavailable" as const),
            }),
          );
          const now = yield* stamp;
          const unknown =
            checked === "target_unavailable" ||
            checked === "evidence_unavailable" ||
            checked === "timeout" ||
            checked === "output_limit";
          const next: WorkbenchReleasePreparation = {
            ...p,
            state: checked ? (unknown ? "unknown" : "invalidated") : "ready",
            reasons: checked ? [checked] : [],
            version: p.version + 1,
            updatedAt: now,
            validation: {
              state: checked ? (unknown ? "unknown" : "invalidated") : "current",
              version: p.validation.version + 1,
              observedAt: now,
            },
          };
          let saved = next;
          if (next.state === "ready") saved = yield* saveFenced(queueId, prepInput, original, next);
          else yield* save(next);
          return yield* envelope(queueId, input, saved);
        }),
      );
    },
    Effect.provide(context),
    Effect.catch((cause) => publicFailure("revalidate", cause)),
  );
  const produce = Effect.fn("WorkbenchReleaseEvidence.produce")(function* (
    queueId: string,
    input: WorkbenchReleasePrepareInput,
    p: WorkbenchReleaseHostProfile,
    initial: Source,
  ) {
    const id = input.preparationId,
      dir = directory(id),
      snapshot = path.join(dir, "source");
    const loaded = yield* read(queueId, id, input);
    let state = yield* decodePreparation(loaded.preparation_json);
    state = { ...state, state: "preparing", updatedAt: yield* stamp };
    yield* save(state);
    yield* fs.makeDirectory(snapshot, { recursive: true, mode: 0o700 });
    const list = yield* git.execute({
      operation: "WorkbenchReleaseEvidence.tree",
      cwd: initial.real,
      args: ["ls-tree", "-rz", "--full-tree", initial.head],
      maxOutputBytes: MAX_DIFF,
    });
    if (list.stdoutTruncated) return yield* problem("artifact_invalid");
    const tracked = list.stdout
      .split("\0")
      .filter(Boolean)
      .map((line) => /^([0-9]+) blob ([a-f0-9]+)\t(.+)$/.exec(line));
    if (tracked.some((m) => !m || !["100644", "100755"].includes(m[1]!) || !safeReleasePath(m[3]!)))
      return yield* problem("artifact_invalid");
    const archive = path.join(dir, "source.tar");
    yield* git.execute({
      operation: "WorkbenchReleaseEvidence.export",
      cwd: initial.real,
      args: ["archive", "--format=tar", `--output=${archive}`, initial.head],
    });
    const unpack = yield* runner
      .run({
        command: "/usr/bin/tar",
        args: ["-xf", archive, "-C", snapshot],
        env: { PATH: "/usr/bin:/bin" },
        extendEnv: false,
        timeout: "120 seconds",
        maxOutputBytes: MAX_DIFF,
      })
      .pipe(Effect.mapError((cause) => causedProblem("snapshot", "artifact_invalid", cause)));
    if (unpack.code !== 0)
      return yield* causedProblem(
        "snapshot",
        "artifact_invalid",
        new ReleaseExecutionExitError({ exitCode: unpack.code, cause: unpack }),
      );
    const unchanged = Effect.fn("WorkbenchReleaseEvidence.snapshotUnchanged")(function* () {
      for (const m of tracked) {
        const bytes = yield* regular(snapshot, m![3]!, "source_changed");
        if (releaseBlobHash(bytes, m![2]!.length === 40 ? "sha1" : "sha256") !== m![2])
          return yield* problem("source_changed");
      }
    });
    yield* unchanged();
    const scripts = yield* releaseProfileScripts(p),
      n = p.policy.checks.length;
    const verification = yield* verificationExecutor
      .seal({
        directory: dir,
        preparationId: id,
        sourceSha: initial.head,
        treeSha: initial.tree,
        originalSourceRoots: [
          initial.real,
          initial.task.worktree_path,
          initial.task.workspace_root,
        ],
        tracked: tracked.map((m) => ({
          path: m![3]!,
          mode: m![1]! as "100644" | "100755",
          blobSha: m![2]!,
        })),
        health: scripts[n + 1]!,
        core: scripts[n + 2]!,
        profile: p,
      })
      .pipe(Effect.mapError((cause) => causedProblem("snapshot", "artifact_invalid", cause)));
    yield* fs.remove(archive);
    const observedTarget = yield* target(p);
    const rollback = yield* regular(
      initial.real,
      p.policy.rollback.artifactRelativePath,
      "rollback_missing",
    );
    const rollbackHash = releaseHash(rollback);
    if (rollbackHash !== observedTarget.basis.artifactSha256)
      return yield* problem("rollback_mismatch");
    yield* fs.writeFile(path.join(dir, "rollback"), rollback, { mode: 0o400 });
    yield* git.execute({
      operation: "WorkbenchReleaseEvidence.diff",
      cwd: initial.real,
      args: [
        "-c",
        "core.quotePath=true",
        "diff",
        "--no-ext-diff",
        "--no-textconv",
        "--binary",
        `--output=${path.join(dir, "diff")}`,
        initial.base,
        initial.head,
        "--",
      ],
      maxOutputBytes: MAX_DIFF,
      appendTruncationMarker: false,
    });
    const patchStat = yield* fs.stat(path.join(dir, "diff"));
    if (patchStat.size > MAX_DIFF) return yield* problem("diff_too_large");
    const patchBytes = yield* regular(dir, "diff", "evidence_unavailable");
    yield* Effect.try({
      try: () => new TextDecoder("utf-8", { fatal: true }).decode(patchBytes),
      catch: (cause) => causedProblem("snapshot", "evidence_unavailable", cause),
    });
    yield* fs.chmod(path.join(dir, "diff"), 0o400);
    const stats = yield* git.execute({
      operation: "WorkbenchReleaseEvidence.diffStats",
      cwd: initial.real,
      args: [
        "diff",
        "--no-ext-diff",
        "--no-textconv",
        "--numstat",
        initial.base,
        initial.head,
        "--",
      ],
      maxOutputBytes: MAX_DIFF,
    });
    if (stats.stdoutTruncated) return yield* problem("diff_too_large");
    const records = stats.stdout.trim() ? stats.stdout.trim().split("\n") : [];
    let insertions = 0,
      deletions = 0;
    for (const r of records) {
      const [a, b] = r.split("\t");
      insertions += Number(a) || 0;
      deletions += Number(b) || 0;
    }
    const receipts: WorkbenchReleaseReceipt[] = [];
    let artifactHash: string | null = null;
    let artifactBytes = 0;
    const roles = [
      "build",
      ...p.policy.checks.map(() => "check" as const),
      "health",
      "core",
      "rollback_verify",
    ] as const;
    const selected = [
      scripts[0]!,
      ...scripts.slice(1, p.policy.checks.length + 3),
      scripts.at(-1)!,
    ];
    for (let i = 0; i < roles.length; i++) {
      const role = roles[i]!,
        script = selected[i]!,
        startedAt = yield* stamp;
      yield* unchanged();
      yield* sql`INSERT INTO workbench_release_stages(preparation_id,ordinal,role,state,started_at) VALUES(${id},${i},${role},'running',${startedAt})`;
      const argv = releaseCommandArgv(script.command);
      if (!argv) return yield* problem("profile_changed");
      const [command, ...args] = argv;
      const env = {
        PATH: path.dirname(process.execPath) + ":/usr/bin:/bin",
        HOME: dir,
        TMPDIR: dir,
        GIT_CEILING_DIRECTORIES: dir,
        GIT_CONFIG_NOSYSTEM: "1",
        GIT_CONFIG_GLOBAL: path.join(dir, "empty.gitconfig"),
        T3_RELEASE_SOURCE_SHA: initial.head,
        T3_RELEASE_ARTIFACT: path.join(dir, "artifact"),
        T3_RELEASE_ROLLBACK_ARTIFACT: path.join(dir, "rollback"),
      };
      const result = yield* runner
        .run({
          command: command!,
          args,
          cwd: snapshot,
          env,
          extendEnv: false,
          timeout: "120 seconds",
          maxOutputBytes: MAX_DIFF,
          outputMode: "error",
        })
        .pipe(
          Effect.mapError((e) =>
            causedProblem(
              role,
              e._tag === "ProcessTimeoutError"
                ? "timeout"
                : e._tag === "ProcessOutputLimitError"
                  ? "output_limit"
                  : role === "build"
                    ? "build_failed"
                    : role === "check"
                      ? "check_failed"
                      : role === "rollback_verify"
                        ? "rollback_failed"
                        : "preflight_failed",
              e,
            ),
          ),
        );
      const completedAt = yield* stamp,
        outputDigest = releaseDigest({ stdout: result.stdout, stderr: result.stderr });
      if (
        result.code !== 0 ||
        result.timedOut ||
        result.stdoutTruncated ||
        result.stderrTruncated
      ) {
        yield* sql`UPDATE workbench_release_stages SET state='failed',exit_code=${result.code},output_digest=${outputDigest},completed_at=${completedAt} WHERE preparation_id=${id} AND ordinal=${i}`;
        return yield* causedProblem(
          role,
          role === "build"
            ? "build_failed"
            : role === "check"
              ? "check_failed"
              : role === "rollback_verify"
                ? "rollback_failed"
                : "preflight_failed",
          new ReleaseExecutionExitError({ exitCode: result.code, cause: result }),
        );
      }
      yield* unchanged();
      if (role === "build") {
        const bytes = yield* regular(snapshot, p.policy.artifact.relativePath, "artifact_missing");
        artifactHash = releaseHash(bytes);
        artifactBytes = bytes.byteLength;
        yield* fs.writeFile(path.join(dir, "artifact"), bytes, { mode: 0o400 });
      }
      if (
        releaseHash(yield* regular(dir, "artifact", "artifact_changed")) !== artifactHash ||
        releaseHash(yield* regular(dir, "rollback", "rollback_mismatch")) !== rollbackHash
      )
        return yield* problem("artifact_changed");
      const receipt: WorkbenchReleaseReceipt = {
        id: `stage:${releaseHash(id)}:${i}`,
        role,
        phase: role === "build" ? "build" : role === "check" ? "source_check" : "preflight",
        scriptId: script.id,
        definitionHash: releaseScriptHash(script),
        subject:
          role === "build" || role === "check"
            ? "source"
            : role === "rollback_verify"
              ? "rollback_artifact"
              : "candidate_artifact",
        sourceSha: initial.head,
        profileDigest: input.profileDigest,
        subjectArtifactSha256:
          role === "build" || role === "check"
            ? null
            : role === "rollback_verify"
              ? rollbackHash
              : artifactHash,
        outputArtifactSha256: role === "build" ? artifactHash : null,
        exitCode: 0,
        outputDigest,
        startedAt,
        completedAt,
      };
      receipts.push(receipt);
      yield* sql`UPDATE workbench_release_stages SET state='completed',receipt_json=${canonicalReleaseJson(receipt)},exit_code=0,output_digest=${outputDigest},completed_at=${completedAt} WHERE preparation_id=${id} AND ordinal=${i}`;
    }
    const deployment = p.policy.deployment
      ? yield* adapter
          .prepare(
            p,
            id,
            initial.head,
            yield* regular(dir, "artifact", "artifact_changed"),
            rollback,
            { serviceId: observedTarget.serviceId, ...observedTarget.basis },
          )
          .pipe(
            Effect.mapError((cause) =>
              causedProblem(
                "target",
                ["adapter_unavailable", "coordinator_unavailable", "target_unavailable"].includes(
                  cause.reason,
                )
                  ? "target_unavailable"
                  : cause.reason === "target_changed"
                    ? "target_changed"
                    : "preflight_failed",
                cause,
              ),
            ),
          )
      : undefined;
    const merge = p.policy.merge
      ? yield* Effect.gen(function* () {
          if (Option.isNone(releaseRefs)) return yield* problem("preflight_failed");
          const sourceRemote = yield* remoteDefinition(initial.real, p.policy.main);
          if (!releaseRefs.value.matchesSource(sourceRemote.url, p.policy.merge!))
            return yield* problem("binding_changed");
          const subject = {
            baseSha: initial.base,
            sourceSha: initial.head,
            sourceTreeSha: initial.tree,
          };
          const preflight = yield* releaseRefs.value
            .inspect({ cwd: initial.real, plan: p.policy.merge!, subject })
            .pipe(Effect.mapError((cause) => causedProblem("source", "preflight_failed", cause)));
          const reason = mergeObservationReason(p.policy.merge!, subject, preflight);
          if (reason)
            return yield* causedProblem(
              "source",
              "preflight_failed",
              new ReleaseRefs.ReleaseRefsError({ reason, stage: "merge_preflight" }),
            );
          return {
            schemaVersion: 1 as const,
            planDigest: releaseDigest(p.policy.merge),
            preflight,
          };
        })
      : undefined;
    const finalProfile = yield* profile(input.profileId, input.profileDigest, input.projectId);
    const finalSource = yield* source(queueId, input, finalProfile);
    if (finalSource.base !== initial.base) return yield* problem("base_changed");
    if (canonicalReleaseJson(finalSource) !== canonicalReleaseJson(initial))
      return yield* problem("source_changed");
    const finalTarget = yield* target(finalProfile);
    if (canonicalReleaseJson(finalTarget.basis) !== canonicalReleaseJson(observedTarget.basis))
      return yield* problem("target_changed");
    if (
      releaseHash(
        yield* regular(initial.real, p.policy.rollback.artifactRelativePath, "rollback_missing"),
      ) !== rollbackHash
    )
      return yield* problem("rollback_mismatch");
    const sealedProfile = yield* profile(input.profileId, input.profileDigest, input.projectId);
    const sealedSource = yield* source(queueId, input, sealedProfile);
    if (sealedSource.base !== initial.base) return yield* problem("base_changed");
    if (canonicalReleaseJson(sealedSource) !== canonicalReleaseJson(initial))
      return yield* problem("source_changed");
    const lastTarget = yield* target(sealedProfile);
    if (canonicalReleaseJson(lastTarget.basis) !== canonicalReleaseJson(observedTarget.basis))
      return yield* problem("target_changed");
    yield* profile(input.profileId, input.profileDigest, input.projectId);
    if (
      releaseHash(yield* regular(dir, "artifact", "artifact_changed")) !== artifactHash ||
      releaseHash(yield* regular(dir, "rollback", "rollback_mismatch")) !== rollbackHash ||
      releaseHash(yield* regular(dir, "diff", "artifact_changed")) !== releaseHash(patchBytes)
    )
      return yield* problem("artifact_changed");
    yield* localFence(queueId, input, initial, sealedProfile.policy.main);
    const now = yield* stamp,
      w = initial.task;
    const unsigned = {
      verification,
      ...(merge ? { merge } : {}),
      ...(deployment ? { deployment } : {}),
      schemaVersion: 1 as const,
      id: `evidence:${releaseHash(id)}`,
      preparationId: id,
      createdAt: now,
      binding: {
        environmentId: yield* environment.getEnvironmentId,
        trackingId: input.expectedTrackingId,
        queueId,
        controllerId: input.controllerId,
        projectId: input.projectId,
        workItemId: w.id,
        taskId: ScheduledTaskId.make(w.task_id),
        taskCreatedAt: w.task_created_at,
        dispatcherThreadId: ThreadId.make(w.dispatcher_thread_id),
        workThreadId: ThreadId.make(w.work_thread_id),
        cycleNumber: w.cycle_number,
        workItemVersion: w.version,
        latestEventSequence: w.latest_event_sequence,
        processedThroughSequence: w.processed_through_sequence,
      },
      profile: { id: p.id, version: p.version, digest: input.profileDigest },
      source: {
        branch: w.branch,
        commitSha: initial.head,
        treeSha: initial.tree,
        clean: true as const,
        main: { ...p.policy.main, commitSha: initial.base, integrated: true as const },
      },
      diff: {
        ref: `diff:${releaseHash(id)}`,
        digest: releaseHash(patchBytes),
        byteCount: patchBytes.byteLength,
        filesChanged: records.length,
        insertions,
        deletions,
        complete: true as const,
      },
      artifact: {
        ref: `artifact:${releaseHash(id)}`,
        sha256: artifactHash!,
        bytes: artifactBytes,
        manifestDigest: releaseDigest([
          {
            relativePath: p.policy.artifact.relativePath,
            bytes: artifactBytes,
            sha256: artifactHash,
          },
        ]),
        fileCount: 1 as const,
      },
      target: observedTarget,
      receipts,
      rollback: {
        artifact: {
          ref: `rollback:${releaseHash(id)}`,
          sha256: rollbackHash,
          bytes: rollback.byteLength,
        },
        basisVersion: observedTarget.basis.version,
        targetServiceId: p.policy.target.serviceId,
        execute: p.policy.rollback.execute,
        verifyReceiptId: receipts.at(-1)!.id,
      },
      observation: p.policy.observation,
    };
    const evidence = yield* decodeEvidence({ ...unsigned, digest: releaseDigest(unsigned) });
    state = {
      ...state,
      state: "ready",
      reasons: [],
      evidence,
      version: state.version + 1,
      updatedAt: now,
      validation: { state: "current", version: state.validation.version + 1, observedAt: now },
    };
    yield* saveFenced(queueId, input, initial, state);
    yield* fs.remove(snapshot, { recursive: true });
  });
  const prepare = Effect.fn("WorkbenchReleaseEvidence.prepare")(
    function* (queueId: string, input: WorkbenchReleasePrepareInput) {
      return yield* locks.withLock(
        input.preparationId,
        Effect.gen(function* () {
          yield* authorize(queueId, input);
          const [prior] =
            yield* sql<Row>`SELECT * FROM workbench_release_preparations WHERE id=${input.preparationId}`;
          if (prior) {
            if (prior.queue_id !== queueId || prior.input_json !== canonicalReleaseJson(input))
              return yield* new WorkbenchReleaseError({ reason: "conflict" });
            return yield* get(queueId, input.preparationId, input);
          }
          const registered = yield* profile(
            input.profileId,
            input.profileDigest,
            input.projectId,
          ).pipe(
            Effect.tapError((cause) => reportFailure("profile", "profile_changed", cause)),
            Effect.mapError(
              (e) =>
                new WorkbenchReleaseError({
                  reason:
                    isPreparationProblem(e) && e.reason === "profile_changed"
                      ? "profile_changed"
                      : "profile_invalid",
                }),
            ),
          );
          const initial = yield* source(queueId, input, registered).pipe(
            Effect.tapError((cause) =>
              reportFailure(
                "source",
                isPreparationProblem(cause)
                  ? (cause.reason as WorkbenchReleaseReason)
                  : "evidence_unavailable",
                cause,
              ),
            ),
            Effect.mapError(() => new WorkbenchReleaseError({ reason: "stale_state" })),
          );
          const now = yield* stamp,
            w = initial.task;
          const p: WorkbenchReleasePreparation = {
            id: input.preparationId,
            workItemId: w.id,
            taskId: ScheduledTaskId.make(w.task_id),
            taskCreatedAt: w.task_created_at,
            dispatcherThreadId: ThreadId.make(w.dispatcher_thread_id),
            workThreadId: ThreadId.make(w.work_thread_id),
            cycleNumber: w.cycle_number,
            workItemVersion: w.version,
            profileDigest: input.profileDigest,
            version: 1,
            state: "queued",
            reasons: [],
            evidence: null,
            validation: { state: "unknown", version: 1, observedAt: now },
            createdAt: now,
            updatedAt: now,
          };
          yield* sql.withTransaction(
            Effect.gen(function* () {
              const current = yield* task(queueId, input);
              if (canonicalReleaseJson(current) !== canonicalReleaseJson(w))
                return yield* new WorkbenchReleaseError({ reason: "stale_state" });
              yield* sql`INSERT OR IGNORE INTO workbench_release_obligations(work_item_id,task_created_at,cycle_number,preparation_id,created_at) VALUES(${w.id},${w.task_created_at},${w.cycle_number},${input.preparationId},${now})`;
              yield* sql`INSERT INTO workbench_release_preparations(id,queue_id,controller_id,project_id,tracking_id,work_item_id,input_json,profile_json,snapshot_json,preparation_json,state,created_at,updated_at) VALUES(${input.preparationId},${queueId},${input.controllerId},${input.projectId},${input.expectedTrackingId},${input.workItemId},${canonicalReleaseJson(input)},${canonicalReleaseJson(registered)},${canonicalReleaseJson(initial)},${canonicalReleaseJson(p)},'queued',${now},${now})`;
            }),
          );
          const done = yield* Deferred.make<void>();
          jobs.set(input.preparationId, done);
          yield* produce(queueId, input, registered, initial).pipe(
            Effect.timeout("20 minutes"),
            Effect.timeout("20 minutes"),
            Effect.catch((error) =>
              Effect.gen(function* () {
                yield* reportFailure(
                  "prepare",
                  isPreparationProblem(error)
                    ? (error.reason as WorkbenchReleaseReason)
                    : "evidence_unavailable",
                  error,
                );
                const row = yield* read(queueId, input.preparationId, input),
                  saved = yield* decodePreparation(row.preparation_json),
                  now = yield* stamp;
                const reason = isPreparationProblem(error)
                  ? (error.reason as WorkbenchReleaseReason)
                  : isReleaseError(error) && error.reason === "profile_changed"
                    ? "profile_changed"
                    : "evidence_unavailable";
                yield* save({
                  ...saved,
                  state: "blocked",
                  reasons: [reason],
                  version: saved.version + 1,
                  updatedAt: now,
                  validation: {
                    state: "unknown",
                    version: saved.validation.version + 1,
                    observedAt: now,
                  },
                });
              }),
            ),
            Effect.ensuring(
              Effect.all([
                fs.remove(path.join(directory(input.preparationId), "source"), {
                  recursive: true,
                  force: true,
                }),
                fs.remove(path.join(directory(input.preparationId), "source.tar"), { force: true }),
              ]).pipe(
                Effect.catch((cause) => reportFailure("cleanup", "evidence_unavailable", cause)),
              ),
            ),
            Effect.ensuring(Deferred.succeed(done, undefined)),
            Effect.ensuring(Effect.sync(() => jobs.delete(input.preparationId))),
            Effect.forkIn(serviceScope),
          );
          return yield* envelope(queueId, input, p);
        }),
      );
    },
    Effect.provide(context),
    Effect.catch((cause) => publicFailure("prepare", cause)),
  );
  const diff = Effect.fn("WorkbenchReleaseEvidence.diff")(
    function* (queueId: string, id: string, scope: Scope) {
      const row = yield* read(queueId, id, scope),
        p = yield* decodePreparation(row.preparation_json);
      if (!p.evidence) return yield* new WorkbenchReleaseError({ reason: "not_found" });
      const bytes = yield* regular(directory(id), "diff", "evidence_unavailable");
      if (bytes.byteLength > MAX_DIFF || releaseHash(bytes) !== p.evidence.diff.digest)
        return yield* new WorkbenchReleaseError({ reason: "unavailable" });
      return {
        environmentId: yield* environment.getEnvironmentId,
        trackingId: scope.expectedTrackingId,
        queueId,
        controllerId: scope.controllerId,
        projectId: scope.projectId,
        preparationId: id,
        diff: {
          ref: p.evidence.diff.ref,
          digest: p.evidence.diff.digest,
          byteCount: bytes.byteLength,
          complete: true as const,
          patch: new TextDecoder("utf-8", { fatal: true }).decode(bytes),
        },
      };
    },
    Effect.catch((cause) => publicFailure("diff", cause)),
  );
  const awaitPrepared = Effect.fn("WorkbenchReleaseEvidence.awaitPrepared")(
    function* (queueId: string, id: string, scope: Scope) {
      yield* authorize(queueId, scope);
      const done = jobs.get(id);
      if (done) yield* Deferred.await(done);
      return yield* get(queueId, id, scope);
    },
    Effect.catch((cause) => publicFailure("await_prepared", cause)),
  );
  const admittedMaterial = Effect.fn("WorkbenchReleaseEvidence.admittedMaterial")(
    function* (queueId: string, id: string, input: WorkbenchReleaseRevalidateInput) {
      const [row] =
        yield* sql<Row>`SELECT * FROM workbench_release_preparations WHERE id=${id} AND queue_id=${queueId} AND controller_id=${input.controllerId} AND project_id=${input.projectId} AND tracking_id=${input.expectedTrackingId}`;
      if (!row) return yield* new WorkbenchReleaseError({ reason: "not_found" });
      const preparation = yield* decodePreparation(row.preparation_json),
        evidence = preparation.evidence;
      if (!evidence || evidence.digest !== input.expectedEvidenceDigest)
        return yield* new WorkbenchReleaseError({ reason: "conflict" });
      const registered = yield* decodeHostProfile(row.profile_json);
      const { digest, ...unsigned } = evidence;
      if (
        !evidence.deployment ||
        !registered.policy.deployment ||
        releaseDigest(unsigned) !== digest ||
        releaseDigest(registered) !== evidence.profile.digest
      )
        return yield* new WorkbenchReleaseError({ reason: "conflict" });
      const artifact = yield* regular(directory(id), "artifact", "artifact_changed"),
        rollback = yield* regular(directory(id), "rollback", "rollback_mismatch");
      if (
        releaseHash(artifact) !== evidence.artifact.sha256 ||
        artifact.length !== evidence.artifact.bytes ||
        releaseHash(rollback) !== evidence.rollback.artifact.sha256 ||
        rollback.length !== evidence.rollback.artifact.bytes
      )
        return yield* new WorkbenchReleaseError({ reason: "conflict" });
      return { evidence, profile: registered, artifact, rollback };
    },
    Effect.catch((cause) => publicFailure("artifact", cause)),
  );
  const admissionMaterial = Effect.fn("WorkbenchReleaseEvidence.admissionMaterial")(function* (
    queueId: string,
    id: string,
    input: WorkbenchReleaseRevalidateInput,
  ) {
    const material = yield* admittedMaterial(queueId, id, input);
    const checked = yield* revalidate(queueId, id, input);
    if (checked.preparation.state !== "ready" || checked.preparation.validation.state !== "current")
      return yield* new WorkbenchReleaseError({ reason: "conflict" });
    return material;
  });
  const mergeFailure = (cause: unknown) => {
    const mapping: Record<string, WorkbenchMergeReason> = {
      base_changed: "main_changed",
      base_not_integrated: "main_changed",
      profile_changed: "profile_changed",
      profile_missing: "profile_changed",
      binding_changed: "work_changed",
      task_not_ready: "work_changed",
      work_active: "work_changed",
      source_changed: "source_changed",
      source_dirty: "source_changed",
      artifact_changed: "candidate_changed",
      rollback_mismatch: "candidate_changed",
    };
    const reason = isPreparationProblem(cause) ? mapping[cause.reason] : undefined;
    return new ReleaseMergeMaterialError({
      reason: reason ?? "state_unknown",
      known: reason !== undefined,
      cause,
    });
  };
  const mergeMaterial = Effect.fn("WorkbenchReleaseEvidence.mergeMaterial")(
    function* (
      queueId: string,
      id: string,
      input: WorkbenchReleaseRevalidateInput,
      phase: "admission" | "finalization" | "recovery",
    ) {
      const material = yield* admittedMaterial(queueId, id, input),
        row = yield* read(queueId, id, input),
        original = (yield* decodeJson(row.snapshot_json)) as Source;
      if (phase === "recovery") return { ...material, cwd: original.real };
      const prep = yield* decodePrepareInput(row.input_json),
        registered = yield* profile(prep.profileId, prep.profileDigest, prep.projectId);
      const observed = yield* source(queueId, prep, registered);
      if (observed.base !== (phase === "admission" ? original.base : original.head))
        return yield* problem("base_changed");
      if (
        canonicalReleaseJson({ ...observed, base: original.base }) !==
        canonicalReleaseJson(original)
      )
        return yield* problem("source_changed");
      yield* verificationExecutor.validate(directory(id), material.evidence, registered);
      const finalProfile = yield* profile(prep.profileId, prep.profileDigest, prep.projectId);
      yield* localFence(queueId, prep, observed, finalProfile.policy.main);
      return { ...material, cwd: original.real };
    },
    Effect.provide(context),
    Effect.mapError(mergeFailure),
  );
  const preserveMergeSource = Effect.fn("WorkbenchReleaseEvidence.preserveMergeSource")(
    function* (queueId: string, id: string, input: WorkbenchReleaseRevalidateInput, ref: string) {
      if (!/^refs\/t3\/workbench-handoffs\/[a-f0-9]{64}$/.test(ref))
        return yield* problem("binding_changed");
      const material = yield* mergeMaterial(queueId, id, input, "finalization");
      const current = yield* git.execute({
        operation: "release.handoff.read",
        cwd: material.cwd,
        args: ["rev-parse", "--verify", ref],
        allowNonZeroExit: true,
      });
      const sha = material.evidence.source.commitSha;
      if (current.exitCode === 0) {
        if (current.stdout.trim() !== sha) return yield* problem("source_changed");
        return;
      }
      yield* git.execute({
        operation: "release.handoff.create",
        cwd: material.cwd,
        args: ["update-ref", ref, sha, "0".repeat(sha.length)],
      });
    },
    Effect.provide(context),
    Effect.mapError((cause) => (isReleaseMergeMaterialError(cause) ? cause : mergeFailure(cause))),
  );
  const runOnlineCheck = Effect.fn("WorkbenchReleaseEvidence.runOnlineCheck")(function* (
    queueId: string,
    id: string,
    input: WorkbenchReleaseRevalidateInput & Verification.OnlineCheckInput,
  ) {
    const material = yield* admittedMaterial(queueId, id, input).pipe(
      Effect.mapError(
        (cause) =>
          new Verification.VerificationError({
            reason: "material_invalid",
            stage: "material",
            cause,
          }),
      ),
    );
    if (
      canonicalReleaseJson(material.evidence.deployment?.desired) !==
      canonicalReleaseJson(input.candidate)
    )
      return yield* new Verification.VerificationError({
        reason: "material_invalid",
        stage: "subject",
      });
    return yield* verificationExecutor.check(
      directory(id),
      material.evidence,
      material.profile,
      input,
    );
  });
  return WorkbenchReleaseEvidence.of({
    catalog,
    prepare,
    get,
    revalidate,
    diff,
    awaitPrepared,
    admissionMaterial,
    admittedMaterial,
    runOnlineCheck,
    mergeMaterial,
    preserveMergeSource,
  });
});
export const layer = Layer.effect(WorkbenchReleaseEvidence, make);
