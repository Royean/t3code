import {
  type CommandId,
  type MessageId,
  OrchestrationV2Command,
  type OrchestrationV2ServerCommand,
  type RunId,
  RuntimeRequestId,
  ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";

export class WorkbenchAttentionGuardError extends Schema.TaggedError<WorkbenchAttentionGuardError>()(
  "WorkbenchAttentionGuardError",
  {
    threadId: ThreadId,
    reason: Schema.Literals([
      "held",
      "stale",
      "resume-mismatch",
      "unavailable",
      "runtime-request-pending",
    ]),
    runtimeRequestId: Schema.optional(RuntimeRequestId),
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message(): string {
    return "This work item requires its recorded Attention recovery before execution can continue.";
  }
}

export interface Admission {
  readonly threadId: ThreadId;
  readonly commandId?: CommandId;
  readonly messageId?: MessageId;
  readonly runId?: RunId;
  readonly expectedEpoch?: number | null;
}

export interface OriginalResume {
  readonly id: string;
  readonly workItemId: string;
  readonly attentionEpoch: number;
  readonly projectId: string;
  readonly providerInstanceId: string;
  readonly providerThreadId: string;
  readonly nativeDriver: string;
  readonly nativeThreadId: string;
  readonly worktreePath: string;
  readonly worktreeRealPath: string;
  readonly branch: string;
  readonly gitCommonDir: string;
  readonly requestJson: string;
}

export class WorkbenchAttentionGuard extends Context.Service<
  WorkbenchAttentionGuard,
  {
    readonly assertAdmission: (
      input: Admission,
    ) => Effect.Effect<void, WorkbenchAttentionGuardError>;
    readonly assertCommand: (
      command: OrchestrationV2ServerCommand,
    ) => Effect.Effect<void, WorkbenchAttentionGuardError>;
    readonly originalResume: (
      input: Admission,
    ) => Effect.Effect<OriginalResume | undefined, WorkbenchAttentionGuardError>;
    /** Register before the durable read, so a pause cannot miss an external call already entering. */
    readonly withAdmission: <A, E, R>(
      input: Admission,
      effect: Effect.Effect<A, E, R>,
    ) => Effect.Effect<A, E | WorkbenchAttentionGuardError, R>;
    /** The caller first commits attention_hold and advances attention_epoch. */
    readonly awaitAdmissions: (threadId: ThreadId) => Effect.Effect<void>;
  }
>()("t3/orchestration-v2/WorkbenchAttentionGuard") {}

type ResumeRow = {
  id: string;
  work_item_id: string;
  attention_epoch: number;
  project_id: string;
  provider_instance_id: string;
  provider_thread_id: string;
  native_driver: string;
  native_thread_id: string;
  worktree_path: string;
  worktree_real_path: string;
  branch: string;
  git_common_dir: string;
  request_json: string;
  state: string;
  current_epoch: number | null;
  current_claim_id: string | null;
  claim_id: string | null;
  queue_state: string | null;
  binding_valid: number;
};

const encodeCommand = Schema.encodeEffect(Schema.fromJsonString(OrchestrationV2Command));

/** SQL-only: command commit repeats this check in its write transaction before accepting Stop. */
export const assertAutomaticPauseStop = Effect.fn(
  "WorkbenchAttentionGuard.assertAutomaticPauseStop",
)(
  function* (command: { readonly commandId: CommandId; readonly threadId: ThreadId }) {
    const sql = yield* SqlClient.SqlClient;
    const rows = yield* sql<{
      phase: string;
      observation_only: number;
      resume_point_id: string | null;
      epoch: number;
      work_thread_id: string | null;
      attention_hold: number | null;
      attention_epoch: number | null;
    }>`SELECT a.phase,a.observation_only,a.resume_point_id,a.epoch,w.work_thread_id,w.attention_hold,w.attention_epoch
      FROM workbench_attention_items a LEFT JOIN workbench_work_items w ON w.id=a.work_item_id
      WHERE a.pause_command_id=${command.commandId} LIMIT 2`;
    if (rows.length === 0) return;
    const row = rows[0]!;
    if (
      rows.length !== 1 ||
      row.work_thread_id !== command.threadId ||
      row.observation_only !== 0 ||
      row.attention_hold !== 1 ||
      row.attention_epoch !== row.epoch ||
      row.resume_point_id !== null ||
      !["pausing", "blocked"].includes(row.phase)
    ) {
      return yield* new WorkbenchAttentionGuardError({
        threadId: command.threadId,
        reason: "stale",
      });
    }
    const [pending] = yield* sql<{ runtime_request_id: string }>`SELECT runtime_request_id
      FROM orchestration_v2_projection_runtime_requests WHERE thread_id=${command.threadId} AND status='pending'
      ORDER BY created_at,runtime_request_id LIMIT 1`;
    if (pending !== undefined)
      return yield* new WorkbenchAttentionGuardError({
        threadId: command.threadId,
        reason: "runtime-request-pending",
        runtimeRequestId: RuntimeRequestId.make(pending.runtime_request_id),
      });
  },
  (effect, command) =>
    effect.pipe(
      Effect.catchTags({
        SqlError: (cause) =>
          Effect.fail(
            new WorkbenchAttentionGuardError({
              threadId: command.threadId,
              reason: "unavailable",
              cause,
            }),
          ),
      }),
    ),
);

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const active = new Map<ThreadId, Set<Deferred.Deferred<void>>>();
  const originalResume = Effect.fn("WorkbenchAttentionGuard.originalResume")(
    function* (input: Admission) {
      const [row] = yield* sql<ResumeRow>`SELECT i.*,w.attention_epoch AS current_epoch,
      e.claim_id AS current_claim_id,e.state AS queue_state,
      CASE WHEN b.task_id IS NOT NULL AND b.project_id=i.project_id AND w.project_id=i.project_id THEN 1 ELSE 0 END AS binding_valid
      FROM workbench_attention_resume_intents i
      LEFT JOIN workbench_work_items w ON w.id=i.work_item_id AND w.work_thread_id=i.thread_id
      LEFT JOIN workbench_queue_entries e ON e.work_item_id=i.work_item_id AND e.queue_id=i.queue_id
      LEFT JOIN workbench_queue_bindings b ON b.queue_id=i.queue_id AND b.task_id=w.task_id
        AND b.task_created_at=w.task_created_at AND b.dispatcher_thread_id=w.dispatcher_thread_id
      WHERE i.thread_id=${input.threadId} AND (
        (${input.commandId ?? null} IS NOT NULL AND i.command_id=${input.commandId ?? null}) OR
        (${input.messageId ?? null} IS NOT NULL AND i.message_id=${input.messageId ?? null}) OR
        (${input.runId ?? null} IS NOT NULL AND (i.run_id=${input.runId ?? null} OR EXISTS(
          SELECT 1 FROM orchestration_v2_projection_runs r WHERE r.thread_id=i.thread_id
          AND r.run_id=${input.runId ?? null} AND json_extract(r.payload_json,'$.userMessageId')=i.message_id))))
      LIMIT 1`;
      if (row === undefined) return;
      if (
        row.current_epoch !== row.attention_epoch ||
        row.binding_valid !== 1 ||
        row.claim_id === null ||
        row.current_claim_id !== row.claim_id ||
        row.queue_state !== "occupied" ||
        !["pending", "accepted", "running"].includes(row.state)
      ) {
        return yield* new WorkbenchAttentionGuardError({
          threadId: input.threadId,
          reason: "stale",
        });
      }
      return {
        id: row.id,
        workItemId: row.work_item_id,
        attentionEpoch: row.attention_epoch,
        projectId: row.project_id,
        providerInstanceId: row.provider_instance_id,
        providerThreadId: row.provider_thread_id,
        nativeDriver: row.native_driver,
        nativeThreadId: row.native_thread_id,
        worktreePath: row.worktree_path,
        worktreeRealPath: row.worktree_real_path,
        branch: row.branch,
        gitCommonDir: row.git_common_dir,
        requestJson: row.request_json,
      } satisfies OriginalResume;
    },
    (effect, input) =>
      effect.pipe(
        Effect.catchTags({
          SqlError: (cause) =>
            Effect.fail(
              new WorkbenchAttentionGuardError({
                threadId: input.threadId,
                reason: "unavailable",
                cause,
              }),
            ),
        }),
      ),
  );

  const assertAdmission = Effect.fn("WorkbenchAttentionGuard.assertAdmission")(
    function* (input: Admission) {
      const rows = yield* sql<{ attention_hold: number; attention_epoch: number }>`
      SELECT attention_hold,attention_epoch FROM workbench_work_items WHERE work_thread_id=${input.threadId}`;
      for (const row of rows) {
        if (
          input.expectedEpoch !== undefined &&
          (input.expectedEpoch ?? 0) !== row.attention_epoch
        ) {
          return yield* new WorkbenchAttentionGuardError({
            threadId: input.threadId,
            reason: "stale",
          });
        }
        if (row.attention_hold === 1 && (yield* originalResume(input)) === undefined) {
          return yield* new WorkbenchAttentionGuardError({
            threadId: input.threadId,
            reason: "held",
          });
        }
      }
    },
    (effect, input) =>
      effect.pipe(
        Effect.catchTags({
          SqlError: (cause) =>
            Effect.fail(
              new WorkbenchAttentionGuardError({
                threadId: input.threadId,
                reason: "unavailable",
                cause,
              }),
            ),
        }),
      ),
  );

  const assertCommand = Effect.fn("WorkbenchAttentionGuard.assertCommand")(function* (
    command: OrchestrationV2ServerCommand,
  ) {
    if (command.type === "thread.stop")
      return yield* assertAutomaticPauseStop(command).pipe(
        Effect.provideService(SqlClient.SqlClient, sql),
      );
    const threadId =
      "threadId" in command
        ? command.threadId
        : "parentThreadId" in command
          ? command.parentThreadId
          : undefined;
    if (threadId === undefined) return;
    const canExecute =
      [
        "message.dispatch",
        "runtime-request.respond",
        "queue.resume",
        "queued-run.start",
        "queued-message.promote-to-steer",
        "prepared-run.release",
        "prepared-run.retry",
        "provider.switch",
        "thread.model-selection.set",
        "thread.runtime-mode.set",
        "thread.interaction-mode.set",
        "thread.guest-message.submit",
        "delegated_task.request",
      ].includes(command.type) ||
      (command.type === "thread.metadata.update" && command.continuation !== undefined);
    if (!canExecute) return;
    const input: Admission = {
      threadId,
      commandId: command.commandId,
      ...(command.type === "message.dispatch" ? { messageId: command.messageId } : {}),
    };
    yield* assertAdmission(input);
    const resume = yield* originalResume(input);
    if (
      resume !== undefined &&
      (command.type !== "message.dispatch" ||
        (yield* encodeCommand(command).pipe(
          Effect.mapError(
            (cause) =>
              new WorkbenchAttentionGuardError({ threadId, reason: "resume-mismatch", cause }),
          ),
        )) !== resume.requestJson)
    ) {
      return yield* new WorkbenchAttentionGuardError({ threadId, reason: "resume-mismatch" });
    }
  });

  return WorkbenchAttentionGuard.of({
    assertAdmission,
    assertCommand,
    originalResume,
    withAdmission: Effect.fn("WorkbenchAttentionGuard.withAdmission")(function* <A, E, R>(
      input: Admission,
      effect: Effect.Effect<A, E, R>,
    ) {
      return yield* Effect.acquireUseRelease(
        Effect.sync(() => {
          const done = Deferred.makeUnsafe<void>();
          const entries = active.get(input.threadId) ?? new Set<Deferred.Deferred<void>>();
          entries.add(done);
          active.set(input.threadId, entries);
          return done;
        }),
        () => assertAdmission(input).pipe(Effect.andThen(effect)),
        (done) =>
          Effect.sync(() => {
            const entries = active.get(input.threadId);
            entries?.delete(done);
            if (entries?.size === 0) active.delete(input.threadId);
          }).pipe(Effect.andThen(Deferred.succeed(done, undefined))),
      );
    }),
    awaitAdmissions: Effect.fn("WorkbenchAttentionGuard.awaitAdmissions")(function* (threadId) {
      let entries = [...(active.get(threadId) ?? [])];
      while (entries.length > 0) {
        yield* Effect.forEach(entries, Deferred.await, { discard: true });
        entries = [...(active.get(threadId) ?? [])];
      }
    }),
  });
});

export const layer = Layer.effect(WorkbenchAttentionGuard, make);
