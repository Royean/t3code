import {
  CommandId,
  OrchestrationV2Command,
  type OrchestrationV2ServerCommand,
  type OrchestrationV2StoredEvent,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import { derive } from "@t3tools/provider-core/server/IdAllocator";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import type { Admission, OriginalResume } from "./WorkbenchAttentionGuard.ts";

export class WorkbenchRepairGuardError extends Schema.TaggedError<WorkbenchRepairGuardError>()(
  "WorkbenchRepairGuardError",
  {
    reason: Schema.Literals([
      "intent-missing",
      "intent-stale",
      "command-mismatch",
      "context-mismatch",
      "budget-exhausted",
      "unavailable",
    ]),
    cause: Schema.optional(Schema.Defect()),
  },
) {}

interface RepairProof {
  id: string;
  generation_id: string;
  work_item_id: string;
  ordinal: number;
  used: number;
  charged: number;
  state: string;
  command_id: string;
  message_id: string;
  run_id: string | null;
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
  model_selection_json: string;
  phase: string;
  superseded_at: string | null;
  fence_valid: number;
  context_valid: number;
}

const encodeCommand = Schema.encodeEffect(Schema.fromJsonString(OrchestrationV2Command));
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const isGuardError = Schema.is(WorkbenchRepairGuardError);
const normalizeFailure = (cause: unknown) =>
  isGuardError(cause) ? cause : new WorkbenchRepairGuardError({ reason: "unavailable", cause });
const fail = (reason: WorkbenchRepairGuardError["reason"]) =>
  new WorkbenchRepairGuardError({ reason });

export const repairAdmission = Effect.fn("WorkbenchRepairGuard.repairAdmission")(function* (
  input: Admission,
) {
  const sql = yield* SqlClient.SqlClient;
  const rows = yield* sql<RepairProof>`SELECT a.*,g.used,g.phase,g.superseded_at,
    CASE WHEN g.work_item_id=a.work_item_id AND g.queue_id=a.queue_id AND g.project_id=a.project_id
      AND g.thread_id=a.thread_id AND g.task_id=w.task_id AND g.task_created_at=w.task_created_at
      AND g.dispatcher_thread_id=w.dispatcher_thread_id AND g.source_run_id=a.source_run_id
      AND w.project_id=a.project_id AND w.work_thread_id=a.thread_id AND w.attention_hold=0
      AND w.attention_epoch=a.attention_epoch AND e.state='occupied' AND e.claim_id=a.claim_id
      AND e.wake_generation=a.wake_generation AND b.project_id=a.project_id
      AND b.dispatcher_thread_id=w.dispatcher_thread_id THEN 1 ELSE 0 END AS fence_valid,
    CASE WHEN t.project_id=a.project_id AND t.deleted_at IS NULL AND t.archived_at IS NULL
      AND json_extract(t.payload_json,'$.providerInstanceId')=a.provider_instance_id
      AND json_extract(t.payload_json,'$.activeProviderThreadId')=a.provider_thread_id
      AND json(json_extract(t.payload_json,'$.modelSelection'))=json(a.model_selection_json)
      AND t.runtime_mode=a.runtime_mode AND t.interaction_mode=a.interaction_mode
      AND json_extract(t.payload_json,'$.worktreePath')=a.worktree_path
      AND json_extract(t.payload_json,'$.branch')=a.branch
      AND p.provider_session_id=a.provider_session_id
      AND json_extract(p.payload_json,'$.providerInstanceId')=a.provider_instance_id
      AND json_extract(p.payload_json,'$.nativeThreadRef.driver')=a.native_driver
      AND json_extract(p.payload_json,'$.nativeThreadRef.nativeId')=a.native_thread_id
      THEN 1 ELSE 0 END AS context_valid
    FROM workbench_repair_attempts a
    JOIN workbench_repair_generations g ON g.id=a.generation_id
    LEFT JOIN workbench_work_items w ON w.id=a.work_item_id
    LEFT JOIN workbench_queue_entries e ON e.work_item_id=a.work_item_id AND e.queue_id=a.queue_id
    LEFT JOIN workbench_queue_bindings b ON b.queue_id=a.queue_id AND b.task_id=w.task_id AND b.task_created_at=w.task_created_at
    LEFT JOIN orchestration_v2_projection_threads t ON t.thread_id=a.thread_id
    LEFT JOIN orchestration_v2_projection_provider_threads p ON p.provider_thread_id=a.provider_thread_id AND p.thread_id=a.thread_id
    WHERE a.thread_id=${input.threadId} AND (
      (${input.commandId ?? null} IS NOT NULL AND a.command_id=${input.commandId ?? null}) OR
      (${input.messageId ?? null} IS NOT NULL AND a.message_id=${input.messageId ?? null}) OR
      (${input.runId ?? null} IS NOT NULL AND a.run_id=${input.runId ?? null})) LIMIT 2`;
  if (rows.length === 0) return;
  if (rows.length !== 1) return yield* fail("intent-stale");
  const row = rows[0]!;
  if (
    row.superseded_at !== null ||
    row.phase !== "repairing" ||
    row.fence_valid !== 1 ||
    (input.expectedEpoch !== undefined && input.expectedEpoch !== row.attention_epoch)
  )
    return yield* fail("intent-stale");
  if (row.context_valid !== 1) return yield* fail("context-mismatch");
  if (
    (input.commandId !== undefined && input.commandId !== row.command_id) ||
    (input.messageId !== undefined && input.messageId !== row.message_id) ||
    (input.runId !== undefined && input.runId !== row.run_id)
  )
    return yield* fail("command-mismatch");
  if (row.state === "reserved") {
    if (
      row.charged !== 0 ||
      row.ordinal !== row.used + 1 ||
      row.ordinal > 3 ||
      input.runId !== undefined
    )
      return yield* fail("budget-exhausted");
  } else if (
    !["accepted", "running"].includes(row.state) ||
    row.charged !== 1 ||
    row.run_id === null ||
    row.used < row.ordinal
  ) {
    return yield* fail("intent-stale");
  }
  if (input.effectType === "provider-turn.restart" || input.effectType === "provider-turn.steer")
    return yield* fail("command-mismatch");
  if (row.charged === 1) {
    const [run] = yield* sql`SELECT 1 FROM orchestration_v2_projection_runs r
      WHERE r.run_id=${row.run_id} AND r.thread_id=${input.threadId}
        AND json_extract(r.payload_json,'$.userMessageId')=${row.message_id}
        AND json_extract(r.payload_json,'$.providerInstanceId')=${row.provider_instance_id}
        AND json_extract(r.payload_json,'$.providerThreadId')=${row.provider_thread_id}
        AND json(json_extract(r.payload_json,'$.modelSelection'))=json(${row.model_selection_json})
        AND COALESCE(json_extract(r.payload_json,'$.freshContext'),0)=0
        AND json_extract(r.payload_json,'$.restartContinuationOfRunId') IS NULL`;
    if (run === undefined) return yield* fail("context-mismatch");
  }
  return row;
}, Effect.mapError(normalizeFailure));

export const originalRepair = Effect.fn("WorkbenchRepairGuard.originalRepair")(function* (
  input: Admission,
) {
  const row = yield* repairAdmission(input);
  if (row === undefined) return;
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
}, Effect.mapError(normalizeFailure));

export const assertRepairDescendant = Effect.fn("WorkbenchRepairGuard.assertRepairDescendant")(
  function* (threadId: ThreadId) {
    const sql = yield* SqlClient.SqlClient;
    const ancestors = yield* sql<{ thread_id: string; run_id: string }>`
      WITH RECURSIVE owners(thread_id,run_id) AS (
        SELECT thread_id,run_id FROM orchestration_v2_projection_subagents
          WHERE child_thread_id=${threadId} AND origin='app_owned'
        UNION
        SELECT s.thread_id,s.run_id FROM orchestration_v2_projection_subagents s
          JOIN owners o ON o.thread_id=s.child_thread_id WHERE s.origin='app_owned'
      ) SELECT DISTINCT a.thread_id,a.run_id FROM owners o
        JOIN workbench_repair_attempts a ON a.thread_id=o.thread_id AND a.run_id=o.run_id`;
    for (const ancestor of ancestors)
      yield* repairAdmission({
        threadId: ThreadId.make(ancestor.thread_id),
        runId: RunId.make(ancestor.run_id),
      });
  },
  Effect.mapError(normalizeFailure),
);

/** Registered business deliveries keep their original capability; arbitrary sends cannot manufacture one. */
export const assertUnregisteredAdmission = Effect.fn(
  "WorkbenchRepairGuard.assertUnregisteredAdmission",
)(function* (input: Admission) {
  const sql = yield* SqlClient.SqlClient;
  const [generation] = yield* sql<{
    phase: string;
    work_item_id: string;
  }>`SELECT g.phase,g.work_item_id
      FROM workbench_repair_generations g JOIN workbench_work_items w ON w.id=g.work_item_id
      WHERE COALESCE(w.work_thread_id,g.thread_id)=${input.threadId} AND g.superseded_at IS NULL`;
  if (generation === undefined) return;
  if (generation.phase === "repairing" || generation.phase === "awaiting_user")
    return yield* fail("intent-missing");
  const [registered] = yield* sql`SELECT 1 FROM workbench_work_items w
      LEFT JOIN workbench_work_batches b ON b.work_item_id=w.id
      WHERE w.id=${generation.work_item_id} AND (
        (${input.commandId ?? null} IS NOT NULL AND b.command_id=${input.commandId ?? null}) OR
        (${input.messageId ?? null} IS NOT NULL AND (b.message_id=${input.messageId ?? null} OR w.message_id=${input.messageId ?? null})) OR
        (${input.runId ?? null} IS NOT NULL AND (b.run_id=${input.runId ?? null} OR b.accepted_run_id=${input.runId ?? null}
          OR w.run_id=${input.runId ?? null} OR w.accepted_run_id=${input.runId ?? null}))) LIMIT 1`;
  if (registered !== undefined) return;
  const [intent] = yield* sql<{ request_id: string }>`SELECT request_id FROM workbench_work_items
    WHERE id=${generation.work_item_id} AND request_id IS NOT NULL`;
  if (intent === undefined) return yield* fail("intent-missing");
  const commandId = CommandId.make(intent.request_id);
  const childId = derive.delegatedTaskThread({ commandId });
  const runId = derive.run({ threadId: childId, ordinal: 1 });
  if (input.threadId !== childId || input.runId !== runId) return yield* fail("intent-missing");
  const nodeId = derive.delegatedTaskNode({ commandId });
  const messageId = derive.delegatedTaskMessage({ commandId });
  const [original] = yield* sql`SELECT 1 FROM workbench_work_items w
    JOIN workbench_repair_generations g ON g.work_item_id=w.id AND g.superseded_at IS NULL
    JOIN workbench_queue_entries e ON e.work_item_id=w.id AND e.queue_id=g.queue_id
    JOIN workbench_queue_bindings b ON b.queue_id=g.queue_id AND b.task_id=w.task_id
      AND b.task_created_at=w.task_created_at AND b.project_id=w.project_id AND b.dispatcher_thread_id=w.dispatcher_thread_id
    JOIN orchestration_command_receipts c ON c.command_id=w.request_id AND c.aggregate_kind='thread'
      AND c.aggregate_id=w.dispatcher_thread_id AND c.command_type='delegated_task.request' AND c.status='accepted'
    JOIN orchestration_v2_projection_threads p ON p.thread_id=w.dispatcher_thread_id AND p.project_id=w.project_id
    JOIN orchestration_v2_projection_threads t ON t.thread_id=${childId} AND t.project_id=w.project_id
    JOIN orchestration_v2_projection_subagents s ON s.subagent_id=${nodeId} AND s.thread_id=w.dispatcher_thread_id
      AND s.child_thread_id=t.thread_id AND s.origin='app_owned' AND s.run_id IS NOT NULL
    JOIN orchestration_v2_projection_runs r ON r.run_id=${runId} AND r.thread_id=t.thread_id AND r.ordinal=1
    JOIN orchestration_v2_projection_messages m ON m.message_id=${messageId} AND m.thread_id=t.thread_id AND m.run_id=r.run_id
    WHERE w.id=${generation.work_item_id} AND w.request_id=${commandId}
      AND w.attention_hold=0 AND e.state='occupied' AND e.claim_id IS NOT NULL
      AND g.task_id=w.task_id AND g.task_created_at=w.task_created_at AND g.project_id=w.project_id
      AND g.dispatcher_thread_id=w.dispatcher_thread_id AND COALESCE(w.work_thread_id,g.thread_id)=t.thread_id
      AND p.deleted_at IS NULL AND p.archived_at IS NULL AND t.deleted_at IS NULL AND t.archived_at IS NULL
      AND json_extract(t.payload_json,'$.lineage.parentThreadId')=p.thread_id
      AND json_extract(t.payload_json,'$.lineage.relationshipToParent')='subagent'
      AND json_extract(t.payload_json,'$.forkedFrom.type')='node'
      AND json_extract(t.payload_json,'$.forkedFrom.nodeId')=${nodeId}
      AND json_extract(r.payload_json,'$.userMessageId')=m.message_id
      AND (json_extract(r.payload_json,'$.delegatedTaskId') IS NULL OR json_extract(r.payload_json,'$.delegatedTaskId')=s.subagent_id)`;
  if (original === undefined) return yield* fail("intent-missing");
}, Effect.mapError(normalizeFailure));

export const assertRepairCommand = Effect.fn("WorkbenchRepairGuard.assertRepairCommand")(function* (
  command: OrchestrationV2ServerCommand,
) {
  const threadId =
    "threadId" in command
      ? command.threadId
      : "parentThreadId" in command
        ? command.parentThreadId
        : undefined;
  if (threadId === undefined) return;
  const proof = yield* repairAdmission({
    threadId,
    ...(command.type === "delegated_task.request" ? {} : { commandId: command.commandId }),
    ...(command.type === "message.dispatch" ? { messageId: command.messageId } : {}),
    ...(command.type === "delegated_task.request" ? { runId: command.parentRunId } : {}),
  });
  if (proof === undefined) return;
  if (command.type === "delegated_task.request") {
    const sql = yield* SqlClient.SqlClient;
    const [parent] = yield* sql`SELECT 1 FROM orchestration_v2_projection_runs r
      JOIN orchestration_v2_projection_nodes n ON n.thread_id=r.thread_id AND n.run_id=r.run_id
      WHERE r.run_id=${proof.run_id} AND r.thread_id=${threadId} AND n.node_id=${command.parentNodeId}
        AND r.status IN ('starting','running','waiting')`;
    if (proof.charged !== 1 || parent === undefined) return yield* fail("command-mismatch");
    return { ...proof, admissionKind: "delegate" as const };
  }
  if (
    command.type !== "message.dispatch" ||
    proof.state !== "reserved" ||
    command.dispatchMode.type !== "queue_after_active" ||
    command.freshContext === true ||
    command.restartContinuationOfRunId !== undefined ||
    command.steerFollowUpOfRunId !== undefined ||
    command.deliveryIntent === "restart" ||
    command.deliveryIntent === "steer" ||
    (yield* encodeCommand(command)) !== proof.request_json
  )
    return yield* fail("command-mismatch");
  return { ...proof, admissionKind: "repair" as const };
}, Effect.mapError(normalizeFailure));

const executesProviderWork = (type: string) =>
  [
    "message.dispatch",
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
    "thread.metadata.update",
  ].includes(type);

export const assertRepairCommandCommit = Effect.fn(
  "WorkbenchRepairGuard.assertRepairCommandCommit",
)(function* (input: {
  readonly threadId: ThreadId;
  readonly commandId: string;
  readonly commandType: string;
  readonly executionCommand?: OrchestrationV2ServerCommand;
}) {
  if (!executesProviderWork(input.commandType)) return;
  if (
    input.executionCommand?.type === "thread.metadata.update" &&
    input.executionCommand.continuation === undefined
  )
    return;
  yield* assertRepairDescendant(input.threadId);
  const sql = yield* SqlClient.SqlClient;
  const [managed] = yield* sql`SELECT 1 FROM workbench_repair_generations g
      JOIN workbench_work_items w ON w.id=g.work_item_id
      WHERE COALESCE(w.work_thread_id,g.thread_id)=${input.threadId} AND g.superseded_at IS NULL`;
  const command = input.executionCommand;
  if (command === undefined) {
    if (managed !== undefined) return yield* fail("command-mismatch");
    return;
  }
  const commandThreadId =
    "threadId" in command
      ? command.threadId
      : "parentThreadId" in command
        ? command.parentThreadId
        : undefined;
  if (
    command.commandId !== input.commandId ||
    command.type !== input.commandType ||
    commandThreadId !== input.threadId
  )
    return yield* fail("command-mismatch");
  const proof = yield* assertRepairCommand(command);
  if (proof !== undefined) return proof;
  if (managed === undefined) return;
  const requestJson = command.type === "message.dispatch" ? yield* encodeCommand(command) : null;
  const [recovery] = yield* sql`SELECT 1 FROM workbench_attention_resume_intents i
      JOIN workbench_work_items w ON w.id=i.work_item_id AND w.work_thread_id=i.thread_id
      JOIN workbench_queue_entries e ON e.work_item_id=w.id AND e.queue_id=i.queue_id
      JOIN workbench_queue_bindings b ON b.queue_id=i.queue_id AND b.task_id=w.task_id AND b.task_created_at=w.task_created_at
      WHERE i.thread_id=${input.threadId} AND i.command_id=${input.commandId}
        AND i.request_json=${requestJson} AND i.attention_epoch=w.attention_epoch
        AND i.claim_id=e.claim_id AND e.state='occupied' AND b.project_id=i.project_id
        AND b.dispatcher_thread_id=w.dispatcher_thread_id AND i.state IN ('pending','accepted','running')`;
  if (recovery === undefined)
    yield* assertUnregisteredAdmission({
      threadId: input.threadId,
      commandId: command.commandId,
      ...(command.type === "message.dispatch" ? { messageId: command.messageId } : {}),
      ...(command.type === "delegated_task.request" ? { runId: command.parentRunId } : {}),
      ...("runId" in command ? { runId: command.runId } : {}),
    });
}, Effect.mapError(normalizeFailure));

/** The caller owns the accepted-command transaction, including its run and receipt. */
export const recordRepairCommit = Effect.fn("WorkbenchRepairGuard.recordRepairCommit")(
  function* (input: {
    readonly proof: RepairProof;
    readonly events: ReadonlyArray<OrchestrationV2StoredEvent>;
    readonly acceptedAt: DateTime.Utc;
    readonly sequence: number;
    readonly threadId: ThreadId;
  }) {
    const sql = yield* SqlClient.SqlClient;
    const proof = input.proof;
    const runs = input.events.flatMap(({ event }) =>
      event.type === "run.created" &&
      event.threadId === input.threadId &&
      event.payload.userMessageId === proof.message_id
        ? [event.payload]
        : [],
    );
    if (runs.length !== 1) return yield* fail("command-mismatch");
    const run = runs[0]!;
    if (
      run.freshContext === true ||
      run.restartContinuationOfRunId !== undefined ||
      run.providerInstanceId !== proof.provider_instance_id ||
      run.providerThreadId !== proof.provider_thread_id ||
      (yield* encodeJson(run.modelSelection)) !== proof.model_selection_json
    )
      return yield* fail("context-mismatch");
    const now = DateTime.formatIso(input.acceptedAt);
    const charged = yield* sql`UPDATE workbench_repair_attempts
    SET charged=1,state='accepted',run_id=${run.id},native_attempt_id=${run.activeAttemptId},
      accepted_at=${now},accepted_sequence=${input.sequence},updated_at=${now}
    WHERE id=${proof.id} AND state='reserved' AND charged=0 AND run_id IS NULL RETURNING id`;
    const generation = yield* sql`UPDATE workbench_repair_generations
    SET used=${proof.ordinal},version=version+1,updated_at=${now}
    WHERE id=${proof.generation_id} AND used=${proof.ordinal - 1} AND phase='repairing'
      AND superseded_at IS NULL AND limit_count=3 RETURNING id`;
    if (charged.length !== 1 || generation.length !== 1) return yield* fail("intent-stale");
  },
  Effect.mapError(normalizeFailure),
);
