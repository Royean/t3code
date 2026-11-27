import {
  CommandId,
  NodeId,
  RunId,
  MessageId,
  type ScheduledTaskId,
  ThreadId,
  WorkbenchQueueError,
  type WorkbenchQueueBindInput,
  type WorkbenchDeliveryIntentInput,
  type WorkbenchDeliveryIntentReceipt,
  type WorkbenchQueuePriorityInput,
  type WorkbenchQueueScope,
  type WorkbenchQueueSnapshot,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import { delegatedTaskWorkProgress } from "../orchestration-v2/delegatedTaskWorkProgress.ts";
import * as WorkItems from "./WorkbenchWorkItems.ts";

import * as Lifecycle from "./workbenchLifecycle.ts";
import { decodeRepairPolicy, freezeRepairPolicy } from "./workbenchRepairPolicy.ts";
import * as Schema from "effect/Schema";
import { ProjectId } from "@t3tools/contracts";
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));

type QueueRow = { id: string; controller_id: string; project_id: string };
type BindingRow = {
  task_id: ScheduledTaskId;
  task_created_at: string;
  queue_id: string;
  project_id: string;
  dispatcher_thread_id: ThreadId;
  repair_policy_json: string | null;
};
type EntryRow = WorkItems.WorkItemRow & {
  enqueue_sequence: number;
  priority: number;
  queue_state: "queued" | "occupied" | "finished" | "paused";
  claim_id: string | null;
  wake_generation: number;
  work_state: WorkbenchQueueSnapshot["items"][number]["workState"];
  blocked_reason: WorkbenchQueueSnapshot["items"][number]["blockedReason"];
  needs_dispatch: number;
  queue_updated_at: string;
};

// Keep diagnostic causes private; the public schema deliberately contains only a safe reason.
class WorkbenchQueueFailure extends WorkbenchQueueError {
  static fromCause(reason: WorkbenchQueueError["reason"], cause: unknown) {
    return Object.defineProperty(new WorkbenchQueueFailure({ reason }), "cause", { value: cause });
  }
}

export class WorkbenchQueue extends Context.Service<
  WorkbenchQueue,
  {
    readonly prepareDeliveryIntent: (
      queueId: string,
      input: WorkbenchDeliveryIntentInput,
    ) => Effect.Effect<Omit<WorkbenchDeliveryIntentReceipt, "environmentId">, WorkbenchQueueError>;
    readonly bind: (
      queueId: string,
      input: WorkbenchQueueBindInput,
    ) => Effect.Effect<WorkbenchQueueSnapshot, WorkbenchQueueError>;
    readonly get: (
      queueId: string,
      scope: WorkbenchQueueScope,
    ) => Effect.Effect<WorkbenchQueueSnapshot, WorkbenchQueueError>;
    readonly priority: (
      queueId: string,
      input: WorkbenchQueuePriorityInput,
    ) => Effect.Effect<WorkbenchQueueSnapshot, WorkbenchQueueError>;
    readonly access: (
      taskId: ScheduledTaskId,
      businessDeliveryId?: string,
    ) => Effect.Effect<
      {
        readonly workItemId: string;
        readonly claimId: string;
        readonly attentionEpoch: number;
      } | null,
      WorkbenchQueueError
    >;
    readonly dispatched: (workItemId: string) => Effect.Effect<void, WorkbenchQueueError>;
    readonly remaining: (taskId: ScheduledTaskId) => Effect.Effect<number, WorkbenchQueueError>;
    readonly reconcile: (
      queueId: string,
      scope: WorkbenchQueueScope,
    ) => Effect.Effect<WorkbenchQueueSnapshot, WorkbenchQueueError>;
  }
>()("t3/scheduledTasks/WorkbenchQueue") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const items = yield* WorkItems.WorkbenchWorkItems;
  const engine = yield* Orchestrator.OrchestratorV2;
  const threads = yield* Threads.ThreadManagementService;
  const locks = yield* KeyedLock.make<string>();
  const checkTracking = Effect.fn("WorkbenchQueue.checkTracking")(
    function* (scope: WorkbenchQueueScope) {
      if (scope.expectedTrackingId === undefined) return;
      const [metadata] = yield* sql<{
        tracking_id: string;
      }>`SELECT tracking_id FROM scheduled_task_execution_metadata WHERE singleton=1`;
      if (metadata?.tracking_id !== scope.expectedTrackingId)
        return yield* new WorkbenchQueueError({ reason: "invalid_binding" });
    },
    Effect.catchTags({
      SqlError: (cause) => Effect.fail(WorkbenchQueueFailure.fromCause("unavailable", cause)),
    }),
  );
  const authorize = Effect.fn("WorkbenchQueue.authorize")(
    function* (queueId: string, scope: WorkbenchQueueScope) {
      const [queue] =
        yield* sql<QueueRow>`SELECT * FROM workbench_queues WHERE id=${queueId} AND controller_id=${scope.controllerId} AND project_id=${scope.projectId}`;
      if (!queue) return yield* new WorkbenchQueueError({ reason: "not_found" });
    },
    Effect.catchTags({
      SqlError: (cause) => Effect.fail(WorkbenchQueueFailure.fromCause("unavailable", cause)),
    }),
  );
  const entries = Effect.fn("WorkbenchQueue.entries")(function* (
    queueId: string,
    occupiedOnly = false,
  ) {
    return yield* sql<EntryRow>`SELECT w.*, e.enqueue_sequence,e.priority,e.state AS queue_state,e.claim_id,e.wake_generation,e.work_state,e.blocked_reason,e.needs_dispatch,e.updated_at AS queue_updated_at
      FROM workbench_queue_entries e JOIN workbench_work_items w ON w.id=e.work_item_id WHERE e.queue_id=${queueId}
      AND ${occupiedOnly ? sql`e.state='occupied'` : sql`(e.state!='finished' OR e.enqueue_sequence IN (SELECT enqueue_sequence FROM workbench_queue_entries WHERE queue_id=${queueId} AND state='finished' ORDER BY updated_at DESC,enqueue_sequence DESC LIMIT 100))`}
      ORDER BY e.priority DESC,e.enqueue_sequence`;
  });
  const snapshot = Effect.fn("WorkbenchQueue.snapshot")(
    function* (queueId: string, scope: WorkbenchQueueScope) {
      yield* authorize(queueId, scope);
      const [metadata] = yield* sql<{
        tracking_id: string;
      }>`SELECT tracking_id FROM scheduled_task_execution_metadata WHERE singleton=1`;
      const bindings =
        yield* sql<BindingRow>`SELECT * FROM workbench_queue_bindings WHERE queue_id=${queueId} ORDER BY rowid`;
      const rows = yield* entries(queueId);
      if (
        rows.some(
          (row) =>
            row.project_id !== scope.projectId ||
            !bindings.some(
              (binding) =>
                binding.task_id === row.task_id &&
                binding.task_created_at === row.task_created_at &&
                binding.dispatcher_thread_id === row.dispatcher_thread_id,
            ),
        )
      )
        return yield* new WorkbenchQueueError({ reason: "invalid_binding" });
      return {
        trackingId: metadata!.tracking_id,
        queueId,
        controllerId: scope.controllerId,
        projectId: scope.projectId,
        concurrency: 2 as const,
        occupied: rows.filter((row) => row.queue_state === "occupied").length,
        bindings: yield* Effect.forEach(
          bindings,
          Effect.fn("WorkbenchQueue.bindingSummary")(function* (row) {
            return {
              taskId: row.task_id,
              taskCreatedAt: row.task_created_at,
              dispatcherThreadId: row.dispatcher_thread_id,
              repairPolicy:
                row.repair_policy_json === null
                  ? null
                  : yield* decodeRepairPolicy(row.repair_policy_json),
            };
          }),
        ),
        items: rows.map((row) => ({
          id: row.id,
          taskId: row.task_id as ScheduledTaskId,
          taskCreatedAt: row.task_created_at,
          businessDeliveryId: row.business_delivery_id,
          dispatcherThreadId: row.dispatcher_thread_id as ThreadId,
          priority: row.priority,
          workItemVersion: row.version,
          latestEventSequence: row.matter_id === null ? 1 : row.latest_event_sequence,
          enqueueSequence: row.enqueue_sequence,
          queueState: row.queue_state,
          claimId: row.claim_id,
          workThreadId: row.work_thread_id,
          runId: row.run_id,
          workState: row.work_state,
          blockedReason: row.blocked_reason,
          createdAt: row.created_at,
          updatedAt: row.queue_updated_at,
        })),
      } satisfies WorkbenchQueueSnapshot;
    },
    Effect.catchTags({
      SqlError: (cause) => Effect.fail(WorkbenchQueueFailure.fromCause("unavailable", cause)),
      SchemaError: (cause) => Effect.fail(WorkbenchQueueFailure.fromCause("unavailable", cause)),
    }),
  );
  const synchronize = Effect.fn("WorkbenchQueue.synchronize")(function* (queueId: string) {
    yield* sql`INSERT OR IGNORE INTO workbench_queue_entries(work_item_id,queue_id,state,claim_id,created_at,updated_at)
      SELECT w.id,${queueId},CASE WHEN w.request_id IS NOT NULL OR w.work_thread_id IS NOT NULL THEN 'occupied' ELSE 'queued' END,
      CASE WHEN w.request_id IS NOT NULL OR w.work_thread_id IS NOT NULL THEN 'queue-claim:' || w.id ELSE NULL END,w.created_at,w.updated_at
      FROM workbench_work_items w JOIN workbench_queue_bindings b ON b.task_id=w.task_id AND b.task_created_at=w.task_created_at
      AND b.project_id=w.project_id AND b.dispatcher_thread_id=w.dispatcher_thread_id WHERE b.queue_id=${queueId} ORDER BY w.rowid`;
  });
  const bind = Effect.fn("WorkbenchQueue.bind")(function* (
    queueId: string,
    input: WorkbenchQueueBindInput,
  ) {
    const task = yield* items
      .binding(input.taskId)
      .pipe(Effect.mapError((cause) => WorkbenchQueueFailure.fromCause("invalid_binding", cause)));
    if (task.projectId !== input.projectId || task.threadId !== input.dispatcherThreadId)
      return yield* new WorkbenchQueueError({ reason: "invalid_binding" });
    yield* sql
      .withTransaction(
        Effect.gen(function* () {
          yield* checkTracking(input);
          const [existing] =
            yield* sql<QueueRow>`SELECT * FROM workbench_queues WHERE id=${queueId} OR (controller_id=${input.controllerId} AND project_id=${input.projectId})`;
          if (
            existing &&
            (existing.id !== queueId ||
              existing.controller_id !== input.controllerId ||
              existing.project_id !== input.projectId)
          )
            return yield* new WorkbenchQueueError({ reason: "conflict" });
          const [dispatcherOwner] =
            yield* sql`SELECT 1 FROM workbench_queue_bindings WHERE dispatcher_thread_id=${task.threadId} AND queue_id!=${queueId} LIMIT 1`;
          if (dispatcherOwner) return yield* new WorkbenchQueueError({ reason: "conflict" });
          const [binding] =
            yield* sql<BindingRow>`SELECT * FROM workbench_queue_bindings WHERE task_id=${task.id}`;
          if (
            binding &&
            (binding.queue_id !== queueId ||
              binding.task_created_at !== task.createdAt ||
              binding.dispatcher_thread_id !== task.threadId)
          )
            return yield* new WorkbenchQueueError({ reason: "conflict" });
          yield* sql`INSERT OR IGNORE INTO workbench_queues(id,controller_id,project_id) VALUES(${queueId},${input.controllerId},${input.projectId})`;
          yield* sql`INSERT OR IGNORE INTO workbench_queue_bindings(task_id,task_created_at,queue_id,project_id,dispatcher_thread_id) VALUES(${task.id},${task.createdAt},${queueId},${task.projectId},${task.threadId})`;
          if (input.repairPolicy !== undefined) {
            yield* freezeRepairPolicy(ProjectId.make(task.projectId), input.repairPolicy).pipe(
              Effect.provideService(SqlClient.SqlClient, sql),
            );
            const encoded =
              input.repairPolicy.checks.length === 0 ? null : yield* encodeJson(input.repairPolicy);
            yield* sql`UPDATE workbench_queue_bindings SET repair_policy_json=${encoded} WHERE task_id=${task.id} AND queue_id=${queueId}`;
          }
          yield* synchronize(queueId);
        }),
      )
      .pipe(
        Effect.catchTags({
          SqlError: (cause) => Effect.fail(WorkbenchQueueFailure.fromCause("unavailable", cause)),
          SchemaError: (cause) =>
            Effect.fail(WorkbenchQueueFailure.fromCause("unavailable", cause)),
        }),
      );
    return yield* snapshot(queueId, input);
  });
  const priority = Effect.fn("WorkbenchQueue.priority")(
    function* (queueId: string, input: WorkbenchQueuePriorityInput) {
      yield* authorize(queueId, input);
      const changed = yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* checkTracking(input);
          return yield* sql`UPDATE workbench_queue_entries SET priority=${input.priority},updated_at=${DateTime.formatIso(yield* DateTime.now)}
      WHERE queue_id=${queueId} AND work_item_id=${input.workItemId} AND state='queued' AND claim_id IS NULL RETURNING work_item_id`;
        }),
      );
      if (changed.length !== 1) return yield* new WorkbenchQueueError({ reason: "not_queued" });
      return yield* snapshot(queueId, input);
    },
    Effect.catchTags({
      SqlError: (cause) => Effect.fail(WorkbenchQueueFailure.fromCause("unavailable", cause)),
    }),
  );
  const observeEntry = Effect.fn("WorkbenchQueue.observeEntry")(
    function* (row: EntryRow) {
      if (row.attention_hold === 1) return;
      const responding =
        yield* sql`SELECT 1 FROM workbench_runtime_responses a WHERE a.work_item_id=${row.id} AND (a.state IN ('recorded','queued','submitted','unknown') OR EXISTS(SELECT 1 FROM orchestration_v2_projection_runs r WHERE r.run_id=a.run_id AND r.thread_id=json_extract(a.source_json,'$.thread_id') AND r.status IN ('preparing','starting','running','waiting','queued'))) LIMIT 1`;
      if (responding.length) return;
      const item = yield* items.observe(row);
      const verified =
        yield* sql`SELECT 1 FROM workbench_repair_generations WHERE work_item_id=${row.id} AND superseded_at IS NULL AND phase='succeeded' LIMIT 1`;
      const repairReady = item.workThreadId === null || verified.length > 0;
      if (row.matter_id !== null) {
        const observedRun =
          item.runId === null || item.workThreadId === null
            ? undefined
            : (yield* threads.getThreadRecords(ThreadId.make(item.workThreadId), ["runs"], {
                runIds: [RunId.make(item.runId)],
              })).runs[0];
        const preparation = observedRun?.status === "preparing";
        const executing =
          observedRun !== undefined &&
          ["preparing", "starting", "running", "waiting", "queued"].includes(observedRun.status);
        const held =
          !executing &&
          item.workThreadId !== null &&
          item.runId !== null &&
          (yield* threads.delegatedTaskResultPending(ThreadId.make(item.workThreadId), undefined, [
            RunId.make(item.runId),
          ]));
        yield* sql.withTransaction(
          Effect.gen(function* () {
            const [current] =
              yield* sql<EntryRow>`SELECT w.*,e.state AS queue_state,e.claim_id,e.wake_generation
              FROM workbench_work_items w JOIN workbench_queue_entries e ON e.work_item_id=w.id WHERE w.id=${row.id}`;
            // Native observation may yield while admission requeues this same cycle under a new claim.
            if (
              current === undefined ||
              current.queue_state !== "occupied" ||
              current.claim_id !== row.claim_id ||
              current.wake_generation !== row.wake_generation ||
              current.version !== item.version ||
              current.latest_event_sequence !== item.latestEventSequence ||
              current.dispatched_through_sequence !== item.dispatchedThroughSequence ||
              current.processed_through_sequence !== item.processedThroughSequence ||
              current.business_state !== item.businessState ||
              current.run_id !== item.runId ||
              current.state !== item.state ||
              current.error_code !== item.errorCode
            )
              return;
            if (item.errorCode === "batch_too_large") {
              yield* sql`UPDATE workbench_queue_entries SET needs_dispatch=0,work_state='unknown',blocked_reason='state_unknown' WHERE work_item_id=${row.id}`;
              return;
            }
            if (item.workThreadId === null || item.delegatedTaskId === null) {
              yield* sql`UPDATE workbench_queue_entries SET work_state='unknown',blocked_reason=${item.requestId === null ? "awaiting_dispatcher" : "state_unknown"},needs_dispatch=1 WHERE work_item_id=${row.id}`;
              return;
            }
            const batches =
              yield* sql<Lifecycle.BatchRow>`SELECT * FROM workbench_work_batches WHERE work_item_id=${row.id}`;
            const uncertain =
              item.state === "unknown" ||
              batches.some((batch) => batch.state === "unknown" || batch.state === "failed");
            const active = batches.filter(
              (batch) => !["completed", "failed"].includes(batch.state),
            );
            const needs =
              current.latest_event_sequence > current.dispatched_through_sequence ||
              batches.some((batch) =>
                ["pending", "dispatching", "unknown"].includes(batch.state),
              ) ||
              preparation;
            const settled =
              repairReady &&
              !uncertain &&
              !needs &&
              !executing &&
              !held &&
              item.state !== "blocked" &&
              active.length === 0 &&
              current.processed_through_sequence === current.latest_event_sequence &&
              ["awaiting_delivery", "completed"].includes(current.business_state);
            const state = uncertain
              ? "unknown"
              : active.some((batch) => batch.work_state === "waiting_for_children")
                ? "waiting_for_children"
                : active.length > 0
                  ? "working"
                  : settled
                    ? "result_available"
                    : !repairReady
                      ? "working"
                      : "unknown";
            yield* sql`UPDATE workbench_queue_entries SET state=${settled ? "finished" : "occupied"},work_state=${state},needs_dispatch=${needs ? 1 : 0},
              blocked_reason=${item.state === "blocked" ? "approval" : uncertain ? "state_unknown" : needs ? "awaiting_dispatcher" : null},updated_at=${DateTime.formatIso(yield* DateTime.now)}
              WHERE work_item_id=${row.id}`;
          }),
        );
        return;
      }
      if (item.errorCode === "batch_too_large") {
        yield* sql`UPDATE workbench_queue_entries SET needs_dispatch=0,work_state='unknown',blocked_reason='state_unknown' WHERE work_item_id=${row.id} AND state='occupied'`;
        return;
      }
      if (item.workThreadId === null || item.delegatedTaskId === null) {
        yield* sql`UPDATE workbench_queue_entries SET work_state='unknown',blocked_reason=${item.requestId === null ? "awaiting_dispatcher" : "state_unknown"},needs_dispatch=1 WHERE work_item_id=${row.id} AND state='occupied'`;
        return;
      }
      const parent = yield* threads.getThreadRecords(
        ThreadId.make(item.dispatcherThreadId),
        ["subagents"],
        { subagentIds: [NodeId.make(item.delegatedTaskId)] },
      );
      const task = parent.subagents.find(
        (task) =>
          task.id === item.delegatedTaskId &&
          task.origin === "app_owned" &&
          task.childThreadId === item.workThreadId &&
          task.runId === item.parentRunId,
      );
      if (task === undefined) return yield* new WorkbenchQueueError({ reason: "invalid_binding" });
      const child = yield* threads.getThreadRecords(
        ThreadId.make(item.workThreadId),
        [
          "runs",
          "messages",
          "contextTransfers",
          "subagents",
          "providerThreads",
          "providerTurns",
          "runtimeRequests",
          "nodes",
        ],
        { messageRoles: ["user"], omitSettledLeafNodes: true, omitSettledSubagents: true },
      );
      const { childRun, taskWorkState } = delegatedTaskWorkProgress(child, task);
      if (childRun === undefined) return yield* new WorkbenchQueueError({ reason: "unavailable" });
      const answeredRun = child.runs.find((run) => run.id === item.runId);
      if (answeredRun !== undefined && answeredRun.id !== childRun.id) {
        const active = ["preparing", "starting", "running", "waiting", "queued"].includes(
          answeredRun.status,
        );
        const pending = yield* threads.delegatedTaskResultPending(
          ThreadId.make(item.workThreadId),
          answeredRun.delegatedTaskId,
          [answeredRun.id],
        );
        if (active || pending || answeredRun.status !== "completed") {
          yield* sql`UPDATE workbench_queue_entries SET work_state=${active || pending ? "working" : "unknown"},blocked_reason=${active || pending ? null : "state_unknown"},needs_dispatch=0
            WHERE work_item_id=${row.id} AND state='occupied' AND claim_id=${row.claim_id} AND wake_generation=${row.wake_generation}`;
          return;
        }
      }
      const held =
        task.result === null &&
        taskWorkState === "result_available" &&
        (yield* threads.delegatedTaskResultPending(
          ThreadId.make(item.workThreadId),
          childRun.delegatedTaskId,
        ));
      const workState =
        task.result !== null ? "result_available" : held ? "working" : taskWorkState;
      const settled = repairReady && workState === "result_available";
      const visibleWorkState =
        !repairReady && workState === "result_available" ? "working" : workState;
      const preparation = childRun.status === "preparing";
      yield* sql`UPDATE workbench_queue_entries SET state=${settled ? "finished" : "occupied"},work_state=${visibleWorkState},needs_dispatch=${settled ? 0 : preparation ? 1 : 0},
      blocked_reason=${item.state === "blocked" ? "approval" : preparation ? "awaiting_dispatcher" : null},updated_at=${DateTime.formatIso(yield* DateTime.now)}
      WHERE work_item_id=${row.id} AND state='occupied' AND claim_id=${row.claim_id} AND wake_generation=${row.wake_generation}
      AND NOT EXISTS(SELECT 1 FROM workbench_runtime_responses a WHERE a.work_item_id=${row.id} AND a.state IN ('recorded','queued','submitted','unknown'))`;
    },
    (effect, row) =>
      effect.pipe(
        Effect.catch(
          () =>
            sql`UPDATE workbench_queue_entries SET work_state='unknown',blocked_reason='state_unknown' WHERE work_item_id=${row.id} AND state='occupied'`,
        ),
      ),
  );
  const recoverWakes = Effect.fn("WorkbenchQueue.recoverWakes")(
    function* (
      queueId: string,
      scope: WorkbenchQueueScope,
      eligible: ReadonlyArray<ScheduledTaskId>,
    ) {
      if (eligible.length === 0) return;
      const rows = yield* sql<{
        work_item_id: string;
        dispatcher_thread_id: ThreadId;
        claim_id: string;
        wake_generation: number;
        recovery_signal: string;
        created_at: string;
      }>`SELECT e.*,w.dispatcher_thread_id,w.created_at FROM workbench_queue_entries e JOIN workbench_work_items w ON w.id=e.work_item_id
      WHERE e.queue_id=${queueId} AND e.state='occupied' AND e.needs_dispatch=1 AND e.wake_sent=1 AND e.recovery_signal IS NOT NULL
      AND (e.handled_recovery_signal IS NULL OR e.handled_recovery_signal!=e.recovery_signal) AND w.task_id IN ${sql.in(eligible)}`;
      for (const row of rows) {
        yield* checkTracking(scope);
        const control = yield* threads.getThreadRecords(
          row.dispatcher_thread_id,
          ["runs", "turnItems"],
          {
            runStatuses: ["preparing", "starting", "running", "waiting"],
            turnItemTypes: ["run_interrupt_request"],
          },
        );
        if (
          control.runs.length > 0 ||
          control.thread.archivedAt !== null ||
          control.thread.deletedAt !== null
        )
          continue;
        const key = `${row.claim_id}:${row.wake_generation}`;
        const prior = yield* threads.getThreadRecords(row.dispatcher_thread_id, ["messages"], {
          messageIds: [MessageId.make(`message:workbench-queue:${key}`)],
        });
        const message = prior.messages[0];
        if (message?.runId == null) continue;
        const run = (yield* threads.getThreadRecords(row.dispatcher_thread_id, ["runs"], {
          runIds: [message.runId],
        })).runs[0];
        if (run === undefined) continue;
        if (
          control.turnItems.some(
            (item) =>
              DateTime.toEpochMillis(item.startedAt ?? item.updatedAt) >=
              DateTime.toEpochMillis(run.requestedAt),
          )
        ) {
          yield* sql`UPDATE workbench_queue_entries SET blocked_reason='state_unknown',handled_recovery_signal=${row.recovery_signal} WHERE work_item_id=${row.work_item_id}`;
          continue;
        }
        if (run.status === "queued") {
          const cancelled = yield* engine
            .dispatch({
              type: "queued-run.cancel",
              commandId: CommandId.make(`server:queue-wake-retire:${key}:${row.recovery_signal}`),
              threadId: row.dispatcher_thread_id,
              runId: run.id,
            })
            .pipe(Effect.result);
          if (cancelled._tag === "Failure") continue;
        }
        yield* sql.withTransaction(
          Effect.gen(function* () {
            yield* checkTracking(scope);
            yield* sql`UPDATE workbench_queue_entries SET wake_generation=wake_generation+1,wake_sent=0,handled_recovery_signal=${row.recovery_signal}
          WHERE work_item_id=${row.work_item_id} AND state='occupied' AND needs_dispatch=1 AND wake_sent=1 AND wake_generation=${row.wake_generation}
          AND recovery_signal=${row.recovery_signal}`;
          }),
        );
      }
    },
    Effect.mapError((error) =>
      error._tag === "WorkbenchQueueError"
        ? error
        : WorkbenchQueueFailure.fromCause("unavailable", error),
    ),
  );
  const reconcile = Effect.fn("WorkbenchQueue.reconcile")(
    function* (queueId: string, scope: WorkbenchQueueScope) {
      yield* authorize(queueId, scope);
      return yield* locks.withLock(
        queueId,
        Effect.gen(function* () {
          yield* checkTracking(scope);
          const bindings =
            yield* sql<BindingRow>`SELECT * FROM workbench_queue_bindings WHERE queue_id=${queueId}`;
          const eligible: ScheduledTaskId[] = [];
          for (const binding of bindings) {
            const task = yield* items.binding(binding.task_id).pipe(Effect.option);
            if (
              task._tag === "Some" &&
              task.value.createdAt === binding.task_created_at &&
              task.value.projectId === binding.project_id &&
              task.value.threadId === binding.dispatcher_thread_id &&
              task.value.enabled &&
              task.value.blockedByPolicy === undefined
            )
              eligible.push(binding.task_id);
          }
          yield* sql.withTransaction(
            Effect.gen(function* () {
              yield* checkTracking(scope);
              yield* synchronize(queueId);
            }),
          );
          for (const row of yield* entries(queueId, true)) yield* observeEntry(row);
          yield* sql.withTransaction(
            Effect.gen(function* () {
              yield* checkTracking(scope);
              if (eligible.length === 0) return;
              const [count] = yield* sql<{
                occupied: number;
              }>`SELECT COUNT(*) AS occupied FROM workbench_queue_entries WHERE queue_id=${queueId} AND state='occupied'`;
              let free = Math.max(0, 2 - (count?.occupied ?? 0));
              const waiting = yield* sql<{
                work_item_id: string;
              }>`SELECT e.work_item_id FROM workbench_queue_entries e JOIN workbench_work_items w ON w.id=e.work_item_id
          WHERE e.queue_id=${queueId} AND e.state='queued' AND (w.attention_hold=0 OR EXISTS(SELECT 1 FROM workbench_attention_items a WHERE a.work_item_id=w.id AND a.phase='requeue_pending') OR EXISTS(SELECT 1 FROM workbench_runtime_responses r WHERE r.work_item_id=w.id AND r.state IN ('recorded','queued'))) AND w.task_id IN ${sql.in(eligible)} ORDER BY e.priority DESC,e.enqueue_sequence LIMIT ${free}`;
              for (const row of waiting) {
                if (free-- <= 0) break;
                yield* sql`UPDATE workbench_queue_entries SET state='occupied',claim_id='queue-claim:' || work_item_id || CASE WHEN wake_generation=0 THEN '' ELSE ':' || wake_generation END,blocked_reason='awaiting_dispatcher',wake_sent=0,
            updated_at=${DateTime.formatIso(yield* DateTime.now)} WHERE work_item_id=${row.work_item_id} AND state='queued'
            AND (SELECT COUNT(*) FROM workbench_queue_entries WHERE queue_id=${queueId} AND state='occupied')<2`;
              }
              yield* sql`UPDATE workbench_queue_entries SET blocked_reason=CASE WHEN (SELECT COUNT(*) FROM workbench_queue_entries WHERE queue_id=${queueId} AND state='occupied')>2 THEN 'over_capacity' ELSE 'queue_full' END WHERE queue_id=${queueId} AND state='queued'`;
            }),
          );
          yield* recoverWakes(queueId, scope, eligible);
          const wakes = yield* sql<{
            work_item_id: string;
            task_id: string;
            dispatcher_thread_id: ThreadId;
            claim_id: string;
            wake_generation: number;
          }>`SELECT e.work_item_id,e.claim_id,e.wake_generation,w.task_id,w.dispatcher_thread_id FROM workbench_queue_entries e JOIN workbench_work_items w ON w.id=e.work_item_id
        WHERE e.queue_id=${queueId} AND e.state='occupied' AND e.wake_sent=0 AND e.needs_dispatch=1 AND ${eligible.length === 0 ? sql`0=1` : sql`w.task_id IN ${sql.in(eligible)}`}`;
          for (const row of wakes) {
            yield* checkTracking(scope);
            const key = `${row.claim_id}:${row.wake_generation}`;
            yield* engine
              .dispatch({
                type: "message.dispatch",
                commandId: CommandId.make(`server:workbench-queue:${key}`),
                threadId: row.dispatcher_thread_id,
                messageId: MessageId.make(`message:workbench-queue:${key}`),
                text: `Workbench queue ${queueId} reserved work item ${row.work_item_id}. Call t3_workbench_dispatch with taskId ${row.task_id}; drain only immediately available reservations, then end your turn. Do not poll running children or bypass the queue with delegate_task.`,
                attachments: [],
                dispatchMode: { type: "queue_after_active" },
                createdBy: "agent",
                creationSource: "server",
              })
              .pipe(
                Effect.andThen(
                  sql`UPDATE workbench_queue_entries SET wake_sent=1,handled_recovery_signal=recovery_signal WHERE work_item_id=${row.work_item_id} AND claim_id=${row.claim_id} AND wake_generation=${row.wake_generation}`,
                ),
                Effect.catchCause(() => Effect.void),
              );
          }
          return yield* snapshot(queueId, scope);
        }),
      );
    },
    Effect.catchTags({
      SqlError: (cause) => Effect.fail(WorkbenchQueueFailure.fromCause("unavailable", cause)),
    }),
  );
  const access = Effect.fn("WorkbenchQueue.access")(
    function* (taskId: ScheduledTaskId, businessDeliveryId?: string) {
      const [binding] = yield* sql<
        BindingRow & { controller_id: string }
      >`SELECT b.*,q.controller_id FROM workbench_queue_bindings b JOIN workbench_queues q ON q.id=b.queue_id WHERE b.task_id=${taskId}`;
      if (!binding) return yield* new WorkbenchQueueError({ reason: "invalid_binding" });
      const task = yield* items
        .binding(taskId)
        .pipe(
          Effect.mapError((cause) => WorkbenchQueueFailure.fromCause("invalid_binding", cause)),
        );
      if (
        task.createdAt !== binding.task_created_at ||
        task.projectId !== binding.project_id ||
        task.threadId !== binding.dispatcher_thread_id
      )
        return yield* new WorkbenchQueueError({ reason: "invalid_binding" });
      yield* reconcile(binding.queue_id, {
        controllerId: binding.controller_id,
        projectId: task.projectId,
      });
      const [selected] = yield* sql<{
        work_item_id: string;
        claim_id: string;
        attention_epoch: number;
      }>`SELECT e.work_item_id,e.claim_id,w.attention_epoch FROM workbench_queue_entries e JOIN workbench_work_items w ON w.id=e.work_item_id
      WHERE e.queue_id=${binding.queue_id} AND w.task_id=${taskId} AND w.task_created_at=${binding.task_created_at} AND w.project_id=${binding.project_id} AND w.dispatcher_thread_id=${binding.dispatcher_thread_id}
      AND (w.attention_hold=0 OR EXISTS(SELECT 1 FROM workbench_attention_items a WHERE a.work_item_id=w.id AND a.phase IN ('requeue_pending','resuming')))
      AND ${businessDeliveryId === undefined ? sql`e.state='occupied' AND e.needs_dispatch=1` : sql`(w.business_delivery_id=${businessDeliveryId} OR EXISTS(SELECT 1 FROM workbench_work_item_events m WHERE m.work_item_id=w.id AND m.business_delivery_id=${businessDeliveryId})) AND e.state IN ('occupied','finished')`}
      ORDER BY e.priority DESC,e.enqueue_sequence LIMIT 1`;
      return selected === undefined
        ? null
        : {
            workItemId: selected.work_item_id,
            claimId: selected.claim_id,
            attentionEpoch: selected.attention_epoch,
          };
    },
    Effect.catchTags({
      SqlError: (cause) => Effect.fail(WorkbenchQueueFailure.fromCause("unavailable", cause)),
    }),
  );
  const dispatched = Effect.fn("WorkbenchQueue.dispatched")(
    function* (workItemId: string) {
      yield* sql`UPDATE workbench_queue_entries SET needs_dispatch=0,wake_sent=1 WHERE work_item_id=${workItemId} AND state='occupied' AND EXISTS(SELECT 1 FROM workbench_work_items w WHERE w.id=${workItemId} AND w.attention_hold=1)`;
      yield* sql`UPDATE workbench_queue_entries SET
        needs_dispatch=CASE WHEN EXISTS(SELECT 1 FROM workbench_work_items w WHERE w.id=${workItemId} AND w.matter_id IS NOT NULL AND w.latest_event_sequence>w.dispatched_through_sequence) THEN 1 ELSE 0 END,
        wake_sent=CASE WHEN EXISTS(SELECT 1 FROM workbench_work_items w WHERE w.id=${workItemId} AND w.matter_id IS NOT NULL AND w.latest_event_sequence>w.dispatched_through_sequence) THEN 0 ELSE wake_sent END,
        wake_generation=wake_generation+CASE WHEN EXISTS(SELECT 1 FROM workbench_work_items w WHERE w.id=${workItemId} AND w.matter_id IS NOT NULL AND w.latest_event_sequence>w.dispatched_through_sequence) THEN 1 ELSE 0 END,
        blocked_reason=NULL WHERE work_item_id=${workItemId} AND state='occupied' AND EXISTS(SELECT 1 FROM workbench_work_items w WHERE w.id=${workItemId} AND w.attention_hold=0)`;
    },
    Effect.catchTags({
      SqlError: (cause) => Effect.fail(WorkbenchQueueFailure.fromCause("unavailable", cause)),
    }),
  );
  const remaining = Effect.fn("WorkbenchQueue.remaining")(
    function* (taskId: ScheduledTaskId) {
      const [count] = yield* sql<{
        remaining: number;
      }>`SELECT COUNT(*) AS remaining FROM workbench_queue_entries e JOIN workbench_work_items w ON w.id=e.work_item_id
      JOIN workbench_queue_bindings b ON b.task_id=w.task_id AND b.task_created_at=w.task_created_at AND b.queue_id=e.queue_id
      WHERE w.task_id=${taskId} AND e.state='occupied' AND e.needs_dispatch=1`;
      return count?.remaining ?? 0;
    },
    Effect.catchTags({
      SqlError: (cause) => Effect.fail(WorkbenchQueueFailure.fromCause("unavailable", cause)),
    }),
  );
  const prepareDeliveryIntent = Effect.fn("WorkbenchQueue.prepareIntent")(function* (
    queueId: string,
    input: WorkbenchDeliveryIntentInput,
  ) {
    const task = yield* items
      .binding(input.taskId)
      .pipe(Effect.mapError((cause) => WorkbenchQueueFailure.fromCause("invalid_binding", cause)));
    if (
      task.createdAt !== input.taskCreatedAt ||
      task.threadId !== input.dispatcherThreadId ||
      task.projectId !== input.projectId ||
      !task.enabled ||
      task.blockedByPolicy !== undefined
    )
      return yield* new WorkbenchQueueError({ reason: "invalid_binding" });
    return yield* Lifecycle.prepareDeliveryIntent(queueId, input).pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
      Effect.catchTags({
        SqlError: (cause) => Effect.fail(WorkbenchQueueFailure.fromCause("unavailable", cause)),
        SchemaError: (cause) =>
          Effect.fail(WorkbenchQueueFailure.fromCause("invalid_binding", cause)),
      }),
    );
  });
  return {
    bind,
    get: snapshot,
    priority,
    reconcile,
    access,
    dispatched,
    remaining,
    prepareDeliveryIntent,
  };
});
export const layer = Layer.effect(WorkbenchQueue, make);
