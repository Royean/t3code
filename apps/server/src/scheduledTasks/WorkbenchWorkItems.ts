import {
  NodeId,
  type ScheduledTaskId,
  type ScheduledTask,
  ScheduledTaskError,
  ThreadId,
  WorkbenchMatter,
  type OrchestrationV2ThreadProjection,
  type WorkbenchWorkItem,
  type WorkbenchWorkItemsResult,
  type WorkbenchDeployment,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Option from "effect/Option";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as ScheduledTasks from "./ScheduledTaskService.ts";
import { ledgerError } from "./executionLedger.ts";
import * as GitDriver from "../vcs/GitVcsDriver.ts";
import * as Relations from "./WorkbenchChildRelations.ts";
import * as Lifecycle from "./workbenchLifecycle.ts";
import { canonicalReleaseJson, releaseDigest } from "./workbenchReleaseProfile.ts";
import {
  delegatedTaskAwaitsWork,
  delegatedTaskResultRun,
} from "../orchestration-v2/RestartBackgroundNote.ts";
const decodeMatter = Schema.decodeUnknownEffect(Schema.fromJsonString(WorkbenchMatter));

export const completeAcceptedDelivery = Effect.fn("WorkbenchWorkItems.completeAcceptedDelivery")(
  function* (previous: WorkbenchDeployment, next: WorkbenchDeployment) {
    const sql = yield* SqlClient.SqlClient,
      b = previous.binding,
      m = next.merge,
      r = next.delivery;
    const refused = () =>
      new ScheduledTaskError({ message: "Accepted delivery changed before atomic completion." });
    if (
      next.state !== "delivered" ||
      !r ||
      !m ||
      m.state !== "completed" ||
      !m.result ||
      m.failure ||
      m.recovery ||
      m.claim.state !== "committed" ||
      previous.acceptance?.state !== "passed" ||
      m.input.acceptanceDigest !== releaseDigest(previous.acceptance) ||
      m.result.commitSha !== next.desired.sourceSha ||
      r.mergeDigest !== releaseDigest(m) ||
      r.workItemVersionBefore !== b.workItemVersion ||
      r.workItemVersionAfter !== b.workItemVersion + 1 ||
      r.latestEventSequence !== b.latestEventSequence ||
      r.processedThroughSequence !== b.processedThroughSequence ||
      b.latestEventSequence !== b.processedThroughSequence
    )
      return yield* refused();
    yield* sql.withTransaction(
      Effect.gen(function* () {
        const active =
          yield* sql`WITH RECURSIVE related(thread_id) AS (SELECT ${b.workThreadId} UNION SELECT s.child_thread_id FROM orchestration_v2_projection_subagents s JOIN related x ON s.thread_id=x.thread_id WHERE s.child_thread_id IS NOT NULL)
      SELECT 1 FROM orchestration_v2_projection_runs WHERE thread_id IN (SELECT thread_id FROM related) AND status IN ('preparing','starting','running','waiting','queued')
      UNION SELECT 1 FROM orchestration_v2_projection_runtime_requests WHERE thread_id IN (SELECT thread_id FROM related) AND status='pending'
      UNION SELECT 1 FROM orchestration_v2_projection_subagents WHERE thread_id IN (SELECT thread_id FROM related) AND (status IN ('pending','running','waiting') OR completion_delivery_state IN ('pending','claimed'))
      UNION SELECT 1 FROM orchestration_v2_projection_provider_threads WHERE thread_id IN (SELECT thread_id FROM related) AND json_array_length(json_extract(payload_json,'$.pendingBackgroundTasks'))>0 LIMIT 1`;
        if (active.length) return yield* refused();
        const changed =
          yield* sql`UPDATE workbench_work_items SET business_state='completed',completed_at=${r.completedAt},version=version+1,handoff_ref=${r.handoffRef},preserved_ref=${r.preservedRef} WHERE id=${b.workItemId} AND task_id=${b.taskId} AND task_created_at=${b.taskCreatedAt} AND project_id=${b.projectId} AND dispatcher_thread_id=${b.dispatcherThreadId} AND work_thread_id=${b.workThreadId} AND cycle_number=${b.cycleNumber} AND version=${b.workItemVersion} AND latest_event_sequence=${b.latestEventSequence} AND processed_through_sequence=${b.processedThroughSequence} AND business_state='awaiting_delivery' AND attention_hold=0 AND recovery_ref IS NULL
      AND EXISTS(SELECT 1 FROM workbench_release_obligations o WHERE o.work_item_id=${b.workItemId} AND o.task_created_at=${b.taskCreatedAt} AND o.cycle_number=${b.cycleNumber})
      AND EXISTS(SELECT 1 FROM scheduled_tasks st
        JOIN workbench_queue_bindings qb ON qb.task_id=st.task_id AND qb.task_created_at=st.created_at AND qb.project_id=st.project_id AND qb.dispatcher_thread_id=st.thread_id
        JOIN workbench_queues q ON q.id=qb.queue_id AND q.id=${b.queueId} AND q.controller_id=${b.controllerId} AND q.project_id=${b.projectId}
        JOIN scheduled_task_execution_metadata meta ON meta.singleton=1 AND meta.tracking_id=${b.trackingId}
        JOIN projection_projects p ON p.project_id=st.project_id AND p.deleted_at IS NULL AND p.archived_at IS NULL
        JOIN orchestration_v2_projection_threads parent ON parent.thread_id=st.thread_id AND parent.project_id=st.project_id AND parent.deleted_at IS NULL AND parent.archived_at IS NULL
        JOIN orchestration_v2_projection_threads child ON child.thread_id=${b.workThreadId} AND child.project_id=st.project_id AND child.deleted_at IS NULL AND child.archived_at IS NULL
        WHERE st.task_id=${b.taskId} AND st.created_at=${b.taskCreatedAt} AND st.project_id=${b.projectId} AND st.thread_id=${b.dispatcherThreadId}
          AND json_extract(child.payload_json,'$.worktreePath')=workbench_work_items.worktree_path AND json_extract(child.payload_json,'$.branch')=workbench_work_items.branch)
      AND NOT EXISTS(SELECT 1 FROM workbench_work_batches WHERE work_item_id=${b.workItemId} AND state!='completed')
      AND NOT EXISTS(SELECT 1 FROM workbench_runtime_responses WHERE work_item_id=${b.workItemId} AND state IN ('recorded','queued','submitted','unknown')) RETURNING id`;
        if (changed.length !== 1) return yield* refused();
        yield* sql`INSERT INTO workbench_delivery_receipts(deployment_id,work_item_id,receipt_json,created_at) VALUES(${next.id},${b.workItemId},${canonicalReleaseJson(r)},${r.completedAt})`;
        const saved =
          yield* sql`UPDATE workbench_deployments SET deployment_json=${canonicalReleaseJson(next)},state='delivered',updated_at=${next.updatedAt} WHERE id=${previous.id} AND deployment_json=${canonicalReleaseJson(previous)} RETURNING id`;
        if (saved.length !== 1) return yield* refused();
      }),
    );
  },
  Effect.mapError(ledgerError),
);

function batchRuns(
  child: Pick<OrchestrationV2ThreadProjection, "runs" | "messages" | "nodes" | "runtimeRequests">,
  rootId: string,
  repairs: ReadonlyArray<{ source_run_id: string; run_id: string }> = [],
) {
  const owned = new Set([rootId]);
  for (let grew = true; grew;) {
    grew = false;
    for (const repair of repairs)
      if (owned.has(repair.source_run_id) && !owned.has(repair.run_id)) {
        owned.add(repair.run_id);
        grew = true;
      }
    for (const run of child.runs)
      if (
        run.restartContinuationOfRunId !== undefined &&
        owned.has(run.restartContinuationOfRunId) &&
        !owned.has(run.id)
      ) {
        owned.add(run.id);
        grew = true;
      }
    for (const request of child.runtimeRequests) {
      if (request.status !== "resolved" || request.responseCapability.type !== "message") continue;
      const source = child.nodes.find((node) => node.id === request.nodeId)?.runId;
      const answer = child.messages.find(
        (message) =>
          message.id === `async-answer:${request.id}` && message.creationSource === "server",
      );
      if (
        source !== undefined &&
        source !== null &&
        owned.has(source) &&
        answer?.runId != null &&
        !owned.has(answer.runId)
      ) {
        owned.add(answer.runId);
        grew = true;
      }
    }
    for (const message of child.messages)
      if (
        message.runId !== null &&
        message.delegatedCompletion !== undefined &&
        owned.has(message.delegatedCompletion.parentRunId) &&
        !owned.has(message.runId)
      ) {
        owned.add(message.runId);
        grew = true;
      }
  }
  return child.runs.filter((run) => owned.has(run.id));
}

export interface WorkItemRow {
  attention_epoch: number;
  attention_hold: number;
  matter_id: string | null;
  cycle_number: number;
  previous_work_item_id: string | null;
  business_state: "active" | "awaiting_delivery" | "completed";
  latest_event_sequence: number;
  dispatched_through_sequence: number;
  processed_through_sequence: number;
  version: number;
  handoff_ref: string | null;
  preserved_ref: string | null;
  recovery_ref: string | null;
  id: string;
  task_id: string;
  task_created_at: string;
  project_id: string;
  business_delivery_id: string;
  dispatcher_thread_id: string;
  source_execution_id: string | null;
  event_json: string;
  base_ref: string | null;
  request_id: string | null;
  request_json: string | null;
  parent_run_id: string | null;
  delegated_task_id: string | null;
  work_thread_id: string | null;
  message_id: string | null;
  accepted_run_id: string | null;
  run_id: string | null;
  branch: string | null;
  worktree_path: string | null;
  state: WorkbenchWorkItem["state"];
  error_code: string | null;
  created_at: string;
  updated_at: string;
}

export class WorkbenchWorkItems extends Context.Service<
  WorkbenchWorkItems,
  {
    readonly complete: (input: {
      workItemId: string;
      expectedVersion: number;
      expectedLatestEventSequence: number;
      processedThroughSequence: number;
      handoffRef: string;
      preservedRef: string;
    }) => Effect.Effect<WorkbenchWorkItem, ScheduledTaskError>;
    readonly binding: (taskId: ScheduledTaskId) => Effect.Effect<ScheduledTask, ScheduledTaskError>;
    readonly observe: (
      row: WorkItemRow,
      queriedDeliveryId?: string,
    ) => Effect.Effect<WorkbenchWorkItem, ScheduledTaskError>;
    readonly get: (
      taskId: ScheduledTaskId,
      businessDeliveryId: string,
    ) => Effect.Effect<WorkbenchWorkItemsResult, ScheduledTaskError>;
  }
>()("t3/scheduledTasks/WorkbenchWorkItems") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const git = yield* GitDriver.GitVcsDriver;
  const relations = yield* Relations.WorkbenchChildRelations;
  const tasks = yield* ScheduledTasks.ScheduledTaskService;
  const threads = yield* ThreadManagement.ThreadManagementService;
  const binding = Effect.fn("WorkbenchWorkItems.binding")(function* (taskId: ScheduledTaskId) {
    const task = (yield* tasks.list()).tasks.find((task) => task.id === taskId);
    if (
      task === undefined ||
      task.schedule.type !== "webhook" ||
      task.threadMode !== "continue" ||
      task.threadId === null
    )
      return yield* new ScheduledTaskError({
        message: "A fixed webhook dispatcher task is required.",
      });
    const parent = yield* threads.getThreadShell(task.threadId).pipe(Effect.mapError(ledgerError));
    if (
      parent === null ||
      parent.deletedAt !== null ||
      parent.archivedAt !== null ||
      parent.projectId !== task.projectId
    )
      return yield* new ScheduledTaskError({
        message: "The dispatcher no longer belongs to this project.",
      });
    return task;
  });
  const observeBatches = Effect.fn("WorkbenchWorkItems.observeBatches")(function* (
    row: WorkItemRow,
  ) {
    const repairs = yield* sql<{
      source_run_id: string;
      run_id: string;
    }>`SELECT source_run_id,run_id FROM workbench_repair_attempts WHERE work_item_id=${row.id} AND charged=1 AND run_id IS NOT NULL`;
    const batches =
      yield* sql<Lifecycle.BatchRow>`SELECT * FROM workbench_work_batches WHERE work_item_id=${row.id} ORDER BY from_sequence`.pipe(
        Effect.mapError(ledgerError),
      );
    for (const batch of batches) {
      if (batch.command_id === null || row.request_id === null) continue;
      const relation = yield* relations.resolveBatch(
        {
          id: row.id,
          requestId: row.request_id,
          dispatcherThreadId: row.dispatcher_thread_id,
          projectId: row.project_id,
        },
        batch,
      );
      if (Option.isNone(relation)) {
        yield* sql`UPDATE workbench_work_batches SET state='unknown',work_state='unknown' WHERE id=${batch.id}`.pipe(
          Effect.mapError(ledgerError),
        );
        continue;
      }
      const link = relation.value;
      const child = yield* threads.getThreadRecords(link.workThreadId, [
        "runs",
        "messages",
        "contextTransfers",
        "subagents",
        "providerThreads",
        "runtimeRequests",
        "nodes",
      ]);
      const run = child.runs.find((run) => run.id === link.runId)!;
      const resumeIntents = yield* sql<{
        run_id: string;
        message_id: string;
      }>`SELECT run_id,message_id FROM workbench_attention_resume_intents WHERE work_item_id=${row.id} AND batch_id=${batch.id} AND run_id IS NOT NULL AND state IN ('accepted','running','completed')`;
      const responseRuns = yield* sql<{
        run_id: string;
        async_message_id: string;
      }>`SELECT run_id,async_message_id FROM workbench_runtime_responses WHERE work_item_id=${row.id} AND json_extract(source_json,'$.batch_id')=${batch.id} AND run_id IS NOT NULL AND state NOT IN ('rejected')`;
      const resumeRuns = child.runs.filter((candidate) =>
        resumeIntents.some(
          (intent) =>
            candidate.id === intent.run_id && candidate.userMessageId === intent.message_id,
        ),
      );
      const related = [
        ...new Map(
          [
            run,
            ...resumeRuns,
            ...child.runs.filter((candidate) =>
              responseRuns.some(
                (response) =>
                  response.run_id === candidate.id &&
                  response.async_message_id === candidate.userMessageId,
              ),
            ),
          ]
            .flatMap((root) => batchRuns(child, root.id, repairs))
            .map((candidate) => [candidate.id, candidate]),
        ).values(),
      ];
      const result = delegatedTaskResultRun(related);
      const ended =
        result !== undefined &&
        ["completed", "failed", "cancelled", "interrupted", "rolled_back"].includes(result.status);
      const awaits = delegatedTaskAwaitsWork(related, child);
      const held =
        ended &&
        (yield* threads.delegatedTaskResultPending(
          link.workThreadId,
          link.delegatedTaskId,
          related.map((run) => run.id),
        ));
      const workState =
        !ended || held ? "working" : awaits ? "waiting_for_children" : "result_available";
      const terminal =
        result !== undefined &&
        ["completed", "failed", "cancelled", "interrupted", "rolled_back"].includes(
          result.status,
        ) &&
        workState === "result_available";
      const outcome = terminal
        ? result?.status === "completed"
          ? "succeeded"
          : result?.status === "failed"
            ? "failed"
            : "cancelled"
        : null;
      yield* sql`UPDATE workbench_work_batches SET message_id=${link.messageId},accepted_run_id=COALESCE(accepted_run_id,${link.acceptedRunId}),run_id=${result?.id ?? link.runId},
        delegated_task_id=${link.delegatedTaskId},parent_run_id=${link.parentRunId},state=${terminal ? (outcome === "succeeded" ? "completed" : "failed") : "dispatched"},
        outcome=${outcome},work_state=${workState},updated_at=${DateTime.formatIso(yield* DateTime.now)} WHERE id=${batch.id}`.pipe(
        Effect.mapError(ledgerError),
      );
      yield* sql`UPDATE workbench_work_items SET dispatched_through_sequence=MAX(dispatched_through_sequence,${batch.through_sequence}) WHERE id=${row.id}`.pipe(
        Effect.mapError(ledgerError),
      );
    }
    return yield* sql.withTransaction(
      Effect.gen(function* () {
        const [cycle] =
          yield* sql<WorkItemRow>`SELECT * FROM workbench_work_items WHERE id=${row.id}`;
        const latest =
          yield* sql<Lifecycle.BatchRow>`SELECT * FROM workbench_work_batches WHERE work_item_id=${row.id} ORDER BY from_sequence`;
        let processed = 0;
        for (const batch of latest) {
          if (batch.state !== "completed" || batch.from_sequence !== processed + 1) break;
          processed = batch.through_sequence;
        }
        const selected = latest.find((batch) => batch.state !== "completed") ?? latest.at(-1);
        const current = latest.findLast((batch) => batch.run_id !== null);
        // Admission can advance this cycle while the native projections above are being read.
        const state =
          processed === cycle!.latest_event_sequence && processed > 0
            ? "awaiting_delivery"
            : "active";
        yield* sql`UPDATE workbench_work_items SET processed_through_sequence=${processed},
          business_state=CASE WHEN business_state='completed' THEN 'completed' ELSE ${state} END,
          version=version+CASE WHEN processed_through_sequence!=${processed} OR (business_state!='completed' AND business_state!=${state}) THEN 1 ELSE 0 END,
          run_id=COALESCE(${current?.run_id ?? null},run_id),message_id=COALESCE(${current?.message_id ?? null},message_id)
          WHERE id=${row.id}`;
        return selected === undefined ? null : Lifecycle.batchView(selected);
      }),
    );
  }, Effect.mapError(ledgerError));
  const observe = Effect.fn("WorkbenchWorkItems.observe")(function* (
    row: WorkItemRow,
    queriedDeliveryId?: string,
  ) {
    if (row.request_id !== null && row.work_thread_id === null) {
      const relation = yield* relations.resolve({
        id: row.id,
        requestId: row.request_id,
        dispatcherThreadId: row.dispatcher_thread_id,
        projectId: row.project_id,
      });
      if (Option.isSome(relation)) {
        const link = relation.value;
        yield* sql`UPDATE workbench_work_items SET delegated_task_id = ${link.delegatedTaskId}, work_thread_id = ${link.workThreadId}, parent_run_id = ${link.parentRunId},
          accepted_run_id = ${link.acceptedRunId}, state = 'unknown', error_code = NULL WHERE id = ${row.id}`.pipe(
          Effect.mapError(ledgerError),
        );
        const [current] =
          yield* sql<WorkItemRow>`SELECT * FROM workbench_work_items WHERE id = ${row.id}`.pipe(
            Effect.mapError(ledgerError),
          );
        row = current!;
      }
    }

    const deliveryId = queriedDeliveryId ?? row.business_delivery_id;
    const [membership] = yield* sql<{
      business_delivery_id: string;
      sequence: number;
      batch_id: string | null;
      source_execution_id: string;
    }>`
      SELECT * FROM workbench_work_item_events WHERE work_item_id=${row.id} AND business_delivery_id=${deliveryId}`.pipe(
      Effect.mapError(ledgerError),
    );
    const sourceExecutionId = membership?.source_execution_id ?? row.source_execution_id;
    // Reconcile the formal source message/run through its existing receipt-aware observer.
    const source = yield* tasks.getExecutions({
      id: row.task_id as ScheduledTaskId,
      businessDeliveryId: deliveryId,
    });
    const sourceRunId =
      source.effectiveExecutions.find((execution) => execution.id === sourceExecutionId)?.runId ??
      null;
    if (row.matter_id === null && row.work_thread_id !== null && row.delegated_task_id !== null) {
      const child = yield* threads
        .getThreadRecords(ThreadId.make(row.work_thread_id), [
          "runs",
          "messages",
          "runtimeRequests",
          "nodes",
        ])
        .pipe(Effect.mapError(ledgerError));
      const parent = yield* threads
        .getThreadRecords(ThreadId.make(row.dispatcher_thread_id), ["subagents"], {
          subagentIds: [NodeId.make(row.delegated_task_id)],
        })
        .pipe(Effect.mapError(ledgerError));
      const task = parent.subagents.find(
        (task) =>
          task.id === row.delegated_task_id &&
          task.origin === "app_owned" &&
          task.childThreadId === row.work_thread_id &&
          task.runId === row.parent_run_id,
      );
      if (
        task === undefined ||
        parent.thread.projectId !== row.project_id ||
        child.thread.projectId !== row.project_id ||
        child.thread.lineage.parentThreadId !== row.dispatcher_thread_id ||
        child.thread.lineage.relationshipToParent !== "subagent"
      )
        return yield* new ScheduledTaskError({
          message: "Work thread ownership does not match the dispatcher.",
        });
      const originalRuns = child.runs
        .filter(
          (run) =>
            run.delegatedTaskId === row.delegated_task_id ||
            (run.delegatedTaskId === undefined &&
              run.id === row.accepted_run_id &&
              child.thread.forkedFrom?.type === "node" &&
              child.thread.forkedFrom.nodeId === row.delegated_task_id),
        )
        .toSorted((a, b) => b.ordinal - a.ordinal);
      const responses = yield* sql<{
        run_id: string;
        async_message_id: string;
      }>`SELECT run_id,async_message_id FROM workbench_runtime_responses WHERE work_item_id=${row.id} AND run_id IS NOT NULL AND state!='rejected'`;
      const roots = [
        ...originalRuns,
        ...child.runs.filter((candidate) =>
          responses.some(
            (response) =>
              candidate.id === response.run_id &&
              candidate.userMessageId === response.async_message_id,
          ),
        ),
      ];
      const repairs = yield* sql<{
        source_run_id: string;
        run_id: string;
      }>`SELECT source_run_id,run_id FROM workbench_repair_attempts WHERE work_item_id=${row.id} AND charged=1 AND run_id IS NOT NULL`;
      const runs = [
        ...new Map(
          roots.flatMap((root) => batchRuns(child, root.id, repairs)).map((run) => [run.id, run]),
        ).values(),
      ].toSorted((a, b) => b.ordinal - a.ordinal);
      const run = runs[0];
      const accepted = originalRuns.at(-1);
      const nodeIds = new Set(
        child.nodes.filter((node) => node.runId === run?.id).map((node) => node.id),
      );
      const blocked = child.runtimeRequests.some(
        (request) =>
          run !== undefined &&
          !["completed", "failed", "cancelled", "interrupted", "rolled_back"].includes(
            run.status,
          ) &&
          request.status === "pending" &&
          ["command", "file-change", "file-read", "permission", "mcp-elicitation"].includes(
            request.kind,
          ) &&
          nodeIds.has(request.nodeId),
      );
      const now = DateTime.formatIso(yield* DateTime.now);
      yield* sql`UPDATE workbench_work_items SET run_id = ${run?.id ?? null},
        accepted_run_id = COALESCE(accepted_run_id, ${accepted?.id ?? null}), message_id = ${run?.userMessageId ?? null},
        branch = ${child.thread.branch}, worktree_path = ${child.thread.worktreePath},
        state = ${blocked ? "blocked" : run === undefined || run.status === "preparing" || child.thread.worktreePath === null ? "unknown" : "linked"}, error_code = NULL, updated_at = ${now}
        WHERE id = ${row.id}`.pipe(Effect.mapError(ledgerError));
      const [current] =
        yield* sql<WorkItemRow>`SELECT * FROM workbench_work_items WHERE id = ${row.id}`.pipe(
          Effect.mapError(ledgerError),
        );
      row = current!;
    }
    const activeBatch = row.matter_id === null ? null : yield* observeBatches(row);
    const [resume] = yield* sql<{
      run_id: string;
      message_id: string;
    }>`SELECT run_id,message_id FROM workbench_attention_resume_intents WHERE work_item_id=${row.id} AND run_id IS NOT NULL AND state IN ('accepted','running','completed') ORDER BY rowid DESC LIMIT 1`;
    if (resume && row.work_thread_id !== null) {
      const proof = yield* threads.getThreadRecords(
        ThreadId.make(row.work_thread_id),
        ["runs", "messages"],
        { messageIds: [resume.message_id as never] },
      );
      const resumedRun = proof.runs.find(
        (run) => run.id === resume.run_id && run.userMessageId === resume.message_id,
      );
      if (
        resumedRun &&
        proof.messages.some(
          (message) => message.id === resume.message_id && message.runId === resumedRun.id,
        )
      )
        yield* sql`UPDATE workbench_work_items SET run_id=${resumedRun.id},message_id=${resume.message_id} WHERE id=${row.id}`.pipe(
          Effect.mapError(ledgerError),
        );
    }
    if (row.matter_id !== null) {
      row = (yield* sql<WorkItemRow>`SELECT * FROM workbench_work_items WHERE id=${row.id}`.pipe(
        Effect.mapError(ledgerError),
      ))[0]!;
      if (row.work_thread_id !== null) {
        const child = yield* threads
          .getThreadRecords(ThreadId.make(row.work_thread_id), ["runs", "nodes", "runtimeRequests"])
          .pipe(Effect.mapError(ledgerError));
        const pending = child.runtimeRequests.some(
          (request) =>
            request.status === "pending" &&
            child.nodes.some(
              (node) =>
                node.id === request.nodeId &&
                child.runs.some(
                  (run) =>
                    run.id === node.runId &&
                    !["completed", "failed", "cancelled", "interrupted", "rolled_back"].includes(
                      run.status,
                    ),
                ),
            ),
        );
        const workspaceReady =
          child.thread.worktreePath !== null &&
          child.runs.some((run) => run.id === row.accepted_run_id && run.status !== "preparing");
        const preparedPath = workspaceReady ? child.thread.worktreePath : null,
          preparedBranch = workspaceReady ? child.thread.branch : null;
        yield* sql`UPDATE workbench_work_items SET branch=CASE WHEN worktree_path IS NULL AND ${preparedPath} IS NOT NULL THEN ${preparedBranch} ELSE branch END,worktree_path=COALESCE(worktree_path,${preparedPath}),
          state=${row.error_code === "batch_too_large" || pending ? "blocked" : !workspaceReady || activeBatch?.state === "unknown" ? "unknown" : "linked"},updated_at=${DateTime.formatIso(yield* DateTime.now)} WHERE id=${row.id}`.pipe(
          Effect.mapError(ledgerError),
        );
        row = (yield* sql<WorkItemRow>`SELECT * FROM workbench_work_items WHERE id=${row.id}`.pipe(
          Effect.mapError(ledgerError),
        ))[0]!;
      }
    }
    const [matter] =
      row.matter_id === null
        ? []
        : yield* sql<{
            matter_json: string | null;
          }>`SELECT matter_json FROM workbench_matters WHERE id=${row.matter_id}`.pipe(
            Effect.mapError(ledgerError),
          );
    return {
      id: row.id,
      businessTaskId: row.id,
      matter:
        matter?.matter_json == null
          ? null
          : yield* decodeMatter(matter.matter_json).pipe(Effect.mapError(ledgerError)),
      cycleNumber: row.cycle_number,
      previousTaskId: row.previous_work_item_id,
      businessState: row.business_state,
      latestEventSequence: row.matter_id === null ? 1 : row.latest_event_sequence,
      dispatchedThroughSequence:
        row.matter_id === null
          ? row.request_id === null
            ? 0
            : 1
          : row.dispatched_through_sequence,
      processedThroughSequence: row.processed_through_sequence,
      version: row.version,
      membership: {
        businessDeliveryId: deliveryId,
        sequence: membership?.sequence ?? 1,
        batchId: membership?.batch_id ?? null,
      },
      activeBatch,
      handoffRef: row.handoff_ref,
      businessDeliveryId: deliveryId,
      dispatcherThreadId: row.dispatcher_thread_id,
      sourceExecutionId,
      sourceRunId,
      requestId: row.request_id,
      parentRunId: row.parent_run_id,
      delegatedTaskId: row.delegated_task_id,
      workThreadId: row.work_thread_id,
      messageId: row.message_id,
      acceptedRunId: row.accepted_run_id,
      runId: row.run_id,
      branch: row.branch,
      worktreePath: row.worktree_path,
      state: row.state,
      createdAt: row.created_at,
      updatedAt: row.updated_at,
      errorCode: row.error_code,
    } satisfies WorkbenchWorkItem;
  }, Effect.mapError(ledgerError));
  const get = Effect.fn("WorkbenchWorkItems.get")(function* (
    taskId: ScheduledTaskId,
    businessDeliveryId: string,
  ) {
    const task = yield* binding(taskId);
    const [metadata] = yield* sql<{
      tracking_id: string;
    }>`SELECT tracking_id FROM scheduled_task_execution_metadata WHERE singleton = 1`.pipe(
      Effect.mapError(ledgerError),
    );
    const rows =
      yield* sql<WorkItemRow>`SELECT * FROM workbench_work_items WHERE task_id = ${task.id} AND task_created_at = ${task.createdAt} AND project_id = ${task.projectId} AND dispatcher_thread_id = ${task.threadId} AND (business_delivery_id = ${businessDeliveryId} OR EXISTS(SELECT 1 FROM workbench_work_item_events e WHERE e.work_item_id=workbench_work_items.id AND e.task_id=${task.id} AND e.task_created_at=${task.createdAt} AND e.business_delivery_id=${businessDeliveryId})) ORDER BY rowid`.pipe(
        Effect.mapError(ledgerError),
      );
    return {
      trackingId: metadata!.tracking_id,
      taskId: task.id,
      taskCreatedAt: task.createdAt,
      projectId: task.projectId,
      workItems: yield* Effect.forEach(rows, (row) => observe(row, businessDeliveryId)),
    };
  });
  const complete = Effect.fn("WorkbenchWorkItems.complete")(function* (input: {
    workItemId: string;
    expectedVersion: number;
    expectedLatestEventSequence: number;
    processedThroughSequence: number;
    handoffRef: string;
    preservedRef: string;
  }) {
    const refusal = () =>
      new ScheduledTaskError({
        message:
          "Verified business handoff and all current requirements are required before completion.",
      });
    if (
      input.preservedRef.length > 500 ||
      !/^refs\/t3\/workbench-handoffs\/[A-Za-z0-9._/-]+$/.test(input.preservedRef) ||
      !/^[A-Za-z0-9:._/-]{1,200}$/.test(input.handoffRef) ||
      ![
        input.expectedVersion,
        input.expectedLatestEventSequence,
        input.processedThroughSequence,
      ].every((value) => Number.isSafeInteger(value) && value >= 0)
    )
      return yield* refusal();
    const [saved] =
      yield* sql<WorkItemRow>`SELECT * FROM workbench_work_items WHERE id=${input.workItemId}`;
    if (
      (yield* sql`SELECT 1 FROM workbench_release_obligations WHERE work_item_id=${input.workItemId}`)
        .length
    )
      return yield* refusal();
    if (
      !saved ||
      saved.matter_id === null ||
      saved.work_thread_id === null ||
      saved.worktree_path === null ||
      saved.branch === null
    )
      return yield* refusal();
    const current = yield* observe(saved);
    if (
      current.businessState !== "awaiting_delivery" ||
      current.version !== input.expectedVersion ||
      current.latestEventSequence !== input.expectedLatestEventSequence ||
      current.processedThroughSequence !== input.processedThroughSequence ||
      input.processedThroughSequence !== input.expectedLatestEventSequence
    )
      return yield* refusal();
    const target = yield* threads.getThreadRecords(ThreadId.make(saved.work_thread_id), []);
    if (target.thread.worktreePath !== saved.worktree_path || target.thread.branch !== saved.branch)
      return yield* refusal();
    const branch = yield* git.execute({
      operation: "WorkbenchWorkItems.verifyHandoffBranch",
      cwd: saved.worktree_path,
      args: ["symbolic-ref", "--quiet", "--short", "HEAD"],
    });
    const status = yield* git.execute({
      operation: "WorkbenchWorkItems.verifyHandoffChanges",
      cwd: saved.worktree_path,
      args: ["status", "--porcelain=v1", "--untracked-files=all"],
    });
    const head = yield* git.resolveCommit({ cwd: saved.worktree_path, revision: "HEAD" });
    const proof = yield* git.resolveCommit({
      cwd: saved.worktree_path,
      revision: input.preservedRef,
    });
    if (
      branch.stdout.trim() !== saved.branch ||
      status.stdout.trim() !== "" ||
      head.commitSha !== proof.commitSha
    )
      return yield* refusal();
    yield* sql.withTransaction(
      Effect.gen(function* () {
        const [changed] =
          yield* sql`UPDATE workbench_work_items SET business_state='completed',completed_at=${DateTime.formatIso(yield* DateTime.now)},version=version+1,
        handoff_ref=${input.handoffRef},preserved_ref=${input.preservedRef}
        WHERE id=${input.workItemId} AND version=${input.expectedVersion} AND latest_event_sequence=${input.expectedLatestEventSequence}
        AND processed_through_sequence=${input.processedThroughSequence} AND processed_through_sequence=latest_event_sequence AND business_state='awaiting_delivery'
        AND recovery_ref IS NULL AND NOT EXISTS(SELECT 1 FROM workbench_release_obligations o WHERE o.work_item_id=${input.workItemId}) AND NOT EXISTS(SELECT 1 FROM workbench_runtime_responses a WHERE a.work_item_id=${input.workItemId} AND a.state IN ('recorded','queued','submitted','unknown')) AND NOT EXISTS(SELECT 1 FROM workbench_work_batches b WHERE b.work_item_id=${input.workItemId} AND b.state!='completed')
        AND (NOT EXISTS(SELECT 1 FROM workbench_queue_bindings q WHERE q.task_id=workbench_work_items.task_id AND q.task_created_at=workbench_work_items.task_created_at)
          OR EXISTS(SELECT 1 FROM workbench_repair_generations g WHERE g.work_item_id=workbench_work_items.id AND g.superseded_at IS NULL AND g.phase='succeeded'))
        AND NOT EXISTS(SELECT 1 FROM orchestration_v2_projection_runs r WHERE r.thread_id=${saved.work_thread_id} AND r.status IN ('preparing','starting','running','waiting','queued'))
        AND NOT EXISTS(SELECT 1 FROM orchestration_v2_projection_runtime_requests r WHERE r.thread_id=${saved.work_thread_id} AND r.status='pending')
        RETURNING id`;
        if (!changed) return yield* refusal();
      }),
    );
    return yield* observe(
      (yield* sql<WorkItemRow>`SELECT * FROM workbench_work_items WHERE id=${input.workItemId}`)[0]!,
    );
  }, Effect.mapError(ledgerError));
  return { binding, observe, get, complete };
});
export const layer = Layer.effect(WorkbenchWorkItems, make).pipe(Layer.provide(Relations.layer));
