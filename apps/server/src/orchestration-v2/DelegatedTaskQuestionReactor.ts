import {
  EventId,
  CommandId,
  MessageId,
  type OrchestrationV2DomainEvent,
  type OrchestrationV2RuntimeRequest,
  type OrchestrationV2Subagent,
  type RuntimeRequestId,
  type ThreadId,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import type * as Scope from "effect/Scope";
import * as SqlClient from "effect/sql/SqlClient";
import * as Stream from "effect/Stream";

import { reportLabel } from "@t3tools/provider-core/server/notification";
import * as KeyedLock from "@t3tools/shared/KeyedLock";

import { forkParked } from "../serverActivation.ts";
import * as Orchestrator from "./Orchestrator.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

export class DelegatedTaskQuestionReactor extends Context.Service<
  DelegatedTaskQuestionReactor,
  {
    readonly start: () => Effect.Effect<void, never, Scope.Scope>;
    readonly handle: (event: OrchestrationV2DomainEvent) => Effect.Effect<void>;
  }
>()("t3/orchestration-v2/DelegatedTaskQuestionReactor") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  const engine = yield* Orchestrator.OrchestratorV2;
  const projections = yield* ProjectionStore.ProjectionStoreV2;

  const wakeParent = Effect.fnUntraced(function* (
    task: OrchestrationV2Subagent,
    request: OrchestrationV2RuntimeRequest,
  ) {
    if (task.childThreadId === null) return;
    const key = `${task.threadId}:${request.id}`;
    const label = reportLabel(task.title?.trim() || task.prompt);
    const situation =
      request.responseCapability.type === "message"
        ? `Delegated task ${task.id} asked a question and is still working.`
        : `Delegated task ${task.id} is blocked on a question and cannot continue until it is answered.`;
    yield* engine.dispatch({
      type: "message.dispatch",
      commandId: CommandId.make(`server:delegated-task-question:${key}`),
      threadId: task.threadId,
      messageId: MessageId.make(`message:delegated-task-question:${key}`),
      text: `${situation} Read it with t3_pending_request action read, threadId ${task.childThreadId}, and requestId ${request.id}. Answer it with action respond when the answer follows from the task or the user's instructions; otherwise ask the user, then answer it. If it is no longer pending, it was answered or dismissed.`,
      notification: {
        source: { kind: "delegated_task", taskIds: [task.id], childThreadId: task.childThreadId },
        outcome: "updated",
        summary: `Delegated task${label === undefined ? "" : ` "${label}"`} has a question`,
      },
      attachments: [],
      dispatchMode: { type: "queue_after_active" },
      createdBy: "agent",
      creationSource: "server",
    });
  });

  const cancelQueuedWakes = Effect.fnUntraced(function* (
    parentThreadId: ThreadId,
    requestIds: ReadonlyArray<RuntimeRequestId>,
  ) {
    const parent = yield* projections.getThreadRecords(parentThreadId, ["runs"]);
    for (const requestId of requestIds) {
      const key = `${parentThreadId}:${requestId}`;
      const messageId = MessageId.make(`message:delegated-task-question:${key}`);
      const queued = parent.runs.find(
        (run) => run.status === "queued" && run.userMessageId === messageId,
      );
      if (queued === undefined) continue;
      yield* engine
        .dispatch({
          type: "queued-run.cancel",
          commandId: CommandId.make(`server:delegated-task-question-cancel:${key}`),
          threadId: parentThreadId,
          runId: queued.id,
        })
        .pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("cancelling a delegated task question wake failed", {
              threadId: parentThreadId,
              requestId,
              cause,
            }),
          ),
        );
    }
  });

  // Runs for every domain event; most events return before any work is needed.
  const handle: DelegatedTaskQuestionReactor["Service"]["handle"] = Effect.fnUntraced(
    function* (event) {
      if (event.type === "run.updated") {
        if (!["completed", "failed", "waiting"].includes(event.payload.status)) return;
        const parent = yield* projections.getThreadRecords(event.threadId, ["subagents"]);
        if (parent.thread.archivedAt !== null || parent.thread.deletedAt !== null) return;
        for (const task of parent.subagents) {
          if (
            task.origin !== "app_owned" ||
            task.runId !== event.payload.id ||
            task.completionWake !== "settled_only" ||
            task.completionDelivery?.state === "disposed" ||
            task.childThreadId === null ||
            ["completed", "failed", "cancelled", "interrupted"].includes(task.status)
          ) {
            continue;
          }
          const child = yield* projections.getThreadRecords(task.childThreadId, [
            "runtimeRequests",
          ]);
          for (const request of child.runtimeRequests) {
            if (
              request.kind !== "user_input" ||
              request.status !== "pending" ||
              request.responseCapability.type !== "live"
            ) {
              continue;
            }
            yield* wakeParent(task, request).pipe(
              Effect.catchCause((cause) =>
                Effect.logWarning("catching up a delegated task question failed", {
                  taskId: task.id,
                  requestId: request.id,
                  cause,
                }),
              ),
            );
          }
        }
        return;
      }
      if (event.type === "subagent.updated") {
        const task = event.payload;
        if (
          task.origin !== "app_owned" ||
          task.childThreadId === null ||
          !["completed", "failed", "cancelled", "interrupted"].includes(task.status)
        ) {
          return;
        }
        // A settled task can no longer use an answer to its still-pending questions.
        const { runtimeRequests } = yield* projections.getThreadRecords(task.childThreadId, [
          "runtimeRequests",
        ]);
        const questions = runtimeRequests.filter((request) => request.kind === "user_input");
        if (questions.length === 0) return;
        const child = yield* projections.getThreadRecords(task.childThreadId, ["runs", "nodes"]);
        const taskRunIds = new Set(
          child.runs
            .filter(
              (run) =>
                run.delegatedTaskId === task.id ||
                (run.delegatedTaskId === undefined &&
                  child.thread.forkedFrom?.type === "node" &&
                  child.thread.forkedFrom.nodeId === task.id),
            )
            .map((run) => run.id),
        );
        const taskNodeIds = new Set(
          child.nodes
            .filter((node) => node.runId !== null && taskRunIds.has(node.runId))
            .map((node) => node.id),
        );
        yield* cancelQueuedWakes(
          task.threadId,
          questions
            .filter((request) => taskNodeIds.has(request.nodeId))
            .map((request) => request.id),
        );
        return;
      }
      if (
        event.type === "runtime-request.updated" &&
        event.payload.status === "pending" &&
        ["command", "file-change", "file-read", "permission", "mcp-elicitation"].includes(
          event.payload.kind,
        )
      ) {
        const rows = yield* sql<{
          dispatcher_thread_id: string;
          delegated_task_id: string;
          project_id: string;
          id: string;
        }>`SELECT id, dispatcher_thread_id, delegated_task_id, project_id FROM workbench_work_items WHERE work_thread_id = ${event.threadId} AND delegated_task_id IS NOT NULL`;
        for (const row of rows) {
          const child = yield* projections.getThreadRecords(event.threadId, [
            "runs",
            "nodes",
            "runtimeRequests",
          ]);
          const parent = yield* projections.getThreadRecords(row.dispatcher_thread_id as ThreadId, [
            "subagents",
          ]);
          const task = parent.subagents.find(
            (task) =>
              task.origin === "app_owned" &&
              task.id === row.delegated_task_id &&
              task.childThreadId === event.threadId,
          );
          const request = child.runtimeRequests.find(
            (request) => request.id === event.payload.id && request.status === "pending",
          );
          const node = child.nodes.find((node) => node.id === request?.nodeId);
          const run = child.runs.find(
            (run) =>
              run.id === node?.runId &&
              (run.delegatedTaskId === task?.id ||
                (run.delegatedTaskId === undefined &&
                  child.thread.forkedFrom?.type === "node" &&
                  child.thread.forkedFrom.nodeId === task?.id)),
          );
          if (
            task === undefined ||
            request === undefined ||
            run === undefined ||
            ["completed", "failed", "cancelled", "interrupted", "rolled_back"].includes(
              run.status,
            ) ||
            child.thread.projectId !== row.project_id ||
            parent.thread.projectId !== row.project_id ||
            parent.thread.deletedAt !== null ||
            parent.thread.archivedAt !== null
          )
            continue;
          const key = `${row.id}:${request.id}`;
          yield* engine.dispatch({
            type: "message.dispatch",
            commandId: CommandId.make(`server:workbench-approval:${key}`),
            threadId: task.threadId,
            messageId: MessageId.make(`message:workbench-approval:${key}`),
            text: `Workbench child ${task.childThreadId} is blocked on approval ${request.id}. Read the native pending request and ask the user when approval is required. Do not infer approval from waiting status or grant broader permissions.`,
            attachments: [],
            dispatchMode: { type: "queue_after_active" },
            createdBy: "agent",
            creationSource: "server",
            notification: {
              source: { kind: "delegated_task", taskIds: [task.id], childThreadId: event.threadId },
              outcome: "updated",
              summary: "Workbench child needs approval",
            },
          });
        }
        return;
      }
      if (event.type !== "runtime-request.updated" || event.payload.kind !== "user_input") {
        return;
      }
      const child = yield* projections.getThread(event.threadId);
      const parentThreadId = child.lineage.parentThreadId;
      if (parentThreadId === null) return;
      const request = yield* projections.getRuntimeRequest(event.threadId, event.payload.id);
      if (event.payload.status !== "pending" || request?.status !== "pending") {
        yield* cancelQueuedWakes(parentThreadId, [event.payload.id]);
        return;
      }
      const parent = yield* projections.getThreadRecords(parentThreadId, ["subagents", "runs"]);
      const { runs } = yield* projections.getThreadRecords(child.id, ["runs"]);
      const run =
        runs.find((run) => run.id === event.runId) ??
        runs.toSorted((a, b) => b.ordinal - a.ordinal)[0];
      const taskId =
        run?.delegatedTaskId ??
        (child.forkedFrom?.type === "node" ? child.forkedFrom.nodeId : undefined);
      const task = parent.subagents.find(
        (candidate) => candidate.origin === "app_owned" && candidate.id === taskId,
      );
      if (
        task === undefined ||
        parent.thread.archivedAt !== null ||
        parent.thread.deletedAt !== null
      ) {
        return;
      }
      // A blocking delegate_task wait delivers the question itself.
      if (
        task.completionWake === "settled_only" &&
        event.payload.responseCapability.type === "live" &&
        parent.runs.some(
          (run) =>
            run.id === task.runId &&
            (run.status === "preparing" || run.status === "starting" || run.status === "running"),
        )
      ) {
        return;
      }
      yield* wakeParent(task, request);
    },
    (effect, event) =>
      effect.pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("waking the parent of a delegated task question failed", {
            threadId: event.threadId,
            cause,
          }),
        ),
      ),
  );

  // Every wake and cancel goes to the parent, so keying by it keeps a question's wake ahead of
  // its task's cancel while a busy parent's lock delays only that parent's reactions.
  const parentOf = (event: OrchestrationV2DomainEvent) => {
    switch (event.type) {
      case "run.updated":
        return Effect.succeed(event.threadId);
      case "subagent.updated":
        return Effect.succeed(event.payload.threadId);
      case "runtime-request.updated":
        return event.payload.kind === "user_input" ||
          ["command", "file-change", "file-read", "permission", "mcp-elicitation"].includes(
            event.payload.kind,
          )
          ? projections
              .getThread(event.threadId)
              .pipe(Effect.map((child) => child.lineage.parentThreadId))
          : Effect.succeed(null);
      default:
        return Effect.succeed(null);
    }
  };

  const start: DelegatedTaskQuestionReactor["Service"]["start"] = Effect.fn(
    "DelegatedTaskQuestionReactor.start",
  )(function* () {
    const parentReactions = yield* KeyedLock.makeRunner<ThreadId>();
    yield* forkParked(
      Stream.runForEach(engine.streamDomainEvents, (event) =>
        parentOf(event).pipe(
          Effect.flatMap((parentThreadId) =>
            parentThreadId === null
              ? Effect.void
              : parentReactions.run(parentThreadId, handle(event)),
          ),
          Effect.catchCause((cause) =>
            Effect.logWarning("finding the parent of a delegated task question failed", {
              threadId: event.threadId,
              cause,
            }),
          ),
        ),
      ).pipe(
        Effect.catchCause((cause) =>
          Effect.logWarning("delegated task question event stream failed", { cause }),
        ),
      ),
    );
    // Recover an approval accepted just before a shutdown. The dispatch receipt makes each wake
    // idempotent with the live-event path; waiting runs alone never qualify.
    yield* Effect.gen(function* () {
      const rows = yield* sql<{
        work_thread_id: string;
      }>`SELECT DISTINCT work_thread_id FROM workbench_work_items WHERE work_thread_id IS NOT NULL`;
      for (const row of rows) {
        const child = yield* projections.getThreadRecords(row.work_thread_id as ThreadId, [
          "runtimeRequests",
        ]);
        for (const request of child.runtimeRequests) {
          if (
            request.status !== "pending" ||
            !["command", "file-change", "file-read", "permission", "mcp-elicitation"].includes(
              request.kind,
            )
          )
            continue;
          yield* handle({
            id: EventId.make(`server:workbench-approval-catchup:${request.id}`),
            type: "runtime-request.updated",
            threadId: child.thread.id,
            nodeId: request.nodeId,
            occurredAt: request.createdAt,
            payload: request,
          });
        }
      }
    }).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("recovering workbench approval notifications failed", { cause }),
      ),
    );
  });

  return { start, handle } satisfies DelegatedTaskQuestionReactor["Service"];
});

export const layer = Layer.effect(DelegatedTaskQuestionReactor, make);
