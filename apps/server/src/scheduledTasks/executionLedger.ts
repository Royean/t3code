import { ScheduledTaskError, type ScheduledTask } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as SqlClient from "effect/sql/SqlClient";
import type { WebhookRequest } from "./webhookTemplate.ts";

export interface ExecutionRow {
  id: string;
  task_id: string;
  task_created_at: string;
  project_id: string | null;
  owner_delivery_id: string;
  command_id: string | null;
  message_id: string | null;
  thread_id: string | null;
  accepted_run_id: string | null;
  run_id: string | null;
  state: "pending" | "dispatching" | "dispatched" | "failed_to_start" | "unknown";
  updated_at: string;
  error: string | null;
}

export function businessDeliveryId(task: ScheduledTask, request: WebhookRequest): string | null {
  const header = request.headers["x-t3-business-delivery-id"];
  if (
    header === undefined &&
    (task.schedule.type !== "webhook" ||
      !/^{{\s*body\.deliveryId\s*}}$/.test(task.schedule.dedupe?.replayKey ?? ""))
  )
    return null;
  try {
    const body: unknown = JSON.parse(request.bodyText);
    const value =
      body !== null && typeof body === "object" && "deliveryId" in body
        ? body.deliveryId
        : undefined;
    return typeof value === "string" &&
      value.length <= 200 &&
      (header === undefined || (header.trim().length > 0 && header === value))
      ? value
      : null;
  } catch {
    return null;
  }
}

export const ledgerError = (cause: unknown) =>
  new ScheduledTaskError({ message: "Could not access scheduled task execution tracking.", cause });

export const recordAlias = Effect.fn("ScheduledTaskService.recordExecutionAlias")(
  function* (input: {
    task: ScheduledTask;
    attemptId: string;
    deliveryId: string;
    businessDeliveryId: string | null;
    outcome: string;
    executionId: string | null;
    receivedAt: string;
    create?: boolean;
  }) {
    const sql = yield* SqlClient.SqlClient;
    if (input.create && input.executionId !== null) {
      yield* sql`INSERT INTO scheduled_task_executions (id, task_id, task_created_at, project_id, owner_delivery_id, state, updated_at)
      VALUES (${input.executionId}, ${input.task.id}, ${input.task.createdAt}, ${input.task.projectId}, ${input.deliveryId}, 'pending', ${input.receivedAt})`;
    }
    yield* sql`INSERT INTO scheduled_task_execution_aliases (attempt_id, task_id, task_created_at, business_delivery_id, delivery_id, outcome, execution_id, received_at)
    VALUES (${input.attemptId}, ${input.task.id}, ${input.task.createdAt}, ${input.businessDeliveryId}, ${input.deliveryId}, ${input.outcome}, ${input.executionId}, ${input.receivedAt})`;
  },
  (effect) => effect.pipe(Effect.mapError(ledgerError)),
);

export const captureProjection = Effect.fn("ScheduledTaskService.captureExecutionProjection")(
  function* (
    row: ExecutionRow,
    projection: Pick<
      import("@t3tools/contracts").OrchestrationV2ThreadProjection,
      "thread" | "messages" | "runs"
    >,
    now: string,
  ) {
    const sql = yield* SqlClient.SqlClient;
    const message = projection.messages.find(
      (candidate) =>
        candidate.id === row.message_id &&
        candidate.threadId === projection.thread.id &&
        candidate.role === "user" &&
        candidate.scheduledTaskId === row.task_id,
    );
    const run = projection.runs.find(
      (candidate) => candidate.id === message?.runId && candidate.threadId === projection.thread.id,
    );
    if (message === undefined || run === undefined) return false;
    const acceptedRunId =
      projection.runs.find(
        (candidate) =>
          candidate.threadId === projection.thread.id && candidate.userMessageId === row.message_id,
      )?.id ?? run.id;
    yield* sql`UPDATE scheduled_task_executions SET project_id = ${projection.thread.projectId}, thread_id = ${projection.thread.id},
    accepted_run_id = COALESCE(accepted_run_id, ${acceptedRunId}), run_id = ${run.id}, state = 'dispatched', error = NULL, updated_at = ${now}
    WHERE id = ${row.id}`;
    return true;
  },
  (effect) => effect.pipe(Effect.mapError(ledgerError)),
);
