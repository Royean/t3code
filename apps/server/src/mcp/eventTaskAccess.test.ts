import { expect, it } from "@effect/vitest";
import {
  EnvironmentId,
  CommandId,
  ProviderInstanceId,
  ThreadId,
  type EventOrchestrationJobDetail,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Result from "effect/Result";
import * as SqlClient from "effect/sql/SqlClient";

import * as WorkbenchGuard from "../scheduledTasks/WorkbenchQueueGuard.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as EventOrchestrationService from "../eventOrchestration/EventOrchestrationService.ts";
import * as McpInvocationContext from "./McpInvocationContext.ts";
import { requireManagedEventHandoff } from "./eventTaskAccess.ts";

const threadId = ThreadId.make("event:coordinator");
const job: EventOrchestrationJobDetail = {
  id: "event:one",
  coordinatorThreadId: threadId,
  coordinatorCommandId: CommandId.make("event:one:launch"),
  coordinatorLaunchMode: "launch",
  coordinatorLaunchCount: 1,
  coordinatorStatus: "running",
  coordinatorFailureKind: null,
  coordinatorStopped: false,
  coordinatorLastHeartbeatAt: null,
  coordinatorLastProgressAt: null,
  status: "running",
  summary: null,
  evidence: [],
  createdAt: "2026-11-15T00:00:00.000Z",
  updatedAt: "2026-11-15T00:00:00.000Z",
  tasks: [],
  event: {
    source: "github",
    deliveryId: "one",
    repository: "acme/app",
    eventType: "issues.ready",
    text: "Fix issue one.",
  },
};

it.effect("managed events cannot bypass role admission through ordinary handoff tools", () =>
  Effect.gen(function* () {
    for (const tool of [
      "delegate_task",
      "t3_thread_launch",
      "t3_thread_send",
      "schedule_task",
      "t3_thread_configure",
      "t3_worktree_handoff",
    ]) {
      const result = yield* requireManagedEventHandoff(tool).pipe(Effect.result);
      expect(Result.isFailure(result) && result.failure.code).toBe("capability_denied");
    }
    expect(
      yield* requireManagedEventHandoff("event_task_assign").pipe(Effect.result),
    ).toMatchObject({ _tag: "Success" });
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        WorkbenchGuard.layer.pipe(Layer.provide(SqlitePersistence.layerMemory)),
        Layer.mock(EventOrchestrationService.EventOrchestrationService)({
          ownsThread: (caller) => caller === job.coordinatorThreadId,
        }),
        Layer.succeed(McpInvocationContext.McpInvocationContext, {
          environmentId: EnvironmentId.make("environment:event-test"),
          requestNamespace: "event-test",
          issuedAt: 0,
          capabilities: new Set<McpInvocationContext.McpCapability>(["orchestration"]),
          client: undefined,
          thread: {
            threadId,
            providerSessionId: "event-session",
            providerInstanceId: ProviderInstanceId.make("codex"),
          },
        }),
      ),
    ),
  ),
);

it.effect.each([false, true])(
  "ordinary delegation remains available without event storage (managed: %s)",
  (managed) =>
    Effect.gen(function* () {
      const result = yield* requireManagedEventHandoff("delegate_task").pipe(Effect.result);
      expect(Result.isSuccess(result)).toBe(!managed);
    }).pipe(
      Effect.provide(
        Layer.mergeAll(
          WorkbenchGuard.layer.pipe(Layer.provide(SqlitePersistence.layerMemory)),
          Layer.mock(EventOrchestrationService.EventOrchestrationService)({
            ownsThread: () => managed,
            getForCaller: () =>
              Effect.fail(
                new EventOrchestrationService.EventOrchestrationError({
                  operation: "read",
                  reason: "storage",
                }),
              ),
          }),
          Layer.succeed(McpInvocationContext.McpInvocationContext, {
            environmentId: EnvironmentId.make("environment:event-test"),
            requestNamespace: "event-test",
            issuedAt: 0,
            capabilities: new Set<McpInvocationContext.McpCapability>(["orchestration"]),
            client: undefined,
            thread: {
              threadId,
              providerSessionId: "event-session",
              providerInstanceId: ProviderInstanceId.make("codex"),
            },
          }),
        ),
      ),
    ),
);

it.effect("workbench admissions block unmanaged handoffs before queue binding", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    yield* sql`INSERT INTO workbench_work_items(id,task_id,task_created_at,project_id,business_delivery_id,dispatcher_thread_id,event_json,created_at,updated_at)
    VALUES('work-item','task','2026-11-26T00:00:00.000Z','project','delivery',${threadId},'{}','2026-11-26T00:00:00.000Z','2026-11-26T00:00:00.000Z')`;
    for (const tool of [
      "delegate_task",
      "create_threads",
      "t3_thread_launch",
      "t3_thread_send",
      "t3_thread_transfer",
      "schedule_task",
    ]) {
      const result = yield* requireManagedEventHandoff(tool).pipe(Effect.result);
      expect(Result.isFailure(result) && result.failure.code).toBe("capability_denied");
    }
    expect(
      yield* requireManagedEventHandoff("t3_workbench_dispatch").pipe(Effect.result),
    ).toMatchObject({ _tag: "Success" });
  }).pipe(
    Effect.provide(
      Layer.mergeAll(
        WorkbenchGuard.layer.pipe(Layer.provide(SqlitePersistence.layerMemory)),
        SqlitePersistence.layerMemory,
        Layer.succeed(McpInvocationContext.McpInvocationContext, {
          environmentId: EnvironmentId.make("environment:event-test"),
          requestNamespace: "test",
          issuedAt: 0,
          capabilities: new Set<McpInvocationContext.McpCapability>(["orchestration"]),
          client: undefined,
          thread: {
            threadId,
            providerSessionId: "session",
            providerInstanceId: ProviderInstanceId.make("codex"),
          },
        }),
      ),
    ),
  ),
);
