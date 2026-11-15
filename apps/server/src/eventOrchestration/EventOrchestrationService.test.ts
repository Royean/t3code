import { assert, it } from "@effect/vitest";
import {
  type EventOrchestrationConfig,
  type GitHubEventSubmission,
  type OrchestrationV2ServerCommand,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  ProviderSessionId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as DateTime from "effect/DateTime";
import * as Fiber from "effect/Fiber";
import * as TestClock from "effect/testing/TestClock";

import * as Receipts from "../orchestration-v2/CommandReceiptStore.ts";
import {
  OrchestratorCommandRejectedError,
  OrchestratorDispatchError,
} from "../orchestration-v2/Orchestrator.ts";
import * as Option from "effect/Option";
import * as ProviderSessions from "../orchestration-v2/ProviderSessionManager.ts";
import * as ThreadLaunch from "../orchestration-v2/ThreadLaunchService.ts";
import * as ThreadManagement from "../orchestration-v2/ThreadManagementService.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as ManagedPolicy from "../policy/ManagedPolicy.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as EventOrchestration from "./EventOrchestrationService.ts";

const config: EventOrchestrationConfig = {
  enabled: true,
  projectId: ProjectId.make("project:events"),
  coordinatorExecutorId: "primary",
  executors: [
    {
      id: "primary",
      modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "test-model" },
    },
    {
      id: "backup",
      modelSelection: { instanceId: ProviderInstanceId.make("claudeAgent"), model: "test-backup" },
    },
  ],
  roles: [
    {
      id: "developer",
      title: "Developer",
      instructionsPath: "/roles/developer.md",
      executorIds: ["primary", "backup"],
    },
  ],
  allowedRepositories: ["acme/app"],
  allowedEventTypes: ["issues.ready"],
  runtimeMode: "approval-required",
  interactionMode: "default",
};
const event: GitHubEventSubmission = {
  source: "github",
  deliveryId: "delivery-1",
  repository: "acme/app",
  eventType: "issues.ready",
  text: "PRIVATE-EVENT-CONTEXT",
  subject: "Implement requested change",
};
type Observation = {
  thread: { updatedAt: DateTime.Utc; runtimeMode: "approval-required"; interactionMode: "default" };
  runs: Array<{ id: RunId; rootNodeId: NodeId; status: string }>;
  providerThreads: Array<{ providerSessionId: ProviderSessionId; pendingBackgroundTasks: [] }>;
  turnItems: Array<{
    type: "error";
    failure: {
      class: "transport_error" | "permission_error";
      retryable: boolean;
      code: string | null;
    };
  }>;
  subagents: Array<{ status: string }>;
};
const observation = (threadId: ThreadId): Observation => ({
  thread: {
    updatedAt: DateTime.makeUnsafe(0),
    runtimeMode: "approval-required",
    interactionMode: "default",
  },
  runs: [
    {
      id: RunId.make(`${threadId}:run`),
      rootNodeId: NodeId.make(`${threadId}:root`),
      status: "running",
    },
  ],
  providerThreads: [
    { providerSessionId: ProviderSessionId.make("session:test"), pendingBackgroundTasks: [] },
  ],
  turnItems: [],
  subagents: [],
});

const makeHarness = Effect.gen(function* () {
  const ids = yield* IdAllocator.IdAllocatorV2;
  const receipts = yield* Ref.make(new Set<string>());
  const receiptReadFailure = yield* Ref.make(false);
  const unreadableReceipts = yield* Ref.make(new Set<string>());
  const records = yield* Ref.make(new Map<ThreadId, Observation>());
  const live = yield* Ref.make(true);
  const commands = yield* Ref.make<ReadonlyArray<OrchestrationV2ServerCommand>>([]);
  const launches = yield* Ref.make<ReadonlyArray<ThreadLaunch.ThreadLaunchInput>>([]);
  const instructions = yield* Ref.make("PRIVATE-ROLE-INSTRUCTIONS-v1");
  const scheduled = yield* Ref.make(0);
  const sequence = yield* Ref.make(1);
  const pendingSettlement = yield* Ref.make(false);
  const launchFailures = yield* Ref.make(false);
  const rejected = yield* Ref.make(new Set<string>());
  const failDispatch = yield* Ref.make(false);
  const failAcceptedDispatch = yield* Ref.make(false);
  const dispatched = yield* Deferred.make<void>();
  const holdDispatch = yield* Ref.make(false);
  const releaseDispatch = yield* Deferred.make<void>();
  const dependencies = Layer.mergeAll(
    IdAllocator.layer,
    ManagedPolicy.layerTest(),
    FileSystem.layerNoop({ readFileString: () => Ref.get(instructions) }),
    Layer.succeed(Scheduler.Scheduler, { register: () => Ref.update(scheduled, (n) => n + 1) }),
    Layer.mock(Receipts.CommandReceiptStoreV2)({
      getByCommandId: (commandId) =>
        Effect.gen(function* () {
          if (
            (yield* Ref.get(receiptReadFailure)) ||
            (yield* Ref.get(unreadableReceipts)).has(commandId)
          )
            return yield* new Receipts.CommandReceiptStoreReadError({ commandId });
          return (yield* Ref.get(rejected)).has(commandId)
            ? Option.some({ status: "rejected" } as never)
            : (yield* Ref.get(receipts)).has(commandId)
              ? Option.some({ status: "accepted" } as never)
              : Option.none();
        }),
    }),
    Layer.mock(ProviderSessions.ProviderSessionManagerV2)({ isLive: () => Ref.get(live) }),
    Layer.mock(ThreadLaunch.ThreadLaunchService)({
      launch: (input) =>
        Effect.gen(function* () {
          yield* Ref.update(launches, (all) => [...all, input]);
          if (yield* Ref.get(launchFailures))
            return yield* new ThreadLaunch.ThreadLaunchError({
              commandId: input.commandId,
              projectId: input.projectId,
              operation: "resolve-project",
              cause: "fixture failure",
            });
          yield* Ref.update(receipts, (all) => new Set(all).add(input.commandId));
          yield* Ref.update(records, (all) =>
            new Map(all).set(input.threadId!, observation(input.threadId!)),
          );
          return { threadId: input.threadId!, resumed: false } as never;
        }),
    }),
    Layer.mock(ThreadManagement.ThreadManagementService)({
      hasReceipt: (commandId) => Ref.get(receipts).pipe(Effect.map((all) => all.has(commandId))),
      getThreadRecords: (threadId) =>
        Ref.get(records).pipe(
          Effect.flatMap((all) =>
            all.has(threadId)
              ? Effect.succeed(all.get(threadId)! as never)
              : (Effect.fail(
                  new EventOrchestration.EventOrchestrationError({
                    operation: "observe",
                    reason: "not_found",
                  }),
                ) as never),
          ),
        ),
      getThreadEventSequence: () => Ref.get(sequence),
      delegatedTaskResultPending: () => Ref.get(pendingSettlement),
      dispatch: (command) =>
        Effect.gen(function* () {
          if (command.type === "delegated_task.request" && (yield* Ref.get(failDispatch))) {
            yield* Ref.update(rejected, (all) => new Set(all).add(command.commandId));
            return yield* new OrchestratorCommandRejectedError({
              commandId: command.commandId,
              commandType: command.type,
            });
          }
          if ((yield* Ref.get(receipts)).has(command.commandId)) return { sequence: 1 } as never;
          yield* Ref.update(receipts, (all) => new Set(all).add(command.commandId));
          yield* Ref.update(commands, (all) => [...all, command]);
          if (command.type === "delegated_task.request") {
            const threadId = ids.derive.delegatedTaskThread({ commandId: command.commandId });
            yield* Ref.update(records, (all) => new Map(all).set(threadId, observation(threadId)));
            yield* Deferred.succeed(dispatched, undefined);
            if (yield* Ref.get(holdDispatch)) yield* Deferred.await(releaseDispatch);
            if (yield* Ref.get(failAcceptedDispatch))
              return yield* new OrchestratorDispatchError({
                commandId: command.commandId,
                commandType: command.type,
              });
          }
          return { sequence: 1 } as never;
        }),
      sendToThread: (input) =>
        Effect.gen(function* () {
          yield* Ref.update(receipts, (all) => new Set(all).add(input.commandId));
          yield* Ref.update(records, (all) =>
            new Map(all).set(input.threadId, observation(input.threadId)),
          );
          return {} as never;
        }),
    }),
  );
  const layer = EventOrchestration.layer.pipe(Layer.provide(dependencies));
  const setStatus = (
    threadId: ThreadId,
    status: string,
    failure?: Observation["turnItems"][number]["failure"],
  ) =>
    Ref.update(records, (all) => {
      const next = new Map(all);
      const record = next.get(threadId)!;
      next.set(threadId, {
        ...record,
        runs: record.runs.map((run) => ({ ...run, status })),
        turnItems: failure === undefined ? [] : [{ type: "error", failure }],
      });
      return next;
    });
  const delegated = Ref.get(commands).pipe(
    Effect.map((all) => all.filter((command) => command.type === "delegated_task.request")),
  );
  return {
    layer,
    records,
    receipts,
    receiptReadFailure,
    unreadableReceipts,
    live,
    commands,
    launches,
    instructions,
    scheduled,
    sequence,
    pendingSettlement,
    launchFailures,
    rejected,
    failDispatch,
    failAcceptedDispatch,
    dispatched,
    holdDispatch,
    releaseDispatch,
    setStatus,
    delegated,
  };
});

it.effect(
  "persists gated events and isolated assignments, enforces dependencies and evidence acceptance",
  () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness;
      const jobId = yield* Effect.gen(function* () {
        const service = yield* EventOrchestration.EventOrchestrationService;
        assert.equal(yield* Ref.get(harness.scheduled), 0);
        assert.isFalse(service.ownsThread(ThreadId.make("ordinary-thread")));
        yield* service.runDueWork;
        assert.equal((yield* service.submit(event).pipe(Effect.flip)).reason, "disabled");
        yield* service.configure({ ...config, enabled: false });
        assert.equal(yield* Ref.get(harness.scheduled), 0);
        yield* service.configure(config);
        assert.equal(yield* Ref.get(harness.scheduled), 1);
        assert.equal(
          (yield* service.submit({ ...event, eventType: "issues.opened" }).pipe(Effect.flip))
            .reason,
          "not_allowed",
        );
        const job = yield* service.submit(event);
        assert.isTrue(service.ownsThread(job.coordinatorThreadId));
        assert.equal((yield* service.submit({ ...event, text: "overridden" })).id, job.id);
        assert.equal((yield* service.get(job.id))?.event.text, event.text);
        yield* service.runDueWork;
        assert.equal((yield* Ref.get(harness.launches)).length, 1);
        assert.equal(
          (yield* service
            .assign(ThreadId.make("unrelated"), {
              jobId: job.id,
              taskId: "build",
              roleId: "developer",
              objective: "Build the change",
            })
            .pipe(Effect.flip)).reason,
          "forbidden",
        );
        const first = yield* service.assign(job.coordinatorThreadId, {
          jobId: job.id,
          taskId: "build",
          roleId: "developer",
          objective: "Build the change",
        });
        assert.deepEqual(
          yield* service.assign(job.coordinatorThreadId, {
            jobId: job.id,
            taskId: "build",
            roleId: "developer",
            objective: "Build the change",
          }),
          first,
        );
        const second = yield* service.assign(job.coordinatorThreadId, {
          jobId: job.id,
          taskId: "verify",
          roleId: "developer",
          objective: "Verify independently",
          dependsOn: ["build"],
        });
        yield* Ref.set(harness.instructions, "PRIVATE-ROLE-INSTRUCTIONS-v2");
        yield* service.runDueWork;
        yield* Deferred.await(harness.dispatched);
        const calls = yield* harness.delegated;
        assert.equal(calls.length, 1);
        assert.include(calls[0]!.task, "PRIVATE-ROLE-INSTRUCTIONS-v1");
        const attempt = first.attempts[0]!;
        assert.isTrue(service.ownsThread(attempt.threadId));
        assert.deepEqual(
          (yield* service.getForCaller(attempt.threadId)).tasks.map((task) => task.id),
          ["build"],
        );
        assert.equal(
          (yield* service.getForCaller(ThreadId.make("unrelated")).pipe(Effect.flip)).reason,
          "forbidden",
        );
        assert.equal(
          (yield* service
            .report(attempt.threadId, {
              kind: "completed",
              jobId: job.id,
              attemptId: second.attempts[0]!.id,
              summary: "fake",
              evidence: ["fake"],
            })
            .pipe(Effect.flip)).reason,
          "forbidden",
        );
        yield* service.report(attempt.threadId, {
          kind: "progress",
          jobId: job.id,
          attemptId: attempt.id,
          summary: "Build is complete",
        });
        yield* service.report(attempt.threadId, {
          kind: "completed",
          jobId: job.id,
          attemptId: attempt.id,
          summary: "Tests pass",
          evidence: ["test output"],
        });
        assert.equal(
          (yield* service
            .report(job.coordinatorThreadId, {
              kind: "accept",
              jobId: job.id,
              taskId: "build",
              evidence: ["checked tests"],
            })
            .pipe(Effect.flip)).reason,
          "conflict",
        );
        yield* harness.setStatus(attempt.threadId, "completed");
        yield* service.runDueWork;
        assert.equal(
          (yield* service.get(job.id))?.tasks.find((task) => task.id === "build")?.status,
          "awaiting_review",
        );
        assert.equal((yield* harness.delegated).length, 1);
        yield* service.report(job.coordinatorThreadId, {
          kind: "accept",
          jobId: job.id,
          taskId: "build",
          evidence: ["checked test evidence"],
        });
        yield* service.runDueWork;
        assert.equal((yield* harness.delegated).length, 2);
        const feed = yield* service.changes(0);
        assert.isAbove(feed.nextCursor, 0);
        const serialized = JSON.stringify(feed);
        assert.notInclude(serialized, "PRIVATE-ROLE");
        assert.notInclude(serialized, event.text);
        assert.notInclude(serialized, "test output");
        assert.deepEqual((yield* service.changes(feed.nextCursor)).changes, []);
        return job.id;
      }).pipe(Effect.provide(harness.layer));
      yield* Effect.gen(function* () {
        const restarted = yield* EventOrchestration.EventOrchestrationService;
        yield* restarted.runDueWork;
        assert.equal((yield* harness.delegated).length, 2);
        const job = (yield* restarted.get(jobId))!;
        assert.isTrue(restarted.ownsThread(job.coordinatorThreadId));
        assert.isTrue(restarted.ownsThread(job.tasks[0]!.attempts[0]!.threadId));
        const attempt = job.tasks.find((task) => task.id === "verify")!.attempts[0]!;
        yield* harness.setStatus(attempt.threadId, "completed");
        yield* restarted.runDueWork;
        yield* restarted.report(job.coordinatorThreadId, {
          kind: "accept",
          jobId,
          taskId: "verify",
          evidence: ["independent verification passed"],
        });
        const done = yield* restarted.report(job.coordinatorThreadId, {
          kind: "job_complete",
          jobId,
          evidence: ["both assignments accepted"],
        });
        assert.equal(done.status, "succeeded");
      }).pipe(Effect.provide(harness.layer));
    }).pipe(Effect.provide(Layer.mergeAll(IdAllocator.layer, Sqlite.layerMemory))),
);

it.effect(
  "keeps lost executions unknown and allows at most three confirmed stopped executions with fallback",
  () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness;
      yield* Effect.gen(function* () {
        const service = yield* EventOrchestration.EventOrchestrationService;
        yield* service.configure(config);
        const job = yield* service.submit(event);
        yield* service.runDueWork;
        const task = yield* service.assign(job.coordinatorThreadId, {
          jobId: job.id,
          taskId: "build",
          roleId: "developer",
          objective: "Build",
        });
        yield* service.runDueWork;
        const first = task.attempts[0]!;
        yield* service.runDueWork;
        const before = (yield* service.get(job.id))!.tasks[0]!.attempts[0]!;
        yield* Ref.set(harness.live, false);
        yield* TestClock.adjust("2 minutes");
        yield* service.runDueWork;
        const unknown = (yield* service.get(job.id))!.tasks[0]!.attempts[0]!;
        assert.equal(unknown.status, "unknown");
        assert.equal(unknown.lastHeartbeatAt, before.lastHeartbeatAt);
        assert.equal(unknown.lastProgressAt, before.lastProgressAt);
        assert.equal((yield* harness.delegated).length, 1);
        assert.equal(
          (yield* service
            .report(job.coordinatorThreadId, { kind: "retry", jobId: job.id, taskId: task.id })
            .pipe(Effect.flip)).reason,
          "conflict",
        );
        yield* service.report(first.threadId, {
          kind: "failed",
          jobId: job.id,
          attemptId: first.id,
          failureKind: "transient",
          summary: "Disconnected",
        });
        yield* service.runDueWork;
        assert.equal((yield* harness.delegated).length, 1);
        assert.equal((yield* service.get(job.id))!.coordinatorStatus, "unknown");
        yield* Ref.set(harness.live, true);
        yield* service.resume(job.id);
        yield* harness.setStatus(first.threadId, "interrupted");
        yield* service.runDueWork;
        let attempts = (yield* service.get(job.id))!.tasks[0]!.attempts;
        assert.equal(attempts.length, 2);
        assert.equal(attempts[1]!.executorId, "backup");
        yield* harness.setStatus(attempts[1]!.threadId, "failed", {
          class: "transport_error",
          retryable: true,
          code: null,
        });
        yield* service.runDueWork;
        attempts = (yield* service.get(job.id))!.tasks[0]!.attempts;
        assert.equal(attempts.length, 3);
        yield* harness.setStatus(attempts[2]!.threadId, "failed", {
          class: "transport_error",
          retryable: true,
          code: null,
        });
        yield* service.runDueWork;
        yield* service.runDueWork;
        const failed = (yield* service.get(job.id))!.tasks[0]!;
        assert.equal(failed.status, "failed");
        assert.equal(failed.attempts.length, 3);
        assert.equal((yield* harness.delegated).length, 3);
        assert.equal(
          (yield* service
            .report(job.coordinatorThreadId, { kind: "retry", jobId: job.id, taskId: task.id })
            .pipe(Effect.flip)).reason,
          "exhausted",
        );
      }).pipe(Effect.provide(harness.layer));
    }).pipe(Effect.provide(Layer.mergeAll(IdAllocator.layer, Sqlite.layerMemory))),
);

it.effect(
  "blocks authorization failures and keeps cancellation pending until all execution stops",
  () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness;
      yield* Effect.gen(function* () {
        const service = yield* EventOrchestration.EventOrchestrationService;
        yield* service.configure(config);
        const job = yield* service.submit(event);
        yield* service.runDueWork;
        const task = yield* service.assign(job.coordinatorThreadId, {
          jobId: job.id,
          taskId: "build",
          roleId: "developer",
          objective: "Build",
        });
        yield* service.runDueWork;
        const attempt = task.attempts[0]!;
        yield* harness.setStatus(attempt.threadId, "failed", {
          class: "permission_error",
          retryable: true,
          code: "login_required",
        });
        yield* service.runDueWork;
        yield* service.runDueWork;
        const blocked = (yield* service.get(job.id))!.tasks[0]!;
        assert.equal(blocked.status, "blocked");
        assert.equal(blocked.attempts[0]!.failureKind, "authentication");
        assert.equal((yield* harness.delegated).length, 1);
        yield* service.report(job.coordinatorThreadId, {
          kind: "retry",
          jobId: job.id,
          taskId: task.id,
        });
        yield* service.runDueWork;
        const second = (yield* service.get(job.id))!.tasks[0]!.attempts[1]!;
        yield* Ref.set(harness.live, false);
        yield* TestClock.adjust("2 minutes");
        yield* service.runDueWork;
        assert.equal((yield* service.cancel(job.id)).status, "cancelling");
        assert.equal(
          (yield* service
            .assign(job.coordinatorThreadId, {
              jobId: job.id,
              taskId: "new",
              roleId: "developer",
              objective: "No new work",
            })
            .pipe(Effect.flip)).reason,
          "conflict",
        );
        yield* harness.setStatus(second.threadId, "cancelled");
        yield* harness.setStatus(job.coordinatorThreadId, "cancelled");
        yield* Ref.set(harness.pendingSettlement, true);
        yield* service.runDueWork;
        assert.equal((yield* service.get(job.id))!.status, "cancelling");
        assert.isFalse((yield* service.get(job.id))!.tasks[0]!.attempts[1]!.stopped);
        yield* Ref.set(harness.pendingSettlement, false);
        yield* service.runDueWork;
        assert.equal((yield* service.get(job.id))!.status, "cancelled");
        assert.equal((yield* service.cancel(job.id)).status, "cancelled");
        assert.equal((yield* harness.delegated).length, 2);
      }).pipe(Effect.provide(harness.layer));
    }).pipe(Effect.provide(Layer.mergeAll(IdAllocator.layer, Sqlite.layerMemory))),
);

it.effect(
  "recovers a dispatch accepted before interruption without starting a second executor",
  () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness;
      const jobId = yield* Effect.gen(function* () {
        const service = yield* EventOrchestration.EventOrchestrationService;
        yield* service.configure(config);
        const job = yield* service.submit(event);
        yield* service.runDueWork;
        yield* Ref.set(harness.holdDispatch, true);
        const dispatch = yield* service
          .assign(job.coordinatorThreadId, {
            jobId: job.id,
            taskId: "build",
            roleId: "developer",
            objective: "Build",
          })
          .pipe(Effect.forkChild);
        yield* Deferred.await(harness.dispatched);
        yield* Fiber.interrupt(dispatch);
        assert.equal((yield* service.get(job.id))!.tasks[0]!.attempts[0]!.status, "queued");
        return job.id;
      }).pipe(Effect.provide(harness.layer));
      yield* Effect.gen(function* () {
        const service = yield* EventOrchestration.EventOrchestrationService;
        const initial = (yield* service.get(jobId))!;
        const childId = initial.tasks[0]!.attempts[0]!.threadId;
        yield* Ref.update(harness.records, (all) => {
          const next = new Map(all);
          next.delete(childId);
          return next;
        });
        yield* Ref.set(harness.receiptReadFailure, true);
        yield* service.runDueWork;
        yield* service.runDueWork;
        const uncertain = (yield* service.get(jobId))!.tasks[0]!;
        assert.equal(uncertain.attempts[0]!.status, "unknown");
        assert.isFalse(uncertain.attempts[0]!.stopped);
        assert.equal(
          (yield* service
            .report(initial.coordinatorThreadId, { kind: "retry", jobId, taskId: uncertain.id })
            .pipe(Effect.flip)).reason,
          "conflict",
        );
        yield* Ref.set(harness.receiptReadFailure, false);
        yield* Ref.update(harness.records, (all) =>
          new Map(all).set(childId, observation(childId)),
        );
        yield* service.runDueWork;
        assert.equal((yield* harness.delegated).length, 1);
        const task = (yield* service.get(jobId))!.tasks[0]!;
        assert.equal(task.attempts.length, 1);
        assert.equal(task.attempts[0]!.status, "running");
      }).pipe(Effect.provide(harness.layer));
    }).pipe(Effect.provide(Layer.mergeAll(IdAllocator.layer, Sqlite.layerMemory))),
);

it.effect("reconciles queued workspace owners before earlier tasks or jobs can dispatch", () =>
  Effect.forEach(
    ["same_job", "earlier_job", "receipt_unknown"] as const,
    (scenario) =>
      Effect.gen(function* () {
        const harness = yield* makeHarness;
        const { earlierJobId, ownerJobId, owner } = yield* Effect.gen(function* () {
          const service = yield* EventOrchestration.EventOrchestrationService;
          yield* service.configure(config);
          const earlier = yield* service.submit({ ...event, deliveryId: "earlier" });
          yield* service.runDueWork;
          yield* service.report(earlier.coordinatorThreadId, {
            kind: "job_blocked",
            jobId: earlier.id,
            summary: "Hold a pending assignment",
          });
          yield* service.assign(earlier.coordinatorThreadId, {
            jobId: earlier.id,
            taskId: "a-next",
            roleId: "developer",
            objective: "Run after the confirmed workspace owner stops",
          });
          const ownerJob = scenario === "same_job" ? earlier : yield* service.submit(event);
          if (scenario === "same_job") yield* service.resume(earlier.id);
          else yield* service.runDueWork;
          yield* Ref.set(harness.holdDispatch, true);
          const dispatch = yield* service
            .assign(ownerJob.coordinatorThreadId, {
              jobId: ownerJob.id,
              taskId: "z-owner",
              roleId: "developer",
              objective: "Keep the accepted execution as workspace owner",
            })
            .pipe(Effect.forkChild);
          yield* Deferred.await(harness.dispatched);
          yield* Fiber.interrupt(dispatch);
          yield* Ref.set(harness.holdDispatch, false);
          const owner = (yield* service.get(ownerJob.id))!.tasks.find(
            (task) => task.id === "z-owner",
          )!.attempts[0]!;
          assert.equal(owner.status, "queued");
          return { earlierJobId: earlier.id, ownerJobId: ownerJob.id, owner };
        }).pipe(Effect.provide(harness.layer));
        yield* Effect.gen(function* () {
          const service = yield* EventOrchestration.EventOrchestrationService;
          if (scenario === "receipt_unknown") {
            yield* Ref.update(harness.unreadableReceipts, (all) =>
              new Set(all).add(owner.commandId),
            );
            yield* Ref.update(harness.records, (all) => {
              const next = new Map(all);
              next.delete(owner.threadId);
              return next;
            });
          }
          yield* service.resume(earlierJobId);
          yield* service.runDueWork;
          assert.equal((yield* harness.delegated).length, 1, scenario);
          assert.equal(
            (yield* service.get(earlierJobId))!.tasks.find((task) => task.id === "a-next")!.status,
            "queued",
          );
          const recovered = (yield* service.get(ownerJobId))!.tasks.find(
            (task) => task.id === "z-owner",
          )!;
          assert.equal(
            recovered.attempts[0]!.status,
            scenario === "receipt_unknown" ? "unknown" : "running",
          );
          assert.isFalse(recovered.attempts[0]!.stopped);
          yield* Ref.set(harness.unreadableReceipts, new Set());
          if (scenario === "receipt_unknown")
            yield* Ref.update(harness.records, (all) =>
              new Map(all).set(owner.threadId, observation(owner.threadId)),
            );
          yield* harness.setStatus(owner.threadId, "completed");
          yield* service.runDueWork;
          yield* service.runDueWork;
          assert.equal((yield* harness.delegated).length, 2);
          assert.equal(
            (yield* service.get(ownerJobId))!.tasks.find((task) => task.id === "z-owner")!.attempts
              .length,
            1,
          );
        }).pipe(Effect.provide(harness.layer));
      }).pipe(Effect.provide(Layer.mergeAll(IdAllocator.layer, Sqlite.layerMemory))),
    { discard: true },
  ),
);

it.effect(
  "keeps stalled workspace ownership until progress and resumes blocked coordination explicitly",
  () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness;
      yield* Effect.gen(function* () {
        const service = yield* EventOrchestration.EventOrchestrationService;
        yield* service.configure(config);
        const job = yield* service.submit(event);
        yield* service.runDueWork;
        yield* service.report(job.coordinatorThreadId, {
          kind: "job_blocked",
          jobId: job.id,
          summary: "Missing acceptance criteria",
        });
        const first = yield* service.assign(job.coordinatorThreadId, {
          jobId: job.id,
          taskId: "build",
          roleId: "developer",
          objective: "Build",
        });
        yield* service.assign(job.coordinatorThreadId, {
          jobId: job.id,
          taskId: "other",
          roleId: "developer",
          objective: "Other root workspace write",
        });
        yield* service.runDueWork;
        assert.equal((yield* harness.delegated).length, 0);
        yield* service.report(job.coordinatorThreadId, {
          kind: "job_resume",
          jobId: job.id,
          evidence: ["Acceptance criteria supplied"],
        });
        yield* service.configure({ ...config, enabled: false });
        const paused = yield* service.assign(job.coordinatorThreadId, {
          jobId: job.id,
          taskId: "paused",
          roleId: "developer",
          objective: "Wait for operator to enable execution",
        });
        assert.equal(paused.attempts[0]!.status, "queued");
        assert.equal((yield* harness.delegated).length, 0);
        yield* service.configure(config);
        yield* service.runDueWork;
        yield* service.runDueWork;
        assert.equal((yield* harness.delegated).length, 1);
        yield* TestClock.adjust("10 minutes");
        yield* service.runDueWork;
        const stalled = (yield* service.get(job.id))!.tasks.find((task) => task.id === "build")!;
        assert.equal(stalled.status, "stalled");
        assert.equal(stalled.attempts[0]!.status, "stalled");
        assert.isFalse(stalled.attempts[0]!.stopped);
        assert.equal(stalled.attempts[0]!.lastHeartbeatAt, "1970-01-01T00:10:00.000Z");
        assert.equal(stalled.attempts[0]!.lastProgressAt, "1970-01-01T00:00:00.000Z");
        assert.equal((yield* harness.delegated).length, 1);
        assert.equal(
          (yield* service
            .report(job.coordinatorThreadId, { kind: "retry", jobId: job.id, taskId: first.id })
            .pipe(Effect.flip)).reason,
          "conflict",
        );
        const checkpoint = yield* service.report(first.attempts[0]!.threadId, {
          kind: "progress",
          jobId: job.id,
          attemptId: first.attempts[0]!.id,
          summary: "New verification checkpoint",
        });
        assert.equal(checkpoint.tasks.find((task) => task.id === "build")!.status, "running");
        assert.equal(
          checkpoint.tasks.find((task) => task.id === "build")!.attempts[0]!.status,
          "running",
        );
        yield* service.runDueWork;
        const resumed = (yield* service.get(job.id))!.tasks.find((task) => task.id === "build")!;
        assert.equal(resumed.status, "running");
        assert.equal(resumed.attempts.length, 1);
        assert.equal((yield* harness.delegated).length, 1);
      }).pipe(Effect.provide(harness.layer));
    }).pipe(Effect.provide(Layer.mergeAll(IdAllocator.layer, Sqlite.layerMemory))),
);

it.effect(
  "blocks rejected dispatches and reconciles accepted dispatch errors without replacing execution",
  () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness;
      yield* Effect.gen(function* () {
        const service = yield* EventOrchestration.EventOrchestrationService;
        yield* service.configure(config);
        const job = yield* service.submit(event);
        yield* service.runDueWork;
        yield* Ref.set(harness.failDispatch, true);
        const rejected = yield* service.assign(job.coordinatorThreadId, {
          jobId: job.id,
          taskId: "rejected",
          roleId: "developer",
          objective: "Fail before execution",
        });
        assert.equal(rejected.status, "blocked");
        assert.equal(rejected.attempts[0]!.failureKind, "permanent");
        assert.isTrue(rejected.attempts[0]!.stopped);
        yield* service.runDueWork;
        assert.equal((yield* service.get(job.id))!.tasks[0]!.attempts.length, 1);

        yield* Ref.set(harness.failDispatch, false);
        yield* Ref.set(harness.failAcceptedDispatch, true);
        const uncertain = yield* service.assign(job.coordinatorThreadId, {
          jobId: job.id,
          taskId: "a".repeat(160),
          roleId: "developer",
          objective: "Continue an accepted command after response failure",
        });
        assert.equal(uncertain.status, "unknown");
        assert.isBelow(uncertain.attempts[0]!.id.length, 160);
        assert.isFalse(uncertain.attempts[0]!.stopped);
        assert.isNull(uncertain.attempts[0]!.failureKind);
        yield* service.runDueWork;
        yield* harness.setStatus(uncertain.attempts[0]!.threadId, "completed");
        yield* service.runDueWork;
        const reconciled = (yield* service.get(job.id))!.tasks.find(
          (task) => task.id === uncertain.id,
        )!;
        assert.equal(reconciled.status, "awaiting_review");
        assert.equal(reconciled.attempts.length, 1);
        assert.equal((yield* harness.delegated).length, 1);
      }).pipe(Effect.provide(harness.layer));
    }).pipe(Effect.provide(Layer.mergeAll(IdAllocator.layer, Sqlite.layerMemory))),
);

it.effect(
  "bounds coordinator relaunches and preserves pending restart ownership during resume",
  () =>
    Effect.gen(function* () {
      const harness = yield* makeHarness;
      yield* Effect.gen(function* () {
        const service = yield* EventOrchestration.EventOrchestrationService;
        yield* service.configure(config);
        yield* Ref.set(harness.launchFailures, true);
        const failed = yield* service.submit(event);
        for (const ordinal of [1, 2, 3]) {
          yield* service.runDueWork;
          yield* service.runDueWork;
          const blocked = (yield* service.get(failed.id))!;
          assert.equal(blocked.status, "blocked");
          assert.equal(blocked.coordinatorLaunchCount, ordinal);
          assert.equal((yield* Ref.get(harness.launches)).length, ordinal);
          if (ordinal < 3) yield* service.resume(failed.id);
        }
        assert.equal((yield* service.resume(failed.id).pipe(Effect.flip)).reason, "exhausted");

        yield* Ref.set(harness.launchFailures, false);
        const active = yield* service.submit({ ...event, deliveryId: "pending-restart" });
        yield* service.runDueWork;
        yield* harness.setStatus(active.coordinatorThreadId, "completed");
        yield* Ref.set(harness.pendingSettlement, true);
        const resumed = yield* service.resume(active.id);
        assert.equal(resumed.coordinatorLaunchCount, 1);
        assert.equal(resumed.coordinatorCommandId, active.coordinatorCommandId);
        yield* service.runDueWork;
        assert.equal((yield* service.get(active.id))!.coordinatorStatus, "waiting");
        assert.isFalse((yield* service.get(active.id))!.coordinatorStopped);
        yield* Ref.set(harness.pendingSettlement, false);
        const queued = yield* service.resume(active.id);
        assert.equal(queued.coordinatorLaunchCount, 2);
        assert.equal(queued.coordinatorLaunchMode, "resume");
        yield* service.runDueWork;
        assert.equal((yield* service.get(active.id))!.coordinatorStatus, "running");
        assert.equal((yield* Ref.get(harness.launches)).length, 4);
      }).pipe(Effect.provide(harness.layer));
    }).pipe(Effect.provide(Layer.mergeAll(IdAllocator.layer, Sqlite.layerMemory))),
);
