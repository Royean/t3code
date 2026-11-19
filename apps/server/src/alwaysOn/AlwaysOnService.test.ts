import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as TestClock from "effect/testing/TestClock";
import { assert, expect, it } from "@effect/vitest";
import {
  AlwaysOnAgentId,
  AlwaysOnMemoryId,
  ProjectId,
  type AlwaysOnAction,
} from "@t3tools/contracts";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as AlwaysOnService from "./AlwaysOnService.ts";
import { containsCredential, selectInboxBatch } from "./alwaysOnLogic.ts";

const SECRET = "token=SUPER-SECRET-VALUE";
const projectId = ProjectId.make("project-always-on");

const schedulerMock = Layer.succeed(
  Scheduler.Scheduler,
  Scheduler.Scheduler.of({
    register: () => Effect.void,
  }),
);

const manual = AlwaysOnService.layerManual.pipe(Layer.provide(schedulerMock));

const withService = <A, E, R>(effect: Effect.Effect<A, E, R | AlwaysOnService.AlwaysOnService>) =>
  effect.pipe(Effect.provide(manual.pipe(Layer.provideMerge(SqlitePersistence.layerMemory))));

it("rejects credential-shaped text and keeps fresh events out of a user session", () => {
  expect(containsCredential({ actions: [{ body: SECRET }] })).toBe(true);
  expect(containsCredential({ actions: [{ body: "Check the inbox" }] })).toBe(false);
  expect(
    selectInboxBatch([
      { kind: "user_reply", relatedSessionId: "session-1" },
      { kind: "user_reply", relatedSessionId: "session-1" },
      { kind: "timer", relatedSessionId: null },
    ]).map((event) => event.kind),
  ).toEqual(["user_reply", "user_reply"]);
  expect(
    selectInboxBatch([
      { kind: "external", relatedSessionId: null },
      { kind: "timer", relatedSessionId: null },
      { kind: "approval", relatedSessionId: "session-2" },
    ]).map((event) => event.kind),
  ).toEqual(["external", "timer"]);
});

it.effect("acks an event, runs it, stores memory and a delivery, then sleeps", () =>
  withService(
    Effect.gen(function* () {
      const service = yield* AlwaysOnService.AlwaysOnService;
      const created = yield* service.create({
        projectId,
        title: "Desk",
        instructions: "Stay quiet unless asked.",
      });
      const submitted = yield* service.submitEvent({
        agentId: created.agent.id,
        source: "user",
        sourceEventId: "msg-1",
        kind: "user_reply",
        actions: [
          { type: "remember_note", key: "note-1", title: "Role", pointer: "directory:people/role" },
          {
            type: "record_progress",
            key: "progress-1",
            title: "Inbox",
            taskKey: "inbox",
            done: "Seen msg-1",
          },
          { type: "deliver", key: "deliver-1", dedupeKey: "reply-1", body: "Noted." },
        ],
      });
      const duplicate = yield* service.submitEvent({
        agentId: created.agent.id,
        source: "user",
        sourceEventId: "msg-1",
        kind: "user_reply",
        actions: [],
      });
      expect(duplicate).toEqual({ eventId: submitted.eventId, duplicate: true });
      expect(created.agent.phase).toBe("sleeping");

      yield* service.drain();
      const detail = yield* service.get({ agentId: created.agent.id });
      expect(detail.agent.phase).toBe("sleeping");
      expect(detail.inbox.map((event) => event.status)).toEqual(["consumed"]);
      expect(detail.runs.map((run) => [run.status, run.outcome])).toEqual([
        ["completed", "deliver"],
      ]);
      expect(detail.runs[0]?.loadedContext.instructions).toBe("Stay quiet unless asked.");
      expect(detail.runs[0]?.loadedContext.noteIndex).toEqual([]);
      expect(detail.deliveries).toEqual([
        expect.objectContaining({
          body: "Noted.",
          status: "delivered",
          attempts: 1,
          dedupeKey: "reply-1",
        }),
      ]);
      const note = detail.memoryIndex.find((entry) => entry.kind === "note");
      expect(note?.pointer).toBe("directory:people/role");
      expect(note?.hasBody).toBe(false);
      const progress = detail.memoryIndex.find((entry) => entry.kind === "task_progress");
      assert(progress !== undefined);
      const record = yield* service.readMemory({
        agentId: created.agent.id,
        memoryId: progress.id,
      });
      expect(record.body).toBe("Seen msg-1");

      yield* service.submitEvent({
        agentId: created.agent.id,
        source: "user",
        sourceEventId: "msg-2",
        kind: "user_reply",
        actions: [{ type: "silent", key: "quiet-2" }],
      });
      yield* service.drain();
      const continued = yield* service.get({ agentId: created.agent.id });
      expect(continued.runs.map((run) => run.sessionId)).toEqual([
        detail.runs[0]!.sessionId,
        detail.runs[0]!.sessionId,
      ]);
      expect(continued.runs[1]?.loadedContext.noteIndex.map((entry) => entry.title)).toEqual([
        "Role",
      ]);
      expect(JSON.stringify(continued.runs[1]?.loadedContext.noteIndex)).not.toContain(
        "Seen msg-1",
      );
    }),
  ),
);

it.effect("defers events that arrive during a run and does not treat a heartbeat as progress", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const gate = Layer.succeed(AlwaysOnService.AlwaysOnRunGate, {
      afterClaim: Effect.gen(function* () {
        yield* Deferred.succeed(entered, undefined);
        yield* Deferred.await(release);
      }),
      afterUncertain: Effect.void,
    });
    yield* Effect.gen(function* () {
      const service = yield* AlwaysOnService.AlwaysOnService;
      const created = yield* service.create({
        projectId,
        title: "Desk",
        instructions: "Stay quiet.",
      });
      yield* service.submitEvent({
        agentId: created.agent.id,
        source: "github",
        sourceEventId: "evt-1",
        kind: "external",
        actions: [{ type: "remember_note", key: "during", title: "First", body: "one" }],
      });
      const fiber = yield* Effect.forkChild(service.drain());
      yield* Deferred.await(entered);
      yield* service.submitEvent({
        agentId: created.agent.id,
        source: "github",
        sourceEventId: "evt-2",
        kind: "external",
        actions: [{ type: "silent", key: "later" }],
      });
      const mid = yield* service.get({ agentId: created.agent.id });
      expect(mid.agent.phase).toBe("running");
      expect(mid.runs.map((run) => run.eventIds)).toEqual([[expect.any(String)]]);
      expect(mid.runs[0]?.lastProgressAt).toBeNull();
      expect(mid.inbox.filter((event) => event.status === "pending")).toHaveLength(1);
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(fiber);
      const done = yield* service.get({ agentId: created.agent.id });
      expect(done.runs).toHaveLength(2);
      expect(done.runs[0]?.eventIds).not.toEqual(done.runs[1]?.eventIds);
      expect(done.runs[0]?.sessionId).not.toBe(done.runs[1]?.sessionId);
      expect(done.agent.phase).toBe("sleeping");
    }).pipe(
      Effect.provide(
        AlwaysOnService.layerManual.pipe(Layer.provide(Layer.mergeAll(schedulerMock, gate))),
      ),
    );
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("fires a due timer into the inbox on the scheduler and runs it afterward", () =>
  Effect.gen(function* () {
    const service = yield* AlwaysOnService.AlwaysOnService;
    const created = yield* service.create({
      projectId,
      title: "Desk",
      instructions: "Check in.",
    });
    const dueAt = DateTime.formatIso(DateTime.add(yield* DateTime.now, { seconds: 1 }));
    yield* service.submitEvent({
      agentId: created.agent.id,
      source: "agent",
      sourceEventId: "arm-timer",
      kind: "system",
      actions: [{ type: "set_timer", key: "daily", timerKind: "deadline", dueAt }],
    });
    yield* service.drain();
    const armed = yield* service.get({ agentId: created.agent.id });
    expect(armed.timers).toEqual([expect.objectContaining({ kind: "deadline", status: "armed" })]);
    expect(armed.inbox.some((event) => event.kind === "timer")).toBe(false);

    yield* TestClock.adjust(Duration.seconds(5));
    const fired = yield* service.get({ agentId: created.agent.id });
    expect(fired.inbox.some((event) => event.kind === "timer" && event.status === "consumed")).toBe(
      true,
    );
    expect(fired.timers[0]?.status).toBe("fired");
    expect(fired.runs.at(-1)?.outcome).toBe("silent");
  }).pipe(
    Effect.provide(
      AlwaysOnService.layerManual.pipe(
        Layer.provide(Scheduler.layer),
        Layer.provideMerge(SqlitePersistence.layerMemory),
      ),
    ),
  ),
);

it.effect("releases the worker while an approval waits, then resumes or skips the action", () =>
  withService(
    Effect.gen(function* () {
      const service = yield* AlwaysOnService.AlwaysOnService;
      const created = yield* service.create({
        projectId,
        title: "Desk",
        instructions: "Ask first.",
      });
      const approved = yield* agentWithApproval(service, created.agent.id, "approval-a", "ship-a");
      yield* service.drain();
      const waiting = yield* service.get({ agentId: created.agent.id });
      expect(waiting.agent.phase).toBe("sleeping");
      expect(waiting.runs[0]?.status).toBe("awaiting_approval");
      expect(waiting.approvals[0]?.status).toBe("pending");

      yield* service.submitEvent({
        agentId: created.agent.id,
        source: "github",
        sourceEventId: "while-waiting",
        kind: "external",
        actions: [{ type: "remember_note", key: "meanwhile", title: "Other work", body: "done" }],
      });
      yield* service.drain();
      const parallel = yield* service.get({ agentId: created.agent.id });
      expect(parallel.runs.map((run) => run.status)).toEqual(["awaiting_approval", "completed"]);
      expect(parallel.memoryIndex.some((entry) => entry.title === "Other work")).toBe(true);
      expect(parallel.deliveries).toEqual([]);

      yield* service.resolveApproval({
        agentId: created.agent.id,
        approvalId: approved,
        decision: "approved",
      });
      yield* service.drain();
      const resumed = yield* service.get({ agentId: created.agent.id });
      expect(resumed.deliveries.map((delivery) => delivery.body)).toEqual(["Ship it."]);
      expect(resumed.approvals[0]?.status).toBe("approved");

      const rejected = yield* service.create({
        projectId,
        title: "Other desk",
        instructions: "Ask first.",
      });
      const rejectedApproval = yield* agentWithApproval(
        service,
        rejected.agent.id,
        "approval-b",
        "ship-b",
      );
      yield* service.drain();
      yield* service.resolveApproval({
        agentId: rejected.agent.id,
        approvalId: rejectedApproval,
        decision: "rejected",
      });
      yield* service.drain();
      const declined = yield* service.get({ agentId: rejected.agent.id });
      expect(declined.deliveries).toEqual([]);
      expect(declined.approvals[0]?.status).toBe("rejected");
    }),
  ),
);

it.effect("retries a delivery once and deduplicates a repeated send", () =>
  Effect.gen(function* () {
    const attempts = yield* Ref.make(0);
    const delivery = Layer.succeed(AlwaysOnService.AlwaysOnDeliveryAttempt, () =>
      Effect.gen(function* () {
        const attempt = yield* Ref.updateAndGet(attempts, (current) => current + 1);
        if (attempt === 1) return yield* new AlwaysOnService.AlwaysOnDeliveryFailed();
      }),
    );
    yield* Effect.gen(function* () {
      const service = yield* AlwaysOnService.AlwaysOnService;
      const created = yield* service.create({
        projectId,
        title: "Desk",
        instructions: "Be quiet.",
      });
      yield* service.submitEvent({
        agentId: created.agent.id,
        source: "user",
        sourceEventId: "send",
        kind: "user_reply",
        actions: [{ type: "deliver", key: "send", dedupeKey: "once", body: "Hello" }],
      });
      yield* service.drain();
      const failed = yield* service.get({ agentId: created.agent.id });
      expect(failed.deliveries[0]).toMatchObject({ status: "pending", attempts: 1 });
      yield* service.drain();
      const stillWaiting = yield* service.get({ agentId: created.agent.id });
      expect(stillWaiting.deliveries[0]?.status).toBe("pending");
      yield* TestClock.adjust(Duration.seconds(1));
      yield* service.drain();
      const delivered = yield* service.get({ agentId: created.agent.id });
      expect(delivered.deliveries[0]).toMatchObject({ status: "delivered", attempts: 2 });

      yield* service.submitEvent({
        agentId: created.agent.id,
        source: "user",
        sourceEventId: "send-again",
        kind: "user_reply",
        actions: [{ type: "deliver", key: "send-again", dedupeKey: "once", body: "Hello again" }],
      });
      yield* service.drain();
      const deduped = yield* service.get({ agentId: created.agent.id });
      expect(deduped.deliveries.filter((delivery) => delivery.status === "delivered")).toHaveLength(
        1,
      );
      expect(deduped.deliveries[0]?.body).toBe("Hello");
    }).pipe(
      Effect.provide(
        AlwaysOnService.layerManual.pipe(Layer.provide(Layer.mergeAll(schedulerMock, delivery))),
      ),
    );
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("requeues a crashed run and withholds an uncertain effect instead of replaying it", () =>
  Effect.gen(function* () {
    const safeEntered = yield* Deferred.make<void>();
    const safeRelease = yield* Deferred.make<void>();
    const uncertainEntered = yield* Deferred.make<void>();
    const uncertainRelease = yield* Deferred.make<void>();
    const safeId = yield* crashDuring(
      {
        afterClaim: Effect.gen(function* () {
          yield* Deferred.succeed(safeEntered, undefined);
          yield* Deferred.await(safeRelease);
        }),
        afterUncertain: Effect.void,
      },
      safeEntered,
      [{ type: "remember_note", key: "note", title: "Kept", body: "body" }],
      "safe-event",
    );
    const unknownId = yield* crashDuring(
      {
        afterClaim: Effect.void,
        afterUncertain: Effect.gen(function* () {
          yield* Deferred.succeed(uncertainEntered, undefined);
          yield* Deferred.await(uncertainRelease);
        }),
      },
      uncertainEntered,
      [{ type: "record_uncertain", key: "external", summary: "An unverified external call" }],
      "unknown-event",
    );

    return yield* Effect.gen(function* () {
      const service = yield* AlwaysOnService.AlwaysOnService;
      yield* service.drain();
      const retried = yield* service.get({ agentId: safeId });
      expect(retried.runs.map((run) => run.status)).toEqual(["interrupted", "completed"]);
      expect(retried.inbox.every((event) => event.status === "consumed")).toBe(true);
      expect(retried.memoryIndex.filter((entry) => entry.kind === "note")).toHaveLength(1);

      const unknown = yield* service.get({ agentId: unknownId });
      expect(unknown.agent.phase).toBe("needs_confirmation");
      expect(unknown.runs.map((run) => run.status)).toEqual(["unknown"]);
      expect(unknown.inbox[0]?.status).toBe("consumed");
      expect(unknown.memoryIndex.map((entry) => entry.kind)).toEqual(["instruction"]);
      yield* service.drain();
      const stillUnknown = yield* service.get({ agentId: unknownId });
      expect(stillUnknown.runs).toHaveLength(1);
      const confirmed = yield* service.confirmUnknown({ agentId: unknownId });
      expect(confirmed.phase).toBe("sleeping");
      yield* service.submitEvent({
        agentId: unknownId,
        source: "user",
        sourceEventId: "after-confirm",
        kind: "system",
        actions: [{ type: "deliver", key: "after", dedupeKey: "after", body: "Still here" }],
      });
      yield* service.drain();
      const continued = yield* service.get({ agentId: unknownId });
      expect(continued.deliveries.map((delivery) => delivery.body)).toEqual(["Still here"]);
      expect(continued.runs.map((run) => run.outcome)).toEqual(["silent", "deliver"]);
    }).pipe(Effect.provide(manual));
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("cancels a running run and pauses dispatch without dropping the inbox", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const gate = Layer.succeed(AlwaysOnService.AlwaysOnRunGate, {
      afterClaim: Effect.gen(function* () {
        yield* Deferred.succeed(entered, undefined);
        yield* Deferred.await(release);
      }),
      afterUncertain: Effect.void,
    });
    yield* Effect.gen(function* () {
      const service = yield* AlwaysOnService.AlwaysOnService;
      const created = yield* service.create({
        projectId,
        title: "Desk",
        instructions: "Stay quiet.",
      });
      yield* service.submitEvent({
        agentId: created.agent.id,
        source: "user",
        sourceEventId: "cancel-me",
        kind: "system",
        actions: [{ type: "remember_note", key: "nope", title: "Should not land", body: "no" }],
      });
      const fiber = yield* Effect.forkChild(service.drain());
      yield* Deferred.await(entered);
      const running = yield* service.get({ agentId: created.agent.id });
      const runId = running.runs[0]?.id;
      assert(runId !== undefined);
      yield* service.cancel({ agentId: created.agent.id, runId });
      yield* Deferred.succeed(release, undefined);
      yield* Fiber.join(fiber);
      const cancelled = yield* service.get({ agentId: created.agent.id });
      expect(cancelled.runs[0]?.status).toBe("cancelled");
      expect(cancelled.memoryIndex.some((entry) => entry.title === "Should not land")).toBe(false);

      yield* service.setEnabled({ agentId: created.agent.id, enabled: false });
      yield* service.submitEvent({
        agentId: created.agent.id,
        source: "user",
        sourceEventId: "while-paused",
        kind: "external",
        actions: [{ type: "silent", key: "paused" }],
      });
      yield* service.drain();
      const paused = yield* service.get({ agentId: created.agent.id });
      expect(paused.agent.phase).toBe("paused");
      expect(paused.inbox.some((event) => event.status === "pending")).toBe(true);
      yield* service.setEnabled({ agentId: created.agent.id, enabled: true });
      yield* service.drain();
      const resumed = yield* service.get({ agentId: created.agent.id });
      expect(resumed.agent.phase).toBe("sleeping");
      expect(resumed.inbox.every((event) => event.status !== "pending")).toBe(true);
    }).pipe(
      Effect.provide(
        AlwaysOnService.layerManual.pipe(Layer.provide(Layer.mergeAll(schedulerMock, gate))),
      ),
    );
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("refuses credential text and another agent's memory", () =>
  withService(
    Effect.gen(function* () {
      const service = yield* AlwaysOnService.AlwaysOnService;
      const created = yield* service.create({
        projectId,
        title: "Desk",
        instructions: "Stay quiet.",
      });
      const failure = yield* service
        .submitEvent({
          agentId: created.agent.id,
          source: "user",
          sourceEventId: "secret",
          kind: "user_reply",
          actions: [{ type: "remember_note", key: "secret", title: "Nope", body: SECRET }],
        })
        .pipe(Effect.flip);
      expect(failure.reason).toBe("rejected");
      expect(failure.message).not.toContain(SECRET);
      const clean = yield* service.get({ agentId: created.agent.id });
      expect(clean.inbox).toEqual([]);
      expect(JSON.stringify(clean)).not.toContain(SECRET);

      yield* service.submitEvent({
        agentId: created.agent.id,
        source: "user",
        sourceEventId: "note",
        kind: "user_reply",
        actions: [{ type: "remember_note", key: "owned", title: "Mine", body: "private note" }],
      });
      yield* service.drain();
      const detail = yield* service.get({ agentId: created.agent.id });
      const memoryId = detail.memoryIndex.find((entry) => entry.kind === "note")?.id;
      assert(memoryId !== undefined);
      const other = yield* service.create({
        projectId,
        title: "Other",
        instructions: "Stay quiet.",
      });
      const denied = yield* service
        .readMemory({ agentId: other.agent.id, memoryId })
        .pipe(Effect.flip);
      expect(denied.reason).toBe("denied");
      expect(denied.message).not.toContain("private note");
      const missing = yield* service
        .readMemory({
          agentId: created.agent.id,
          memoryId: AlwaysOnMemoryId.make("always-on-memory:missing"),
        })
        .pipe(Effect.flip);
      expect(missing.reason).toBe("not_found");
      expect(AlwaysOnAgentId.make(created.agent.id)).toBe(created.agent.id);
    }),
  ),
);

const agentWithApproval = (
  service: AlwaysOnService.AlwaysOnService["Service"],
  agentId: AlwaysOnAgentId,
  sourceEventId: string,
  dedupeKey: string,
) =>
  Effect.gen(function* () {
    yield* service.submitEvent({
      agentId,
      source: "user",
      sourceEventId,
      kind: "user_reply",
      actions: [
        {
          type: "request_approval",
          key: `${sourceEventId}-ask`,
          summary: "Send the update?",
          onApprove: [{ type: "deliver", key: `${dedupeKey}-send`, dedupeKey, body: "Ship it." }],
        },
      ],
    });
    yield* service.drain();
    const detail = yield* service.get({ agentId });
    const approvalId = detail.approvals[0]?.id;
    assert(approvalId !== undefined);
    return approvalId;
  });

const crashDuring = (
  gate: {
    readonly afterClaim: Effect.Effect<void>;
    readonly afterUncertain: Effect.Effect<void>;
  },
  entered: Deferred.Deferred<void>,
  actions: readonly AlwaysOnAction[],
  sourceEventId: string,
) =>
  Effect.scoped(
    Effect.gen(function* () {
      const service = yield* AlwaysOnService.AlwaysOnService;
      const created = yield* service.create({
        projectId,
        title: sourceEventId,
        instructions: "Stay quiet.",
      });
      yield* service.submitEvent({
        agentId: created.agent.id,
        source: "fixture",
        sourceEventId,
        kind: "external",
        actions: [...actions],
      });
      const fiber = yield* Effect.forkChild(service.drain());
      yield* Deferred.await(entered);
      yield* Fiber.interrupt(fiber);
      return created.agent.id;
    }).pipe(
      Effect.provide(
        AlwaysOnService.layerManual.pipe(
          Layer.provide(
            Layer.mergeAll(schedulerMock, Layer.succeed(AlwaysOnService.AlwaysOnRunGate, gate)),
          ),
        ),
      ),
    ),
  );
