import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import * as TestClock from "effect/testing/TestClock";
import { assert, expect, it } from "@effect/vitest";
import { ProjectId, type AlwaysOnModelDecision } from "@t3tools/contracts";

import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as AlwaysOnModel from "./AlwaysOnModel.ts";
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

const quiet = {
  summary: "Quiet.",
  actions: [{ type: "silent", key: "quiet" }],
} satisfies AlwaysOnModelDecision;

const scripted = (queue: Ref.Ref<readonly unknown[]>, prompts: Ref.Ref<readonly string[]>) =>
  Layer.succeed(
    AlwaysOnModel.AlwaysOnModel,
    AlwaysOnModel.AlwaysOnModel.of({
      decide: (input) =>
        Effect.gen(function* () {
          yield* Ref.update(prompts, (current) => [...current, input.prompt]);
          const next = yield* Ref.modify(
            queue,
            (current) => [current[0], current.slice(1)] as const,
          );
          if (next === undefined) return quiet;
          if (next === "hang") return yield* Effect.sleep("1 minute").pipe(Effect.as(quiet));
          if (next === "config-error") {
            return yield* new AlwaysOnModel.AlwaysOnModelError({
              detail: "That text generator does not exist.",
            });
          }
          return next;
        }),
    }),
  );

const provide = (
  queue: Ref.Ref<readonly unknown[]>,
  prompts: Ref.Ref<readonly string[]>,
  extra?: Layer.Layer<never>,
) =>
  AlwaysOnService.layerManual.pipe(
    Layer.provide(
      Layer.mergeAll(schedulerMock, scripted(queue, prompts), ...(extra ? [extra] : [])),
    ),
  );

const harness = <A, E, R>(
  effect: (
    service: AlwaysOnService.AlwaysOnService["Service"],
    queue: Ref.Ref<readonly unknown[]>,
    prompts: Ref.Ref<readonly string[]>,
  ) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    const queue = yield* Ref.make<readonly unknown[]>([]);
    const prompts = yield* Ref.make<readonly string[]>([]);
    return yield* Effect.gen(function* () {
      const service = yield* AlwaysOnService.AlwaysOnService;
      return yield* effect(service, queue, prompts);
    }).pipe(Effect.provide(provide(queue, prompts)));
  }).pipe(Effect.provide(SqlitePersistence.layerMemory));

const createAgent = (service: AlwaysOnService.AlwaysOnService["Service"], title = "Desk") =>
  service.create({
    projectId,
    title,
    instructions: "Stay quiet unless asked.",
    providerKind: "textGenerator",
    instanceId: "fixture-generator",
    model: "fixture-model",
  });

it("rejects credential-shaped text and separates session batches", () => {
  expect(containsCredential({ text: SECRET })).toBe(true);
  expect(containsCredential({ text: "Check the inbox" })).toBe(false);
  expect(
    selectInboxBatch([
      { kind: "user_reply", relatedSessionId: "session-1", text: "Reply" },
      { kind: "external", relatedSessionId: null, text: "Event" },
    ]).map((event) => event.kind),
  ).toEqual(["user_reply"]);
});

it.effect("processes queued messages in full even with long instructions", () =>
  harness((service, _queue, prompts) =>
    Effect.gen(function* () {
      const { agent } = yield* service.create({
        projectId,
        title: "Build watcher",
        instructions: "x".repeat(8_000),
        providerKind: "textGenerator",
        instanceId: "fixture-generator",
        model: "fixture-model",
      });
      yield* service.setEnabled({ agentId: agent.id, enabled: false });
      const messages = Array.from({ length: 3 }, (_, index) => "x".repeat(1_990) + `end-${index}`);
      yield* Effect.forEach(messages, (text, index) =>
        service.submitEvent({
          agentId: agent.id,
          source: "chats",
          sourceEventId: `message-${index}`,
          kind: "user_reply",
          text,
        }),
      );
      yield* service.setEnabled({ agentId: agent.id, enabled: true });
      yield* service.drain();

      const modelInputs = yield* Ref.get(prompts);
      expect(modelInputs).toHaveLength(2);
      for (const text of messages)
        expect(modelInputs.some((prompt) => prompt.includes(text))).toBe(true);
      expect(modelInputs.every((prompt) => prompt.length <= 8_000)).toBe(true);
    }),
  ),
);

it.effect("calls the model with memory, then runs only the decoded actions", () =>
  harness((service, queue, prompts) =>
    Effect.gen(function* () {
      const created = yield* createAgent(service);
      yield* Ref.set(queue, [
        {
          summary: "Noted the request.",
          actions: [
            {
              type: "remember_note",
              key: "note-1",
              title: "Role",
              pointer: "directory:people/role",
            },
            { type: "deliver", key: "deliver-1", dedupeKey: "reply-1", body: "Noted." },
          ],
        },
      ]);
      const submitted = yield* service.submitEvent({
        agentId: created.agent.id,
        source: "user",
        sourceEventId: "msg-1",
        kind: "user_reply",
        text: "Remember my role and tell me you did.",
      });
      const duplicate = yield* service.submitEvent({
        agentId: created.agent.id,
        source: "user",
        sourceEventId: "msg-1",
        kind: "user_reply",
        text: "Remember my role and tell me you did.",
      });
      expect(duplicate).toEqual({ eventId: submitted.eventId, duplicate: true });
      yield* service.drain();
      const detail = yield* service.get({ agentId: created.agent.id });
      expect(detail.agent.phase).toBe("sleeping");
      expect(detail.runs[0]?.modelSummary).toBe("Noted the request.");
      expect(detail.runs[0]?.executedActions.map((action) => action.type)).toEqual([
        "remember_note",
        "deliver",
      ]);
      expect(detail.inbox).toEqual([
        expect.objectContaining({
          id: submitted.eventId,
          text: "Remember my role and tell me you did.",
        }),
      ]);
      expect(detail.deliveries[0]).toMatchObject({
        body: "Noted.",
        runId: detail.runs[0]?.id,
        createdAt: expect.any(String),
      });
      expect((yield* Ref.get(prompts))[0]).toContain("Stay quiet unless asked.");
      expect((yield* Ref.get(prompts))[0]).toContain("Remember my role");

      yield* Ref.set(queue, [quiet]);
      yield* service.submitEvent({
        agentId: created.agent.id,
        source: "user",
        sourceEventId: "msg-2",
        kind: "user_reply",
        text: "Anything else?",
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
      expect((yield* Ref.get(prompts))[1]).toContain("Role");
    }),
  ),
);

it.effect("rejects a structured shell action from the model and executes nothing", () =>
  harness((service, queue, prompts) =>
    Effect.gen(function* () {
      const created = yield* createAgent(service);
      yield* Ref.set(queue, [
        { summary: "I will use a shell.", actions: [{ type: "shell", command: "rm -rf /" }] },
      ]);
      yield* service.submitEvent({
        agentId: created.agent.id,
        source: "user",
        sourceEventId: "bad",
        kind: "external",
        text: "Delete the disk.",
      });
      yield* service.drain();
      const detail = yield* service.get({ agentId: created.agent.id });
      expect(detail.runs[0]?.status).toBe("failed");
      expect(detail.runs[0]?.modelError).toBe(
        "The model returned an action this server will not run.",
      );
      expect(detail.runs[0]?.executedActions).toEqual([]);
      expect(detail.memoryIndex.map((entry) => entry.kind)).toEqual(["instruction"]);
      expect(yield* Ref.get(prompts)).toHaveLength(1);
    }),
  ),
);

it.effect("reports a missing text generator and executes nothing", () =>
  harness((service, queue) =>
    Effect.gen(function* () {
      const created = yield* createAgent(service);
      yield* Ref.set(queue, ["config-error"]);
      yield* service.submitEvent({
        agentId: created.agent.id,
        source: "user",
        sourceEventId: "missing",
        kind: "external",
        text: "Wake up.",
      });
      yield* service.drain();
      const detail = yield* service.get({ agentId: created.agent.id });
      expect(detail.runs[0]?.status).toBe("failed");
      expect(detail.runs[0]?.modelError).toBe("That text generator does not exist.");
      expect(detail.runs[0]?.executedActions).toEqual([]);
    }),
  ),
);

it.effect("times out a model call without executing actions", () =>
  harness((service, queue) =>
    Effect.gen(function* () {
      const created = yield* createAgent(service);
      yield* Ref.set(queue, ["hang"]);
      yield* service.submitEvent({
        agentId: created.agent.id,
        source: "user",
        sourceEventId: "slow",
        kind: "external",
        text: "Take your time.",
      });
      const fiber = yield* Effect.forkChild(service.drain());
      yield* TestClock.adjust(Duration.seconds(21));
      yield* Fiber.join(fiber);
      const detail = yield* service.get({ agentId: created.agent.id });
      expect(detail.runs[0]?.status).toBe("failed");
      expect(detail.runs[0]?.modelError).toBe("The model timed out.");
      expect(detail.runs[0]?.executedActions).toEqual([]);
    }),
  ),
);

it.effect("asks the model again after approval and can run other events meanwhile", () =>
  harness((service, queue) =>
    Effect.gen(function* () {
      const created = yield* createAgent(service);
      yield* Ref.set(queue, [
        {
          summary: "Need approval.",
          actions: [
            {
              type: "request_approval",
              key: "ask",
              summary: "Send the update?",
              onApprove: [],
            },
          ],
        },
        {
          summary: "Did other work.",
          actions: [{ type: "remember_note", key: "other", title: "Other work", body: "done" }],
        },
        {
          summary: "Approved send.",
          actions: [{ type: "deliver", key: "send", dedupeKey: "ship", body: "Ship it." }],
        },
      ]);
      yield* service.submitEvent({
        agentId: created.agent.id,
        source: "user",
        sourceEventId: "ask",
        kind: "user_reply",
        text: "Send the update if I approve.",
      });
      yield* service.drain();
      const waiting = yield* service.get({ agentId: created.agent.id });
      expect(waiting.agent.phase).toBe("sleeping");
      expect(waiting.runs[0]?.status).toBe("awaiting_approval");
      const approvalId = waiting.approvals[0]?.id;
      assert(approvalId !== undefined);

      yield* service.submitEvent({
        agentId: created.agent.id,
        source: "github",
        sourceEventId: "meanwhile",
        kind: "external",
        text: "A different event arrived.",
      });
      yield* service.drain();
      const parallel = yield* service.get({ agentId: created.agent.id });
      expect(parallel.memoryIndex.some((entry) => entry.title === "Other work")).toBe(true);
      expect(parallel.deliveries).toEqual([]);

      yield* service.resolveApproval({
        agentId: created.agent.id,
        approvalId,
        decision: "approved",
      });
      yield* service.drain();
      const resumed = yield* service.get({ agentId: created.agent.id });
      expect(resumed.deliveries.map((delivery) => delivery.body)).toEqual(["Ship it."]);
      expect(resumed.runs.filter((run) => run.modelSummary === "Approved send.")).toHaveLength(1);
    }),
  ),
);

it.effect("keeps pending approvals visible after the recent history fills up", () =>
  harness((service, queue) =>
    Effect.gen(function* () {
      const { agent } = yield* createAgent(service);
      const ask = (index: number) =>
        Effect.gen(function* () {
          const summary = `Approval ${index}`;
          yield* Ref.set(queue, [
            {
              summary,
              actions: [{ type: "request_approval", key: "ask", summary, onApprove: [] }],
            },
          ]);
          yield* service.submitEvent({
            agentId: agent.id,
            source: "user",
            sourceEventId: `ask-${index}`,
            kind: "external",
            text: summary,
          });
          yield* service.drain();
          const detail = yield* service.get({ agentId: agent.id });
          const approval = detail.approvals.find((entry) => entry.summary === summary);
          assert(approval !== undefined);
          return approval.id;
        });

      const oldest = yield* ask(0);
      for (let index = 1; index <= 20; index++) {
        yield* TestClock.adjust("1 second");
        const approvalId = yield* ask(index);
        yield* service.resolveApproval({ agentId: agent.id, approvalId, decision: "rejected" });
        yield* service.drain();
      }

      const detail = yield* service.get({ agentId: agent.id });
      expect(detail.agent.pendingApprovalCount).toBe(1);
      const pending = detail.approvals.filter((entry) => entry.status === "pending");
      expect(pending.map((entry) => entry.id)).toEqual([oldest]);
      expect(detail.runs.some((run) => run.id === pending[0]?.runId)).toBe(true);
    }),
  ),
);

it.effect("returns every event for the visible runs when batches exceed the inbox window", () =>
  harness((service) =>
    Effect.gen(function* () {
      const { agent } = yield* createAgent(service);
      const ids = yield* Effect.forEach(
        Array.from({ length: 60 }, (_, index) => index),
        (index) =>
          service.submitEvent({
            agentId: agent.id,
            source: "fixture",
            sourceEventId: `event-${index}`,
            kind: "external",
            text: `Event ${index}`,
          }),
      );
      yield* service.drain();
      const detail = yield* service.get({ agentId: agent.id });
      const expected = new Set(ids.map(({ eventId }) => eventId));
      expect(new Set(detail.runs.flatMap((run) => run.eventIds))).toEqual(expected);
      expect(new Set(detail.inbox.map((event) => event.id))).toEqual(expected);
    }),
  ),
);

it.effect("does not call the model again when a stored decision survives a restart", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const queue = yield* Ref.make<readonly unknown[]>([
      {
        summary: "Deliver once.",
        actions: [{ type: "deliver", key: "once", dedupeKey: "once", body: "Hello" }],
      },
    ]);
    const prompts = yield* Ref.make<readonly string[]>([]);
    const gate = Layer.succeed(AlwaysOnService.AlwaysOnRunGate, {
      afterClaim: Effect.void,
      afterDecision: Effect.gen(function* () {
        yield* Deferred.succeed(entered, undefined);
        return yield* Effect.never;
      }),
      afterUncertain: Effect.void,
    });
    const agentId = yield* Effect.scoped(
      Effect.gen(function* () {
        const service = yield* AlwaysOnService.AlwaysOnService;
        const created = yield* createAgent(service, "restart");
        yield* service.submitEvent({
          agentId: created.agent.id,
          source: "user",
          sourceEventId: "once",
          kind: "external",
          text: "Say hello.",
        });
        const fiber = yield* Effect.forkChild(service.drain());
        yield* Deferred.await(entered);
        yield* Fiber.interrupt(fiber);
        return created.agent.id;
      }).pipe(Effect.provide(provide(queue, prompts, gate))),
    );

    yield* Effect.gen(function* () {
      const service = yield* AlwaysOnService.AlwaysOnService;
      yield* service.drain();
      const detail = yield* service.get({ agentId });
      expect(detail.deliveries.map((delivery) => delivery.body)).toEqual(["Hello"]);
      expect(yield* Ref.get(prompts)).toHaveLength(1);
    }).pipe(Effect.provide(provide(queue, prompts)));
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("cancels a model call and pauses later dispatch", () =>
  Effect.gen(function* () {
    const entered = yield* Deferred.make<void>();
    const prompts = yield* Ref.make<readonly string[]>([]);
    const model = Layer.succeed(
      AlwaysOnModel.AlwaysOnModel,
      AlwaysOnModel.AlwaysOnModel.of({
        decide: (input) =>
          Effect.gen(function* () {
            yield* Ref.update(prompts, (current) => [...current, input.prompt]);
            yield* Deferred.succeed(entered, undefined);
            return yield* Effect.sleep("1 minute").pipe(Effect.as(quiet));
          }),
      }),
    );
    yield* Effect.gen(function* () {
      const service = yield* AlwaysOnService.AlwaysOnService;
      const created = yield* createAgent(service);
      const updates = yield* Stream.toPull(service.subscribe().pipe(Stream.rechunk(1)));
      const [initial] = yield* updates;
      yield* service.submitEvent({
        agentId: created.agent.id,
        source: "user",
        sourceEventId: "cancel",
        kind: "external",
        text: "Work forever.",
      });
      const [queued] = yield* updates;
      expect(queued.agents[0]?.pendingInboxCount).toBe(1);
      expect(queued.revision).toBeGreaterThan(initial.revision);
      const fiber = yield* Effect.forkChild(service.drain());
      yield* Deferred.await(entered);
      const next = yield* service.submitEvent({
        agentId: created.agent.id,
        source: "user",
        sourceEventId: "paused",
        kind: "user_reply",
        text: "This waits.",
      });
      const [started] = yield* updates;
      expect(started.agents[0]).toMatchObject({ phase: "running", pendingInboxCount: 0 });
      expect(started.revision).toBeGreaterThan(queued.revision);
      const [nextQueued] = yield* updates;
      expect(nextQueued.agents[0]).toMatchObject({ phase: "running", pendingInboxCount: 1 });
      expect(nextQueued.revision).toBeGreaterThan(started.revision);
      const running = yield* service.get({ agentId: created.agent.id });
      const runId = running.runs[0]?.id;
      assert(runId !== undefined);
      const paused = yield* service.setEnabled({ agentId: created.agent.id, enabled: false });
      expect(paused).toMatchObject({ enabled: false, phase: "running" });
      yield* service.cancel({ agentId: created.agent.id, runId });
      yield* Fiber.join(fiber);
      const cancelled = yield* service.get({ agentId: created.agent.id });
      expect(cancelled.runs[0]?.status).toBe("cancelled");
      expect(cancelled.runs[0]?.executedActions).toEqual([]);

      expect(cancelled.agent.phase).toBe("paused");
      expect(cancelled.inbox.find((event) => event.id === next.eventId)).toMatchObject({
        status: "pending",
        text: "This waits.",
      });
    }).pipe(
      Effect.provide(
        AlwaysOnService.layerManual.pipe(Layer.provide(Layer.mergeAll(schedulerMock, model))),
      ),
    );
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("fires a timer into the inbox and lets the model answer it", () =>
  Effect.gen(function* () {
    const queue = yield* Ref.make<readonly unknown[]>([]);
    const prompts = yield* Ref.make<readonly string[]>([]);
    yield* Effect.gen(function* () {
      const service = yield* AlwaysOnService.AlwaysOnService;
      const created = yield* createAgent(service);
      const dueAt = DateTime.formatIso(DateTime.add(yield* DateTime.now, { seconds: 1 }));
      yield* Ref.set(queue, [
        {
          summary: "Armed.",
          actions: [{ type: "set_timer", key: "daily", timerKind: "deadline", dueAt }],
        },
        quiet,
      ]);
      yield* service.submitEvent({
        agentId: created.agent.id,
        source: "agent",
        sourceEventId: "arm",
        kind: "system",
        text: "Check back in a second.",
      });
      yield* service.drain();
      yield* TestClock.adjust(Duration.seconds(5));
      const fired = yield* service.get({ agentId: created.agent.id });
      expect(
        fired.inbox.some((event) => event.kind === "timer" && event.status === "consumed"),
      ).toBe(true);
      expect(fired.timers[0]?.status).toBe("fired");
      expect((yield* Ref.get(prompts)).some((prompt) => prompt.includes("Timer due."))).toBe(true);
    }).pipe(
      Effect.provide(
        AlwaysOnService.layerManual.pipe(
          Layer.provide(Layer.mergeAll(Scheduler.layer, scripted(queue, prompts))),
          Layer.provideMerge(SqlitePersistence.layerMemory),
        ),
      ),
    );
  }),
);

it.effect("does not show an agent created in another environment database", () =>
  Effect.gen(function* () {
    const queue = yield* Ref.make<readonly unknown[]>([]);
    const prompts = yield* Ref.make<readonly string[]>([]);
    const office = provide(queue, prompts).pipe(Layer.provideMerge(SqlitePersistence.layerMemory));
    const home = provide(queue, prompts).pipe(Layer.provideMerge(SqlitePersistence.layerMemory));
    const agentId = yield* Effect.gen(function* () {
      const service = yield* AlwaysOnService.AlwaysOnService;
      return (yield* createAgent(service, "Office")).agent.id;
    }).pipe(Effect.provide(office));

    const other = yield* Effect.gen(function* () {
      const service = yield* AlwaysOnService.AlwaysOnService;
      const missing = yield* service.get({ agentId }).pipe(Effect.exit);
      const created = yield* createAgent(service, "Home");
      return { missing, createdId: created.agent.id };
    }).pipe(Effect.provide(home));
    expect(other.missing._tag).toBe("Failure");
    expect(other.createdId).not.toBe(agentId);
  }),
);
