import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Duration from "effect/Duration";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
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
      { kind: "user_reply", relatedSessionId: "session-1" },
      { kind: "external", relatedSessionId: null },
    ]).map((event) => event.kind),
  ).toEqual(["user_reply"]);
});

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
      expect(detail.deliveries[0]?.body).toBe("Noted.");
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
      yield* service.submitEvent({
        agentId: created.agent.id,
        source: "user",
        sourceEventId: "cancel",
        kind: "external",
        text: "Work forever.",
      });
      const fiber = yield* Effect.forkChild(service.drain());
      yield* Deferred.await(entered);
      const running = yield* service.get({ agentId: created.agent.id });
      const runId = running.runs[0]?.id;
      assert(runId !== undefined);
      yield* service.cancel({ agentId: created.agent.id, runId });
      yield* Fiber.join(fiber);
      const cancelled = yield* service.get({ agentId: created.agent.id });
      expect(cancelled.runs[0]?.status).toBe("cancelled");
      expect(cancelled.runs[0]?.executedActions).toEqual([]);

      yield* service.setEnabled({ agentId: created.agent.id, enabled: false });
      yield* service.submitEvent({
        agentId: created.agent.id,
        source: "user",
        sourceEventId: "paused",
        kind: "external",
        text: "This waits.",
      });
      yield* service.drain();
      const paused = yield* service.get({ agentId: created.agent.id });
      expect(paused.agent.phase).toBe("paused");
      expect(paused.inbox.some((event) => event.status === "pending")).toBe(true);
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
