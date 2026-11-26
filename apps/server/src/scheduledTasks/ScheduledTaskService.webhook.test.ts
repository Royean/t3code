// @effect-diagnostics-next-line nodeBuiltinImport:off -- Effect's Crypto has no createHmac.
import * as NodeCrypto from "node:crypto";

import * as NodePlatformCrypto from "@effect/platform-node/NodeCrypto";
import { assert, it } from "@effect/vitest";
import {
  type ManagedPolicyDocument,
  ThreadId,
  OrchestrationV2ThreadProjection,
  ScheduledTaskUpsertInput,
  ScheduledTaskWebhookDeliveryId,
  SecretRequestError,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as EffectScheduler from "effect/Scheduler";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";
import * as Tracer from "effect/Tracer";

import { threadShellFromProjection } from "../orchestration-v2/ProjectionStore.ts";
import * as CommandReceiptStore from "../orchestration-v2/CommandReceiptStore.ts";
import * as ThreadLaunchService from "../orchestration-v2/ThreadLaunchService.ts";
import * as ManagedPolicy from "../policy/ManagedPolicy.ts";
import * as ThreadManagementService from "../orchestration-v2/ThreadManagementService.ts";
import * as SecretRequests from "../secrets/SecretRequests.ts";
import * as ManagedProjectFolders from "../project/ManagedProjectFolders.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as Scheduler from "../scheduling/Scheduler.ts";
import * as ScheduledTaskService from "./ScheduledTaskService.ts";
import { renderWebhookPrompt } from "./webhookTemplate.ts";

const decodeUpsertInput = Schema.decodeUnknownEffect(ScheduledTaskUpsertInput);
const decodeThreadProjection = Schema.decodeUnknownSync(OrchestrationV2ThreadProjection);

it.effect(
  "preserves omitted dedupe settings, releases duplicate claims, and retains coalesced claims",
  () =>
    withService(({ service, sql, launches }) =>
      Effect.gen(function* () {
        const dedupe = {
          replayKey: "{{headers.delivery-id}}",
          coalesceKey: "{{body.group}}",
          coalesceWindowSeconds: 10,
        };
        const { task } = yield* service.upsert(
          yield* webhookTaskInput({ schedule: { type: "webhook", dedupe } }),
        );
        const updated = yield* service.upsert(yield* webhookTaskInput());
        assert.deepEqual(
          updated.task.schedule.type === "webhook" && updated.task.schedule.dedupe,
          dedupe,
        );
        const request = (id: string, bodyText = "{}", relayDeliveryId?: string) =>
          requestFor(task, {
            headers: { "delivery-id": id },
            bodyText,
            body: new TextEncoder().encode(bodyText),
            ...(relayDeliveryId === undefined ? {} : { relayDeliveryId }),
          });
        yield* service.triggerWebhook(request("A"));
        yield* Queue.take(launches);
        yield* service.triggerWebhook(request("B1", '{"group":"B"}'));
        for (let i = 0; i < 5; i++) {
          const duplicate = yield* service.triggerWebhook(request("A", "{}", `duplicate-${i}`));
          assert.equal(duplicate._tag === "accepted" && duplicate.outcome, "duplicate");
          const coalesced = yield* service.triggerWebhook(
            request(`B${i + 2}`, '{"group":"B"}', `coalesced-${i}`),
          );
          assert.equal(coalesced._tag === "accepted" && coalesced.outcome, "coalesced");
        }
        const claims = yield* sql<{
          relay_delivery_id: string;
        }>`SELECT relay_delivery_id FROM scheduled_task_webhook_relay_deliveries ORDER BY relay_delivery_id`;
        assert.deepEqual(
          claims.map((claim) => claim.relay_delivery_id),
          Array.from({ length: 5 }, (_, i) => `coalesced-${i}`),
        );
        assert.isTrue(
          (yield* service.listWebhookDeliveries({ id: task.id })).deliveries.every(
            (delivery) => delivery.outcome !== "duplicate" && delivery.outcome !== "coalesced",
          ),
        );
        const fresh = yield* service.triggerWebhook(request("C", "{}", "duplicate-0"));
        assert.equal(fresh._tag === "accepted" && fresh.outcome, "accepted");
        yield* Queue.take(launches);
        const cleared = yield* service.upsert(
          yield* webhookTaskInput({ schedule: { type: "webhook", dedupe: {} } }),
        );
        assert.deepEqual(
          cleared.task.schedule.type === "webhook" && cleared.task.schedule.dedupe,
          {},
        );
      }),
    ),
);

it.effect("tracks filtered business deliveries independently of sampled logs", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(
        yield* webhookTaskInput({
          schedule: {
            type: "webhook",
            dedupe: { replayKey: "{{ body.deliveryId }}" },
            filters: [{ path: "body.kind", op: "equals", value: "wanted" }],
          },
        }),
      );
      const bodyText = JSON.stringify({ deliveryId: "business-filtered", kind: "other" });
      for (let i = 0; i < 12; i++) {
        yield* service.triggerWebhook(
          requestFor(task, { bodyText, body: new TextEncoder().encode(bodyText) }),
        );
      }
      const result = yield* service.getExecutions({
        id: task.id,
        businessDeliveryId: "business-filtered",
      });
      assert.equal(result.aliases.length, 12);
      assert.isTrue(
        result.aliases.every((alias) => alias.outcome === "filtered" && alias.executionId === null),
      );
      assert.deepEqual(result.effectiveExecutions, []);
      assert.isNotEmpty(result.trackingId);
    }),
  ),
);

it.effect("keeps every coalesced and replayed business alias independently of sampled logs", () =>
  withService(({ service, sql }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(
        yield* webhookTaskInput({
          threadMode: "continue",
          schedule: {
            type: "webhook",
            dedupe: {
              replayKey: "{{body.deliveryId}}",
              coalesceKey: "{{body.group}}",
              coalesceWindowSeconds: 10,
            },
          },
        }),
      );
      const trigger = (id: string, relayDeliveryId?: string) => {
        const bodyText = JSON.stringify({ deliveryId: id, group: "one" });
        return service.triggerWebhook(
          requestFor(task, {
            bodyText,
            body: new TextEncoder().encode(bodyText),
            ...(relayDeliveryId ? { relayDeliveryId } : {}),
          }),
        );
      };
      yield* sql`UPDATE scheduled_tasks SET thread_id = 'thread-1' WHERE task_id = ${task.id}`;
      const first = yield* trigger("one");
      const firstQuery = yield* service.getExecutions({ id: task.id, businessDeliveryId: "one" });
      assert.equal(firstQuery.effectiveExecutions.length, 1);
      assert.equal(firstQuery.effectiveExecutions[0]!.state, "pending");
      const executionId = firstQuery.effectiveExecutions[0]!.id;
      for (let i = 0; i < 25; i++) yield* trigger("one");
      const other = yield* trigger("two", "same-ack");
      yield* trigger("two", "same-ack");
      const repeated = yield* service.getExecutions({ id: task.id, businessDeliveryId: "one" });
      assert.equal(repeated.aliases.length, 26);
      assert.isTrue(repeated.aliases.every((alias) => alias.executionId === executionId));
      const coalesced = yield* service.getExecutions({ id: task.id, businessDeliveryId: "two" });
      assert.equal(coalesced.aliases.length, 2);
      assert.isTrue(coalesced.aliases.every((alias) => alias.executionId === executionId));
      assert.equal(coalesced.effectiveExecutions[0]!.id, executionId);
      const items = yield* sql<{
        business_delivery_id: string;
        event_json: string;
        source_execution_id: string;
      }>`SELECT * FROM workbench_work_items ORDER BY rowid`;
      assert.deepEqual(
        items.map((item) => item.business_delivery_id),
        ["one", "two"],
      );
      assert.deepEqual(
        items.map((item) => JSON.parse(item.event_json).bodyText),
        [
          JSON.stringify({ deliveryId: "one", group: "one" }),
          JSON.stringify({ deliveryId: "two", group: "one" }),
        ],
      );
      assert.isTrue(items.every((item) => item.source_execution_id === executionId));
      // A pre-migration alias has no provable first payload. A later duplicate cannot invent it.
      yield* sql`DELETE FROM workbench_work_items WHERE business_delivery_id = 'one'`;
      yield* trigger("one");
      assert.equal(
        (yield* sql`SELECT id FROM workbench_work_items WHERE business_delivery_id = 'one'`).length,
        0,
      );
      assert.isTrue(first._tag === "accepted" && other._tag === "accepted");
      if (first._tag === "accepted") {
        const native = yield* service.getExecutions({ id: task.id, deliveryId: first.deliveryId });
        assert.equal(native.aliases.length, 1);
      }
    }),
  ),
);

it.effect("captures the formal message and run before task completion and replays to it", () =>
  withService(
    ({ service }) =>
      Effect.gen(function* () {
        const { task } = yield* service.upsert(
          yield* webhookTaskInput({
            schedule: { type: "webhook", dedupe: { replayKey: "{{body.deliveryId}}" } },
          }),
        );
        const bodyText = '{"deliveryId":"formal"}';
        const request = requestFor(task, { bodyText, body: new TextEncoder().encode(bodyText) });
        yield* waitForSpan(
          service.triggerWebhook(request),
          "ScheduledTaskService.runWebhookDelivery",
        );
        yield* service.triggerWebhook(request);
        const result = yield* service.getExecutions({ id: task.id, businessDeliveryId: "formal" });
        assert.equal(result.aliases.length, 2);
        assert.equal(result.effectiveExecutions.length, 1);
        assert.equal(result.effectiveExecutions[0]!.state, "dispatched");
        assert.equal(result.effectiveExecutions[0]!.threadId, "thread-1");
        assert.equal(result.effectiveExecutions[0]!.acceptedRunId, "run-formal");
        assert.equal(result.effectiveExecutions[0]!.runId, "run-formal");
        assert.isNotNull(result.effectiveExecutions[0]!.messageId);
      }),
    { formalLaunch: { runId: "run-formal" } },
  ),
);

it.effect("updates the effective run after a queued message is promoted to steer", () => {
  const formalLaunch = { runId: "run-queued" };
  return withService(
    ({ service }) =>
      Effect.gen(function* () {
        const { task } = yield* service.upsert(yield* webhookTaskInput());
        const ack = yield* waitForSpan(
          service.triggerWebhook(requestFor(task)),
          "ScheduledTaskService.runWebhookDelivery",
        );
        assert.equal(ack._tag, "accepted");
        if (ack._tag !== "accepted") return;
        const first = yield* service.getExecutions({ id: task.id, deliveryId: ack.deliveryId });
        assert.equal(first.effectiveExecutions[0]!.acceptedRunId, "run-queued");
        formalLaunch.runId = "run-active";
        const promoted = yield* service.getExecutions({ id: task.id, deliveryId: ack.deliveryId });
        assert.equal(promoted.effectiveExecutions[0]!.acceptedRunId, "run-queued");
        assert.equal(promoted.effectiveExecutions[0]!.runId, "run-active");
      }),
    { formalLaunch },
  );
});

it.effect("recovers a committed command when launch loses its projection response", () =>
  withService(
    ({ service, launches }) =>
      Effect.gen(function* () {
        const { task } = yield* service.upsert(
          yield* webhookTaskInput({
            schedule: { type: "webhook", dedupe: { replayKey: "{{body.deliveryId}}" } },
          }),
        );
        const bodyText = '{"deliveryId":"committed"}';
        const request = requestFor(task, { bodyText, body: new TextEncoder().encode(bodyText) });
        yield* waitForSpan(
          service.triggerWebhook(request),
          "ScheduledTaskService.runWebhookDelivery",
        );
        const retry = yield* service.triggerWebhook(request);
        assert.equal(retry._tag === "accepted" && retry.outcome, "duplicate");
        const result = yield* service.getExecutions({
          id: task.id,
          businessDeliveryId: "committed",
        });
        assert.equal(result.effectiveExecutions[0]!.state, "dispatched");
        assert.equal(result.effectiveExecutions[0]!.runId, "run-committed");
        assert.equal(yield* Queue.size(launches), 1);
      }),
    { formalLaunch: { runId: "run-committed", failAfterCommit: true } },
  ),
);

it.effect("uses a matching descriptive business header across replay template changes", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(
        yield* webhookTaskInput({
          schedule: {
            type: "webhook",
            dedupe: { replayKey: "{{headers.other-id}}" },
            filters: [{ path: "body.kind", op: "equals", value: "wanted" }],
          },
        }),
      );
      const bodyText = '{"deliveryId":"stable-header","kind":"other"}';
      const trigger = (tag: string) =>
        service.triggerWebhook(
          requestFor(task, {
            bodyText,
            body: new TextEncoder().encode(bodyText),
            headers: { "x-t3-business-delivery-id": tag },
          }),
        );
      yield* trigger("stable-header");
      yield* trigger("wrong-header");
      yield* trigger("");
      const tagged = yield* service.getExecutions({
        id: task.id,
        businessDeliveryId: "stable-header",
      });
      assert.equal(tagged.aliases.length, 1);
      assert.deepEqual(tagged.effectiveExecutions, []);
      const wrong = yield* service.getExecutions({
        id: task.id,
        businessDeliveryId: "wrong-header",
      });
      assert.deepEqual(wrong.aliases, []);
    }),
  ),
);

it.effect(
  "retains uncertain acceptance through restart and resolves it without another dispatch",
  () => {
    const formalLaunch: {
      runId: string;
      failAfterCommit: boolean;
      unavailable: boolean;
      input?: LaunchInput;
    } = { runId: "run-recovered", failAfterCommit: true, unavailable: true };
    return withService(
      ({ service, sql }) =>
        Effect.gen(function* () {
          const { task } = yield* service.upsert(
            yield* webhookTaskInput({
              schedule: { type: "webhook", dedupe: { replayKey: "{{body.deliveryId}}" } },
            }),
          );
          const bodyText = '{"deliveryId":"restart"}';
          const request = requestFor(task, { bodyText, body: new TextEncoder().encode(bodyText) });
          yield* waitForSpan(
            service.triggerWebhook(request),
            "ScheduledTaskService.runWebhookDelivery",
          );
          const before = yield* service.getExecutions({
            id: task.id,
            businessDeliveryId: "restart",
          });
          assert.equal(before.effectiveExecutions[0]!.state, "unknown");
          const retry = yield* service.triggerWebhook(request);
          assert.equal(retry._tag === "accepted" && retry.outcome, "duplicate");
          formalLaunch.unavailable = false;
          yield* withService(
            ({ service: restarted, launches }) =>
              Effect.gen(function* () {
                const recovered = yield* restarted.getExecutions({
                  id: task.id,
                  businessDeliveryId: "restart",
                });
                assert.equal(recovered.trackingId, before.trackingId);
                assert.equal(
                  recovered.effectiveExecutions[0]!.id,
                  before.effectiveExecutions[0]!.id,
                );
                assert.equal(recovered.effectiveExecutions[0]!.state, "dispatched");
                assert.equal(recovered.effectiveExecutions[0]!.runId, "run-recovered");
                assert.equal(yield* Queue.size(launches), 0);
              }),
            { sql, formalLaunch },
          );
        }),
      { formalLaunch },
    );
  },
);

it.effect("does not reuse an execution after the seven day replay window", () =>
  withService(
    ({ service }) =>
      Effect.gen(function* () {
        const { task } = yield* service.upsert(
          yield* webhookTaskInput({
            schedule: { type: "webhook", dedupe: { replayKey: "{{body.deliveryId}}" } },
          }),
        );
        const bodyText = '{"deliveryId":"expires"}';
        const request = requestFor(task, { bodyText, body: new TextEncoder().encode(bodyText) });
        yield* waitForSpan(
          service.triggerWebhook(request),
          "ScheduledTaskService.runWebhookDelivery",
        );
        const first = yield* service.getExecutions({ id: task.id, businessDeliveryId: "expires" });
        yield* TestClock.adjust("7 days");
        yield* waitForSpan(
          service.triggerWebhook(request),
          "ScheduledTaskService.runWebhookDelivery",
        );
        const next = yield* service.getExecutions({ id: task.id, businessDeliveryId: "expires" });
        assert.equal(next.aliases.length, 2);
        assert.equal(next.effectiveExecutions.length, 2);
        assert.notEqual(next.aliases[0]!.executionId, next.aliases[1]!.executionId);
        assert.equal(next.trackingId, first.trackingId);
      }),
    { workers: new Map() },
  ),
);

it.effect("captures formal sends to a continuing scheduled thread", () =>
  withService(
    ({ service }) =>
      Effect.gen(function* () {
        const { task } = yield* service.upsert(yield* webhookTaskInput({ threadMode: "continue" }));
        yield* waitForSpan(
          service.triggerWebhook(requestFor(task)),
          "ScheduledTaskService.runWebhookDelivery",
        );
        const bound = (yield* service.list()).tasks[0]!;
        assert.equal(bound.threadId, "thread-1");
        const ack = yield* waitForSpan(
          service.triggerWebhook(requestFor(bound)),
          "ScheduledTaskService.runWebhookDelivery",
        );
        assert.equal(ack._tag, "accepted");
        if (ack._tag !== "accepted") return;
        const result = yield* service.getExecutions({ id: task.id, deliveryId: ack.deliveryId });
        assert.equal(result.effectiveExecutions[0]!.state, "dispatched");
        assert.equal(result.effectiveExecutions[0]!.runId, "run-bound");
        assert.equal(result.effectiveExecutions[0]!.threadId, "thread-1");
      }),
    { formalLaunch: { runId: "run-bound" } },
  ),
);

it.effect.each([
  ["{{body.deliveryId}}", '{"deliveryId":17}'],
  ["{{body.deliveryId}}", '{"deliveryId":"' + "x".repeat(201) + '"}'],
  ["{{body.deliveryId}}", "not-json"],
  ["prefix:{{body.deliveryId}}", '{"deliveryId":"untracked"}'],
] as const)(
  "keeps native ACK lookup when business ID cannot be extracted (%s, %s)",
  ([replayKey, bodyText]) =>
    withService(({ service }) =>
      Effect.gen(function* () {
        const { task } = yield* service.upsert(
          yield* webhookTaskInput({
            schedule: {
              type: "webhook",
              dedupe: { replayKey },
              filters: [{ path: "body.kind", op: "equals", value: "wanted" }],
            },
          }),
        );
        const result = yield* service.triggerWebhook(
          requestFor(task, { bodyText, body: new TextEncoder().encode(bodyText) }),
        );
        assert.equal(result._tag, "accepted");
        if (result._tag !== "accepted") return;
        const tracked = yield* service.getExecutions({
          id: task.id,
          deliveryId: result.deliveryId,
        });
        assert.equal(tracked.aliases.length, 1);
        assert.equal(tracked.aliases[0]!.businessDeliveryId, null);
      }),
    ),
);

it.effect("does not attach a deleted task's executions to a replacement with the same id", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput());
      const ack = yield* waitForSpan(
        service.triggerWebhook(requestFor(task)),
        "ScheduledTaskService.runWebhookDelivery",
      );
      assert.equal(ack._tag, "accepted");
      if (ack._tag !== "accepted") return;
      const before = yield* service.getExecutions({ id: task.id, deliveryId: ack.deliveryId });
      yield* service.delete({ id: task.id });
      yield* TestClock.adjust(1);
      yield* service.upsert(yield* webhookTaskInput());
      const replacement = yield* service.getExecutions({ id: task.id, deliveryId: ack.deliveryId });
      assert.notEqual(replacement.taskCreatedAt, before.taskCreatedAt);
      assert.equal(replacement.trackingId, before.trackingId);
      assert.deepEqual(replacement.aliases, []);
      assert.deepEqual(replacement.effectiveExecutions, []);
    }),
  ),
);

const waitForSpan = <A, E, R>(work: Effect.Effect<A, E, R>, name: string) =>
  Effect.gen(function* () {
    const done = yield* Deferred.make<void>();
    const tracer = Tracer.make({
      span: (options) => {
        const span = new Tracer.NativeSpan(options);
        const end = span.end.bind(span);
        span.end = (...args) => {
          end(...args);
          if (span.name === name) Deferred.doneUnsafe(done, Effect.void);
        };
        return span;
      },
    });
    const result = yield* work.pipe(Effect.withTracer(tracer));
    yield* Deferred.await(done);
    return result;
  });

const drainPendingDispatch = (workers: Map<string, Effect.Effect<void>>) =>
  waitForSpan(workers.get("webhook-coalescing")!, "ScheduledTaskService.dispatchPendingWebhook");

type LaunchInput = ThreadLaunchService.ThreadLaunchInput;

const webhookTaskInput = (overrides: Record<string, unknown> = {}) =>
  decodeUpsertInput({
    id: "scheduled-task:hook",
    title: "Review PRs",
    prompt: "Review this PR: {{body.pull_request.url}}",
    enabled: true,
    schedule: { type: "webhook" },
    projectId: "project-webhook",
    workspaceStrategy: { type: "root" },
    modelSelection: { instanceId: "codex", model: "gpt-5.4" },
    runtimeMode: "full-access",
    interactionMode: "default",
    ...overrides,
  });

const pullRequestBody = new TextEncoder().encode(
  JSON.stringify({ pull_request: { url: "https://github.com/org/repo/pull/45" } }),
);

const requestFor = (
  task: { readonly id: string; readonly webhook?: { readonly path: string } | undefined },
  overrides: Partial<ScheduledTaskService.WebhookTriggerRequest> = {},
): ScheduledTaskService.WebhookTriggerRequest => ({
  hookId: task.id,
  token: task.webhook?.path.split("/").at(-1) ?? "",
  method: "POST",
  path: `/api/hooks/${task.id}`,
  query: "",
  headers: { "content-type": "application/json" },
  body: pullRequestBody,
  bodyText: new TextDecoder().decode(pullRequestBody),
  ...overrides,
});

const withService = <A, E>(
  body: (input: {
    readonly service: ScheduledTaskService.ScheduledTaskService["Service"];
    readonly launches: Queue.Queue<LaunchInput>;
    readonly sql: SqlClient.SqlClient;
    readonly secretsByRef: Map<string, string>;
  }) => Effect.Effect<A, E, never>,
  options: {
    readonly onFrame?: () => void;
    readonly frameGate?: {
      readonly started: Deferred.Deferred<void>;
      readonly release: Deferred.Deferred<void>;
    };
    readonly completedLaunches?: Set<string>;
    readonly formalLaunch?: {
      runId: string;
      failAfterCommit?: boolean;
      unavailable?: boolean;
      input?: LaunchInput;
    };
    readonly failLaunches?: { remaining: number };
    readonly gate?: Deferred.Deferred<void>;
    readonly gateForTask?: (input: LaunchInput) => Deferred.Deferred<void> | undefined;
    readonly relayHookBaseUrl?: string;
    readonly sql?: SqlClient.SqlClient;
    readonly workers?: Map<string, Effect.Effect<void>>;
    readonly policy?: ManagedPolicyDocument;
  } = {},
) =>
  Effect.gen(function* () {
    const receipts = yield* CommandReceiptStore.CommandReceiptStoreV2;
    const launches = yield* Queue.unbounded<LaunchInput>();
    let lastLaunch: LaunchInput | undefined = options.formalLaunch?.input;
    const formalProjection = () => {
      const input = lastLaunch!;
      const at = DateTime.makeUnsafe("2026-11-26T00:00:00.000Z");
      return decodeThreadProjection({
        thread: {
          id: "thread-1",
          projectId: input.projectId,
          title: input.title,
          providerInstanceId: input.modelSelection.instanceId,
          modelSelection: input.modelSelection,
          runtimeMode: input.runtimeMode,
          interactionMode: input.interactionMode,
          branch: null,
          worktreePath: null,
          activeProviderThreadId: null,
          lineage: { parentThreadId: null, relationshipToParent: null, rootThreadId: "thread-1" },
          forkedFrom: null,
          createdAt: at,
          updatedAt: at,
          archivedAt: null,
          deletedAt: null,
          createdBy: input.createdBy,
          creationSource: input.creationSource,
        },
        messages: [
          {
            id: input.initialMessage!.messageId,
            threadId: "thread-1",
            role: "user",
            scheduledTaskId: input.initialMessage!.scheduledTaskId,
            runId: options.formalLaunch!.runId,
            nodeId: null,
            text: input.initialMessage!.text,
            attachments: [],
            streaming: false,
            createdAt: at,
            updatedAt: at,
            createdBy: input.createdBy,
            creationSource: input.creationSource,
          },
        ],
        runs: [
          {
            id: options.formalLaunch!.runId,
            threadId: "thread-1",
            userMessageId: input.initialMessage!.messageId,
            ordinal: 1,
            providerInstanceId: input.modelSelection.instanceId,
            modelSelection: input.modelSelection,
            providerThreadId: null,
            rootNodeId: null,
            activeAttemptId: null,
            status: "queued",
            requestedAt: at,
            startedAt: null,
            completedAt: null,
            checkpointId: null,
            contextHandoffId: null,
          },
        ],
        attempts: [],
        nodes: [],
        subagents: [],
        providerSessions: [],
        providerThreads: [],
        providerTurns: [],
        runtimeRequests: [],
        plans: [],
        turnItems: [],
        checkpointScopes: [],
        checkpoints: [],
        contextHandoffs: [],
        contextTransfers: [],
        visibleTurnItems: [],
        updatedAt: at,
      });
    };
    const secretsByRef = new Map<string, string>();
    const layerDependencies = Layer.mergeAll(
      NodePlatformCrypto.layerDigest,
      NodePlatformCrypto.layerMac,
      options.frameGate === undefined && options.onFrame === undefined
        ? NodePlatformCrypto.layer
        : Layer.effect(
            SecureRandom.SecureRandom,
            Effect.gen(function* () {
              const crypto = yield* SecureRandom.SecureRandom;
              return {
                ...crypto,
                randomBytes: (size: number) =>
                  Effect.suspend(() => {
                    if (size === 6) options.onFrame?.();
                    return size === 6 && options.frameGate !== undefined
                      ? Deferred.succeed(options.frameGate.started, undefined).pipe(
                          Effect.andThen(Deferred.await(options.frameGate.release)),
                          Effect.andThen(crypto.randomBytes(size)),
                        )
                      : crypto.randomBytes(size);
                  }),
              };
            }),
          ).pipe(Layer.provide(NodePlatformCrypto.layer)),
      options.workers === undefined
        ? Scheduler.layer
        : Layer.succeed(Scheduler.Scheduler, {
            register: <E, R>(name: string, work: Effect.Effect<void, E, R>) =>
              Effect.gen(function* () {
                const context = yield* Effect.context<R>();
                options.workers!.set(name, work.pipe(Effect.provideContext(context), Effect.orDie));
              }),
          }),
      Layer.mock(ThreadLaunchService.ThreadLaunchService)({
        launch: (input) =>
          Effect.suspend(() => {
            if (options.completedLaunches?.has(input.commandId))
              return Effect.succeed({ threadId: "thread-1", resumed: false } as never);
            options.completedLaunches?.add(input.commandId);
            lastLaunch = input;
            if (options.formalLaunch) options.formalLaunch.input = input;
            return Queue.offer(launches, input).pipe(
              Effect.andThen(
                (options.gateForTask?.(input) ?? options.gate)
                  ? Deferred.await((options.gateForTask?.(input) ?? options.gate)!)
                  : Effect.void,
              ),
              Effect.andThen(
                Effect.suspend(() =>
                  options.failLaunches && options.failLaunches.remaining-- > 0
                    ? Effect.fail(
                        new ThreadManagementService.ThreadManagementProviderInstanceDisabledError({
                          projectId: input.projectId,
                          instanceId: input.modelSelection.instanceId,
                        }),
                      )
                    : Effect.void,
                ),
              ),
              Effect.andThen(
                options.formalLaunch
                  ? Effect.gen(function* () {
                      yield* receipts
                        .insertIfAbsent({
                          commandId: input.commandId,
                          threadId: ThreadId.make("thread-1"),
                          commandType: "thread.create",
                          acceptedAt: yield* DateTime.now,
                          resultSequence: 1,
                          status: "accepted",
                          error: null,
                        })
                        .pipe(Effect.orDie);
                      if (options.formalLaunch!.failAfterCommit)
                        return yield* Effect.die(new Error("projection unavailable after commit"));
                    })
                  : Effect.void,
              ),
              Effect.as({
                threadId: "thread-1",
                resumed: false,
                ...(options.formalLaunch ? { projection: formalProjection() } : {}),
              } as never),
            );
          }),
      }),
      Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
        namedProjectsRoot: "/projects",
        isScratchProject: () => Effect.succeed(false),
      }),
      Layer.mock(ThreadManagementService.ThreadManagementService)(
        options.formalLaunch
          ? {
              sendToThread: (input: ThreadManagementService.ThreadManagementSendInput) =>
                Effect.gen(function* () {
                  lastLaunch = {
                    ...lastLaunch!,
                    commandId: input.commandId,
                    initialMessage: {
                      messageId: input.messageId,
                      scheduledTaskId: input.scheduledTaskId!,
                      text: input.text,
                      attachments: input.attachments,
                    },
                  };
                  options.formalLaunch!.input = lastLaunch;
                  yield* receipts
                    .insertIfAbsent({
                      commandId: input.commandId,
                      threadId: input.threadId,
                      commandType: "message.dispatch",
                      acceptedAt: yield* DateTime.now,
                      resultSequence: 2,
                      status: "accepted",
                      error: null,
                    })
                    .pipe(Effect.orDie);
                  const projection = formalProjection();
                  return {
                    projection,
                    message: projection.messages[0]!,
                    run: projection.runs[0]!,
                    turnItem: null,
                    delivery: "queued" as const,
                    dispatch: { sequence: 2, storedEvents: [] },
                  };
                }),
              getThreadShell: () =>
                options.formalLaunch!.unavailable
                  ? Effect.die("projection unavailable")
                  : Effect.succeed(threadShellFromProjection(formalProjection())),
              getProjectThreadRecords: () => Effect.succeed(formalProjection()),
              getThreadRecords: () => Effect.succeed(formalProjection()),
            }
          : {},
      ),
      Layer.mock(SecretRequests.SecretRequests)({
        consume: ({ ref }) => {
          const value = secretsByRef.get(ref);
          secretsByRef.delete(ref);
          return value === undefined
            ? Effect.fail(new SecretRequestError({ reason: "ref_unavailable" }))
            : Effect.succeed(value);
        },
      }),
      Layer.succeed(
        ScheduledTaskService.ScheduledTaskWebhookOrigin,
        Effect.succeed({ relayHookBaseUrl: options.relayHookBaseUrl ?? null }),
      ),
      ManagedPolicy.layerTest(options.policy),
    );
    return yield* Effect.gen(function* () {
      const service = yield* ScheduledTaskService.ScheduledTaskService;
      const sql = yield* SqlClient.SqlClient;
      return yield* body({ service, launches, sql, secretsByRef });
    }).pipe(
      Effect.provide(
        Layer.fresh(ScheduledTaskService.layer.pipe(Layer.provide(layerDependencies))),
      ),
    );
  }).pipe(
    Effect.provide(
      CommandReceiptStore.layer.pipe(
        Layer.provideMerge(
          options.sql === undefined
            ? SqlitePersistence.layerMemory
            : Layer.succeed(SqlClient.SqlClient, options.sql),
        ),
      ),
    ),
  );

it.effect("dispatches exactly the rendered prompt and logs the delivery", () =>
  withService(({ service, launches }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput());
      assert.equal(task.nextRunAt, null);
      assert.isDefined(task.webhook);
      assert.isTrue(task.webhook!.path.startsWith("/api/hooks/scheduled-task%3Ahook/"));
      assert.equal(task.webhook!.url, null);

      const result = yield* service.triggerWebhook(requestFor(task));
      assert.equal(result._tag, "accepted");
      const launched = yield* Queue.take(launches);
      const framed =
        /^Review this PR: <(webhook-data-[0-9a-f]{12})>https:\/\/github\.com\/org\/repo\/pull\/45<\/\1>\n\nText between <\1> and <\/\1> was copied from the webhook request\./;
      const frame = framed.exec(launched.initialMessage?.text ?? "")?.[1];
      assert.isDefined(frame);
      assert.isTrue(
        launched.commandId.startsWith(
          `scheduled-task:${task.id}:webhook:${result._tag === "accepted" ? result.deliveryId : ""}:execution:`,
        ),
      );

      const { deliveries } = yield* service.listWebhookDeliveries({
        id: task.id,
        acceptAnyOutcome: true,
      });
      assert.equal(deliveries.length, 1);
      assert.equal(deliveries[0]?.outcome, "accepted");
      const { delivery } = yield* service.getWebhookDelivery({
        id: task.id,
        deliveryId: deliveries[0]!.id,
      });
      assert.equal(delivery.body, new TextDecoder().decode(pullRequestBody));
      assert.equal(delivery.renderedPrompt, launched.initialMessage?.text);

      yield* service.triggerWebhook(requestFor(task));
      const next = framed.exec((yield* Queue.take(launches)).initialMessage?.text ?? "")?.[1];
      assert.isDefined(next);
      assert.notEqual(next, frame);
    }),
  ),
);

it("builds the relay hook URL from the managed tunnel's key, never the environment id", () => {
  const relayUrl = "https://relay.example.com/";
  assert.equal(
    ScheduledTaskService.relayHookBaseUrl({
      relayUrl,
      tunnelName: "t3coderelay-managedendpoint-dev-julius-0123456789abcdef",
    }),
    "https://relay.example.com/v1/hooks/0123456789abcdef",
  );
  for (const tunnelName of [undefined, "t3coderelay-managedendpoint", "x-0123456789ABCDEF"]) {
    assert.isNull(ScheduledTaskService.relayHookBaseUrl({ relayUrl, tunnelName }));
  }
});

it.effect("gives webhook tasks a relay URL when the environment has a managed tunnel", () =>
  withService(
    ({ service }) =>
      Effect.gen(function* () {
        const { task } = yield* service.upsert(yield* webhookTaskInput());
        const token = task.webhook!.path.split("/").at(-1);
        assert.equal(
          task.webhook?.url,
          `https://relay.example.com/v1/hooks/0123456789abcdef/scheduled-task%3Ahook/${token}`,
        );
      }),
    { relayHookBaseUrl: "https://relay.example.com/v1/hooks/0123456789abcdef" },
  ),
);

it.effect("answers not found for a wrong token or unknown hook without logging", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput());
      const wrongToken = yield* service.triggerWebhook(requestFor(task, { token: "nope" }));
      assert.equal(wrongToken._tag, "not_found");
      const unknown = yield* service.triggerWebhook(requestFor(task, { hookId: "missing" }));
      assert.equal(unknown._tag, "not_found");
      assert.equal(
        (yield* service.listWebhookDeliveries({ id: task.id, acceptAnyOutcome: true })).deliveries
          .length,
        0,
      );
    }),
  ),
);

it.effect("rotating the token retires the old URL and saving keeps it", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      const input = yield* webhookTaskInput();
      const { task } = yield* service.upsert(input);
      const saved = yield* service.upsert(yield* webhookTaskInput({ title: "Renamed" }));
      assert.equal(saved.task.webhook?.path, task.webhook?.path);

      const rotated = yield* service.rotateWebhookToken({ id: task.id });
      assert.notEqual(rotated.task.webhook?.path, task.webhook?.path);
      assert.equal((yield* service.triggerWebhook(requestFor(task)))._tag, "not_found");
      assert.equal((yield* service.triggerWebhook(requestFor(rotated.task)))._tag, "accepted");
    }),
  ),
);

it.effect("checks the configured signature and keeps the secret write-only", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(
        yield* webhookTaskInput({
          schedule: {
            type: "webhook",
            signature: {
              header: "X-Hub-Signature-256",
              encoding: "hex",
              prefix: "sha256=",
              secret: "s3cret",
            },
          },
        }),
      );
      assert.deepEqual(task.schedule, {
        type: "webhook",
        signature: { header: "x-hub-signature-256", encoding: "hex", prefix: "sha256=" },
        maxDeliveryAgeMinutes: null,
      });
      assert.isTrue(task.webhook?.hasSecret);

      const unsigned = yield* service.triggerWebhook(requestFor(task));
      assert.equal(unsigned._tag, "rejected_signature");

      const signature = `sha256=${NodeCrypto.createHmac("sha256", "s3cret").update(pullRequestBody).digest("hex")}`;
      const signed = yield* service.triggerWebhook(
        requestFor(task, {
          headers: { "content-type": "application/json", "x-hub-signature-256": signature },
        }),
      );
      assert.equal(signed._tag, "accepted");

      const resaved = yield* service.upsert(
        yield* webhookTaskInput({
          schedule: {
            type: "webhook",
            signature: { header: "x-hub-signature-256", encoding: "hex", prefix: "sha256=" },
          },
        }),
      );
      assert.isTrue(resaved.task.webhook?.hasSecret);

      const outcomes = (yield* service.listWebhookDeliveries({
        id: task.id,
        acceptAnyOutcome: true,
      })).deliveries.map((delivery) => [delivery.outcome, delivery.signatureVerified]);
      assert.deepEqual(outcomes.toSorted(), [
        ["accepted", true],
        ["rejected_signature", false],
      ]);
    }),
  ),
);

it.effect("a policy without webhook tasks keeps existing ones but refuses saves and requests", () =>
  withService(({ service, sql }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput());
      const timer = yield* webhookTaskInput({
        id: "scheduled-task:timer",
        schedule: { type: "interval", everyMs: 3_600_000 },
      });
      yield* withService(
        ({ service, launches }) =>
          Effect.gen(function* () {
            const blocked = "Example Corp's policy does not allow webhook tasks.";
            const listed = (yield* service.list()).tasks.find(({ id }) => id === task.id);
            assert.equal(listed?.blockedByPolicy, blocked);
            assert.isTrue(listed?.enabled);

            const refused = yield* service.upsert(yield* webhookTaskInput()).pipe(Effect.flip);
            assert.equal(refused.message, blocked);
            const created = yield* service
              .upsert(yield* webhookTaskInput({ id: "scheduled-task:new-hook" }))
              .pipe(Effect.flip);
            assert.equal(created.message, blocked);

            // Turned off, it can still be edited, but not turned back on.
            yield* service.setEnabled({ id: task.id, enabled: false });
            yield* service.upsert(yield* webhookTaskInput({ enabled: false }));
            const reenabled = yield* service
              .setEnabled({ id: task.id, enabled: true })
              .pipe(Effect.flip);
            assert.equal(reenabled.message, blocked);

            const result = yield* service.triggerWebhook(requestFor(task));
            assert.equal(result._tag, "blocked_by_policy");
            assert.equal(yield* Queue.size(launches), 0);
            assert.equal((yield* sql`SELECT * FROM scheduled_task_webhook_deliveries`).length, 0);

            // Timer tasks are unaffected, including a webhook task switched to a timer.
            assert.isUndefined((yield* service.upsert(timer)).task.blockedByPolicy);
            const switched = yield* service.upsert(
              yield* webhookTaskInput({ schedule: { type: "interval", everyMs: 3_600_000 } }),
            );
            assert.isUndefined(switched.task.blockedByPolicy);
          }),
        {
          sql,
          policy: {
            schemaVersion: 1,
            organization: "Example Corp",
            limits: { webhookTasks: false },
          },
        },
      );
    }),
  ),
);

it.effect("logs but does not run deliveries to a disabled task, and refuses run now", () =>
  withService(({ service, launches }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput({ enabled: false }));
      assert.equal((yield* service.triggerWebhook(requestFor(task)))._tag, "disabled");
      assert.equal(yield* Queue.size(launches), 0);
      const runNow = yield* service.runNow({ id: task.id }).pipe(Effect.flip);
      assert.equal(runNow.message, "Webhook tasks run when their URL receives a request.");
    }),
  ),
);

it.effect("a save carrying a stale token cannot undo a rotation", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput());
      const rotated = yield* service.rotateWebhookToken({ id: task.id });
      const saved = yield* service.upsert(yield* webhookTaskInput({ title: "Edited" }));
      assert.equal(saved.task.webhook?.path, rotated.task.webhook?.path);
      assert.equal((yield* service.triggerWebhook(requestFor(task)))._tag, "not_found");
    }),
  ),
);

it.effect("keeps credential headers and query values out of the delivery log", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput({ enabled: false }));
      yield* service.triggerWebhook(
        requestFor(task, {
          query: "page=2&api_key=k",
          headers: {
            "content-type": "application/json",
            authorization: "Bearer sender-token",
            "x-webhook-key": "k",
            "x-github-event": "push",
          },
        }),
      );
      const [summary] = (yield* service.listWebhookDeliveries({
        id: task.id,
        acceptAnyOutcome: true,
      })).deliveries;
      const { delivery } = yield* service.getWebhookDelivery({
        id: task.id,
        deliveryId: summary!.id,
      });
      assert.equal(delivery.query, "page=2&api_key=[redacted]");
      assert.equal(delivery.headers.authorization, "[redacted]");
      assert.equal(delivery.headers["x-webhook-key"], "[redacted]");
      assert.equal(delivery.headers["x-github-event"], "push");
    }),
  ),
);

it.effect("drops a character split by the delivery log's body limit", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput({ enabled: false }));
      const limit = 64 * 1024;
      const euro = new TextEncoder().encode("€");
      const body = new Uint8Array(limit - 1 + euro.length).fill(0x61);
      body.set(euro, limit - 1);
      yield* service.triggerWebhook(
        requestFor(task, { body, bodyText: new TextDecoder().decode(body) }),
      );
      const [summary] = (yield* service.listWebhookDeliveries({
        id: task.id,
        acceptAnyOutcome: true,
      })).deliveries;
      const { delivery } = yield* service.getWebhookDelivery({
        id: task.id,
        deliveryId: summary!.id,
      });
      assert.isTrue(delivery.bodyTruncated);
      assert.equal(delivery.bodyBytes, body.byteLength);
      assert.equal(delivery.body.slice(-3), "aaa");
      assert.equal(delivery.body.length, limit - 1);
    }),
  ),
);

const queuedDeliveryCases = [
  {
    change: "paused",
    reason: "The task was paused before this delivery ran.",
    apply: (service: ScheduledTaskService.ScheduledTaskService["Service"], id: string) =>
      service.setEnabled({ id: id as never, enabled: false }),
  },
  {
    change: "switched to an interval trigger",
    reason: "The task's trigger changed before this delivery ran.",
    apply: (service: ScheduledTaskService.ScheduledTaskService["Service"]) =>
      webhookTaskInput({ schedule: { type: "interval", everyMs: 3_600_000 } }).pipe(
        Effect.flatMap(service.upsert),
      ),
  },
] as const;

it.effect.each(queuedDeliveryCases)(
  "a delivery queued behind a run does not start once the task is $change",
  ({ reason, apply }) =>
    Effect.gen(function* () {
      const gate = yield* Deferred.make<void>();
      const deliveryReleased = yield* Deferred.make<void>();
      const tracer = Tracer.make({
        span: (options) => {
          const span = new Tracer.NativeSpan(options);
          const end = span.end.bind(span);
          span.end = (...args) => {
            end(...args);
            if (span.name === "ScheduledTaskService.releaseFailedDelivery") {
              Deferred.doneUnsafe(deliveryReleased, Effect.void);
            }
          };
          return span;
        },
      });
      yield* withService(
        ({ service, launches }) =>
          Effect.gen(function* () {
            const { task } = yield* service.upsert(yield* webhookTaskInput());
            yield* service.triggerWebhook(requestFor(task));
            const queued = yield* service.triggerWebhook(requestFor(task));
            yield* Queue.take(launches);
            yield* apply(service, task.id);
            yield* Deferred.succeed(gate, undefined);
            const deliveryId = queued._tag === "accepted" ? queued.deliveryId : undefined;
            yield* Deferred.await(deliveryReleased);
            const delivery = (yield* service.getWebhookDelivery({
              id: task.id,
              deliveryId: deliveryId!,
            })).delivery;
            assert.equal(delivery.outcome, "dispatch_failed");
            assert.equal(delivery.error, reason);
            assert.equal(yield* Queue.size(launches), 0);
          }),
        { gate },
      ).pipe(Effect.withTracer(tracer));
    }),
);

it.effect("a save without a secret keeps a secret changed after it was read", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      const signature = { header: "x-hub-signature-256", encoding: "hex", prefix: "sha256=" };
      const { task } = yield* service.upsert(
        yield* webhookTaskInput({
          schedule: { type: "webhook", signature: { ...signature, secret: "old" } },
        }),
      );
      yield* service.upsert(
        yield* webhookTaskInput({
          schedule: { type: "webhook", signature: { ...signature, secret: "new" } },
        }),
      );
      yield* service.upsert(
        yield* webhookTaskInput({ title: "Edited", schedule: { type: "webhook", signature } }),
      );
      const sign = (secret: string) =>
        `sha256=${NodeCrypto.createHmac("sha256", secret).update(pullRequestBody).digest("hex")}`;
      const withSignature = (secret: string) =>
        requestFor(task, {
          headers: { "content-type": "application/json", "x-hub-signature-256": sign(secret) },
        });
      assert.equal(
        (yield* service.triggerWebhook(withSignature("old")))._tag,
        "rejected_signature",
      );
      assert.equal((yield* service.triggerWebhook(withSignature("new")))._tag, "accepted");
    }),
  ),
);

it.effect("a held request already delivered directly runs only once", () =>
  withService(({ service, launches }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput());
      const direct = yield* service.triggerWebhook(
        requestFor(task, { relayDeliveryId: "relay-1" }),
      );
      const replayed = yield* service.triggerWebhook(
        requestFor(task, { relayDeliveryId: "relay-1", receivedAt: "2026-10-04T10:00:00.000Z" }),
      );
      assert.equal(direct._tag, "accepted");
      assert.deepEqual(replayed, { ...direct, outcome: "duplicate" } as typeof replayed);
      yield* Queue.take(launches);
      const logged = (yield* service.listWebhookDeliveries({ id: task.id, acceptAnyOutcome: true }))
        .deliveries;
      assert.equal(logged.length, 1);
    }),
  ),
);

it.effect("relay retries recover failed acceptance without releasing another request's claim", () =>
  withService(({ service, launches, sql }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput());
      const request = requestFor(task, { relayDeliveryId: "retry-after-log-failure" });
      yield* sql`
        CREATE TRIGGER fail_webhook_delivery_log
        BEFORE INSERT ON scheduled_task_webhook_deliveries
        BEGIN
          SELECT RAISE(FAIL, 'injected delivery log failure');
        END
      `;
      const error = yield* service.triggerWebhook(request).pipe(Effect.flip);
      assert.equal(error.message, "Could not record webhook delivery.");
      assert.equal(yield* Queue.size(launches), 0);
      assert.isEmpty(
        (yield* service.listWebhookDeliveries({ id: task.id, acceptAnyOutcome: true })).deliveries,
      );
      yield* sql`DROP TRIGGER fail_webhook_delivery_log`;

      const retried = yield* Effect.all(
        [service.triggerWebhook(request), service.triggerWebhook(request)],
        { concurrency: "unbounded" },
      );
      assert.deepEqual(
        retried.map((result) => (result._tag === "accepted" ? result.outcome : result._tag)).sort(),
        ["accepted", "duplicate"],
      );
      assert.include((yield* Queue.take(launches)).commandId, request.relayDeliveryId!);

      yield* sql`
        CREATE TRIGGER fail_webhook_claim
        BEFORE INSERT ON scheduled_task_webhook_relay_deliveries
        BEGIN
          SELECT RAISE(FAIL, 'injected claim failure');
        END
      `;
      const retryError = yield* service.triggerWebhook(request).pipe(Effect.flip);
      assert.equal(retryError.message, "Could not record webhook delivery.");
      yield* sql`DROP TRIGGER fail_webhook_claim`;
      const replayed = yield* service.triggerWebhook(request);
      assert.equal(replayed._tag === "accepted" ? replayed.outcome : replayed._tag, "duplicate");
      assert.equal(yield* Queue.size(launches), 0);
      assert.equal(
        (yield* service.listWebhookDeliveries({ id: task.id, acceptAnyOutcome: true })).deliveries
          .length,
        1,
      );
    }),
  ),
);

it.effect("a held request whose sender hung up mid-request still runs", () =>
  withService(({ service, launches }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput());
      for (let step = 0; step < 60; step++) {
        const request = requestFor(task, { relayDeliveryId: `hung-up-${step}` });
        const fiber = yield* service
          .triggerWebhook(request)
          .pipe(Effect.provideService(EffectScheduler.MaxOpsBeforeYield, 8), Effect.forkChild);
        for (let yields = 0; yields < step; yields++) yield* Effect.yieldNow;
        yield* Fiber.interrupt(fiber);
        const retried = yield* service.triggerWebhook(request);
        assert.equal(retried._tag, "accepted");
        const deliveryId = retried._tag === "accepted" ? retried.deliveryId : undefined;
        yield* service.getWebhookDelivery({ id: task.id, deliveryId: deliveryId! });
        const launch = yield* Queue.take(launches);
        assert.include(launch.commandId, `hung-up-${step}`);
        yield* TestClock.adjust("61 seconds");
      }
      assert.equal(yield* Queue.size(launches), 0);
    }),
  ),
);

it.effect("a held request runs once even after the log has trimmed it", () =>
  withService(({ service, launches }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput());
      const first = yield* service.triggerWebhook(requestFor(task, { relayDeliveryId: "kept" }));
      assert.equal(first._tag, "accepted");
      yield* Queue.take(launches);
      const paused = yield* service.upsert(yield* webhookTaskInput({ enabled: false }));
      yield* Effect.forEach(Array.from({ length: 55 }), () =>
        service.triggerWebhook(requestFor(paused.task)),
      );
      yield* service.upsert(yield* webhookTaskInput());
      const replay = yield* service.triggerWebhook(requestFor(task, { relayDeliveryId: "kept" }));
      assert.equal(replay._tag, "accepted");
      assert.equal(yield* Queue.size(launches), 0);
      if (first._tag !== "accepted") return;
      const retained = yield* service.getExecutions({ id: task.id, deliveryId: first.deliveryId });
      assert.equal(retained.aliases.length, 2);
      assert.equal(retained.effectiveExecutions.length, 1);
      assert.equal(retained.effectiveExecutions[0]!.ownerDeliveryId, first.deliveryId);
    }),
  ),
);

it.effect("skips a held request older than the task's max age", () =>
  withService(({ service, launches }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(
        yield* webhookTaskInput({ schedule: { type: "webhook", maxDeliveryAgeMinutes: 30 } }),
      );
      const now = yield* DateTime.now;
      const old = DateTime.formatIso(DateTime.subtract(now, { minutes: 31 }));
      const fresh = DateTime.formatIso(DateTime.subtract(now, { minutes: 5 }));
      const tooOld = yield* service.triggerWebhook(
        requestFor(task, { relayDeliveryId: "old", receivedAt: old }),
      );
      assert.equal(tooOld._tag, "expired");
      assert.equal(yield* Queue.size(launches), 0);
      const ok = yield* service.triggerWebhook(
        requestFor(task, { relayDeliveryId: "fresh", receivedAt: fresh }),
      );
      assert.equal(ok._tag, "accepted");
      const outcomes = (yield* service.listWebhookDeliveries({
        id: task.id,
        acceptAnyOutcome: true,
      })).deliveries.map((delivery) => delivery.outcome);
      assert.includeMembers(outcomes, ["expired", "accepted"]);
    }),
  ),
);

const githubSignature = (secret: string) =>
  `sha256=${NodeCrypto.createHmac("sha256", secret).update(pullRequestBody).digest("hex")}`;

it.effect("a signature can take the user's secret by ref, which works only once", () =>
  withService(({ service, launches, secretsByRef }) =>
    Effect.gen(function* () {
      secretsByRef.set("secret-ref:00000000000000000000000000000001", "github-secret");
      const githubSchedule = (secretRef: string) => ({
        type: "webhook",
        signature: { header: "x-hub-signature-256", encoding: "hex", prefix: "sha256=", secretRef },
      });
      const { task } = yield* service.upsert(
        yield* webhookTaskInput({
          schedule: githubSchedule("secret-ref:00000000000000000000000000000001"),
        }),
      );
      assert.isTrue(task.webhook!.hasSecret);
      const signed = yield* service.triggerWebhook(
        requestFor(task, {
          headers: {
            "content-type": "application/json",
            "x-hub-signature-256": githubSignature("github-secret"),
          },
        }),
      );
      assert.equal(signed._tag, "accepted");
      yield* Queue.take(launches);

      const reused = yield* service
        .upsert(
          yield* webhookTaskInput({
            id: "scheduled-task:other",
            schedule: githubSchedule("secret-ref:00000000000000000000000000000001"),
          }),
        )
        .pipe(Effect.flip);
      assert.include(reused.message, "already used");
    }),
  ),
);

it.effect("a retried save with an already used secretRef keeps the stored secret", () =>
  withService(({ service, launches, secretsByRef }) =>
    Effect.gen(function* () {
      secretsByRef.set("secret-ref:00000000000000000000000000000002", "github-secret");
      const save = webhookTaskInput({
        id: undefined,
        commandId: "command:mcp:schedule-task:release-hook",
        schedule: {
          type: "webhook",
          signature: {
            header: "x-hub-signature-256",
            encoding: "hex",
            prefix: "sha256=",
            secretRef: "secret-ref:00000000000000000000000000000002",
          },
        },
      });
      const first = yield* service.upsert(yield* save);
      const retried = yield* service.upsert(
        yield* webhookTaskInput({
          id: undefined,
          commandId: "command:mcp:schedule-task:release-hook",
          schedule: {
            type: "webhook",
            signature: {
              header: "x-hub-signature-256",
              encoding: "hex",
              prefix: "sha256=",
              secretRef: "secret-ref:00000000000000000000000000000002",
              secret: "made-up-secret",
            },
          },
        }),
      );
      assert.equal(retried.task.id, first.task.id);
      const signed = yield* service.triggerWebhook(
        requestFor(retried.task, {
          headers: {
            "content-type": "application/json",
            "x-hub-signature-256": githubSignature("github-secret"),
          },
        }),
      );
      assert.equal(signed._tag, "accepted");
      yield* Queue.take(launches);
    }),
  ),
);

it.effect("a signature without any secret is refused", () =>
  withService(({ service }) =>
    Effect.gen(function* () {
      const failure = yield* service
        .upsert(
          yield* webhookTaskInput({
            schedule: {
              type: "webhook",
              signature: { header: "x-hub-signature-256", encoding: "hex", prefix: "sha256=" },
            },
          }),
        )
        .pipe(Effect.flip);
      assert.include(failure.message, "needs a signing secret");
    }),
  ),
);

it.effect("dedupes replay keys per task, persists them, and expires them after seven days", () =>
  withService(
    ({ service, launches, sql }) =>
      Effect.gen(function* () {
        const schedule = { type: "webhook", dedupe: { replayKey: "{{headers.delivery-id}}" } };
        const { task } = yield* service.upsert(yield* webhookTaskInput({ schedule }));
        const request = requestFor(task, { headers: { "delivery-id": "A" } });
        const first = yield* service.triggerWebhook(request);
        assert.equal(first._tag === "accepted" && first.outcome, "accepted");
        yield* Queue.take(launches);
        const replay = yield* service.triggerWebhook(request);
        assert.equal(replay._tag === "accepted" && replay.outcome, "duplicate");
        assert.equal(yield* Queue.size(launches), 0);
        const other = yield* service.upsert(
          yield* webhookTaskInput({ id: "scheduled-task:other", schedule }),
        );
        yield* service.triggerWebhook(requestFor(other.task, { headers: request.headers }));
        yield* Queue.take(launches);
        yield* withService(
          ({ service: restarted }) =>
            Effect.gen(function* () {
              const replay = yield* restarted.triggerWebhook(request);
              assert.equal(replay._tag === "accepted" && replay.outcome, "duplicate");
            }),
          { sql, workers: new Map() },
        );
        yield* TestClock.adjust("7 days");
        const expired = yield* service.triggerWebhook(request);
        assert.equal(expired._tag === "accepted" && expired.outcome, "accepted");
        yield* Queue.take(launches);
        assert.include(
          (yield* service.listWebhookDeliveries({
            id: task.id,
            acceptAnyOutcome: true,
          })).deliveries.map((d) => d.outcome),
          "duplicate",
        );
      }),
    { workers: new Map() },
  ),
);

it.effect.each([1, 3])(
  "coalesces %i deliveries into one run with the latest body, headers and query",
  (count) => {
    const workers = new Map<string, Effect.Effect<void>>();
    return withService(
      ({ service, launches }) =>
        Effect.gen(function* () {
          const { task } = yield* service.upsert(
            yield* webhookTaskInput({
              prompt: "{{body.value}} {{headers.version}} {{query.version}}",
              schedule: {
                type: "webhook",
                dedupe: { coalesceKey: "{{body.key}}", coalesceWindowSeconds: 10 },
              },
            }),
          );
          for (let value = 1; value <= count; value++) {
            const bodyText = JSON.stringify({ key: "B", value });
            yield* service.triggerWebhook(
              requestFor(task, {
                bodyText,
                body: new TextEncoder().encode(bodyText),
                headers: { version: String(value) },
                query: `version=${value}`,
              }),
            );
            if (value < count) yield* TestClock.adjust("2 seconds");
          }
          const owner = (yield* service.listWebhookDeliveries({
            id: task.id,
            acceptAnyOutcome: true,
          })).deliveries.find((delivery) => delivery.outcome === "accepted")!;
          assert.isTrue(owner.pendingWindow);
          const beforeDispatch = (yield* service.getWebhookDelivery({
            id: task.id,
            deliveryId: owner.id,
            acceptAnyOutcome: true,
          })).delivery;
          assert.isTrue(beforeDispatch.pendingWindow);
          assert.equal(beforeDispatch.body, JSON.stringify({ key: "B", value: count }));
          assert.equal(beforeDispatch.headers.version, String(count));
          assert.equal(beforeDispatch.query, `version=${count}`);
          assert.equal(
            beforeDispatch.bodyBytes,
            new TextEncoder().encode(beforeDispatch.body).byteLength,
          );
          assert.equal(yield* Queue.size(launches), 0);
          yield* workers.get("webhook-coalescing")!;
          assert.equal(yield* Queue.size(launches), 0);
          yield* TestClock.adjust(count === 1 ? "10 seconds" : "6 seconds");
          yield* workers.get("webhook-coalescing")!;
          const launched = yield* Queue.take(launches);
          assert.equal(
            (launched.initialMessage!.text.match(new RegExp(`>${count}<`, "g")) ?? []).length,
            3,
          );
          yield* workers.get("webhook-coalescing")!;
          assert.equal(yield* Queue.size(launches), 0);
          const outcomes = (yield* service.listWebhookDeliveries({
            id: task.id,
            acceptAnyOutcome: true,
          })).deliveries.map((d) => d.outcome);
          const afterDispatch = (yield* service.getWebhookDelivery({
            id: task.id,
            deliveryId: owner.id,
            acceptAnyOutcome: true,
          })).delivery;
          assert.isFalse(afterDispatch.pendingWindow);
          assert.equal(afterDispatch.body, beforeDispatch.body);
          assert.include(afterDispatch.renderedPrompt!, `>${count}<`);
          assert.equal(outcomes.filter((o) => o === "accepted").length, 1);
          assert.equal(outcomes.filter((o) => o === "coalesced").length, count - 1);
        }),
      { workers },
    );
  },
);

it.effect("a coalesced window dispatches the prompt saved after it opened", () => {
  const workers = new Map<string, Effect.Effect<void>>();
  return withService(
    ({ service, launches }) =>
      Effect.gen(function* () {
        const schedule = {
          type: "webhook",
          dedupe: { coalesceKey: "{{body.id}}", coalesceWindowSeconds: 10 },
        };
        const { task } = yield* service.upsert(
          yield* webhookTaskInput({ prompt: "Use the original instructions.", schedule }),
        );
        const bodyText = JSON.stringify({ id: "review" });
        yield* service.triggerWebhook(
          requestFor(task, { bodyText, body: new TextEncoder().encode(bodyText) }),
        );
        yield* service.upsert(
          yield* webhookTaskInput({ prompt: "Use the updated instructions.", schedule }),
        );
        yield* TestClock.adjust("10 seconds");
        yield* workers.get("webhook-coalescing")!;
        const launched = yield* Queue.take(launches);
        assert.equal(launched.initialMessage?.text, "Use the updated instructions.");
        const owner = (yield* service.listWebhookDeliveries({ id: task.id })).deliveries[0]!;
        const { delivery } = yield* service.getWebhookDelivery({
          id: task.id,
          deliveryId: owner.id,
        });
        assert.equal(delivery.renderedPrompt, "Use the updated instructions.");
      }),
    { workers },
  );
});

it.effect("retrying a running window keeps the prompt it was dispatched with", () => {
  const workers = new Map<string, Effect.Effect<void>>();
  return withService(
    ({ service, sql, launches }) =>
      Effect.gen(function* () {
        const schedule = {
          type: "webhook",
          dedupe: { coalesceKey: "{{body}}", coalesceWindowSeconds: 10 },
        };
        const save = (prompt: string) =>
          Effect.flatMap(webhookTaskInput({ prompt, schedule }), service.upsert);
        const { task } = yield* save("Prompt when the window opened");
        yield* service.triggerWebhook(requestFor(task));
        yield* save("Prompt at dispatch");
        yield* sql`CREATE TRIGGER fail_completion BEFORE UPDATE ON scheduled_tasks
          WHEN NEW.last_run_status = 'succeeded' BEGIN SELECT RAISE(FAIL, 'completion unavailable'); END`;
        yield* TestClock.adjust("10 seconds");
        yield* drainPendingDispatch(workers);
        const launched = yield* Queue.take(launches);
        assert.equal(launched.initialMessage?.text, "Prompt at dispatch");
        yield* save("Prompt saved after dispatch");
        yield* sql`DROP TRIGGER fail_completion`;
        yield* drainPendingDispatch(workers);
        assert.equal((yield* sql`SELECT 1 FROM scheduled_task_webhook_pending`).length, 0);
        const owner = (yield* service.listWebhookDeliveries({ id: task.id })).deliveries[0]!;
        const { delivery } = yield* service.getWebhookDelivery({
          id: task.id,
          deliveryId: owner.id,
        });
        assert.equal(delivery.renderedPrompt, "Prompt at dispatch");
      }),
    { workers },
  );
});

it.effect("a coalesced window fails its delivery when the task row cannot be read", () => {
  const workers = new Map<string, Effect.Effect<void>>();
  return withService(
    ({ service, sql, launches }) =>
      Effect.gen(function* () {
        const { task } = yield* service.upsert(
          yield* webhookTaskInput({
            schedule: {
              type: "webhook",
              dedupe: { coalesceKey: "{{body}}", coalesceWindowSeconds: 10 },
            },
          }),
        );
        yield* service.triggerWebhook(requestFor(task));
        yield* sql`UPDATE scheduled_tasks SET model_selection_json = 'not json' WHERE task_id = ${task.id}`;
        yield* TestClock.adjust("10 seconds");
        yield* drainPendingDispatch(workers);
        assert.equal(yield* Queue.size(launches), 0);
        assert.equal((yield* sql`SELECT 1 FROM scheduled_task_webhook_pending`).length, 0);
        const [delivery] = yield* sql<{
          outcome: string;
          error: string | null;
        }>`SELECT outcome, error FROM scheduled_task_webhook_deliveries`;
        assert.equal(delivery?.outcome, "dispatch_failed");
        // Failing before dispatch means a later successful read cannot send the stale snapshot.
        assert.equal(delivery?.error, "The pending webhook payload could not be prepared.");
      }),
    { workers },
  );
});

it.effect("recovers a persisted coalescing window after restarting the service", () => {
  const workers = new Map<string, Effect.Effect<void>>();
  return withService(
    ({ service, sql }) =>
      Effect.gen(function* () {
        const { task } = yield* service.upsert(
          yield* webhookTaskInput({
            schedule: {
              type: "webhook",
              dedupe: { coalesceKey: "{{headers.content-type}}", coalesceWindowSeconds: 10 },
            },
          }),
        );
        yield* service.triggerWebhook(requestFor(task));
        yield* TestClock.adjust("4 seconds");
        const restartedWorkers = new Map<string, Effect.Effect<void>>();
        yield* withService(
          ({ launches }) =>
            Effect.gen(function* () {
              yield* restartedWorkers.get("webhook-coalescing")!;
              assert.equal(yield* Queue.size(launches), 0);
              yield* TestClock.adjust("6 seconds");
              yield* restartedWorkers.get("webhook-coalescing")!;
              yield* Queue.take(launches);
              yield* restartedWorkers.get("webhook-coalescing")!;
              assert.equal(yield* Queue.size(launches), 0);
              assert.equal((yield* sql`SELECT * FROM scheduled_task_webhook_pending`).length, 0);
            }),
          { sql, workers: restartedWorkers },
        );
      }),
    { workers },
  );
});

it.effect("a pending dispatch interrupted at its permit does not block later polls", () => {
  const workers = new Map<string, Effect.Effect<void>>();
  // One shared dispatcher lets the test run the retry's forked worker to completion.
  const mixed = new EffectScheduler.MixedScheduler("sync");
  const dispatcher = mixed.makeDispatcher();
  return withService(
    ({ service, sql, launches }) =>
      Effect.gen(function* () {
        const { task } = yield* service.upsert(
          yield* webhookTaskInput({
            schedule: {
              type: "webhook",
              dedupe: { coalesceKey: "{{headers.content-type}}", coalesceWindowSeconds: 10 },
            },
          }),
        );
        yield* service.triggerWebhook(requestFor(task));
        yield* TestClock.adjust("10 seconds");
        const poll = workers.get("webhook-coalescing")!;

        const entered = yield* Deferred.make<Fiber.Fiber<unknown, unknown>>();
        const interruptAtPermit = Tracer.make({
          span: (options) => {
            if (options.name === "ScheduledTaskService.webhookPermit") {
              const fiber = Fiber.getCurrent()!;
              Deferred.doneUnsafe(entered, Effect.succeed(fiber));
              fiber.interruptUnsafe();
            }
            return new Tracer.NativeSpan(options);
          },
        });
        const firstPoll = yield* poll.pipe(Effect.withTracer(interruptAtPermit), Effect.forkChild);
        assert.isTrue(Exit.hasInterrupts(yield* Fiber.await(yield* Deferred.await(entered))));
        yield* Fiber.await(firstPoll);

        yield* poll;
        dispatcher.flush();

        assert.equal(yield* Queue.size(launches), 1);
        assert.lengthOf(yield* sql`SELECT 1 FROM scheduled_task_webhook_pending`, 0);
      }),
    { workers },
  ).pipe(
    Effect.provideService(EffectScheduler.Scheduler, {
      executionMode: mixed.executionMode,
      makeDispatcher: () => dispatcher,
      shouldYield: (fiber) => mixed.shouldYield(fiber),
    }),
  );
});

it.effect("rolls back replay and pending state when recording a delivery fails", () => {
  const workers = new Map<string, Effect.Effect<void>>();
  return withService(
    ({ service, sql, launches }) =>
      Effect.gen(function* () {
        const { task } = yield* service.upsert(
          yield* webhookTaskInput({
            schedule: {
              type: "webhook",
              dedupe: {
                replayKey: "{{headers.content-type}}",
                coalesceKey: "{{headers.content-type}}",
                coalesceWindowSeconds: 10,
              },
            },
          }),
        );
        yield* sql`CREATE TRIGGER fail_dedupe_log BEFORE INSERT ON scheduled_task_webhook_deliveries BEGIN SELECT RAISE(FAIL, 'injected'); END`;
        yield* service.triggerWebhook(requestFor(task)).pipe(Effect.flip);
        assert.equal((yield* sql`SELECT * FROM scheduled_task_webhook_pending`).length, 0);
        assert.equal((yield* sql`SELECT * FROM scheduled_task_webhook_replay_keys`).length, 0);
        yield* sql`DROP TRIGGER fail_dedupe_log`;
        const retry = yield* service.triggerWebhook(requestFor(task));
        assert.equal(retry._tag === "accepted" && retry.outcome, "accepted");
        yield* TestClock.adjust("10 seconds");
        yield* workers.get("webhook-coalescing")!;
        yield* Queue.take(launches);
      }),
    { workers },
  );
});

it.effect("queue-full deliveries release replay keys so a later retry runs", () =>
  Effect.gen(function* () {
    const gate = yield* Deferred.make<void>();
    yield* withService(
      ({ service, launches }) =>
        Effect.gen(function* () {
          const { task } = yield* service.upsert(
            yield* webhookTaskInput({
              schedule: { type: "webhook", dedupe: { replayKey: "{{headers.delivery-id}}" } },
            }),
          );
          const request = (i: number) =>
            requestFor(task, { headers: { "delivery-id": String(i) } });
          for (let i = 0; i < 20; i++) yield* service.triggerWebhook(request(i));
          yield* Queue.take(launches);
          const full = yield* service.triggerWebhook(request(20));
          assert.equal(full._tag === "rate_limited" && full.outcome, "queue_full");
          yield* Deferred.succeed(gate, undefined);
          for (let i = 1; i < 20; i++) yield* Queue.take(launches);
          const retry = yield* service.triggerWebhook(request(20));
          assert.equal(retry._tag === "accepted" && retry.outcome, "accepted");
          yield* Queue.take(launches);
        }),
      { gate },
    );
  }),
);

it.effect.each(["task_json", "request_json"] as const)(
  "removes corrupt pending %s instead of retrying forever",
  (column) => {
    const workers = new Map<string, Effect.Effect<void>>();
    return withService(
      ({ service, sql }) =>
        Effect.gen(function* () {
          const { task } = yield* service.upsert(
            yield* webhookTaskInput({
              schedule: {
                type: "webhook",
                dedupe: {
                  replayKey: "{{body}}",
                  coalesceKey: "{{body}}",
                  coalesceWindowSeconds: 10,
                },
              },
            }),
          );
          yield* service.triggerWebhook(requestFor(task));
          yield* sql`UPDATE scheduled_task_webhook_pending SET ${sql(column)} = 'broken'`;
          yield* TestClock.adjust("10 seconds");
          yield* drainPendingDispatch(workers);
          assert.equal((yield* sql`SELECT * FROM scheduled_task_webhook_pending`).length, 0);
          assert.equal((yield* sql`SELECT * FROM scheduled_task_webhook_replay_keys`).length, 0);
          assert.equal(
            (yield* service.listWebhookDeliveries({ id: task.id })).deliveries[0]!.outcome,
            "dispatch_failed",
          );
          yield* workers.get("webhook-coalescing")!;
        }),
      { workers },
    );
  },
);

it.effect(
  "restarting after a coalesced launch before pending deletion does not launch or count it twice",
  () => {
    const workers = new Map<string, Effect.Effect<void>>();
    return withService(
      ({ service, sql, launches }) =>
        Effect.gen(function* () {
          const { task } = yield* service.upsert(
            yield* webhookTaskInput({
              schedule: {
                type: "webhook",
                dedupe: { coalesceKey: "{{body}}", coalesceWindowSeconds: 10 },
              },
            }),
          );
          const accepted = yield* service.triggerWebhook(requestFor(task));
          yield* sql`CREATE TRIGGER fail_pending_delete BEFORE DELETE ON scheduled_task_webhook_pending BEGIN SELECT RAISE(FAIL,'crash after launch'); END`;
          yield* TestClock.adjust("10 seconds");
          yield* drainPendingDispatch(workers);
          const launch = yield* Queue.take(launches);
          assert.isTrue(
            launch.commandId.startsWith(
              `scheduled-task:${task.id}:webhook:${accepted._tag === "accepted" && accepted.deliveryId}:execution:`,
            ),
          );
          assert.equal((yield* service.list()).tasks[0]!.runCount, 1);
          assert.equal(
            (yield* sql`SELECT * FROM scheduled_task_webhook_pending WHERE state = 'completed'`)
              .length,
            1,
          );
          yield* sql`DROP TRIGGER fail_pending_delete`;
          const restartedWorkers = new Map<string, Effect.Effect<void>>();
          yield* withService(
            ({ service: restarted, launches: secondLaunches }) =>
              Effect.gen(function* () {
                yield* drainPendingDispatch(restartedWorkers);
                assert.equal(yield* Queue.size(secondLaunches), 0);
                assert.equal((yield* restarted.list()).tasks[0]!.runCount, 1);
                assert.equal((yield* sql`SELECT * FROM scheduled_task_webhook_pending`).length, 0);
              }),
            { sql, workers: restartedWorkers },
          );
        }),
      { workers },
    );
  },
);

it.effect("guards bad-signature floods separately from valid run admission", () =>
  withService(({ service, sql }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(
        yield* webhookTaskInput({
          schedule: {
            type: "webhook",
            signature: { header: "signature", encoding: "hex", prefix: "", secret: "secret" },
          },
        }),
      );
      const results = yield* Effect.forEach(Array.from({ length: 75 }), () =>
        service.triggerWebhook(requestFor(task)),
      );
      assert.equal(results.filter((result) => result._tag === "rejected_signature").length, 60);
      assert.equal(results.filter((result) => result._tag === "rate_limited").length, 15);
      assert.equal((yield* sql`SELECT * FROM scheduled_task_webhook_deliveries`).length, 51);
    }),
  ),
);

it.effect("start failures release both replay keys and relay claims for redelivery", () =>
  withService(
    ({ service, sql, launches }) =>
      Effect.gen(function* () {
        const { task } = yield* service.upsert(
          yield* webhookTaskInput({
            schedule: { type: "webhook", dedupe: { replayKey: "{{body}}" } },
          }),
        );
        const request = requestFor(task, { relayDeliveryId: "failed-start" });
        yield* waitForSpan(
          service.triggerWebhook(request),
          "ScheduledTaskService.runWebhookDelivery",
        );
        yield* Queue.take(launches);
        assert.equal((yield* sql`SELECT * FROM scheduled_task_webhook_replay_keys`).length, 0);
        assert.equal((yield* sql`SELECT * FROM scheduled_task_webhook_relay_deliveries`).length, 0);
        const failed = yield* service.getExecutions({
          id: task.id,
          deliveryId: "delivery:relay:failed-start",
        });
        assert.equal(failed.effectiveExecutions[0]!.state, "failed_to_start");
        assert.equal(
          (yield* service.list()).tasks[0]!.lastRunError,
          'Provider instance "codex" is disabled for project project-webhook.',
        );
        const retry = yield* waitForSpan(
          service.triggerWebhook(request),
          "ScheduledTaskService.runWebhookDelivery",
        );
        assert.equal(retry._tag === "accepted" && retry.outcome, "accepted");
        yield* Queue.take(launches);
        assert.equal((yield* service.list()).tasks[0]!.lastRunStatus, "succeeded");
        const retried = yield* service.getExecutions({
          id: task.id,
          deliveryId: "delivery:relay:failed-start",
        });
        const duplicate = yield* service.triggerWebhook(request);
        assert.equal(duplicate._tag === "accepted" && duplicate.outcome, "duplicate");
        const again = yield* service.getExecutions({
          id: task.id,
          deliveryId: "delivery:relay:failed-start",
        });
        assert.equal(again.aliases[2]!.executionId, retried.aliases[1]!.executionId);
        assert.equal(retried.aliases.length, 2);
        assert.equal(retried.effectiveExecutions.length, 2);
        assert.equal(
          retried.effectiveExecutions.filter((execution) => execution.state === "dispatched")
            .length,
          1,
        );
      }),
    { failLaunches: { remaining: 1 }, formalLaunch: { runId: "run-after-failure" } },
  ),
);

it.effect("a start failure releases the relay claim when the task row is replaced mid-run", () =>
  Effect.gen(function* () {
    const gate = yield* Deferred.make<void>();
    yield* withService(
      ({ service, sql, launches }) =>
        Effect.gen(function* () {
          const { task } = yield* service.upsert(yield* webhookTaskInput());
          const request = requestFor(task, { relayDeliveryId: "replaced-mid-run" });
          const first = yield* waitForSpan(
            service.triggerWebhook(request),
            "ScheduledTaskService.runWebhookDelivery",
          ).pipe(Effect.forkChild);
          yield* Queue.take(launches);
          // A same-id recreate while the launch is out: the stored row is no longer this run's.
          yield* sql`UPDATE scheduled_tasks SET last_run_status = 'never', last_run_at = NULL`;
          yield* Deferred.succeed(gate, undefined);
          yield* Fiber.join(first);
          const retry = yield* service.triggerWebhook(request);
          assert.equal(retry._tag === "accepted" && retry.outcome, "accepted");
          yield* Queue.take(launches);
        }),
      { gate, failLaunches: { remaining: 1 } },
    );
  }),
);

it.effect("main retention keeps fifty runs and preserves filtered and dedupe buckets", () =>
  withService(({ service, sql, launches }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* webhookTaskInput());
      yield* service.triggerWebhook(requestFor(task));
      yield* Queue.take(launches);
      for (const outcome of ["filtered", "duplicate"]) {
        yield* sql`WITH RECURSIVE n(value) AS (SELECT 1 UNION ALL SELECT value + 1 FROM n WHERE value < 20)
        INSERT INTO scheduled_task_webhook_deliveries
        SELECT ${outcome} || n.value, d.task_id, d.received_at, d.method, d.query, d.headers_json, d.body,
          d.body_bytes, d.body_truncated, ${outcome}, d.signature_verified, d.missing_fields_json, d.rendered_prompt, d.error
        FROM n CROSS JOIN (SELECT * FROM scheduled_task_webhook_deliveries WHERE outcome = 'accepted' LIMIT 1) d`;
      }
      for (let i = 0; i < 59; i++) {
        assert.equal((yield* service.triggerWebhook(requestFor(task)))._tag, "accepted");
        yield* Queue.take(launches);
      }
      const counts = yield* sql<{
        outcome: string;
        count: number;
      }>`SELECT outcome, COUNT(*) AS count FROM scheduled_task_webhook_deliveries GROUP BY outcome ORDER BY outcome`;
      assert.deepEqual(counts, [
        { outcome: "accepted", count: 50 },
        { outcome: "duplicate", count: 20 },
        { outcome: "filtered", count: 20 },
      ]);
    }),
  ),
);

it.effect(
  "a valid request refunds its own reservation without expiring an interleaved rejection",
  () =>
    Effect.gen(function* () {
      const started = yield* Deferred.make<void>();
      const release = yield* Deferred.make<void>();
      yield* withService(
        ({ service, launches }) =>
          Effect.gen(function* () {
            const { task } = yield* service.upsert(
              yield* webhookTaskInput({
                schedule: {
                  type: "webhook",
                  signature: { header: "signature", encoding: "hex", prefix: "", secret: "secret" },
                },
              }),
            );
            const signature = NodeCrypto.createHmac("sha256", "secret")
              .update(pullRequestBody)
              .digest("hex");
            const valid = yield* service
              .triggerWebhook(requestFor(task, { headers: { signature } }))
              .pipe(Effect.forkChild);
            yield* Deferred.await(started);
            yield* TestClock.adjust("30 seconds");
            assert.equal(
              (yield* service.triggerWebhook(requestFor(task)))._tag,
              "rejected_signature",
            );
            yield* Deferred.succeed(release, undefined);
            assert.equal((yield* Fiber.join(valid))._tag, "accepted");
            yield* Queue.take(launches);
            // Only the valid reservation would have expired now; the rejection must still count.
            yield* TestClock.adjust("31 seconds");
            for (let i = 0; i < 59; i++)
              assert.equal(
                (yield* service.triggerWebhook(requestFor(task)))._tag,
                "rejected_signature",
              );
            assert.equal((yield* service.triggerWebhook(requestFor(task)))._tag, "rate_limited");
          }),
        { frameGate: { started, release } },
      );
    }),
);

it.effect("keeps replay keys when dispatch succeeds but completion bookkeeping fails", () =>
  withService(({ service, sql, launches }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(
        yield* webhookTaskInput({
          schedule: {
            type: "webhook",
            dedupe: { replayKey: "{{body}}" },
          },
        }),
      );
      yield* sql`CREATE TRIGGER fail_completion BEFORE UPDATE ON scheduled_tasks
      WHEN NEW.last_run_status = 'succeeded' BEGIN SELECT RAISE(FAIL, 'completion unavailable'); END`;
      yield* waitForSpan(
        service.triggerWebhook(requestFor(task)),
        "ScheduledTaskService.runWebhookDelivery",
      );
      yield* Queue.take(launches);
      assert.equal((yield* sql`SELECT 1 FROM scheduled_task_webhook_replay_keys`).length, 1);
      yield* sql`DROP TRIGGER fail_completion`;
      const retry = yield* service.triggerWebhook(requestFor(task));
      assert.equal(retry._tag === "accepted" && retry.outcome, "duplicate");
      assert.equal(yield* Queue.size(launches), 0);
      assert.equal((yield* service.list()).tasks[0]!.runCount, 1);
    }),
  ),
);

it.effect.each([119_000, 120_000])(
  "bounds coalesced prompt logs and checks trimmed length for %i body characters",
  (length) => {
    const workers = new Map<string, Effect.Effect<void>>();
    return withService(
      ({ service, launches }) =>
        Effect.gen(function* () {
          const { task } = yield* service.upsert(
            yield* webhookTaskInput({
              prompt: `{{body.absent}}${" ".repeat(10_000)}{{body.text}}`,
              schedule: {
                type: "webhook",
                dedupe: { coalesceKey: "{{body.group}}", coalesceWindowSeconds: 10 },
              },
            }),
          );
          const bodyText = JSON.stringify({ group: "B", text: "x".repeat(length) });
          const accepted = yield* service.triggerWebhook(
            requestFor(task, { bodyText, body: new TextEncoder().encode(bodyText) }),
          );
          assert.equal(accepted._tag, "accepted");
          yield* TestClock.adjust("10 seconds");
          yield* drainPendingDispatch(workers);
          const delivery = (yield* service.listWebhookDeliveries({ id: task.id })).deliveries[0]!;
          if (length === 119_000) {
            const launch = yield* Queue.take(launches);
            assert.isAbove(launch.initialMessage!.text.length, 120_000);
            assert.equal(delivery.outcome, "accepted");
            const detail = yield* service.getWebhookDelivery({
              id: task.id,
              deliveryId: delivery.id,
            });
            assert.equal(detail.delivery.renderedPrompt!.length, 64 * 1024);
          } else {
            assert.equal(yield* Queue.size(launches), 0);
            assert.equal(delivery.outcome, "dispatch_failed");
            assert.equal(delivery.error, "The filled-in prompt is too long.");
          }
        }),
      { workers },
    );
  },
);

it.effect("oversized coalesced windows stay within the suppression bucket's limit", () => {
  const workers = new Map<string, Effect.Effect<void>>();
  return withService(
    ({ service, sql, launches }) =>
      Effect.gen(function* () {
        const { task } = yield* service.upsert(
          yield* webhookTaskInput({
            prompt: `{{body.absent}}${" ".repeat(10_000)}{{body.text}}`,
            schedule: {
              type: "webhook",
              dedupe: { coalesceKey: "{{body.group}}", coalesceWindowSeconds: 10 },
            },
          }),
        );
        const bodyText = JSON.stringify({ group: "B", text: "x".repeat(120_000) });
        for (let i = 0; i < 21; i++) {
          const accepted = yield* service.triggerWebhook(
            requestFor(task, { bodyText, body: new TextEncoder().encode(bodyText) }),
          );
          assert.equal(accepted._tag === "accepted" && accepted.outcome, "accepted");
          yield* TestClock.adjust("10 seconds");
          yield* drainPendingDispatch(workers);
        }
        assert.equal(yield* Queue.size(launches), 0);
        const counts = yield* sql<{
          outcome: string;
          count: number;
        }>`SELECT outcome, COUNT(*) AS count FROM scheduled_task_webhook_deliveries GROUP BY outcome`;
        assert.deepEqual(counts, [{ outcome: "dispatch_failed", count: 20 }]);
      }),
    { workers },
  );
});

it.effect.each([1, 11])(
  "keeps relay claims for %i coalesced updates after their window closes",
  (count) => {
    const workers = new Map<string, Effect.Effect<void>>();
    return withService(
      ({ service, sql, launches }) =>
        Effect.gen(function* () {
          const { task } = yield* service.upsert(
            yield* webhookTaskInput({
              prompt: "{{body.value}}",
              schedule: {
                type: "webhook",
                dedupe: { coalesceKey: "{{body.group}}", coalesceWindowSeconds: 10 },
              },
            }),
          );
          const request = (value: number) => {
            const bodyText = JSON.stringify({ group: "B", value });
            return requestFor(task, {
              relayDeliveryId: `coalesced-relay-${value}`,
              bodyText,
              body: new TextEncoder().encode(bodyText),
            });
          };
          yield* service.triggerWebhook(request(0));
          for (let i = 1; i <= count; i++) {
            const result = yield* service.triggerWebhook(request(i));
            assert.equal(result._tag === "accepted" && result.outcome, "coalesced");
          }
          assert.equal(
            (yield* sql`SELECT 1 FROM scheduled_task_webhook_relay_deliveries`).length,
            count + 1,
          );
          yield* TestClock.adjust("10 seconds");
          yield* drainPendingDispatch(workers);
          assert.include((yield* Queue.take(launches)).initialMessage!.text, `>${count}<`);
          const retry = yield* service.triggerWebhook(request(count));
          assert.equal(retry._tag === "accepted" && retry.outcome, "duplicate");
          assert.equal((yield* sql`SELECT 1 FROM scheduled_task_webhook_pending`).length, 0);
          assert.equal((yield* service.list()).tasks[0]!.runCount, 1);
          assert.equal(yield* Queue.size(launches), 0);
        }),
      { workers },
    );
  },
);

it.effect("failed windows release every member's replay key and relay claim", () => {
  const workers = new Map<string, Effect.Effect<void>>();
  return withService(
    ({ service, sql, launches }) =>
      Effect.gen(function* () {
        const { task } = yield* service.upsert(
          yield* webhookTaskInput({
            prompt: "{{body.value}}",
            schedule: {
              type: "webhook",
              dedupe: {
                replayKey: "{{headers.delivery-id}}",
                coalesceKey: "{{body.group}}",
                coalesceWindowSeconds: 10,
              },
            },
          }),
        );
        const request = (id: string) => {
          const bodyText = JSON.stringify({ group: "same", value: id });
          return requestFor(task, {
            bodyText,
            body: new TextEncoder().encode(bodyText),
            headers: { "delivery-id": id },
            relayDeliveryId: id,
          });
        };
        yield* service.triggerWebhook(request("A"));
        for (let i = 0; i < 12; i++) yield* service.triggerWebhook(request(`B${i}`));
        assert.equal(
          (yield* sql`SELECT 1 FROM scheduled_task_webhook_relay_deliveries`).length,
          13,
        );
        yield* TestClock.adjust("10 seconds");
        yield* drainPendingDispatch(workers);
        yield* Queue.take(launches);
        assert.equal((yield* sql`SELECT 1 FROM scheduled_task_webhook_replay_keys`).length, 0);
        assert.equal((yield* sql`SELECT 1 FROM scheduled_task_webhook_relay_deliveries`).length, 0);
        const retry = yield* service.triggerWebhook(request("B11"));
        assert.equal(retry._tag === "accepted" && retry.outcome, "accepted");
        yield* TestClock.adjust("10 seconds");
        yield* drainPendingDispatch(workers);
        assert.include((yield* Queue.take(launches)).initialMessage!.text, "B11");
        assert.equal((yield* service.list()).tasks[0]!.runCount, 2);
      }),
    { workers, failLaunches: { remaining: 1 } },
  );
});

it.effect(
  "recovery finishes failed-window claim cleanup without counting the attempt twice",
  () => {
    const workers = new Map<string, Effect.Effect<void>>();
    return withService(
      ({ service, sql, launches }) =>
        Effect.gen(function* () {
          const { task } = yield* service.upsert(
            yield* webhookTaskInput({
              schedule: {
                type: "webhook",
                dedupe: {
                  replayKey: "{{headers.delivery-id}}",
                  coalesceKey: "{{body}}",
                  coalesceWindowSeconds: 10,
                },
              },
            }),
          );
          const request = (id: string) =>
            requestFor(task, { headers: { "delivery-id": id }, relayDeliveryId: id });
          yield* service.triggerWebhook(request("A"));
          yield* service.triggerWebhook(request("B"));
          yield* sql`CREATE TRIGGER fail_claim_cleanup BEFORE DELETE ON scheduled_task_webhook_relay_deliveries
      BEGIN SELECT RAISE(FAIL, 'cleanup unavailable'); END`;
          yield* TestClock.adjust("10 seconds");
          yield* drainPendingDispatch(workers);
          yield* Queue.take(launches);
          assert.equal((yield* service.list()).tasks[0]!.runCount, 1);
          assert.equal(
            (yield* sql<{ state: string }>`SELECT state FROM scheduled_task_webhook_pending`)[0]!
              .state,
            "failed",
          );
          yield* sql`DROP TRIGGER fail_claim_cleanup`;
          const restartedWorkers = new Map<string, Effect.Effect<void>>();
          yield* withService(
            ({ service: restarted, launches: restartedLaunches }) =>
              Effect.gen(function* () {
                yield* drainPendingDispatch(restartedWorkers);
                assert.equal(yield* Queue.size(restartedLaunches), 0);
                assert.equal((yield* restarted.list()).tasks[0]!.runCount, 1);
                assert.equal((yield* sql`SELECT 1 FROM scheduled_task_webhook_pending`).length, 0);
                assert.equal(
                  (yield* sql`SELECT 1 FROM scheduled_task_webhook_replay_keys`).length,
                  0,
                );
                assert.equal(
                  (yield* sql`SELECT 1 FROM scheduled_task_webhook_relay_deliveries`).length,
                  0,
                );
                const retry = yield* restarted.triggerWebhook(request("B"));
                assert.equal(retry._tag === "accepted" && retry.outcome, "accepted");
                yield* TestClock.adjust("10 seconds");
                yield* drainPendingDispatch(restartedWorkers);
                yield* Queue.take(restartedLaunches);
                assert.equal((yield* restarted.list()).tasks[0]!.runCount, 2);
              }),
            { sql, workers: restartedWorkers },
          );
        }),
      { workers, failLaunches: { remaining: 1 } },
    );
  },
);

it.effect("dispatches an overdue window exactly once after its deadline passed while stopped", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const task = yield* withService(
      ({ service, launches }) =>
        Effect.gen(function* () {
          const { task } = yield* service.upsert(
            yield* webhookTaskInput({
              schedule: {
                type: "webhook",
                dedupe: { coalesceKey: "{{body}}", coalesceWindowSeconds: 10 },
              },
            }),
          );
          yield* service.triggerWebhook(requestFor(task));
          assert.equal(yield* Queue.size(launches), 0);
          return task;
        }),
      { sql, workers: new Map() },
    );
    // The first service scope is closed before the deadline advances.
    yield* TestClock.adjust("30 seconds");
    const restartedWorkers = new Map<string, Effect.Effect<void>>();
    yield* withService(
      ({ service, launches }) =>
        Effect.gen(function* () {
          yield* drainPendingDispatch(restartedWorkers);
          yield* Queue.take(launches);
          for (let i = 0; i < 3; i++) yield* restartedWorkers.get("webhook-coalescing")!;
          assert.equal(yield* Queue.size(launches), 0);
          assert.equal((yield* service.list()).tasks[0]!.runCount, 1);
          assert.equal((yield* sql`SELECT 1 FROM scheduled_task_webhook_pending`).length, 0);
        }),
      { sql, workers: restartedWorkers },
    );
    const secondRestartWorkers = new Map<string, Effect.Effect<void>>();
    yield* withService(
      ({ service, launches }) =>
        Effect.gen(function* () {
          yield* secondRestartWorkers.get("webhook-coalescing")!;
          assert.equal(yield* Queue.size(launches), 0);
          assert.equal(
            (yield* service.list()).tasks.find((entry) => entry.id === task.id)!.runCount,
            1,
          );
        }),
      { sql, workers: secondRestartWorkers },
    );
  }).pipe(Effect.provide(SqlitePersistence.layerMemory)),
);

it.effect("removes a window with a render defect and dispatches the task's next window", () => {
  const workers = new Map<string, Effect.Effect<void>>();
  return withService(
    ({ service, sql, launches }) =>
      Effect.gen(function* () {
        const { task } = yield* service.upsert(
          yield* webhookTaskInput({
            prompt: "{{body.deep}}",
            schedule: {
              type: "webhook",
              dedupe: {
                replayKey: "{{headers.delivery-id}}",
                coalesceKey: "{{headers.group}}",
                coalesceWindowSeconds: 10,
              },
            },
          }),
        );
        const request = (id: string, bodyText: string) =>
          requestFor(task, {
            bodyText,
            body: new TextEncoder().encode(bodyText),
            headers: { "delivery-id": id, group: id },
            relayDeliveryId: id,
          });
        const bad = request(
          "bad-render",
          '{"deep":' + "[".repeat(20000) + "0" + "]".repeat(20000) + "}",
        );
        assert.throws(() => renderWebhookPrompt(task.prompt, bad, "frame"), RangeError);
        const accepted = yield* service.triggerWebhook(bad);
        assert.equal(accepted._tag, "accepted");
        yield* service.triggerWebhook(request("good-render", '{"deep":"next window"}'));
        yield* TestClock.adjust("10 seconds");
        yield* drainPendingDispatch(workers);
        assert.include((yield* Queue.take(launches)).initialMessage!.text, "next window");
        assert.equal(yield* Queue.size(launches), 0);
        assert.equal((yield* sql`SELECT 1 FROM scheduled_task_webhook_pending`).length, 0);
        assert.equal(
          (yield* sql`SELECT 1 FROM scheduled_task_webhook_replay_keys WHERE delivery_id = 'delivery:relay:bad-render'`)
            .length,
          0,
        );
        assert.equal(
          (yield* sql`SELECT 1 FROM scheduled_task_webhook_relay_deliveries WHERE relay_delivery_id = 'bad-render'`)
            .length,
          0,
        );
        const failed = (yield* service.getWebhookDelivery({
          id: task.id,
          deliveryId: ScheduledTaskWebhookDeliveryId.make("delivery:relay:bad-render"),
          acceptAnyOutcome: true,
        })).delivery;
        assert.equal(failed.outcome, "dispatch_failed");
        assert.equal(failed.error, "The pending webhook payload could not be prepared.");
        yield* workers.get("webhook-coalescing")!;
        assert.equal((yield* service.list()).tasks[0]!.runCount, 1);
      }),
    { workers },
  );
});

it.effect.each([false, true])(
  "counts an interrupted immediate run beside an unrelated running window (corrupt task: %s)",
  (corrupt) => {
    const workers = new Map<string, Effect.Effect<void>>();
    const completedLaunches = new Set<string>();
    return Effect.gen(function* () {
      const gate = yield* Deferred.make<void>();
      yield* withService(
        ({ service, sql, launches }) =>
          Effect.gen(function* () {
            const { task } = yield* service.upsert(
              yield* webhookTaskInput({
                prompt: "{{body.value}}",
                schedule: {
                  type: "webhook",
                  dedupe: { coalesceKey: "{{headers.group}}", coalesceWindowSeconds: 10 },
                },
              }),
            );
            const window = yield* service.triggerWebhook(
              requestFor(task, { headers: { group: "old" }, bodyText: '{"value":"old"}' }),
            );
            yield* sql`CREATE TRIGGER fail_old_completion BEFORE UPDATE ON scheduled_tasks
        WHEN NEW.last_run_status = 'succeeded' BEGIN SELECT RAISE(FAIL, 'completion unavailable'); END`;
            yield* TestClock.adjust("10 seconds");
            yield* drainPendingDispatch(workers);
            yield* Queue.take(launches);
            yield* sql`DROP TRIGGER fail_old_completion`;
            assert.equal(
              (yield* sql`SELECT 1 FROM scheduled_task_webhook_pending WHERE state = 'running'`)
                .length,
              1,
            );
            const immediate = yield* service.triggerWebhook(
              requestFor(task, { bodyText: '{"value":"immediate"}' }),
            );
            yield* Queue.take(launches);
            const [before] = yield* sql<{
              last_run_webhook_delivery_id: string;
              model_selection_json: string;
              run_count: number;
            }>`SELECT last_run_webhook_delivery_id, model_selection_json, run_count FROM scheduled_tasks WHERE task_id = ${task.id}`;
            assert.equal(
              before!.last_run_webhook_delivery_id,
              immediate._tag === "accepted" ? immediate.deliveryId : "",
            );
            assert.notEqual(
              before!.last_run_webhook_delivery_id,
              window._tag === "accepted" ? window.deliveryId : "",
            );
            assert.equal(before!.run_count, 0);
            if (corrupt)
              yield* sql`UPDATE scheduled_tasks SET model_selection_json = 'broken' WHERE task_id = ${task.id}`;
            const restartedWorkers = new Map<string, Effect.Effect<void>>();
            yield* withService(
              ({ service: restarted, launches: recoveredLaunches }) =>
                Effect.gen(function* () {
                  const [recovered] = yield* sql<{
                    run_count: number;
                    last_run_status: string;
                  }>`SELECT run_count, last_run_status FROM scheduled_tasks WHERE task_id = ${task.id}`;
                  assert.equal(recovered!.run_count, 1);
                  assert.equal(recovered!.last_run_status, "failed");
                  if (corrupt)
                    yield* sql`UPDATE scheduled_tasks SET model_selection_json = ${before!.model_selection_json} WHERE task_id = ${task.id}`;
                  yield* drainPendingDispatch(restartedWorkers);
                  assert.equal(yield* Queue.size(recoveredLaunches), 0);
                  assert.equal((yield* restarted.list()).tasks[0]!.runCount, 2);
                  assert.equal(
                    (yield* sql`SELECT 1 FROM scheduled_task_webhook_pending`).length,
                    0,
                  );
                  yield* restartedWorkers.get("webhook-coalescing")!;
                  assert.equal((yield* restarted.list()).tasks[0]!.runCount, 2);
                }),
              { sql, workers: restartedWorkers, completedLaunches },
            );
          }),
        {
          workers,
          completedLaunches,
          gateForTask: (input) =>
            input.initialMessage?.text.includes(">immediate<") ? gate : undefined,
        },
      );
    });
  },
);

it.effect("checks filters after the signature and max age, and runs only matching deliveries", () =>
  withService(({ service, launches }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(
        yield* webhookTaskInput({
          schedule: {
            type: "webhook",
            signature: {
              header: "x-hub-signature-256",
              encoding: "hex",
              prefix: "sha256=",
              secret: "s3cret",
            },
            maxDeliveryAgeMinutes: 30,
            filters: [
              { path: "headers.x-github-event", op: "equals", value: "pull_request" },
              { path: "body.action", op: "in", value: ["opened", "reopened"] },
            ],
          },
        }),
      );
      const delivery = (
        event: string,
        action: string,
        options: { readonly signed?: boolean; readonly receivedAt?: string } = {},
      ) => {
        const body = new TextEncoder().encode(
          JSON.stringify({ action, pull_request: { url: "https://github.com/org/repo/pull/45" } }),
        );
        const signature = `sha256=${NodeCrypto.createHmac("sha256", "s3cret").update(body).digest("hex")}`;
        return requestFor(task, {
          headers: {
            "content-type": "application/json",
            "x-github-event": event,
            ...(options.signed === false ? {} : { "x-hub-signature-256": signature }),
          },
          body,
          bodyText: new TextDecoder().decode(body),
          ...(options.receivedAt === undefined
            ? {}
            : { relayDeliveryId: "old", receivedAt: options.receivedAt }),
        });
      };
      const now = yield* DateTime.now;
      const old = DateTime.formatIso(DateTime.subtract(now, { minutes: 31 }));

      // A request failing a filter is still refused first for its signature or age.
      const unsigned = yield* service.triggerWebhook(delivery("push", "opened", { signed: false }));
      assert.equal(unsigned._tag, "rejected_signature");
      const tooOld = yield* service.triggerWebhook(delivery("push", "opened", { receivedAt: old }));
      assert.equal(tooOld._tag, "expired");

      const wrongEvent = yield* service.triggerWebhook(delivery("push", "opened"));
      const wrongAction = yield* service.triggerWebhook(delivery("pull_request", "closed"));
      assert.deepInclude(wrongEvent, { _tag: "accepted", outcome: "filtered" });
      assert.deepInclude(wrongAction, { _tag: "accepted", outcome: "filtered" });
      assert.equal(yield* Queue.size(launches), 0);
      const [listed] = (yield* service.list()).tasks;
      assert.equal(listed?.runCount, 0);
      assert.equal(listed?.lastRunStatus, "never");

      const matched = yield* service.triggerWebhook(delivery("pull_request", "opened"));
      assert.deepInclude(matched, { _tag: "accepted", outcome: "accepted" });
      assert.include((yield* Queue.take(launches)).initialMessage?.text ?? "", "pull/45");

      const deliveries = (yield* service.listWebhookDeliveries({
        id: task.id,
        acceptAnyOutcome: true,
      })).deliveries;
      const filtered = deliveries
        .filter((entry) => entry.outcome === "filtered")
        .map((entry) => [entry.signatureVerified, entry.error]);
      assert.sameDeepMembers(filtered, [
        [true, "Filter 1 (headers.x-github-event) did not match."],
        [true, "Filter 2 (body.action) did not match."],
      ]);
    }),
  ),
);

const pullRequestFilterInput = () =>
  webhookTaskInput({
    schedule: {
      type: "webhook",
      filters: [{ path: "headers.x-github-event", op: "equals", value: "pull_request" }],
    },
  });
const githubEvent = (event: string) => ({
  headers: { "content-type": "application/json", "x-github-event": event },
});

it.effect("a filtered relay delivery releases its claim, so a redelivery is evaluated again", () =>
  withService(({ service, launches }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* pullRequestFilterInput());
      const held = { relayDeliveryId: "relay-filtered" };
      const first = yield* service.triggerWebhook(
        requestFor(task, { ...held, ...githubEvent("push") }),
      );
      const again = yield* service.triggerWebhook(
        requestFor(task, { ...held, ...githubEvent("push") }),
      );
      assert.deepInclude(first, { _tag: "accepted", outcome: "filtered" });
      assert.deepInclude(again, { _tag: "accepted", outcome: "filtered" });
      const ran = yield* service.triggerWebhook(
        requestFor(task, { ...held, ...githubEvent("pull_request") }),
      );
      assert.deepInclude(ran, { _tag: "accepted", outcome: "accepted" });
      yield* Queue.take(launches);
      const replayed = yield* service.triggerWebhook(
        requestFor(task, { ...held, ...githubEvent("pull_request") }),
      );
      assert.deepInclude(replayed, { _tag: "accepted", outcome: "duplicate" });
    }),
  ),
);

it.effect("leaves filtered rows out of the list for clients that only decode older outcomes", () =>
  withService(({ service, launches }) =>
    Effect.gen(function* () {
      const { task } = yield* service.upsert(yield* pullRequestFilterInput());
      yield* service.triggerWebhook(requestFor(task, githubEvent("push")));
      yield* service.triggerWebhook(requestFor(task, githubEvent("pull_request")));
      yield* Queue.take(launches);
      const legacy = (yield* service.listWebhookDeliveries({ id: task.id })).deliveries;
      assert.deepEqual(
        legacy.map((delivery) => delivery.outcome),
        ["accepted"],
      );
      const current = (yield* service.listWebhookDeliveries({
        id: task.id,
        acceptAnyOutcome: true,
      })).deliveries;
      assert.sameMembers(
        current.map((delivery) => delivery.outcome),
        ["accepted", "filtered"],
      );
    }),
  ),
);

it.effect("filters run before replay and coalescing without claiming ignored deliveries", () => {
  const workers = new Map<string, Effect.Effect<void>>();
  return withService(
    ({ service, launches, sql }) =>
      Effect.gen(function* () {
        const { task } = yield* service.upsert(
          yield* webhookTaskInput({
            prompt: "{{body.value}}",
            schedule: {
              type: "webhook",
              filters: [{ path: "body.run", op: "equals", value: "true" }],
              dedupe: {
                replayKey: "{{headers.delivery-id}}",
                coalesceKey: "{{body.group}}",
                coalesceWindowSeconds: 10,
              },
            },
          }),
        );
        const request = (id: string, run: boolean, value: string) => {
          const bodyText = JSON.stringify({ run, group: "B", value });
          return requestFor(task, {
            headers: { "delivery-id": id },
            bodyText,
            body: new TextEncoder().encode(bodyText),
            relayDeliveryId: id,
          });
        };
        const ignored = yield* service.triggerWebhook(request("A", false, "ignored"));
        assert.deepInclude(ignored, { _tag: "accepted", outcome: "filtered" });
        assert.equal((yield* sql`SELECT 1 FROM scheduled_task_webhook_replay_keys`).length, 0);
        assert.equal((yield* sql`SELECT 1 FROM scheduled_task_webhook_pending`).length, 0);
        assert.equal((yield* sql`SELECT 1 FROM scheduled_task_webhook_relay_deliveries`).length, 0);
        const filteredId = ignored._tag === "accepted" ? ignored.deliveryId : assert.fail();
        const oldClient = yield* service
          .getWebhookDelivery({ id: task.id, deliveryId: filteredId })
          .pipe(Effect.result);
        assert.equal(oldClient._tag, "Failure");
        const currentClient = yield* service.getWebhookDelivery({
          id: task.id,
          deliveryId: filteredId,
          acceptAnyOutcome: true,
        });
        assert.equal(currentClient.delivery.outcome, "filtered");
        assert.deepInclude(yield* service.triggerWebhook(request("A", true, "first")), {
          _tag: "accepted",
          outcome: "accepted",
        });
        assert.deepInclude(
          yield* service.triggerWebhook({
            ...request("A", false, "ignored replay"),
            relayDeliveryId: "ignored-replay",
          }),
          { _tag: "accepted", outcome: "filtered" },
        );
        assert.deepInclude(yield* service.triggerWebhook(request("B", false, "ignored update")), {
          _tag: "accepted",
          outcome: "filtered",
        });
        assert.deepInclude(yield* service.triggerWebhook(request("B", true, "latest")), {
          _tag: "accepted",
          outcome: "coalesced",
        });
        yield* TestClock.adjust("10 seconds");
        yield* drainPendingDispatch(workers);
        assert.include((yield* Queue.take(launches)).initialMessage!.text, "latest");
        assert.equal(yield* Queue.size(launches), 0);
      }),
    { workers },
  );
});
