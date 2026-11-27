import {
  ProviderDriverKind,
  ProviderInstanceId,
  ProviderThreadId,
  ProviderTurnId,
  PositiveInt,
  TurnItemId,
  type OrchestrationV2ProviderThread,
  type OrchestrationV2ProviderTurn,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import { CodexProviderCapabilitiesV2 } from "../Adapters/CodexAdapterV2.ts";

/** A deterministic external-provider boundary. No CLI, credentials, or model is opened. */
export const makeScriptedProvider = Effect.gen(function* () {
  const started = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2TurnInput>();
  const steered = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2SteerInput>();
  const runs = new Map<
    string,
    {
      input: ProviderAdapter.ProviderAdapterV2TurnInput;
      events: Queue.Queue<ProviderAdapter.ProviderAdapterV2Event>;
      providerTurn: OrchestrationV2ProviderTurn;
    }
  >();
  const driver = ProviderDriverKind.make("codex"),
    instanceId = ProviderInstanceId.make("codex");
  let opens = 0;
  const automatic = new Set<string>();
  const finished = new Set<string>();
  const adapter: ProviderAdapter.ProviderAdapterV2["Service"] = {
    instanceId,
    driver,
    getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
    planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" }),
    openSession: (open) =>
      Effect.gen(function* () {
        opens++;
        const events = yield* Queue.unbounded<ProviderAdapter.ProviderAdapterV2Event>();
        const now = yield* DateTime.now;
        return {
          instanceId,
          driver,
          providerSessionId: open.providerSessionId,
          providerSession: {
            id: open.providerSessionId,
            driver,
            providerInstanceId: instanceId,
            status: "ready",
            cwd: open.runtimePolicy.cwd ?? "/",
            model: open.modelSelection.model,
            capabilities: CodexProviderCapabilitiesV2,
            createdAt: now,
            updatedAt: now,
            lastError: null,
          },
          events: Stream.fromQueue(events),
          backgroundTasksEndWithProcess: true,
          hasPendingBackgroundWork: Effect.sync(() =>
            [...runs].some(([id, run]) => run.events === events && !finished.has(id)),
          ),
          hasPendingBackgroundWorkForThread: (providerThread) =>
            Effect.sync(() =>
              [...runs].some(
                ([id, run]) =>
                  run.input.providerThread.id === providerThread.id && !finished.has(id),
              ),
            ),
          ensureThread: (input) =>
            Effect.gen(function* () {
              const at = yield* DateTime.now;
              const ref = {
                driver,
                nativeId: `scripted:${input.threadId}`,
                strength: "strong" as const,
              };
              return {
                id:
                  input.existingProviderThread?.id ??
                  ProviderThreadId.make(`scripted:${input.threadId}`),
                driver,
                providerInstanceId: instanceId,
                providerSessionId: open.providerSessionId,
                appThreadId: input.threadId,
                ownerNodeId: null,
                nativeThreadRef: ref,
                nativeConversationHeadRef: null,
                status: "idle",
                firstRunOrdinal: null,
                lastRunOrdinal: null,
                handoffIds: [],
                forkedFrom: null,
                pendingBackgroundTasks: [],
                contextUsage: null,
                nativeMetadata: null,
                goal: null,
                createdAt: at,
                updatedAt: at,
              } satisfies OrchestrationV2ProviderThread;
            }),
          resumeThread: (input) => Effect.succeed(input.providerThread),
          startTurn: (input) =>
            Effect.gen(function* () {
              const at = yield* DateTime.now;
              const providerTurn: OrchestrationV2ProviderTurn = {
                id: ProviderTurnId.make(`scripted-turn:${input.runId}`),
                providerThreadId: input.providerThread.id,
                nodeId: input.rootNodeId,
                runAttemptId: input.attemptId,
                nativeTurnRef: {
                  driver,
                  nativeId: `scripted-turn:${input.runId}`,
                  strength: "strong",
                },
                nativeThreadRef: input.providerThread.nativeThreadRef ?? undefined,
                ordinal: PositiveInt.make(input.providerTurnOrdinal),
                status: "running",
                startedAt: at,
                completedAt: null,
              };
              runs.set(input.runId, { input, events, providerTurn });
              yield* Queue.offer(events, {
                type: "provider_turn.updated",
                driver,
                threadId: input.threadId,
                providerTurn,
              });
              yield* Queue.offer(started, input);
              if (automatic.has(input.threadId)) {
                finished.add(input.runId);
                yield* Queue.offer(events, {
                  type: "provider_turn.updated",
                  driver,
                  threadId: input.threadId,
                  providerTurn: { ...providerTurn, status: "completed", completedAt: at },
                });
                yield* Queue.offer(events, {
                  type: "turn.terminal",
                  driver,
                  providerThreadId: input.providerThread.id,
                  providerTurnId: ProviderTurnId.make(`scripted-turn:${input.runId}`),
                  runOrdinal: PositiveInt.make(input.runOrdinal),
                  status: "completed",
                  failure: null,
                  threadDisposition: "reusable",
                });
              }
            }),
          steerTurn: (input) =>
            Queue.offer(steered, input).pipe(
              Effect.andThen(
                Queue.offer(events, {
                  type: "steer.updated",
                  driver,
                  threadId: input.threadId,
                  runId: input.runId,
                  messageId: input.message.messageId,
                  status: "completed",
                }),
              ),
              Effect.asVoid,
            ),
          interruptTurn: (input) =>
            Effect.gen(function* () {
              for (const [runId, value] of runs) {
                if (
                  value.input.providerThread.id !== input.providerThread.id ||
                  finished.has(runId)
                )
                  continue;
                finished.add(runId);
                yield* Queue.offer(value.events, {
                  type: "turn.terminal",
                  driver,
                  providerThreadId: value.input.providerThread.id,
                  providerTurnId: ProviderTurnId.make(`scripted-turn:${runId}`),
                  runOrdinal: PositiveInt.make(value.input.runOrdinal),
                  status: "interrupted",
                  failure: null,
                  threadDisposition: "reusable",
                });
              }
            }),
          respondToRuntimeRequest: () => Effect.die("Unexpected scripted runtime request"),
          readThreadSnapshot: ({ providerThread }) =>
            Effect.succeed({
              providerThread,
              providerTurns: [],
              messages: [],
              runtimeRequests: [],
            }),
          rollbackThread: () => Effect.die("Unexpected scripted rollback"),
          forkThread: () => Effect.die("Unexpected scripted fork"),
        };
      }),
  };
  const complete = (
    runId: string,
    check?: { command: string; cwd: string; output: string; exitCode: number },
  ) =>
    Effect.gen(function* () {
      const run = runs.get(runId);
      if (!run) return yield* Effect.die(`Scripted run ${runId} did not start`);
      if (check) {
        const now = yield* DateTime.now;
        yield* Queue.offer(run.events, {
          type: "turn_item.updated",
          driver,
          turnItem: {
            id: TurnItemId.make(`scripted-check:${runId}`),
            threadId: run.input.threadId,
            runId: run.input.runId,
            nodeId: run.input.rootNodeId,
            providerThreadId: run.input.providerThread.id,
            providerTurnId: ProviderTurnId.make(`scripted-turn:${runId}`),
            nativeItemRef: { driver, nativeId: `actual-check:${runId}`, strength: "strong" },
            parentItemId: null,
            ordinal: 1000 + run.input.runOrdinal,
            status: "completed",
            title: null,
            startedAt: now,
            completedAt: now,
            updatedAt: now,
            type: "command_execution",
            input: check.command,
            cwd: check.cwd,
            output: check.output,
            exitCode: check.exitCode,
          },
        });
      }
      finished.add(runId);
      const completedAt = yield* DateTime.now;
      yield* Queue.offer(run.events, {
        type: "provider_turn.updated",
        driver,
        threadId: run.input.threadId,
        providerTurn: { ...run.providerTurn, status: "completed", completedAt },
      });
      yield* Queue.offer(run.events, {
        type: "turn.terminal",
        driver,
        providerThreadId: run.input.providerThread.id,
        providerTurnId: ProviderTurnId.make(`scripted-turn:${runId}`),
        runOrdinal: PositiveInt.make(run.input.runOrdinal),
        status: "completed",
        failure: null,
        threadDisposition: "reusable",
      });
    });
  return {
    adapter,
    started,
    steered,
    complete,
    autoComplete: (threadId: string) => {
      automatic.add(threadId);
    },
    opens: () => opens,
  };
});
