import {
  CodexSettings,
  ThreadId,
  type OrchestrationV2AppThread,
  type OrchestrationV2ThreadProjection,
  type OrchestrationV2ProviderThread,
  type RunId,
  type ModelSelection,
} from "@t3tools/contracts";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as CodexClient from "effect-codex-app-server/client";
import * as CodexAdapter from "../Adapters/CodexAdapterV2.ts";
import * as Registry from "../ProviderAdapterRegistry.ts";
import * as Threads from "../ThreadManagementService.ts";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as Config from "../../config.ts";
import { AGENT_THREAD_ENV } from "../../preview/PortScanner.ts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as Sink from "effect/Sink";
import * as Stdio from "effect/Stdio";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as FileSystem from "effect/FileSystem";
import * as Digest from "effect/crypto/Digest";
import type * as PlatformError from "effect/PlatformError";

export interface WorkbenchStartedTurn {
  readonly threadId: ThreadId;
  readonly runId: RunId;
  readonly modelSelection: ModelSelection;
  readonly appThread: OrchestrationV2AppThread;
  readonly providerThread: OrchestrationV2ProviderThread;
  readonly message: OrchestrationV2ThreadProjection["messages"][number];
}
type RawTurn = {
  id: string;
  items: unknown[];
  itemsView: "notLoaded";
  status: "inProgress" | "completed" | "interrupted";
  error: null;
  startedAt: number;
  completedAt: number | null;
  durationMs: null;
};
type RawThread = {
  appThreadId: ThreadId;
  id: string;
  cwd: string;
  model: string;
  createdAt: number;
  turns: RawTurn[];
  emit: (frame: unknown) => Effect.Effect<boolean>;
};
type RawStart = { thread: RawThread; turn: RawTurn };

const decodeSettings = Schema.decodeEffect(CodexSettings);
const decodeOutboundFrame = Schema.decodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      id: Schema.optionalKey(Schema.Number),
      method: Schema.String,
      params: Schema.optionalKey(Schema.Record(Schema.String, Schema.Unknown)),
    }),
  ),
);

/** Only the external Codex app-server's NDJSON stdio is scripted. Client, adapter and normalizer are production code. */
export const makeScriptedProvider = Effect.fn("WorkbenchCodexRuntime.acquire")(function* (
  config: Layer.Layer<Config.ServerConfig, PlatformError.PlatformError>,
  root: string,
) {
  const settings = yield* decodeSettings({
    homePath: root + "/codex-runtime",
  }).pipe(Effect.orDie);
  const started = yield* Queue.unbounded<WorkbenchStartedTurn, Error>();
  const rawStarted = yield* Queue.unbounded<RawStart>();
  const nativeThreads = new Map<string, RawThread>();
  const observed = new Map<string, RawStart>();
  const automatic = new Set<string>();
  let opens = 0,
    nativeIds = 0,
    turnIds = 0;
  const nowSeconds = DateTime.now.pipe(
    Effect.map((at) => Math.floor(DateTime.toEpochMillis(at) / 1000)),
  );
  const threadValue = (thread: RawThread) => ({
    id: thread.id,
    sessionId: thread.id,
    projectId: null,
    forkedFromId: null,
    preview: "",
    ephemeral: false,
    modelProvider: "openai",
    historyMode: "legacy",
    createdAt: thread.createdAt,
    updatedAt: thread.createdAt,
    status: thread.turns.some((turn) => turn.status === "inProgress")
      ? { type: "active", activeFlags: [] }
      : { type: "idle" },
    path: root + "/codex-runtime/" + thread.id + ".jsonl",
    cwd: thread.cwd,
    cliVersion: "0.144.0",
    source: "vscode",
    threadSource: null,
    agentNickname: null,
    agentRole: null,
    gitInfo: null,
    name: null,
    turns: thread.turns,
  });
  const finish = (
    raw: RawStart,
    status: "completed" | "interrupted",
    check?: { command: string; cwd: string; output: string; exitCode: number },
  ) =>
    Effect.gen(function* () {
      if (raw.turn.status !== "inProgress")
        return yield* Effect.die("The scripted Codex turn already ended");
      const at = yield* nowSeconds;
      if (check) {
        if (check.cwd !== raw.thread.cwd)
          return yield* Effect.die("The real check receipt belongs to a different Codex cwd");
        const item = {
          type: "commandExecution",
          id: "check:" + raw.turn.id,
          command: check.command,
          cwd: check.cwd,
          processId: "acceptance-check",
          source: "unifiedExecStartup",
          status: "completed",
          commandActions: [{ type: "unknown", command: check.command }],
          aggregatedOutput: check.output,
          exitCode: check.exitCode,
          durationMs: 1,
        };
        raw.turn.items.push(item);
        yield* raw.thread.emit({
          method: "item/completed",
          params: { threadId: raw.thread.id, turnId: raw.turn.id, completedAtMs: at * 1000, item },
        });
      }
      raw.turn.status = status;
      raw.turn.completedAt = at;
      yield* raw.thread.emit({
        method: "turn/completed",
        params: { threadId: raw.thread.id, turn: raw.turn },
      });
    });
  const clientFactory: CodexAdapter.CodexAppServerClientFactoryShape = {
    open: () =>
      Effect.gen(function* () {
        opens++;
        const incoming = yield* Queue.unbounded<Uint8Array>();
        const emit = (frame: unknown) =>
          Queue.offer(incoming, new TextEncoder().encode(JSON.stringify(frame) + "\n"));
        let remainder = "";
        const decoder = new TextDecoder();
        const processFrame = (frame: {
          id?: number;
          method: string;
          params?: Record<string, unknown>;
        }) =>
          Effect.gen(function* () {
            const params = frame.params ?? {};
            const reply = (result: unknown) => emit({ id: frame.id, result });
            if (frame.method === "initialize") {
              yield* reply({
                userAgent: "T3 Code/0.144.0",
                codexHome: root + "/codex-runtime",
                platformFamily: "unix",
                platformOs: "macos",
              });
              return;
            }
            if (frame.method === "initialized") return;
            if (frame.method === "config/read") {
              yield* reply({ config: {}, origins: {} });
              return;
            }
            if (frame.method === "thread/start") {
              const runtimeConfig = params.config as Record<string, unknown>;
              const appId = runtimeConfig?.["shell_environment_policy.set." + AGENT_THREAD_ENV];
              if (typeof appId !== "string")
                return yield* Effect.die("Codex thread/start omitted the T3 app-thread binding");
              const cwd = typeof params.cwd === "string" ? params.cwd : root + "/project";
              if (!cwd.startsWith(root + "/"))
                return yield* Effect.die("Codex thread/start escaped the owned workspace");
              const thread: RawThread = {
                appThreadId: ThreadId.make(appId),
                id: "acceptance-codex-" + ++nativeIds,
                cwd,
                model: typeof params.model === "string" ? params.model : "gpt-5.1-codex",
                createdAt: yield* nowSeconds,
                turns: [],
                emit,
              };
              nativeThreads.set(thread.id, thread);
              yield* reply({
                thread: threadValue(thread),
                model: thread.model,
                modelProvider: "openai",
                serviceTier: null,
                cwd: thread.cwd,
                instructionSources: [],
                approvalPolicy: "on-request",
                approvalsReviewer: "user",
                sandbox: { type: "dangerFullAccess" },
                reasoningEffort: null,
              });
              return;
            }
            const thread = nativeThreads.get(String(params.threadId));
            if (!thread)
              return yield* Effect.die("Unexpected Codex raw thread identity for " + frame.method);
            if (frame.method === "thread/resume") {
              thread.emit = emit;
              yield* reply({
                thread: threadValue(thread),
                model: thread.model,
                modelProvider: "openai",
                serviceTier: null,
                cwd: thread.cwd,
                instructionSources: [],
                approvalPolicy: "on-request",
                approvalsReviewer: "user",
                sandbox: { type: "dangerFullAccess" },
                reasoningEffort: null,
              });
              return;
            }
            if (frame.method === "thread/read") {
              yield* reply({ thread: threadValue(thread) });
              return;
            }
            if (frame.method === "thread/archive" || frame.method === "thread/unsubscribe") {
              yield* reply(frame.method === "thread/unsubscribe" ? { status: "unsubscribed" } : {});
              yield* emit({ method: "thread/closed", params: { threadId: thread.id } });
              return;
            }
            if (frame.method === "turn/start") {
              const cwd = typeof params.cwd === "string" ? params.cwd : thread.cwd;
              if (!cwd.startsWith(root + "/") || !Array.isArray(params.input))
                return yield* Effect.die(
                  "Codex turn/start omitted input or escaped the owned workspace",
                );
              thread.cwd = cwd;
              if (typeof params.model === "string") thread.model = params.model;
              const turn: RawTurn = {
                id: "acceptance-turn-" + ++turnIds,
                items: [],
                itemsView: "notLoaded",
                status: "inProgress",
                error: null,
                startedAt: yield* nowSeconds,
                completedAt: null,
                durationMs: null,
              };
              thread.turns.push(turn);
              yield* reply({ turn });
              yield* emit({ method: "turn/started", params: { threadId: thread.id, turn } });
              yield* Queue.offer(rawStarted, { thread, turn });
              return;
            }
            if (frame.method === "turn/interrupt") {
              yield* reply({});
              const turn = thread.turns.find(
                (value) => value.id === params.turnId && value.status === "inProgress",
              );
              if (turn) yield* finish({ thread, turn }, "interrupted");
              return;
            }
            if (frame.method === "turn/steer") {
              const active = thread.turns.findLast((turn) => turn.status === "inProgress");
              if (!active || active.id !== params.expectedTurnId) {
                yield* emit({
                  id: frame.id,
                  error: {
                    code: -32600,
                    message: "expectedTurnId does not identify the active turn",
                  },
                });
                return;
              }
              yield* reply({ turnId: active.id });
              return;
            }
            return yield* Effect.die("Unhandled outbound Codex app-server method: " + frame.method);
          });
        const stdio = Stdio.make({
          args: Effect.succeed([]),
          stdin: Stream.fromQueue(incoming),
          stdout: () =>
            Sink.forEach((chunk: string | Uint8Array) =>
              Effect.gen(function* () {
                remainder +=
                  typeof chunk === "string" ? chunk : decoder.decode(chunk, { stream: true });
                const lines = remainder.split("\n");
                remainder = lines.pop()!;
                for (const line of lines) {
                  if (!line.trim()) continue;
                  const frame = yield* decodeOutboundFrame(line).pipe(Effect.orDie);
                  yield* processFrame(frame).pipe(
                    Effect.tapCause(() =>
                      Queue.fail(
                        started,
                        new Error("The isolated Codex NDJSON peer rejected " + frame.method),
                      ),
                    ),
                  );
                }
              }),
            ),
          stderr: () => Sink.drain,
        });
        return yield* CodexClient.make(stdio);
      }),
  };
  const registryLayer = Registry.layerFromAdaptersEffect(
    Effect.gen(function* () {
      const adapter = yield* CodexAdapter.makeCodexAdapterV2({
        instanceId: CodexAdapter.CODEX_DEFAULT_INSTANCE_ID,
        settings,
        environment: {},
        clientFactory,
        digest: yield* Digest.Digest,
        fileSystem: yield* FileSystem.FileSystem,
        idAllocator: yield* IdAllocator.IdAllocatorV2,
        serverConfig: yield* Config.ServerConfig,
      });
      return [adapter];
    }),
  ).pipe(
    Layer.provide(
      Layer.mergeAll(
        config.pipe(Layer.orDie),
        NodeServices.layer,
        NodeCrypto.layer,
        IdAllocator.layer,
      ),
    ),
  );
  return {
    registryLayer,
    started,
    attach: (threads: Threads.ThreadManagementService["Service"]) =>
      Stream.fromQueue(rawStarted).pipe(
        Stream.runForEach((raw) =>
          Effect.gen(function* () {
            yield* threads
              .streamStoredEventsFrom({ threadId: raw.thread.appThreadId, afterSequence: 0 })
              .pipe(
                Stream.filter(
                  (row) =>
                    row.event.type === "provider-turn.updated" &&
                    row.event.payload.nativeTurnRef?.nativeId === raw.turn.id,
                ),
                Stream.take(1),
                Stream.runDrain,
              );
            const projection = yield* threads.getThreadProjection(raw.thread.appThreadId);
            const providerTurn = projection.providerTurns.find(
              (turn) => turn.nativeTurnRef?.nativeId === raw.turn.id,
            )!;
            const attempt = projection.attempts.find(
              (value) => value.id === providerTurn.runAttemptId,
            )!;
            const run = projection.runs.find((value) => value.id === attempt.runId)!;
            const providerThread = projection.providerThreads.find(
              (value) => value.id === providerTurn.providerThreadId,
            )!;
            const message = projection.messages.find((value) => value.id === run.userMessageId)!;
            observed.set(run.id, raw);
            yield* Queue.offer(started, {
              threadId: raw.thread.appThreadId,
              runId: run.id,
              modelSelection: run.modelSelection,
              appThread: projection.thread,
              providerThread,
              message,
            });
            if (automatic.has(raw.thread.appThreadId)) yield* finish(raw, "completed");
          }).pipe(Effect.orDie),
        ),
        Effect.forkScoped,
      ),
    complete: (
      runId: string,
      check?: { command: string; cwd: string; output: string; exitCode: number },
    ) =>
      Effect.gen(function* () {
        const raw = observed.get(runId);
        if (!raw)
          return yield* Effect.die(
            "The real Codex adapter has not persisted this run's native turn",
          );
        yield* finish(raw, "completed", check);
      }),
    autoComplete: (threadId: string) => {
      automatic.add(threadId);
    },
    opens: () => opens,
  };
});
