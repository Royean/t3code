import { expect, it } from "@effect/vitest";
import {
  CommandId,
  DEFAULT_SERVER_SETTINGS,
  EnvironmentId,
  type EventOrchestrationJobDetail,
  NodeId,
  type OrchestratorMcpDelegateTaskResult,
  ProviderInstanceId,
  ThreadId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import { HttpRouter } from "effect/http";

import * as EventOrchestrationService from "../../../eventOrchestration/EventOrchestrationService.ts";
import * as PeerForwarding from "../../../peer/PeerForwarding.ts";
import * as PeerLinkRequests from "../../../peer/PeerLinkRequests.ts";
import * as PeerLinks from "../../../peer/PeerLinks.ts";
import * as RemoteDelegation from "../../../peer/RemoteDelegation.ts";
import * as ServerSettings from "../../../serverSettings.ts";
import * as McpHttpServer from "../../McpHttpServer.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpSessionRegistry from "../../McpSessionRegistry.ts";
import * as McpToolAccessTestkit from "../../McpToolAccess.testkit.ts";
import * as OrchestratorMcpService from "../../OrchestratorMcpService.ts";
import * as ThreadMetadataMcpService from "../../ThreadMetadataMcpService.ts";
import * as OrchestratorHandlers from "../orchestrator/handlers.ts";
import { OrchestratorToolkit } from "../orchestrator/tools.ts";

const environmentId = EnvironmentId.make("environment:event-registration");
const ordinaryThreadId = ThreadId.make("thread:ordinary-registration");
const coordinatorThreadId = ThreadId.make("thread:coordinator-registration");
const eventTools = ["event_task_assign", "event_task_read", "event_task_report"];

const scopeFor = (threadId?: ThreadId): McpInvocationContext.McpInvocationScope => ({
  environmentId,
  requestNamespace: threadId ?? "outside-registration",
  issuedAt: 1,
  capabilities: new Set(["orchestration"]),
  thread:
    threadId === undefined
      ? undefined
      : {
          threadId,
          providerSessionId: threadId,
          providerInstanceId: ProviderInstanceId.make("codex"),
        },
  client:
    threadId === undefined
      ? { sessionId: "outside-registration", label: "Outside client", access: "full-access" }
      : undefined,
});

const job: EventOrchestrationJobDetail = {
  id: "event:registration",
  coordinatorThreadId,
  coordinatorCommandId: CommandId.make("event:registration:launch"),
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
    deliveryId: "registration",
    repository: "acme/app",
    eventType: "issues.ready",
    text: "Inspect the registration boundary.",
  },
};

const delegated: OrchestratorMcpDelegateTaskResult = {
  taskId: NodeId.make("task:ordinary-registration"),
  childThreadId: ThreadId.make("thread:ordinary-child"),
  childRunId: null,
  childNodeId: NodeId.make("node:ordinary-child"),
  status: "starting",
  workState: "working",
  hasPendingChildRuns: false,
  latestTerminalRunId: null,
  latestTerminalStatus: null,
  latestTerminalSummary: null,
  latestTerminalResultContextTransferId: null,
  providerInstanceId: ProviderInstanceId.make("codex"),
  model: "replay-test",
  summary: null,
  resultContextTransferId: null,
  hasPendingQuestion: false,
  pendingApproval: null,
  waitTimedOut: false,
};

interface RpcResponse {
  readonly result?: {
    readonly tools?: ReadonlyArray<{ readonly name: string }>;
    readonly isError?: boolean;
    readonly structuredContent?: unknown;
    readonly content?: ReadonlyArray<{ readonly text?: string }>;
  };
  readonly error?: unknown;
}

// Exercise the actual authenticated MCP transport without opening a listening socket.
it.live.each([false, true])(
  "isolates event tool registration from ordinary MCP sessions (enabled: %s)",
  (enabled) =>
    Effect.gen(function* () {
      const reads: Array<ThreadId> = [];
      const delegates: Array<ThreadId | undefined> = [];
      const scopes = new Map([
        ["ordinary", scopeFor(ordinaryThreadId)],
        ["coordinator", scopeFor(coordinatorThreadId)],
        ["outside", scopeFor()],
      ]);
      const registrations = Layer.mergeAll(
        McpHttpServer.toolkitRegistration(OrchestratorToolkit, OrchestratorHandlers.layer),
        McpHttpServer.layerEventOrchestrationToolkit,
        McpHttpServer.layerToolSearch,
      );
      const server = McpHttpServer.layerMcpTransport(registrations).pipe(
        Layer.provide(McpToolAccessTestkit.liveThreadsLayer),
        Layer.provide(
          Layer.mock(EventOrchestrationService.EventOrchestrationService)({
            ownsThread: (threadId) => enabled && threadId === coordinatorThreadId,
            getForCaller: (threadId) =>
              Effect.sync(() => {
                reads.push(threadId);
                return job;
              }),
          }),
        ),
        Layer.provide(
          Layer.mock(OrchestratorMcpService.OrchestratorMcpService)({
            listThreads: (scope) =>
              Effect.succeed({
                projectId: null,
                currentThreadId: scope.thread?.threadId ?? null,
                threads: [],
                nextCursor: null,
                total: 0,
              }),
            delegateTask: (scope) =>
              Effect.sync(() => {
                delegates.push(scope.thread?.threadId);
                return delegated;
              }),
          }),
        ),
        Layer.provide(
          Layer.mergeAll(
            Layer.mock(ThreadMetadataMcpService.ThreadMetadataMcpService)({}),
            Layer.mock(PeerForwarding.PeerForwarding)({}),
            Layer.mock(PeerLinkRequests.PeerLinkRequests)({}),
            Layer.mock(PeerLinks.PeerLinks)({}),
            Layer.mock(RemoteDelegation.RemoteDelegation)({}),
            Layer.mock(ServerSettings.ServerSettingsService)({
              getSettings: Effect.succeed(DEFAULT_SERVER_SETTINGS),
            }),
            Layer.mock(McpSessionRegistry.McpSessionRegistry)({
              resolve: (token) => {
                const scope = scopes.get(token);
                return Effect.succeed(
                  scope?.thread === undefined ? undefined : { ...scope, thread: scope.thread },
                );
              },
            }),
            Layer.succeed(McpHttpServer.McpClientAuthenticator, {
              authenticate: (request) =>
                Effect.succeed(
                  request.headers.authorization === "Bearer outside"
                    ? scopes.get("outside")
                    : undefined,
                ),
            }),
          ),
        ),
      );
      yield* Effect.acquireUseRelease(
        Effect.sync(() => HttpRouter.toWebHandler(server, { disableLogger: true })),
        (web) =>
          Effect.promise(async () => {
            const session = async (token: string) => {
              const headers = {
                accept: "application/json, text/event-stream",
                "content-type": "application/json",
                authorization: `Bearer ${token}`,
              };
              const initialize = await web.handler(
                new Request("http://event.test/mcp", {
                  method: "POST",
                  headers,
                  body: JSON.stringify({
                    jsonrpc: "2.0",
                    id: 1,
                    method: "initialize",
                    params: {
                      protocolVersion: "2025-06-18",
                      capabilities: {},
                      clientInfo: { name: "event-registration-test", version: "1.0.0" },
                    },
                  }),
                }),
              );
              expect(initialize.status).toBe(200);
              await initialize.text();
              const sessionId = initialize.headers.get("mcp-session-id");
              expect(sessionId).toBeTruthy();
              const rpc = async (method: string, params: object): Promise<RpcResponse> => {
                const response = await web.handler(
                  new Request("http://event.test/mcp", {
                    method: "POST",
                    headers: {
                      ...headers,
                      "mcp-session-id": sessionId!,
                      "mcp-protocol-version": "2025-06-18",
                    },
                    body: JSON.stringify({ jsonrpc: "2.0", id: 2, method, params }),
                  }),
                );
                expect(response.status).toBe(200);
                const text = await response.text();
                // tool_search can send tools/list_changed before its answer over SSE.
                const messages = text
                  .split("\n")
                  .filter((line) => line.startsWith("data: ") || line.startsWith("{"))
                  .map(
                    (line) =>
                      JSON.parse(line.replace(/^data: /u, "")) as RpcResponse & { id?: number },
                  );
                const result = messages.find((message) => message.id === 2);
                expect(result).toBeDefined();
                return result!;
              };
              return {
                list: async () =>
                  (await rpc("tools/list", {})).result!.tools!.map((tool) => tool.name),
                call: (name: string, args: object = {}) =>
                  rpc("tools/call", { name, arguments: args }),
              };
            };

            const ordinary = await session("ordinary");
            const outside = await session("outside");
            const coordinator = await session("coordinator");
            for (const [caller, threadId] of [
              [ordinary, ordinaryThreadId],
              [outside, null],
            ] as const) {
              const listed = await caller.list();
              expect(listed).toContain("t3_thread_list");
              expect(listed.filter((name) => name.startsWith("event_task_"))).toEqual([]);
              expect((await caller.call("t3_thread_list")).result).toMatchObject({
                structuredContent: { currentThreadId: threadId, threads: [], total: 0 },
              });
              const hiddenRead = await caller.call("event_task_read");
              expect(hiddenRead.result?.structuredContent).toBeUndefined();
              expect(hiddenRead.error ?? hiddenRead.result?.isError).toBeTruthy();
            }
            expect(
              (await ordinary.call("delegate_task", { task: "An ordinary task." })).result,
            ).toMatchObject({ structuredContent: { taskId: delegated.taskId }, isError: false });
            expect(
              (await ordinary.call("tool_search", { query: "event_task" })).result,
            ).toMatchObject({ structuredContent: { tools: [] } });
            expect(reads).toEqual([]);

            const coordinatorTools = (await coordinator.list()).filter((name) =>
              name.startsWith("event_task_"),
            );
            expect(coordinatorTools.sort()).toEqual(enabled ? eventTools : []);
            const search = await coordinator.call("tool_search", { query: "event_task" });
            const found = search.result?.structuredContent as {
              readonly tools: ReadonlyArray<{ readonly name: string }>;
            };
            expect(found.tools.map((tool) => tool.name).sort()).toEqual(enabled ? eventTools : []);
            expect((await coordinator.call("t3_thread_list")).result).toMatchObject({
              structuredContent: { currentThreadId: coordinatorThreadId, threads: [], total: 0 },
              isError: false,
            });
            const handoff = await coordinator.call("delegate_task", {
              task: "Bypass role admission.",
            });
            if (enabled) {
              expect((await coordinator.call("event_task_read")).result).toMatchObject({
                structuredContent: { id: job.id },
                isError: false,
              });
              expect(handoff.result).toMatchObject({
                isError: true,
                content: [{ text: expect.stringContaining("event_task_assign") }],
              });
              expect(reads).toEqual([coordinatorThreadId]);
              expect(delegates).toEqual([ordinaryThreadId]);
            } else {
              expect(handoff.result).toMatchObject({
                structuredContent: { taskId: delegated.taskId },
                isError: false,
              });
              expect(reads).toEqual([]);
              expect(delegates).toEqual([ordinaryThreadId, coordinatorThreadId]);
            }
          }),
        (web) => Effect.promise(() => web.dispose()),
      );
    }),
);
