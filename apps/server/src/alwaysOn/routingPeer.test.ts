import { expect, it } from "@effect/vitest";
import {
  ALWAYS_ON_PEER_EVENT_TOOL,
  EnvironmentId,
  ThreadId,
  type OrchestrationV2LinkOrigin,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import { McpSchema, McpServer } from "effect/ai";

import * as McpHttpServer from "../mcp/McpHttpServer.ts";
import * as McpInvocationContext from "../mcp/McpInvocationContext.ts";
import { ThreadManagementService } from "../orchestration-v2/ThreadManagementService.ts";
import { AlwaysOnRouting } from "./AlwaysOnRouting.ts";

const environmentId = EnvironmentId.make("environment:target");
const clientInfo = { name: "always-on-peer", version: "1" };
const mcpClient = McpSchema.McpServerClient.of({
  clientId: 1,
  protocolVersion: "2025-06-18",
  clientCapabilities: {},
  clientInfo,
  initializePayload: { protocolVersion: "2025-06-18", capabilities: {}, clientInfo },
  getClient: Effect.die("unused"),
});

const call = (linked: boolean, access: McpInvocationContext.McpClientCaller["access"]) =>
  McpServer.McpServer.pipe(
    Effect.flatMap((server) =>
      server.callTool({
        name: ALWAYS_ON_PEER_EVENT_TOOL,
        arguments: {
          environmentId,
          originEnvironmentId: "environment:source",
          deliveryId: "fixture-delivery",
          harness: "claude",
          effort: "high",
          source: "fixture",
          sourceEventId: "external-id",
          text: "Summarize the build.",
        },
      }),
    ),
    Effect.provideService(McpInvocationContext.McpInvocationContext, {
      environmentId,
      requestNamespace: "client:fixture-peer",
      thread: undefined,
      client: { sessionId: "fixture-peer", label: "Fixture origin", linked, access },
      capabilities: new Set<McpInvocationContext.McpCapability>(["orchestration"]),
      issuedAt: 0,
    }),
    Effect.provideService(McpSchema.McpServerClient, mcpClient),
  );

it.effect(
  "receives only writable linked clients and stamps the launch with the link's identity",
  () =>
    Effect.gen(function* () {
      const calls = yield* Ref.make<ReadonlyArray<OrchestrationV2LinkOrigin>>([]);
      const receipt = {
        environmentId,
        threadId: ThreadId.make("always-on-event-fixture"),
        duplicate: false,
      };
      const routing = Layer.mock(AlwaysOnRouting)({
        receive: (_event, link) =>
          Ref.update(calls, (items) => [...items, link]).pipe(Effect.as(receipt)),
      });
      yield* Effect.gen(function* () {
        const unlinked = yield* call(false, "full-access").pipe(Effect.result);
        const readOnly = yield* call(true, "read-only").pipe(Effect.result);
        for (const refused of [unlinked, readOnly]) {
          if (refused._tag === "Success") expect(refused.success.isError).toBe(true);
        }
        expect(yield* Ref.get(calls)).toEqual([]);

        const accepted = yield* call(true, "approval-required");
        expect(accepted.isError).toBe(false);
        expect(accepted.structuredContent).toEqual(receipt);
        expect(yield* Ref.get(calls)).toEqual([
          { sessionId: "fixture-peer", label: "Fixture origin", access: "approval-required" },
        ]);
      }).pipe(
        Effect.provide(
          McpHttpServer.layerAlwaysOnPeer.pipe(
            Layer.provideMerge(McpHttpServer.layerInProcessMcpServer),
            Layer.provide(routing),
            Layer.provide(Layer.mock(ThreadManagementService)({})),
          ),
        ),
      );
    }),
);
