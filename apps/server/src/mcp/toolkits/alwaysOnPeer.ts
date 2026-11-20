import {
  ALWAYS_ON_PEER_EVENT_TOOL,
  AlwaysOnEventReceipt,
  AlwaysOnPeerEvent,
  OrchestratorMcpFailure,
  type AlwaysOnRoutingError,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import { McpSchema, Tool, Toolkit } from "effect/ai";

import { AlwaysOnRouting } from "../../alwaysOn/AlwaysOnRouting.ts";
import { ThreadManagementService } from "../../orchestration-v2/ThreadManagementService.ts";
import * as McpInvocationContext from "../McpInvocationContext.ts";
import * as McpToolAccess from "../McpToolAccess.ts";

const canReceive = (scope: McpInvocationContext.McpInvocationScope | undefined) =>
  scope?.thread === undefined &&
  scope?.client?.linked === true &&
  scope.client.access !== "read-only" &&
  scope.linkedEnvironments !== false;

const receiveAlwaysOnEvent = Tool.make(ALWAYS_ON_PEER_EVENT_TOOL, {
  description: "Accept a routed external event on this linked environment.",
  parameters: AlwaysOnPeerEvent,
  success: AlwaysOnEventReceipt,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies: [
    AlwaysOnRouting,
    McpInvocationContext.McpInvocationContext,
    ThreadManagementService,
  ],
})
  .annotate(Tool.Destructive, true)
  .annotate(Tool.Idempotent, true)
  .annotate(McpSchema.EnabledWhen, () => {
    const fiber = Fiber.getCurrent();
    return canReceive(
      fiber && Context.getOrUndefined(fiber.context, McpInvocationContext.McpInvocationContext),
    );
  });

export const AlwaysOnPeerToolkit = Toolkit.make(receiveAlwaysOnEvent);

const asToolError = ({ reason, message }: AlwaysOnRoutingError) =>
  new OrchestratorMcpFailure({
    code:
      reason === "forbidden"
        ? "capability_denied"
        : reason === "unavailable"
          ? "orchestration_error"
          : reason === "conflict"
            ? "event_route_conflict"
            : "invalid_request",
    message,
  });

export const layer = McpToolAccess.toLayer(AlwaysOnPeerToolkit, {
  [ALWAYS_ON_PEER_EVENT_TOOL]: McpToolAccess.startsThreads(
    () => ({ runtimeMode: "approval-required", interactionMode: "default" }),
    Effect.fnUntraced(function* (
      input: AlwaysOnPeerEvent,
      { linkOrigin }: McpToolAccess.StartedModes,
    ) {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      if (!canReceive(scope) || linkOrigin === undefined) {
        return yield* new OrchestratorMcpFailure({
          code: "capability_denied",
          message: "Only an authorized linked environment can deliver an Always On event.",
        });
      }
      const routing = yield* AlwaysOnRouting;
      return yield* routing.receive(input, linkOrigin).pipe(Effect.mapError(asToolError));
    }),
    "stamped",
  ),
});
