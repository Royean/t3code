import {
  EventOrchestrationAssignInput,
  EventOrchestrationJobDetail,
  EventOrchestrationReportInput,
  EventOrchestrationTask,
  OrchestratorMcpFailure,
  TrimmedNonEmptyString,
} from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { McpSchema, Tool, Toolkit } from "effect/ai";

import * as EventOrchestrationService from "../../../eventOrchestration/EventOrchestrationService.ts";
import * as ThreadManagementService from "../../../orchestration-v2/ThreadManagementService.ts";
import * as McpInvocationContext from "../../McpInvocationContext.ts";

const shared = {
  failure: OrchestratorMcpFailure,
  failureMode: "return" as const,
  dependencies: [
    EventOrchestrationService.EventOrchestrationService,
    ThreadManagementService.ThreadManagementService,
    McpInvocationContext.McpInvocationContext,
  ],
};

const Assign = Tool.make("event_task_assign", {
  ...shared,
  description:
    "Delegate a logical task for your managed event to a configured role. Reuse taskId on retry. dependsOn lists tasks that must be accepted first. The server chooses only allowlisted executors and enforces attempt limits.",
  parameters: EventOrchestrationAssignInput,
  success: EventOrchestrationTask,
})
  .annotate(Tool.Destructive, true)
  .annotate(Tool.Idempotent, true)
  .annotate(McpSchema.EnabledWhen, McpInvocationContext.hiddenFromOutsideClients);

const Report = Tool.make("event_task_report", {
  ...shared,
  description:
    "Report progress, evidence, failure or a blocker for your managed event attempt. Only its coordinator may accept results, request safe retry or complete the job. Agent completion alone awaits acceptance; never report verification you did not perform.",
  parameters: Schema.Struct({ report: EventOrchestrationReportInput }),
  success: EventOrchestrationJobDetail,
})
  .annotate(Tool.Destructive, true)
  .annotate(McpSchema.EnabledWhen, McpInvocationContext.hiddenFromOutsideClients);

const Read = Tool.make("event_task_read", {
  ...shared,
  description:
    "Read your managed event, role tasks, current attempts and saved progress. Only the associated coordinator or a current task agent can read it. Completion wakes the coordinator; do not poll running work.",
  parameters: Schema.Struct({ jobId: Schema.optional(TrimmedNonEmptyString) }),
  success: EventOrchestrationJobDetail,
})
  .annotate(Tool.Readonly, true)
  .annotate(Tool.Destructive, false)
  .annotate(McpSchema.EnabledWhen, McpInvocationContext.hiddenFromOutsideClients);

export const EventOrchestrationToolkit = Toolkit.make(Assign, Report, Read);
