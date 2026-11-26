import {
  WorkbenchDispatchInput,
  WorkbenchDispatchResult,
  OrchestratorMcpFailure,
} from "@t3tools/contracts";
import * as ThreadManagementService from "../../../orchestration-v2/ThreadManagementService.ts";
import { McpSchema, Tool, Toolkit } from "effect/ai";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as WorkbenchDispatch from "../../../scheduledTasks/WorkbenchDispatch.ts";

const WorkbenchDispatchTool = Tool.make("t3_workbench_dispatch", {
  description:
    'Required input: {taskId: "<scheduled-task ID from dispatcher instructions>"}; never call with {}. Dispatch one persisted workbench event from this task’s fixed dispatcher. Pass taskId; omit businessDeliveryId to claim its next priority/FIFO reservation. The service enforces two occupied work slots per account/project queue. The server fixes identity, data, inherited model/permissions and isolated worktree. remaining counts only immediately claimable items; zero means end your turn and wait for a queue wake, never bypass a full queue with ordinary delegation. Drain reservations one call at a time, then end your turn; native completion, questions and approval blockers wake you. Never poll children or use delegate_task to retry an unknown dispatch.',
  parameters: WorkbenchDispatchInput,
  success: WorkbenchDispatchResult,
  failure: OrchestratorMcpFailure,
  failureMode: "return",
  dependencies: [
    McpInvocationContext.McpInvocationContext,
    ThreadManagementService.ThreadManagementService,
    WorkbenchDispatch.WorkbenchDispatch,
  ],
})
  .annotate(Tool.Destructive, true)
  .annotate(Tool.Idempotent, true)
  .annotate(McpSchema.EnabledWhen, McpInvocationContext.hiddenFromOutsideClients);

export const WorkbenchToolkit = Toolkit.make(WorkbenchDispatchTool);
