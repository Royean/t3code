import type {
  OrchestrationV2Run,
  OrchestrationV2Subagent,
  OrchestrationV2ThreadProjection,
} from "@t3tools/contracts";
import { delegatedTaskProgress } from "@t3tools/provider-core/server/subagentProjection";
import {
  delegatedTaskAwaitsWork,
  delegatedTaskResultRun,
  delegatedTaskRuns,
  runOutcome,
} from "./RestartBackgroundNote.ts";

type Projection = Pick<
  OrchestrationV2ThreadProjection,
  "runs" | "contextTransfers" | "messages" | "subagents" | "providerThreads"
>;

export function delegatedTaskRun(
  child: Pick<Projection, "runs" | "contextTransfers">,
  task: OrchestrationV2Subagent,
): OrchestrationV2Run | undefined {
  const owned = child.runs.find((run) => run.delegatedTaskId === task.id);
  if (owned !== undefined) return owned;
  const spawn = child.contextTransfers.find(
    (transfer) =>
      transfer.type === "subagent_spawn" &&
      transfer.sourceThreadId === task.threadId &&
      transfer.targetThreadId === task.childThreadId,
  );
  return spawn === undefined
    ? child.runs[0]
    : spawn.targetRunId === null
      ? undefined
      : child.runs.find((run) => run.id === spawn.targetRunId);
}

export function delegatedTaskWorkProgress(child: Projection, task: OrchestrationV2Subagent) {
  const childRun = delegatedTaskRun(child, task);
  const progress = delegatedTaskProgress(child);
  const followupRuns =
    childRun?.delegatedTaskId === task.id ? delegatedTaskRuns(child, task.id) : undefined;
  const taskResultRun =
    followupRuns === undefined ? progress.resultRun : delegatedTaskResultRun(followupRuns);
  const terminal =
    taskResultRun !== undefined &&
    ["completed", "failed", "cancelled", "interrupted", "rolled_back"].includes(
      runOutcome(taskResultRun),
    );
  const taskWorkState =
    followupRuns === undefined
      ? progress.state
      : !terminal
        ? ("working" as const)
        : delegatedTaskAwaitsWork(followupRuns, child)
          ? ("waiting_for_children" as const)
          : ("result_available" as const);
  return { childRun, taskResultRun, taskWorkState };
}
