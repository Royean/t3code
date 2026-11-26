import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { expect, it } from "@effect/vitest";
import {
  CommandId,
  NodeId,
  ProjectId,
  ProviderInstanceId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Effect from "effect/Effect";
import * as Exit from "effect/Exit";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlError from "effect/sql/SqlError";
import * as Orchestrator from "../orchestration-v2/Orchestrator.ts";
import * as Threads from "../orchestration-v2/ThreadManagementService.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as Queue from "./WorkbenchQueue.ts";
import * as Guard from "./WorkbenchQueueGuard.ts";
import * as WorkItems from "./WorkbenchWorkItems.ts";

it.effect("retains database failures when queue reads or authorization fail", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const queue = yield* Queue.WorkbenchQueue;
    const guard = yield* Guard.WorkbenchQueueGuard;
    const parent = ThreadId.make("thread:queue-failure");
    const projectId = ProjectId.make("project:queue-failure");
    yield* sql`INSERT INTO workbench_queues VALUES('queue','controller',${projectId})`;
    yield* sql`INSERT INTO workbench_queue_bindings VALUES('task','2026-11-26T00:00:00.000Z','queue',${projectId},${parent})`;
    yield* sql`DROP TABLE workbench_queue_entries`;
    const failure = yield* queue
      .get("queue", { controllerId: "controller", projectId })
      .pipe(Effect.flip);
    expect(failure.reason).toBe("unavailable");
    expect(failure.cause).toBeInstanceOf(SqlError.SqlError);
    expect(Object.keys(failure)).not.toContain("cause");

    const exit = yield* guard
      .assertCommand({
        type: "delegated_task.request",
        commandId: CommandId.make("queue:failed-authorization"),
        parentThreadId: parent,
        parentRunId: RunId.make("run:parent"),
        parentNodeId: NodeId.make("node:parent"),
        task: "Queue work",
        modelSelection: { instanceId: ProviderInstanceId.make("codex"), model: "codex" },
        runtimeMode: "full-access",
        interactionMode: "default",
        createdBy: "agent",
        creationSource: "mcp",
      })
      .pipe(
        Effect.provideService(Guard.WorkbenchQueuePermit, {
          workItemId: "item",
          claimId: "claim",
        }),
        Effect.exit,
      );
    expect(Exit.isFailure(exit)).toBe(true);
    if (Exit.isFailure(exit)) expect(Cause.squash(exit.cause)).toBeInstanceOf(SqlError.SqlError);
  }).pipe(
    Effect.provide(
      Layer.mergeAll(Queue.layer, Guard.layer).pipe(
        Layer.provide([
          Layer.mock(WorkItems.WorkbenchWorkItems)({}),
          Layer.mock(Orchestrator.OrchestratorV2)({}),
          Layer.mock(Threads.ThreadManagementService)({}),
        ]),
        Layer.provideMerge(SqlitePersistence.layerMemory),
        Layer.provide(NodeCrypto.layer),
      ),
    ),
  ),
);
