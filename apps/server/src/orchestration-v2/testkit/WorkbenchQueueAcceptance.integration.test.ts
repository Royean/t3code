import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { CommandId, ScheduledTaskId, ThreadId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Context from "effect/Context";
import * as Queue from "effect/Queue";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as FileSystem from "effect/FileSystem";
import * as Launch from "../ThreadLaunchService.ts";
import * as Dispatch from "../../scheduledTasks/WorkbenchDispatch.ts";
import * as NativeQueue from "../../scheduledTasks/WorkbenchQueue.ts";
import * as Repairs from "../../scheduledTasks/WorkbenchRepairs.ts";
import * as Reactor from "../../scheduledTasks/WorkbenchRepairReactor.ts";
import * as Attention from "../../scheduledTasks/WorkbenchAttention.ts";
import * as Items from "../../scheduledTasks/WorkbenchWorkItems.ts";
import * as ProcessRunner from "../../processRunner.ts";
import type { WorkbenchStartedTurn } from "./WorkbenchScriptedProvider.ts";
import { releaseHash } from "../../scheduledTasks/workbenchReleaseProfile.ts";
import {
  acquireAcceptanceFixture,
  startWorkbench,
  nativeJson,
  dispatcherId,
  projectId,
  secondaryProjectId,
  taskId,
  selection,
} from "./WorkbenchAcceptanceFixture.ts";

// quarantine: #19 A companion Workbench checkout is an explicit acceptance prerequisite.
it.live.skipIf(!process.env.WORKBENCH_ACCEPTANCE_REPO)(
  "composes real fanout, matter batches, two slots, repair exhaustion and human recovery",
  () =>
    Effect.gen(function* () {
      const f = yield* acquireAcceptanceFixture(),
        wb = yield* startWorkbench(f),
        api = wb.api;
      const launches = Context.get(f.services, Launch.ThreadLaunchService);
      const secondaryThread = ThreadId.make("thread:acceptance-secondary"),
        secondaryTask = ScheduledTaskId.make("task:acceptance-secondary");
      yield* launches.launch({
        commandId: CommandId.make("acceptance:secondary"),
        threadId: secondaryThread,
        projectId: secondaryProjectId,
        title: "Secondary dispatcher",
        workspaceStrategy: { type: "root" },
        modelSelection: selection,
        runtimeMode: "full-access",
        interactionMode: "default",
        createdBy: "user",
        creationSource: "web",
      });
      yield* f.tasks.upsert({
        id: secondaryTask,
        title: "Secondary webhook",
        prompt: "Process {{body.deliveryId}}",
        enabled: true,
        schedule: { type: "webhook", dedupe: { replayKey: "{{body.deliveryId}}" } },
        projectId: secondaryProjectId,
        threadId: secondaryThread,
        threadMode: "continue",
        workspaceStrategy: { type: "root" },
        modelSelection: selection,
        runtimeMode: "full-access",
        interactionMode: "default",
        createdBy: "user",
        creationSource: "web",
      });
      yield* wb.call(api + "/environments/acceptance", {
        method: "PUT",
        body: { name: "Shared isolated native project" },
      });
      yield* wb.call(api + "/environments/ingress", {
        method: "PUT",
        body: { name: "Synthetic source boundary" },
      });
      yield* wb.call(api + "/t3-connections/native", {
        method: "PUT",
        body: {
          name: "Real native",
          environmentId: "acceptance",
          origin: f.origin,
          credential: f.credential,
        },
      });
      const repairChecks = [
        {
          scriptId: "check",
          definitionHash: releaseHash(
            JSON.stringify({
              id: "check",
              command: f.scripts.find((script) => script.id === "check")!.command,
            }),
          ),
        },
      ];
      for (const [workerId, threadId, configuredTask] of [
        ["primary", dispatcherId, taskId],
        ["secondary", secondaryThread, secondaryTask],
      ] as const) {
        yield* wb.call(api + "/t3-subscriptions/" + workerId, {
          method: "PUT",
          body: {
            name: workerId,
            connectionId: "native",
            projectId: workerId === "primary" ? projectId : secondaryProjectId,
            threadId,
            taskId: configuredTask,
            repairChecks,
          },
        });
        yield* wb.call(api + "/rules/" + workerId, {
          method: "PUT",
          body: {
            name: workerId,
            environmentId: "acceptance",
            workerId,
            priority: 10,
            enabled: true,
            conditions: [
              { field: "event.type", operator: "eq", value: "issues.ready" },
              ...(workerId === "secondary"
                ? [{ field: "event.subject", operator: "eq", value: "fixture/project#1" }]
                : []),
            ],
          },
        });
      }
      const source = (yield* wb.call(api + "/sources", {
        method: "POST",
        body: { name: "Canonical GitHub fixture", environmentId: "ingress" },
      })).body;
      for (const id of ["primary", "secondary"])
        yield* wb.call(api + "/dispatch-subscriptions/" + id, {
          method: "PUT",
          body: {
            name: id,
            sourceId: source.source.id,
            environmentId: "acceptance",
            ruleIds: [id],
            enabled: true,
          },
        });
      const input = (id: string, issue: number) => ({
        id,
        type: "issues.ready",
        subject: `fixture/project#${issue}`,
        summary: "Fixture requirement " + id,
        payload: {
          repository: {
            id: 42,
            full_name: "fixture/project",
            html_url: "https://github.com/fixture/project",
          },
          issue: {
            id: 300 + issue,
            number: issue,
            html_url: `https://github.com/fixture/project/issues/${issue}`,
          },
        },
      });
      const receive = (id: string, issue: number) =>
        wb
          .call("/api/event-hub/in/" + source.source.id, {
            method: "POST",
            machine: true,
            headers: { authorization: "Bearer " + source.token },
            body: input(id, issue),
          })
          .pipe(Effect.map((value) => value.body));
      const first = yield* receive("queue:first", 1);
      assert.equal(first.deliveries.length, 2);
      const rootOne = yield* Queue.take(f.provider.started),
        rootTwo = yield* Queue.take(f.provider.started);
      assert.deepEqual(
        new Set([rootOne.threadId, rootTwo.threadId]),
        new Set([dispatcherId, secondaryThread]),
      );
      const firstPrimary = first.deliveries.find(
        (delivery: { subscriptionId: string }) => delivery.subscriptionId === "primary",
      )!;
      const firstSecondary = first.deliveries.find(
        (delivery: { subscriptionId: string }) => delivery.subscriptionId === "secondary",
      )!;
      const duplicate = yield* receive("queue:first", 1);
      assert.isTrue(duplicate.duplicate);
      assert.deepEqual(
        duplicate.deliveries.map((row: { deliveryId: string }) => row.deliveryId).sort(),
        first.deliveries.map((row: { deliveryId: string }) => row.deliveryId).sort(),
      );
      const second = yield* receive("queue:second", 2);
      yield* f.threads.streamStoredEventsFrom({ threadId: dispatcherId, afterSequence: 0 }).pipe(
        Stream.filter(
          (row) =>
            row.event.type === "message.updated" &&
            row.event.payload.text.includes(second.deliveries[0].deliveryId),
        ),
        Stream.take(1),
        Stream.runDrain,
      );
      const third = yield* receive("queue:third", 3);
      yield* f.threads.streamStoredEventsFrom({ threadId: dispatcherId, afterSequence: 0 }).pipe(
        Stream.filter(
          (row) =>
            row.event.type === "message.updated" &&
            row.event.payload.text.includes(third.deliveries[0].deliveryId),
        ),
        Stream.take(1),
        Stream.runDrain,
      );
      const items = Context.get(f.services, Items.WorkbenchWorkItems),
        queue = Context.get(f.services, NativeQueue.WorkbenchQueue),
        dispatch = Context.get(f.services, Dispatch.WorkbenchDispatch);
      const primaryFirst = (yield* items.get(taskId, firstPrimary.deliveryId)).workItems[0]!;
      const secondaryFirst = (yield* items.get(secondaryTask, firstSecondary.deliveryId))
        .workItems[0]!;
      assert.notEqual(primaryFirst.businessTaskId, secondaryFirst.businessTaskId);
      assert.equal(primaryFirst.matter?.key, secondaryFirst.matter?.key);
      const tracking = yield* f.tasks.getExecutions({
        id: taskId,
        businessDeliveryId: firstPrimary.deliveryId,
      });
      const [binding] = yield* Context.get(f.services, SqlClient.SqlClient)<{
        id: string;
        controller_id: string;
      }>`SELECT id,controller_id FROM workbench_queues WHERE project_id=${projectId}`;
      const scope = {
        controllerId: binding!.controller_id,
        projectId,
        expectedTrackingId: tracking.trackingId,
      };
      yield* queue.bind(binding!.id, { ...scope, taskId, dispatcherThreadId: dispatcherId });
      const queued = yield* queue.get(binding!.id, scope);
      assert.equal(queued.items.length, 3);
      const secondId = (yield* items.get(taskId, second.deliveries[0].deliveryId)).workItems[0]!.id;
      const thirdId = (yield* items.get(taskId, third.deliveries[0].deliveryId)).workItems[0]!.id;
      yield* nativeJson(f, "/api/workbench/queues/" + binding!.id + "/priority", {
        ...scope,
        workItemId: thirdId,
        priority: 20,
      });
      const caller = (threadId: ThreadId) => ({
        environmentId: f.environmentId,
        issuedAt: 0,
        requestNamespace: "queue-acceptance",
        client: undefined,
        capabilities: new Set(["orchestration" as const]),
        thread: {
          threadId,
          providerSessionId: (threadId === rootOne.threadId ? rootOne : rootTwo).providerThread
            .providerSessionId!,
          providerInstanceId: selection.instanceId,
        },
      });
      const high = yield* dispatch.dispatch(caller(dispatcherId), { taskId });
      assert.equal(high.workItem?.id, thirdId);
      const highRun = yield* Queue.take(f.provider.started);
      const next = yield* dispatch.dispatch(caller(dispatcherId), { taskId });
      const otherRun = yield* Queue.take(f.provider.started);
      assert.equal(next.workItem?.id, primaryFirst.id);
      const independent = yield* dispatch.dispatch(caller(secondaryThread), {
        taskId: secondaryTask,
      });
      const independentRun = yield* Queue.take(f.provider.started);
      assert.equal(independent.workItem?.id, secondaryFirst.id);
      assert.notEqual(
        f.git(highRun.appThread.worktreePath!, ["rev-parse", "--git-common-dir"]),
        f.git(independentRun.appThread.worktreePath!, ["rev-parse", "--git-common-dir"]),
      );
      assert.equal((yield* dispatch.dispatch(caller(dispatcherId), { taskId })).workItem, null);
      assert.equal((yield* queue.get(binding!.id, scope)).occupied, 2);
      assert.equal(f.worktrees(), 3);
      yield* Effect.logInfo("Real project fanout and occupied slots", {
        projects: [projectId, secondaryProjectId],
        worktrees: f.worktrees(),
        occupied: 2,
      });
      const fourth = yield* receive("queue:fourth", 4);
      yield* f.threads.streamStoredEventsFrom({ threadId: dispatcherId, afterSequence: 0 }).pipe(
        Stream.filter(
          (row) =>
            row.event.type === "message.updated" &&
            row.event.payload.text.includes(fourth.deliveries[0].deliveryId),
        ),
        Stream.take(1),
        Stream.runDrain,
      );
      const fourthId = (yield* items.get(taskId, fourth.deliveries[0].deliveryId)).workItems[0]!.id;
      yield* queue.bind(binding!.id, { ...scope, taskId, dispatcherThreadId: dispatcherId });
      yield* wb.observeBoundary(
        (message) =>
          message.kind === "queue" &&
          !!message.body &&
          JSON.parse(message.body).projectId === projectId &&
          JSON.parse(message.body).items.some((item: { id: string }) => item.id === fourthId),
      );
      const browserQueue = (yield* wb.call(api)).body.queues.find(
        (row: { projectId: string }) => row.projectId === projectId,
      );
      yield* wb.call(api + "/queues/" + browserQueue.id + "/priority", {
        method: "POST",
        body: { workItemId: fourthId, priority: 30 },
      });
      const update = yield* receive("queue:third-update", 3);
      yield* f.threads.streamStoredEventsFrom({ threadId: dispatcherId, afterSequence: 0 }).pipe(
        Stream.filter(
          (row) =>
            row.event.type === "message.updated" &&
            row.event.payload.text.includes(update.deliveries[0].deliveryId),
        ),
        Stream.take(1),
        Stream.runDrain,
      );
      const updated = (yield* items.get(
        taskId,
        update.deliveries.find(
          (row: { subscriptionId: string }) => row.subscriptionId === "primary",
        )!.deliveryId,
      )).workItems[0]!;
      assert.equal(updated.id, thirdId);
      assert.equal(updated.workThreadId, highRun.threadId);
      assert.equal(updated.latestEventSequence, 2);
      assert.equal(f.worktrees(), 3);
      const sql = Context.get(f.services, SqlClient.SqlClient),
        reactor = Context.get(f.services, Reactor.WorkbenchRepairReactor);
      const finishCheck = (run: WorkbenchStartedTurn) =>
        Effect.gen(function* () {
          const checked = yield* Context.get(f.services, ProcessRunner.ProcessRunner).run({
            command: process.execPath,
            args: ["check.cjs"],
            cwd: run.appThread.worktreePath!,
            extendEnv: false,
            env: { PATH: "/usr/bin:/bin" },
            timeout: "10 seconds",
          });
          yield* f.provider.complete(run.runId, {
            command: process.execPath + " check.cjs",
            cwd: run.appThread.worktreePath!,
            output: checked.stdout,
            exitCode: checked.code!,
          });
          yield* f.threads
            .streamStoredEventsFrom({ threadId: run.threadId, afterSequence: 0 })
            .pipe(
              Stream.filter(
                (row) =>
                  row.event.type === "run.updated" &&
                  row.event.payload.id === run.runId &&
                  row.event.payload.status === "completed",
              ),
              Stream.take(1),
              Stream.runDrain,
            );
          return checked.code;
        });
      assert.equal(yield* finishCheck(highRun), 7);
      for (let ordinal = 1; ordinal <= 3; ordinal++) {
        yield* reactor.reconcile(thirdId);
        const [acceptedRepair] = yield* sql<{
          run_id: string;
        }>`SELECT a.run_id FROM workbench_repair_attempts a JOIN workbench_repair_generations g ON g.id=a.generation_id WHERE g.work_item_id=${thirdId} AND g.superseded_at IS NULL AND a.ordinal=${ordinal}`;
        const failedStart = f.threads
          .streamStoredEventsFrom({ threadId: highRun.threadId, afterSequence: 0 })
          .pipe(
            Stream.filter(
              (row) =>
                row.event.type === "run.updated" &&
                row.event.payload.id === acceptedRepair!.run_id &&
                row.event.payload.status === "failed",
            ),
            Stream.take(1),
            Stream.runDrain,
            Effect.andThen(
              Effect.die("The actual repair worker failed before its scripted provider started"),
            ),
          );
        const repairRun = yield* Queue.take(f.provider.started).pipe(Effect.raceFirst(failedStart));
        assert.equal(repairRun.threadId, highRun.threadId);
        assert.equal(yield* finishCheck(repairRun), 7);
        const [generation] = yield* sql<{
          used: number;
        }>`SELECT used FROM workbench_repair_generations WHERE work_item_id=${thirdId} AND superseded_at IS NULL`;
        assert.equal(generation!.used, ordinal);
        yield* Effect.logInfo("Real repair budget consumed", {
          ordinal,
          childThreadId: repairRun.threadId,
        });
      }
      // Observe the terminal batch before the system pause takes its optimistic version.
      yield* items.get(taskId, update.deliveries[0].deliveryId);
      yield* reactor.reconcile(thirdId);
      const attention = Context.get(f.services, Attention.WorkbenchAttention);
      yield* attention.reconcile(binding!.id, scope);
      const paused = (yield* attention.get(binding!.id, scope)).attentions.find(
        (row) => row.workItemId === thirdId,
      )!;
      assert.equal(paused.phase, "awaiting_user", JSON.stringify(paused));
      assert.equal(paused.checkpoint.status, "ready");
      yield* Effect.logInfo("Repair exhaustion saved real checkpoint", {
        phase: paused.phase,
        attentionId: paused.id,
      });
      const [point] = yield* sql<{
        checkpoint_ref: string;
      }>`SELECT checkpoint_ref FROM workbench_attention_points WHERE attention_id=${paused.id} AND stage='paused'`;
      assert.equal(
        f.git(highRun.appThread.worktreePath!, ["rev-parse", "--verify", point!.checkpoint_ref])
          .length,
        40,
      );
      const reserved = yield* queue.reconcile(binding!.id, scope);
      assert.equal(reserved.items.find((row) => row.id === fourthId)!.queueState, "occupied");
      assert.equal(reserved.items.find((row) => row.id === secondId)!.queueState, "queued");
      const priorityRun = yield* dispatch.dispatch(caller(dispatcherId), { taskId });
      assert.equal(priorityRun.workItem?.id, fourthId);
      const priorityChild = yield* Queue.take(f.provider.started);
      yield* wb.awaitAttention(thirdId);
      const attentionGroup = (yield* wb.call(api)).body.attentionQueues.find(
        (group: { projectId: string }) => group.projectId === projectId,
      );
      const publicAttention = attentionGroup.attentions.find(
        (row: { workItemId: string }) => row.workItemId === thirdId,
      );
      const record = (yield* wb.call(
        api +
          "/queues/" +
          attentionGroup.id +
          "/attention/" +
          encodeURIComponent(publicAttention.id) +
          "/record",
        {
          method: "POST",
          body: {
            clientRequestId: "acceptance:human-record",
            expectedVersion: publicAttention.version,
            result: "Synthetic configuration corrected",
            priority: 80,
          },
        },
      )).body;
      assert.equal(record.operation.state, "applied");
      const savedAttention = record.attentionQueue.attentions.find(
        (row: { id: string }) => row.id === publicAttention.id,
      );
      assert.equal(savedAttention.phase, "awaiting_user");
      yield* Effect.logInfo("Real human result recorded without dispatch", {
        attentionId: publicAttention.id,
      });
      yield* wb.call(
        api +
          "/queues/" +
          attentionGroup.id +
          "/attention/" +
          encodeURIComponent(publicAttention.id) +
          "/requeue",
        {
          method: "POST",
          body: {
            clientRequestId: "acceptance:human-requeue",
            expectedVersion: savedAttention.version,
          },
        },
      );
      assert.equal(
        (yield* attention.get(binding!.id, scope)).attentions.find((row) => row.id === paused.id)!
          .phase,
        "requeue_pending",
      );
      const generations = yield* sql<{
        generation: number;
        used: number;
        superseded_at: string | null;
      }>`SELECT generation,used,superseded_at FROM workbench_repair_generations WHERE work_item_id=${thirdId} ORDER BY generation`;
      assert.equal(generations.length, 2);
      assert.equal(generations[0]!.used, 3);
      assert.equal(generations[1]!.used, 0);
      // Complete another occupied child with actual check evidence to free one slot for the explicit resume.
      const fs = yield* FileSystem.FileSystem;
      yield* fs.writeFileString(priorityChild.appThread.worktreePath! + "/input.txt", "candidate");
      assert.equal(yield* finishCheck(priorityChild), 0);
      yield* Context.get(f.services, Repairs.WorkbenchRepairs).observe(fourthId);
      yield* attention.reconcile(binding!.id, scope);
      yield* queue.reconcile(binding!.id, scope);
      const resume = yield* dispatch.dispatch(caller(dispatcherId), { taskId });
      assert.equal(resume.workItem?.id, thirdId);
      const resumed = yield* Queue.take(f.provider.started);
      assert.equal(resumed.threadId, highRun.threadId);
      assert.equal(resumed.providerThread.id, highRun.providerThread.id);
      assert.equal(resumed.appThread.worktreePath, highRun.appThread.worktreePath);
      yield* fs.writeFileString(resumed.appThread.worktreePath! + "/input.txt", "candidate");
      assert.equal(yield* finishCheck(resumed), 0);
      yield* attention.reconcile(binding!.id, scope);
      assert.equal(
        (yield* attention.get(binding!.id, scope)).attentions.find((row) => row.id === paused.id)!
          .phase,
        "resolved",
      );
      yield* Context.get(f.services, Repairs.WorkbenchRepairs).observe(thirdId);
      const fresh = (yield* sql<{
        used: number;
        phase: string;
      }>`SELECT used,phase FROM workbench_repair_generations WHERE work_item_id=${thirdId} AND superseded_at IS NULL`)[0]!;
      assert.equal(fresh.used, 0);
      assert.equal(fresh.phase, "succeeded");
      const followup = yield* dispatch.dispatch(caller(dispatcherId), {
        taskId,
        businessDeliveryId: update.deliveries[0].deliveryId,
      });
      assert.equal(followup.workItem?.id, thirdId);
      const followupRun = yield* Queue.take(f.provider.started);
      assert.equal(followupRun.threadId, highRun.threadId);
      assert.equal(followupRun.providerThread.id, highRun.providerThread.id);
      assert.equal(followupRun.appThread.worktreePath, highRun.appThread.worktreePath);
      assert.equal(yield* finishCheck(followupRun), 0);
      yield* Context.get(f.services, Repairs.WorkbenchRepairs).observe(thirdId);
      const finishedMatter = (yield* items.get(taskId, update.deliveries[0].deliveryId))
        .workItems[0]!;
      assert.equal(finishedMatter.latestEventSequence, 2);
      assert.equal(finishedMatter.dispatchedThroughSequence, 2);
      assert.equal(finishedMatter.processedThroughSequence, 2);
      assert.equal(finishedMatter.businessState, "awaiting_delivery");
      assert.equal(f.worktrees(), 4);
      yield* Effect.logInfo("Composed queue milestones", {
        fanoutDeliveries: first.deliveries.length,
        scopedBusinessTasks: [primaryFirst.businessTaskId, secondaryFirst.businessTaskId],
        occupied: 2,
        priorityWinner: high.workItem?.id,
        resumedThreadId: resumed.threadId,
        followupRunId: followupRun.runId,
        processedRequirements: finishedMatter.processedThroughSequence,
        repairBudgets: generations.map((row) => ({ generation: row.generation, used: row.used })),
        childThreads: [highRun.threadId, otherRun.threadId],
        pendingDeliveryId: second.deliveries[0].deliveryId,
      });
    }).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);
