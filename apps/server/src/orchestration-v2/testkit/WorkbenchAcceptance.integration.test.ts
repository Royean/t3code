import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Context from "effect/Context";
import * as Queue from "effect/Queue";
import * as Dispatch from "../../scheduledTasks/WorkbenchDispatch.ts";
import * as FileSystem from "effect/FileSystem";
import * as SqlClient from "effect/sql/SqlClient";
import * as ProcessRunner from "../../processRunner.ts";
import * as Repairs from "../../scheduledTasks/WorkbenchRepairs.ts";
import * as Release from "../../scheduledTasks/WorkbenchReleaseEvidence.ts";
import * as Deployment from "../../scheduledTasks/WorkbenchDeployment.ts";
import { releaseHash } from "../../scheduledTasks/workbenchReleaseProfile.ts";
import * as NativeQueue from "../../scheduledTasks/WorkbenchQueue.ts";
import * as Stream from "effect/Stream";
import {
  acquireAcceptanceFixture,
  startWorkbench,
  configureRelease,
  dispatcherId,
  projectId,
  taskId,
} from "./WorkbenchAcceptanceFixture.ts";

// quarantine: #19 The paired checkout must be selected explicitly; an absent companion is unavailable, never a mock pass.
it.live.skipIf(!process.env.WORKBENCH_ACCEPTANCE_REPO)(
  "delivers an approved Workbench event through real native acceptance and merge across lost responses and both database restarts",
  () => {
    return Effect.gen(function* () {
      const f = yield* acquireAcceptanceFixture();
      const wb = yield* startWorkbench(f);
      const release = yield* configureRelease(f, wb);
      const api = wb.api;
      yield* wb.call(api + "/environments/acceptance", {
        method: "PUT",
        body: { name: "Isolated Native" },
      });
      yield* wb.call(api + "/t3-connections/native", {
        method: "PUT",
        body: {
          name: "Real isolated Native",
          environmentId: "acceptance",
          origin: f.origin,
          credential: f.credential,
        },
      });
      yield* wb.call(api + "/t3-subscriptions/dispatcher", {
        method: "PUT",
        body: {
          name: "Real Native dispatcher",
          connectionId: "native",
          projectId,
          threadId: dispatcherId,
          taskId,
          repairChecks: [
            {
              scriptId: "check",
              definitionHash: releaseHash(
                JSON.stringify({
                  id: "check",
                  command: f.scripts.find((script) => script.id === "check")!.command,
                }),
              ),
            },
          ],
          releasePolicy: { profileId: release.profile.id, profileDigest: release.profile.digest },
        },
      });
      yield* wb.call(api + "/rules/ready", {
        method: "PUT",
        body: {
          name: "Synthetic ready",
          environmentId: "acceptance",
          workerId: "dispatcher",
          priority: 10,
          enabled: true,
          conditions: [{ field: "event.type", operator: "eq", value: "issues.ready" }],
        },
      });
      const source = (yield* wb.call(api + "/sources", {
        method: "POST",
        body: { name: "Synthetic source", environmentId: "acceptance" },
      })).body;
      const received = (yield* wb.call("/api/event-hub/in/" + source.source.id, {
        method: "POST",
        machine: true,
        headers: { authorization: "Bearer " + source.token },
        body: {
          id: "acceptance:first",
          type: "issues.ready",
          subject: "fixture/project#1",
          summary: "Isolated acceptance",
          payload: { repository: { id: "42", full_name: "fixture/project" }, issue: { number: 1 } },
        },
      })).body;
      const parentRun = yield* Queue.take(f.provider.started);
      assert.equal(parentRun.threadId, dispatcherId);
      const tracking = yield* f.tasks.getExecutions({
        id: taskId,
        businessDeliveryId: received.deliveryId,
      });
      assert.equal(tracking.effectiveExecutions.length, 1);
      assert.equal(tracking.effectiveExecutions[0]?.state, "dispatched");
      const result = yield* Context.get(f.services, Dispatch.WorkbenchDispatch).dispatch(
        {
          environmentId: f.environmentId,
          issuedAt: 0,
          requestNamespace: "acceptance",
          client: undefined,
          capabilities: new Set(["orchestration"]),
          thread: {
            threadId: dispatcherId,
            providerSessionId: "scripted:dispatcher",
            providerInstanceId: parentRun.modelSelection.instanceId,
          },
        },
        { taskId, businessDeliveryId: received.deliveryId },
      );
      assert.isNotNull(result.workItem?.workThreadId);
      const childRun = yield* Queue.take(f.provider.started);
      assert.equal(childRun.threadId, result.workItem?.workThreadId);
      assert.equal(f.worktrees(), 1);
      assert.isAbove(f.provider.opens(), 0);
      yield* Stream.fromQueue(f.threadAcks).pipe(
        Stream.filter((ack) => ack.threadId === childRun.threadId),
        Stream.take(1),
        Stream.runDrain,
      );
      const observed = (yield* wb.call(api)).body.events.find(
        (event: { id: string }) => event.id === received.eventId,
      );
      assert.equal(observed.workTasks[0]?.workThreadId, childRun.threadId);
      const fs = yield* FileSystem.FileSystem,
        childPath = childRun.appThread.worktreePath!;
      yield* fs.writeFileString(childPath + "/input.txt", "candidate");
      f.git(childPath, ["add", "input.txt"]);
      f.git(childPath, ["commit", "-qm", "isolated candidate"]);
      f.git(childPath, ["push", "origin", "HEAD:refs/heads/release-candidate"]);
      const check = yield* Context.get(f.services, ProcessRunner.ProcessRunner).run({
        command: process.execPath,
        args: ["check.cjs"],
        cwd: childPath,
        extendEnv: false,
        env: { PATH: "/usr/bin:/bin" },
        timeout: "10 seconds",
      });
      assert.equal(check.code, 0);
      f.provider.autoComplete(dispatcherId);
      yield* f.provider.complete(childRun.runId, {
        command: process.execPath + " check.cjs",
        cwd: childPath,
        output: check.stdout,
        exitCode: check.code!,
      });
      yield* f.provider.complete(parentRun.runId);
      yield* f.threads
        .streamStoredEventsFrom({ threadId: childRun.threadId, afterSequence: 0 })
        .pipe(
          Stream.filter(
            (row) =>
              row.event.type === "run.updated" &&
              row.event.payload.id === childRun.runId &&
              row.event.payload.status === "completed",
          ),
          Stream.take(1),
          Stream.runDrain,
        );
      const repair = yield* Context.get(f.services, Repairs.WorkbenchRepairs).observe(
        result.workItem!.id,
      );
      assert.equal(repair.kind, "none", JSON.stringify(repair));
      const [nativeQueue] = yield* Context.get(f.services, SqlClient.SqlClient)<{
        id: string;
        controller_id: string;
      }>`SELECT id,controller_id FROM workbench_queues`;
      const scope = {
        controllerId: nativeQueue!.controller_id,
        projectId,
        expectedTrackingId: tracking.trackingId,
      };
      const ready = yield* Context.get(f.services, NativeQueue.WorkbenchQueue).reconcile(
        nativeQueue!.id,
        scope,
      );
      assert.equal(
        ready.items.find((item) => item.id === result.workItem!.id)?.workState,
        "result_available",
      );
      const second = (yield* wb.call("/api/event-hub/in/" + source.source.id, {
        method: "POST",
        machine: true,
        headers: { authorization: "Bearer " + source.token },
        body: {
          id: "acceptance:second",
          type: "issues.ready",
          subject: "fixture/project#2",
          summary: "Next independent matter",
          payload: { repository: { id: "42", full_name: "fixture/project" }, issue: { number: 2 } },
        },
      })).body;
      yield* Stream.fromQueue(f.provider.started).pipe(
        Stream.filter((run) => run.message.text.includes(second.deliveryId)),
        Stream.take(1),
        Stream.runDrain,
      );
      const task = (yield* wb.call(api)).body.releaseTasks.find(
        (task: { workItemId: string }) => task.workItemId === result.workItem!.id,
      );
      assert.isTrue(task.canPrepare, JSON.stringify(task));
      const prepareInput = {
        clientRequestId: "acceptance:prepare",
        queueId: task.queueId,
        workerId: task.workerId,
        workItemId: task.workItemId,
        expectedPolicyVersion: task.policyVersion,
      };
      yield* wb.call(api + "/releases/prepare", { method: "POST", body: prepareInput });
      const preparationId =
        "wb-release:" +
        releaseHash(
          JSON.stringify([wb.actor.id, task.queueId, task.workerId, prepareInput.clientRequestId]),
        );
      const prepared = yield* Context.get(
        f.services,
        Release.WorkbenchReleaseEvidence,
      ).awaitPrepared(task.queueId, preparationId, scope);
      assert.equal(
        prepared.preparation.state,
        "ready",
        JSON.stringify(prepared.preparation.reasons),
      );
      const wbPrepared = (yield* wb.call(api + "/releases/prepare", {
        method: "POST",
        body: prepareInput,
      })).body.preparation;
      assert.equal(wbPrepared.state, "ready", JSON.stringify(wbPrepared));
      const candidate = (yield* wb.call(api)).body.releaseCandidates.find(
        (value: { workItemId: string }) => value.workItemId === result.workItem!.id,
      );
      assert.equal(candidate.state, "awaiting_approval");
      f.loseNextAdmissionResponse();
      yield* wb.loseNextClaimResponse;
      yield* wb.call(api + "/releases/" + encodeURIComponent(candidate.id) + "/decision", {
        method: "POST",
        body: {
          clientRequestId: "acceptance:approve",
          expectedVersion: candidate.version,
          packetDigest: candidate.packetDigest,
          decision: "approve",
        },
      });
      const admitted = yield* Queue.take(f.deploymentAdmissions);
      assert.equal(admitted.input.preparationId, preparationId);
      const deployments = Context.get(f.services, Deployment.WorkbenchDeployment);
      yield* deployments.sync;
      yield* deployments.sync;
      const startedDeployment = (yield* deployments.get(
        admitted.queueId,
        admitted.input.deploymentId,
        scope,
      )).deployment;
      assert.notEqual(
        startedDeployment.state,
        "attention",
        JSON.stringify(startedDeployment.reasons),
      );
      yield* deployments.awaitAcceptanceSample(
        admitted.queueId,
        admitted.input.deploymentId,
        scope,
        3,
      );
      yield* deployments.sync;
      const delivered = (yield* deployments.get(
        admitted.queueId,
        admitted.input.deploymentId,
        scope,
      )).deployment;
      assert.equal(delivered.state, "delivered", JSON.stringify(delivered.reasons));
      assert.equal(delivered.acceptance?.state, "passed");
      assert.equal(delivered.merge?.state, "completed");
      assert.equal(
        f.git(f.repo, ["--git-dir", f.remote, "rev-parse", "refs/heads/main"]),
        f.git(childPath, ["rev-parse", "HEAD"]),
      );
      const mirrored = (yield* wb.call(
        api + "/deployments/" + encodeURIComponent(admitted.input.reservation.payload.deploymentId),
      )).body;
      assert.equal(mirrored.deployment.state, "completed");
      assert.equal(f.refs.attempts(), 1);
      assert.equal(release.adapter.writes(), 1);
      assert.equal(f.responseFault.dropped, 1);
      const boundaries = yield* wb.boundaryEvents;
      assert.equal(
        boundaries.filter((event) => event.role === "claim" && event.method === "POST").length,
        1,
      );
      assert.equal(boundaries.filter((event) => event.dropped).length, 1);
      assert.equal(
        boundaries.filter(
          (event) => event.role === "merge" && event.method === "POST" && event.status === 200,
        ).length,
        1,
      );
      yield* f.closeNative;
      const reopened = yield* acquireAcceptanceFixture({ root: f.root, nativePort: f.nativePort });
      assert.equal(reopened.environmentId, f.environmentId);
      const restored = (yield* Context.get(reopened.services, Deployment.WorkbenchDeployment).get(
        admitted.queueId,
        admitted.input.deploymentId,
        scope,
      )).deployment;
      assert.deepEqual(restored.delivery, delivered.delivery);
      assert.equal(restored.state, "delivered");
      const executionsAfterRestart = yield* reopened.tasks.getExecutions({
        id: taskId,
        businessDeliveryId: received.deliveryId,
      });
      assert.equal(
        executionsAfterRestart.effectiveExecutions[0]?.id,
        tracking.effectiveExecutions[0]?.id,
      );
      yield* wb.restart;
      const readAfterRestart = (yield* wb.call(
        api + "/deployments/" + encodeURIComponent(admitted.input.reservation.payload.deploymentId),
      )).body.deployment;
      assert.equal(readAfterRestart.state, "completed");
      assert.deepEqual(readAfterRestart.delivery, mirrored.deployment.delivery);
      const duplicate = (yield* wb.call("/api/event-hub/in/" + source.source.id, {
        method: "POST",
        machine: true,
        headers: { authorization: "Bearer " + source.token },
        body: {
          id: "acceptance:first",
          type: "issues.ready",
          subject: "fixture/project#1",
          summary: "Isolated acceptance",
          payload: { repository: { id: "42", full_name: "fixture/project" }, issue: { number: 1 } },
        },
      })).body;
      assert.isTrue(duplicate.duplicate);
      assert.equal(duplicate.deliveryId, received.deliveryId);
      assert.equal(reopened.worktrees(), 1);
      assert.equal(release.adapter.writes(), 1);
      assert.equal(f.refs.attempts(), 1);
      yield* Effect.logInfo("Cross-repository delivery evidence", {
        eventId: received.eventId,
        businessDeliveryId: received.deliveryId,
        nativeExecutionId: tracking.effectiveExecutions[0]?.id,
        workItemId: result.workItem!.id,
        dispatcherThreadId: dispatcherId,
        childThreadId: childRun.threadId,
        runId: childRun.runId,
        preparationId,
        candidateId: candidate.id,
        nativeDeploymentId: delivered.id,
        authorizationDigest: delivered.authorizationDigest,
        artifactSha256: delivered.desired.artifactSha256,
        mergeSha: delivered.merge?.result?.commitSha,
        nativeState: restored.state,
        workbenchState: readAfterRestart.state,
        duplicate: duplicate.duplicate,
      });
    }).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)));
  },
);
