import { assert, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Queue from "effect/Queue";
import * as SqlClient from "effect/sql/SqlClient";
import * as Stream from "effect/Stream";
import * as ProcessRunner from "../../processRunner.ts";
import * as Dispatch from "../../scheduledTasks/WorkbenchDispatch.ts";
import * as Items from "../../scheduledTasks/WorkbenchWorkItems.ts";
import * as NativeQueue from "../../scheduledTasks/WorkbenchQueue.ts";
import * as Progress from "../../scheduledTasks/WorkbenchTaskProgress.ts";
import * as Repairs from "../../scheduledTasks/WorkbenchRepairs.ts";
import { releaseHash } from "../../scheduledTasks/workbenchReleaseProfile.ts";
import {
  acquireAcceptanceFixture,
  dispatcherId,
  projectId,
  startWorkbench,
  taskId,
} from "./WorkbenchAcceptanceFixture.ts";

type Anchor = {
  id: string;
  workItemId: string;
  state: string;
  checksState: string;
  businessState: string;
  workThreadId: string | null;
  errorCode: string;
};
type GitHubState = {
  actor: { id: number; login: string };
  repository: { id: number; full_name: string; html_url: string };
  issue: {
    id: number;
    number: number;
    html_url: string;
    body: string;
    user: { id: number; login: string };
  };
  comment: { id: number; body: string; user: { id: number; login: string }; html_url: string };
  requests: Array<{ method: string; path: string }>;
  lostResponses: number;
};

// quarantine: #19 This composition requires the explicitly selected real companion checkout.
it.live.skipIf(!process.env.WORKBENCH_ACCEPTANCE_REPO)(
  "summarizes real native child evidence without declaring delivery and reconciles its own GitHub echoes after restart",
  () =>
    Effect.gen(function* () {
      const f = yield* acquireAcceptanceFixture();
      const wb = yield* startWorkbench(f, { github: true });
      const fs = yield* FileSystem.FileSystem;
      const api = wb.api;
      const readRemote = Effect.fn("WorkbenchGithubAcceptance.readRemote")(function* () {
        return JSON.parse(
          yield* fs.readFileString(wb.directory + "/github-boundary-state.json"),
        ) as GitHubState;
      });
      yield* wb.call(api + "/environments/acceptance", {
        method: "PUT",
        body: { name: "Real isolated Native" },
      });
      yield* wb.call(api + "/environments/ingress", {
        method: "PUT",
        body: { name: "Synthetic GitHub source" },
      });
      yield* wb.call(api + "/t3-connections/native", {
        method: "PUT",
        body: {
          name: "Real Native",
          environmentId: "acceptance",
          origin: f.origin,
          credential: f.credential,
        },
      });
      yield* wb.call(api + "/t3-subscriptions/dispatcher", {
        method: "PUT",
        body: {
          name: "Dispatcher with explicit GitHub progress",
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
          githubProgress: { enabled: true, repository: "fixture/project" },
        },
      });
      yield* wb.call(api + "/rules/ready", {
        method: "PUT",
        body: {
          name: "Selected repository ready gate",
          environmentId: "acceptance",
          workerId: "dispatcher",
          enabled: true,
          conditions: [
            { field: "event.type", operator: "eq", value: "issues.ready" },
            { field: "event.payload.repository.id", operator: "eq", value: 42 },
          ],
        },
      });
      const source = (yield* wb.call(api + "/sources", {
        method: "POST",
        body: {
          name: "Selected canonical GitHub source",
          environmentId: "ingress",
        },
      })).body;
      yield* wb.call(api + "/dispatch-subscriptions/primary", {
        method: "PUT",
        body: {
          name: "Only the selected ready stream",
          sourceId: source.source.id,
          environmentId: "acceptance",
          ruleIds: ["ready"],
          enabled: true,
        },
      });
      const repository = {
        id: 42,
        full_name: "fixture/project",
        html_url: "https://github.com/fixture/project",
      };
      const input = {
        id: "github-acceptance:first",
        type: "issues.ready",
        subject: "fixture/project#1",
        summary: "Keep native execution, checks and delivery distinct",
        payload: {
          repository,
          issue: { id: 301, number: 1, html_url: repository.html_url + "/issues/1" },
        },
      };
      const receive = (body: unknown) =>
        wb.call("/api/event-hub/in/" + source.source.id, {
          method: "POST",
          machine: true,
          headers: { authorization: "Bearer " + source.token },
          body,
        });
      yield* fs.writeFileString(
        wb.directory + "/drop-github-response.flag",
        "lose one persisted summary acknowledgement",
      );
      const received = yield* receive(input);
      assert.equal(received.status, 202);
      assert.equal(received.body.deliveries.length, 1);
      const delivery = received.body.deliveries[0];
      const parent = yield* Queue.take(f.provider.started);
      assert.equal(parent.threadId, dispatcherId);
      const tracking = yield* f.tasks.getExecutions({
        id: taskId,
        businessDeliveryId: delivery.deliveryId,
      });
      const dispatched = yield* Context.get(f.services, Dispatch.WorkbenchDispatch).dispatch(
        {
          environmentId: f.environmentId,
          issuedAt: 0,
          requestNamespace: "github-acceptance",
          client: undefined,
          capabilities: new Set(["orchestration"]),
          thread: {
            threadId: dispatcherId,
            providerSessionId: parent.providerThread.providerSessionId!,
            providerInstanceId: parent.modelSelection.instanceId,
          },
        },
        { taskId, businessDeliveryId: delivery.deliveryId },
      );
      const work = dispatched.workItem!;
      const child = yield* Queue.take(f.provider.started);
      assert.equal(child.threadId, work.workThreadId);
      assert.equal(f.worktrees(), 1);
      assert.isAbove(f.provider.opens(), 0);
      yield* Stream.fromQueue(f.threadAcks).pipe(
        Stream.filter((ack) => ack.threadId === child.threadId),
        Stream.take(1),
        Stream.runDrain,
      );
      const nativeItems = Context.get(f.services, Items.WorkbenchWorkItems);
      const actual = (yield* nativeItems.get(taskId, delivery.deliveryId)).workItems[0]!;
      assert.equal(
        actual.matter?.key,
        '["github","https://github.com","issue","repository:42:issue:1"]',
      );
      const firstSummary = yield* wb.observeBoundary(
        (message) => message.kind === "github-summary" && message.method === "POST",
      );
      assert.notInclude(firstSummary.body!, "- 阶段：已完成");
      assert.notInclude(firstSummary.body!, "- 检查：通过");
      yield* wb.observeBoundary((message) => {
        if (message.kind !== "github-sync" || !message.body) return false;
        const row = JSON.parse(message.body) as Anchor;
        return (
          row.workItemId === work.id &&
          row.state === "unknown" &&
          row.errorCode === "github_ambiguous_write"
        );
      });
      const unknown = (yield* wb.call(api)).body.githubSync[0];
      assert.equal(unknown.state, "unknown");
      assert.equal((yield* readRemote()).lostResponses, 1);
      yield* wb.restart;
      const restored = (yield* wb.call(api)).body.githubSync[0];
      assert.equal(restored.id, unknown.id);
      assert.equal(restored.state, "unknown");
      yield* fs.writeFileString(
        wb.directory + "/github-recovery-enabled.flag",
        "restore remote readback before the explicit retry",
      );
      yield* wb.call(api + "/github-sync/" + restored.id + "/retry", {
        method: "POST",
        body: {
          clientRequestId: "github-acceptance:readback",
          expectedVersion: restored.version,
        },
      });
      yield* wb.observeBoundary(
        (message) => message.kind === "github-readback" && message.body === firstSummary.body,
      );
      yield* wb.observeBoundary((message) => {
        if (message.kind !== "github-sync" || !message.body) return false;
        const row = JSON.parse(message.body) as Anchor;
        return (
          row.workItemId === work.id &&
          row.state === "synced" &&
          row.workThreadId === child.threadId
        );
      });
      const recovered = (yield* wb.call(api)).body.githubSync[0];
      assert.equal(recovered.summaryUrl, repository.html_url + "/issues/1#issuecomment-900");
      assert.equal(recovered.target.number, 1);
      const activeRemote = yield* readRemote();
      assert.equal(activeRemote.requests.filter((request) => request.method === "POST").length, 1);
      assert.notInclude(activeRemote.comment.body, "- 阶段：已完成");
      assert.include(activeRemote.comment.body, "工作线程：[在 T3 Code 查看]");
      const childPath = child.appThread.worktreePath!;
      yield* fs.writeFileString(childPath + "/input.txt", "candidate");
      f.git(childPath, ["add", "input.txt"]);
      f.git(childPath, ["commit", "-qm", "native check evidence for GitHub summary"]);
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
      yield* f.provider.complete(child.runId, {
        command: process.execPath + " check.cjs",
        cwd: childPath,
        output: check.stdout,
        exitCode: check.code!,
      });
      yield* f.provider.complete(parent.runId);
      yield* f.threads.streamStoredEventsFrom({ threadId: child.threadId, afterSequence: 0 }).pipe(
        Stream.filter(
          (row) =>
            row.event.type === "run.updated" &&
            row.event.payload.id === child.runId &&
            row.event.payload.status === "completed",
        ),
        Stream.take(1),
        Stream.runDrain,
      );
      assert.equal(
        (yield* Context.get(f.services, Repairs.WorkbenchRepairs).observe(work.id)).kind,
        "none",
      );
      const [binding] = yield* Context.get(f.services, SqlClient.SqlClient)<{
        id: string;
        controller_id: string;
      }>`SELECT id,controller_id FROM workbench_queues WHERE project_id=${projectId}`;
      const scope = {
        controllerId: binding!.controller_id,
        projectId,
        expectedTrackingId: tracking.trackingId,
      };
      const queue = yield* Context.get(f.services, NativeQueue.WorkbenchQueue).reconcile(
        binding!.id,
        scope,
      );
      assert.equal(queue.items.find((item) => item.id === work.id)?.workState, "result_available");
      const progress = (yield* Context.get(f.services, Progress.WorkbenchTaskProgress).get(
        binding!.id,
        work.id,
        scope,
      )).task;
      assert.equal(progress.checks.state, "passed");
      assert.equal(progress.checks.sourceRunId, child.runId);
      assert.equal(progress.businessState, "awaiting_delivery");
      const beforeRefresh = (yield* wb.call(api)).body.githubSync[0];
      yield* wb.call(api + "/github-sync/" + beforeRefresh.id + "/retry", {
        method: "POST",
        body: {
          clientRequestId: "github-acceptance:check-evidence",
          expectedVersion: beforeRefresh.version,
        },
      });
      yield* wb.observeBoundary((message) => {
        if (message.kind !== "github-sync" || !message.body) return false;
        const row = JSON.parse(message.body) as Anchor;
        return (
          row.workItemId === work.id &&
          row.state === "synced" &&
          row.checksState === "passed" &&
          row.businessState === "awaiting_delivery"
        );
      });
      const completeRemote = yield* readRemote();
      assert.include(
        completeRemote.comment.body,
        "- 检查：通过（通过 1，未通过 0；未绑定当前提交）",
      );
      assert.include(
        completeRemote.comment.body,
        "- 当前提交：`" + f.git(childPath, ["rev-parse", "HEAD"]) + "`",
      );
      assert.include(completeRemote.comment.body, "等待交付确认");
      assert.notInclude(completeRemote.comment.body, "- 阶段：已完成");
      assert.equal(
        completeRemote.issue.body,
        "Original user requirement; the progress writer must preserve it.",
      );
      assert.equal(
        completeRemote.requests.filter((request) => request.method === "POST").length,
        1,
      );
      assert.isAbove(
        completeRemote.requests.filter((request) => request.method === "PATCH").length,
        0,
      );
      const echo = {
        id: "github-acceptance:self-created",
        type: "issue_comment",
        payload: {
          action: "created",
          repository,
          sender: completeRemote.actor,
          issue: completeRemote.issue,
          comment: { ...completeRemote.comment, body: firstSummary.body! },
        },
      };
      const filtered = (yield* receive(echo)).body;
      assert.isTrue(filtered.filtered);
      assert.deepEqual(filtered.deliveryIds, []);
      const edited = (yield* receive({
        ...echo,
        id: "github-acceptance:self-edited",
        payload: {
          ...echo.payload,
          action: "edited",
          comment: completeRemote.comment,
        },
      })).body;
      assert.isTrue(edited.filtered);
      const beforeFeedback = (yield* wb.call(api)).body;
      const ordinary = (yield* receive({
        ...echo,
        id: "github-acceptance:human-follow-up",
        payload: {
          ...echo.payload,
          action: "edited",
          sender: completeRemote.issue.user,
          comment: {
            ...completeRemote.comment,
            user: completeRemote.issue.user,
            body: completeRemote.comment.body + "\nA new human requirement",
          },
        },
      })).body;
      assert.isUndefined(ordinary.filtered);
      assert.deepEqual(ordinary.deliveries, []);
      for (const type of ["push", "status", "pull_request.closed"]) {
        const feedback = (yield* receive({
          id: "github-acceptance:feedback:" + type,
          type,
          payload: { repository },
        })).body;
        assert.deepEqual(feedback.deliveries, []);
      }
      const afterFeedback = (yield* wb.call(api)).body;
      assert.equal(afterFeedback.sourceEvents.length, beforeFeedback.sourceEvents.length + 4);
      assert.equal(afterFeedback.events.length, beforeFeedback.events.length);
      assert.equal(
        (yield* Context.get(f.services, NativeQueue.WorkbenchQueue).get(binding!.id, scope)).items
          .length,
        1,
      );
      yield* wb.restart;
      const durableEcho = (yield* receive(echo)).body;
      assert.isTrue(durableEcho.filtered);
      assert.isTrue(durableEcho.duplicate);
      assert.equal(durableEcho.eventId, filtered.eventId);
      const duplicate = (yield* receive(input)).body;
      assert.isTrue(duplicate.duplicate);
      assert.equal(duplicate.deliveries[0].deliveryId, delivery.deliveryId);
      assert.equal(f.worktrees(), 1);
      assert.equal(
        (yield* readRemote()).requests.filter((request) => request.method === "POST").length,
        1,
      );
      yield* Effect.logInfo("Cross-repository GitHub summary evidence", {
        sourceEventId: received.body.eventId,
        deliveryId: delivery.deliveryId,
        workItemId: work.id,
        childThreadId: child.threadId,
        childRunId: child.runId,
        anchorId: recovered.id,
        summaryUrl: recovered.summaryUrl,
        checks: progress.checks.state,
        businessState: progress.businessState,
        lostResponses: completeRemote.lostResponses,
        commentsCreated: 1,
        durableSelfEcho: durableEcho.eventId,
      });
    }).pipe(Effect.provide(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer))),
);
