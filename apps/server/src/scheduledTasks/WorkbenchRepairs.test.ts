import { expect, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as SqlClient from "effect/sql/SqlClient";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as FileSystem from "effect/FileSystem";
import * as Schema from "effect/Schema";
import * as DateTime from "effect/DateTime";
import {
  EventId,
  OrchestrationV2Command,
  ProjectId,
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as Git from "../vcs/GitVcsDriver.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as Config from "../config.ts";
import * as EventSink from "../orchestration-v2/EventSink.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Repairs from "./WorkbenchRepairs.ts";

const stamp = "2026-11-27T00:00:00.000Z";
const seed = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO workbench_queues VALUES('queue','controller','project')`;
  yield* sql`INSERT INTO workbench_queue_bindings(task_id,task_created_at,queue_id,project_id,dispatcher_thread_id) VALUES('task',${stamp},'queue','project','dispatcher')`;
  yield* sql`INSERT INTO workbench_work_items(id,task_id,task_created_at,project_id,business_delivery_id,dispatcher_thread_id,event_json,work_thread_id,run_id,created_at,updated_at)
    VALUES('item','task',${stamp},'project','event','dispatcher','{}','child','initial',${stamp},${stamp})`;
  yield* sql`INSERT INTO workbench_queue_entries(work_item_id,queue_id,state,claim_id,created_at,updated_at) VALUES('item','queue','occupied','claim',${stamp},${stamp})`;
  yield* sql`INSERT INTO orchestration_v2_projection_runs(run_id,thread_id,ordinal,provider,status,requested_at,payload_json) VALUES('initial','child',1,'codex','completed',${stamp},'{"id":"initial","status":"completed","activeAttemptId":null,"providerThreadId":null}')`;
});
const layer = Repairs.layer.pipe(Layer.provideMerge(Sqlite.layerMemory));
const encode = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const decodeCommand = Schema.decodeEffect(Schema.fromJsonString(OrchestrationV2Command));
const gitLayer = Git.layer.pipe(
  Layer.provide(Config.layerTest(process.cwd(), { prefix: "repair-domain-test-" })),
  Layer.provideMerge(VcsProcess.layer),
  Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
);
const nativeLayer = Layer.merge(Repairs.layer, EventSink.layer).pipe(
  Layer.provideMerge(Layer.merge(EventStore.layer, ProjectionStore.layer)),
  Layer.provideMerge(Sqlite.layerMemory),
  Layer.provideMerge(gitLayer),
);
const modelSelection = { instanceId: ProviderInstanceId.make("provider"), model: "fixture-model" };
const evidenceFixture = Effect.fn("WorkbenchRepairs.test.evidence")(function* (
  cwd: string,
  runId: string,
  ordinal: number,
  exitCode: number | null = 1,
  command = "npm test",
  observedCwd: string | null = cwd,
) {
  const sql = yield* SqlClient.SqlClient,
    attemptId = `native-attempt:${ordinal}`,
    nodeId = `node:${ordinal}`,
    turnId = `turn:${ordinal}`,
    itemId = `check:${ordinal}`;
  const run = {
    id: runId,
    status: "completed",
    activeAttemptId: attemptId,
    providerThreadId: "provider-thread",
  };
  yield* sql`UPDATE orchestration_v2_projection_runs SET status='completed',payload_json=json_patch(payload_json,${yield* encode(run)}) WHERE run_id=${runId}`;
  yield* sql`INSERT INTO orchestration_v2_projection_run_attempts(attempt_id,thread_id,run_id,attempt_ordinal,root_node_id,provider,provider_thread_id,status,payload_json) VALUES(${attemptId},'child',${runId},1,${nodeId},'codex','provider-thread','completed','{}')`;
  yield* sql`INSERT INTO orchestration_v2_projection_nodes(node_id,thread_id,run_id,root_node_id,kind,status,provider_thread_id,payload_json) VALUES(${nodeId},'child',${runId},${nodeId},'provider_turn','completed','provider-thread','{}')`;
  yield* sql`INSERT INTO orchestration_v2_projection_provider_turns(provider_turn_id,thread_id,provider_thread_id,node_id,run_attempt_id,ordinal,status,payload_json) VALUES(${turnId},'child','provider-thread',${nodeId},${attemptId},${ordinal + 1},'completed',${yield* encode({ runAttemptId: attemptId, nativeThreadRef: { nativeId: "native-original" } })})`;
  const item = {
    id: itemId,
    threadId: "child",
    runId,
    nodeId,
    providerThreadId: "provider-thread",
    providerTurnId: turnId,
    nativeItemRef: { driver: "codex", nativeId: `native:${itemId}`, strength: "strong" },
    parentItemId: null,
    ordinal: ordinal + 1,
    status: "completed",
    title: null,
    startedAt: stamp,
    completedAt: stamp,
    updatedAt: stamp,
    type: "command_execution",
    input: command,
    output: "private failure output SECRET",
    ...(exitCode === null ? {} : { exitCode }),
    ...(observedCwd === null ? {} : { cwd: observedCwd }),
  };
  const payload = yield* encode(item);
  yield* sql`INSERT INTO orchestration_v2_projection_turn_items(turn_item_id,thread_id,run_id,node_id,provider_thread_id,provider_turn_id,ordinal,type,status,updated_at,payload_json) VALUES(${itemId},'child',${runId},${nodeId},'provider-thread',${turnId},${ordinal + 1},'command_execution','completed',${stamp},${payload})`;
  yield* sql`INSERT INTO orchestration_events(event_id,aggregate_kind,stream_id,stream_version,event_type,occurred_at,actor_kind,payload_json,metadata_json) VALUES(${`event:${itemId}`},'thread','child',(SELECT COALESCE(MAX(stream_version),0)+1 FROM orchestration_events WHERE aggregate_kind='thread' AND stream_id='child'),'turn-item.updated',${stamp},'provider',${payload},'{}')`;
});
const nativeFixture = Effect.gen(function* () {
  yield* seed;
  const fs = yield* FileSystem.FileSystem,
    git = yield* Git.GitVcsDriver,
    sql = yield* SqlClient.SqlClient;
  const cwd = yield* fs.makeTempDirectoryScoped({ prefix: "t3-repair-domain-" });
  yield* git.execute({ operation: "test.init", cwd, args: ["init", "-b", "repair-fixture"] });
  const real = yield* fs.realPath(cwd);
  const policy = {
    checks: [
      {
        scriptId: "check-unit",
        definitionHash: "d235de7a84d89f561bce7abb7821c1ee7eee845e94b3e33b362b2fb177acce24",
      },
    ],
  };
  yield* sql`INSERT INTO projection_projects(project_id,title,workspace_root,scripts_json,created_at,updated_at) VALUES('project','Fixture',${real},${yield* encode([{ id: "check-unit", name: "Unit checks", command: "npm test", icon: "test", runOnWorktreeCreate: false }])},${stamp},${stamp})`;
  yield* sql`UPDATE workbench_queue_bindings SET repair_policy_json=${yield* encode(policy)}`;
  yield* sql`UPDATE workbench_work_items SET worktree_path=${real},branch='repair-fixture'`;
  yield* sql`INSERT INTO orchestration_v2_projection_threads(thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,payload_json) VALUES('child','project','Fixture','codex','full-access','default',${stamp},${stamp},${yield* encode({ projectId: "project", providerInstanceId: "provider", activeProviderThreadId: "provider-thread", worktreePath: real, branch: "repair-fixture", modelSelection, runtimeMode: "full-access", interactionMode: "default" })})`;
  yield* sql`INSERT INTO orchestration_v2_projection_provider_threads(provider_thread_id,thread_id,provider,provider_session_id,status,updated_at,payload_json) VALUES('provider-thread','child','codex','original-session','active',${stamp},${yield* encode({ id: "provider-thread", providerInstanceId: "provider", providerSessionId: "original-session", nativeThreadRef: { driver: "codex", nativeId: "native-original", strength: "strong" } })})`;
  return cwd;
});

it.effect("requires a configured check before allocating an automatic repair", () =>
  Effect.gen(function* () {
    yield* seed;
    const repairs = yield* Repairs.WorkbenchRepairs;
    expect(yield* repairs.observe("item")).toMatchObject({
      kind: "attention",
      reason: "policy_missing",
    });
    const sql = yield* SqlClient.SqlClient;
    expect(yield* sql`SELECT used,phase,reason FROM workbench_repair_generations`).toEqual([
      { used: 0, phase: "awaiting_user", reason: "policy_missing" },
    ]);
    expect(yield* sql`SELECT id FROM workbench_repair_attempts`).toEqual([]);
  }).pipe(Effect.provide(layer)),
);

it.live.each(["failure", "success"] as const)(
  "keeps three accepted repairs durable through replay and restart after %s",
  (ending) =>
    Effect.gen(function* () {
      const cwd = yield* nativeFixture,
        sql = yield* SqlClient.SqlClient,
        repairs = yield* Repairs.WorkbenchRepairs,
        sink = yield* EventSink.EventSinkV2;
      yield* evidenceFixture(cwd, "initial", 0);
      for (let ordinal = 1; ordinal <= 3; ordinal++) {
        const reserved = yield* repairs.observe("item");
        expect(reserved.kind).toBe("repair");
        expect(yield* repairs.observe("item")).toEqual(reserved);
        const [intent] = yield* sql<{
          request_json: string;
        }>`SELECT request_json FROM workbench_repair_attempts WHERE ordinal=${ordinal}`;
        const command = yield* decodeCommand(intent!.request_json);
        if (command.type !== "message.dispatch") throw new Error("Expected Native repair message");
        const now = yield* DateTime.now,
          runId = RunId.make(`repair-run:${ordinal}`);
        const commit = {
          commandId: command.commandId,
          commandType: command.type,
          threadId: ThreadId.make("child"),
          executionCommand: command,
          acceptedAt: now,
          events: [
            {
              id: EventId.make(`accepted:${ordinal}`),
              type: "run.created" as const,
              threadId: ThreadId.make("child"),
              runId,
              occurredAt: now,
              payload: {
                id: runId,
                threadId: ThreadId.make("child"),
                ordinal: ordinal + 1,
                providerInstanceId: ProviderInstanceId.make("provider"),
                modelSelection,
                providerThreadId: ProviderThreadId.make("provider-thread"),
                userMessageId: command.messageId,
                rootNodeId: null,
                activeAttemptId: null,
                status: "queued" as const,
                requestedAt: now,
                startedAt: null,
                completedAt: null,
                checkpointId: null,
                contextHandoffId: null,
              },
            },
          ],
          effects: [],
        };
        yield* sink.commitCommand(commit);
        yield* sink.commitCommand(commit);
        expect(
          (yield* sql<{ used: number }>`SELECT used FROM workbench_repair_generations`)[0]!.used,
        ).toBe(ordinal);
        yield* evidenceFixture(cwd, runId, ordinal, ending === "success" && ordinal === 3 ? 0 : 1);
      }
      if (ending === "success") {
        expect(yield* repairs.observe("item")).toEqual({ kind: "none" });
        expect((yield* sql`SELECT used,phase FROM workbench_repair_generations`)[0]).toEqual({
          used: 3,
          phase: "succeeded",
        });
        yield* sql`INSERT INTO orchestration_v2_projection_runs(run_id,thread_id,ordinal,provider,status,requested_at,payload_json) VALUES('next-batch','child',5,'codex','completed',${stamp},'{"id":"next-batch","status":"completed","activeAttemptId":null,"providerThreadId":"provider-thread"}')`;
        yield* sql`UPDATE workbench_work_items SET run_id='next-batch' WHERE id='item'`;
        yield* evidenceFixture(cwd, "next-batch", 4);
      }
      expect(yield* repairs.observe("item")).toMatchObject({
        kind: "attention",
        reason: "budget_exhausted",
      });
      const afterRestart = yield* Repairs.WorkbenchRepairs.pipe(
        Effect.flatMap((service) => service.observe("item")),
        Effect.provide(Repairs.layer),
      );
      expect(afterRestart).toMatchObject({ kind: "attention", reason: "budget_exhausted" });
      expect((yield* sql`SELECT id FROM workbench_repair_attempts`).length).toBe(3);
      const [tracking] = yield* sql<{
        tracking_id: string;
      }>`SELECT tracking_id FROM scheduled_task_execution_metadata`;
      const view = yield* repairs.list("queue", {
        controllerId: "controller",
        projectId: ProjectId.make("project"),
        expectedTrackingId: tracking!.tracking_id,
      });
      expect(view.repairs[0]).toMatchObject({
        generation: 1,
        used: 3,
        phase: "awaiting_user",
        dispatcherThreadId: "dispatcher",
      });
      expect(yield* encode(view)).not.toContain("SECRET");
      expect(yield* encode(view)).not.toContain(cwd);
      expect(yield* encode(view)).not.toContain("npm test");
    }).pipe(Effect.scoped, Effect.provide(nativeLayer)),
);

it.live.each(["cwd", "exit", "command", "provider", "approval", "definition"] as const)(
  "routes unverifiable or blocked %s evidence to a human without spending a repair",
  (shape) =>
    Effect.gen(function* () {
      const cwd = yield* nativeFixture,
        sql = yield* SqlClient.SqlClient,
        repairs = yield* Repairs.WorkbenchRepairs;
      yield* evidenceFixture(
        cwd,
        "initial",
        0,
        shape === "exit" ? null : 1,
        shape === "command" ? "npm test && echo pass" : "npm test",
        shape === "cwd" ? null : cwd,
      );
      if (shape === "provider")
        yield* sql`UPDATE orchestration_v2_projection_runs SET status='failed',payload_json=json_set(payload_json,'$.status','failed') WHERE run_id='initial'`;
      if (shape === "approval")
        yield* sql`INSERT INTO orchestration_v2_projection_runtime_requests(runtime_request_id,thread_id,node_id,kind,status,created_at,payload_json) VALUES('approval','child','node:0','permission','pending',${stamp},'{}')`;
      if (shape === "definition")
        yield* sql`UPDATE projection_projects SET scripts_json=json_set(scripts_json,'$[0].command','npm run changed') WHERE project_id='project'`;
      expect(yield* repairs.observe("item")).toMatchObject({
        kind: "attention",
        reason:
          shape === "provider"
            ? "provider_failed"
            : shape === "approval"
              ? "approval"
              : shape === "definition"
                ? "policy_changed"
                : "evidence_unavailable",
      });
      expect((yield* sql`SELECT used FROM workbench_repair_generations`)[0]).toEqual({ used: 0 });
      expect(yield* sql`SELECT id FROM workbench_repair_attempts`).toEqual([]);
    }).pipe(Effect.scoped, Effect.provide(nativeLayer)),
);
