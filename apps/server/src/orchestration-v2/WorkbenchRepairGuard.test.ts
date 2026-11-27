import { expect, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  NodeId,
  OrchestrationV2Command,
  ProviderInstanceId,
  ProviderThreadId,
  RunId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { derive } from "@t3tools/provider-core/server/IdAllocator";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Guard from "./WorkbenchAttentionGuard.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

const threadId = ThreadId.make("repair-child");
const providerInstanceId = ProviderInstanceId.make("provider");
const providerThreadId = ProviderThreadId.make("original-provider-thread");
const modelSelection = { instanceId: providerInstanceId, model: "original-model" };
const encodeJson = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const encodeCommand = Schema.encodeEffect(Schema.fromJsonString(OrchestrationV2Command));
const command = {
  type: "message.dispatch" as const,
  commandId: CommandId.make("command:workbench-repair:third"),
  messageId: MessageId.make("message:workbench-repair:third"),
  threadId,
  text: "Repair the recorded check and run the same checks again.",
  attachments: [],
  createdBy: "agent" as const,
  creationSource: "server" as const,
  dispatchMode: { type: "queue_after_active" as const },
};
const layer = Layer.merge(Guard.layer, EventSink.layer).pipe(
  Layer.provideMerge(Layer.merge(EventStore.layer, ProjectionStore.layer)),
  Layer.provideMerge(Sqlite.layerMemory),
);

const seed = Effect.fn("WorkbenchRepairGuard.test.seed")(function* () {
  const sql = yield* SqlClient.SqlClient;
  const now = DateTime.formatIso(yield* DateTime.now);
  yield* sql`INSERT INTO workbench_queues VALUES('repair-queue','controller','project')`;
  yield* sql`INSERT INTO workbench_queue_bindings(task_id,task_created_at,queue_id,project_id,dispatcher_thread_id)
    VALUES('task','2026-11-27','repair-queue','project','dispatcher')`;
  yield* sql`INSERT INTO workbench_work_items(id,task_id,task_created_at,project_id,business_delivery_id,dispatcher_thread_id,event_json,work_thread_id,run_id,branch,worktree_path,created_at,updated_at)
    VALUES('repair-item','task','2026-11-27','project','event','dispatcher','{}',${threadId},'source-run','feature','/workspace',${now},${now})`;
  yield* sql`INSERT INTO workbench_queue_entries(work_item_id,queue_id,state,claim_id,wake_generation,created_at,updated_at)
    VALUES('repair-item','repair-queue','occupied','claim',2,${now},${now})`;
  const thread = yield* encodeJson({
    projectId: "project",
    providerInstanceId,
    activeProviderThreadId: providerThreadId,
    modelSelection,
    runtimeMode: "full-access",
    interactionMode: "default",
    worktreePath: "/workspace",
    branch: "feature",
  });
  yield* sql`INSERT INTO orchestration_v2_projection_threads(thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,payload_json)
    VALUES(${threadId},'project','Original','codex','full-access','default',${now},${now},${thread})`;
  const provider = yield* encodeJson({
    providerInstanceId,
    nativeThreadRef: { driver: "codex", nativeId: "original-native-thread", strength: "strong" },
  });
  yield* sql`INSERT INTO orchestration_v2_projection_provider_threads(provider_thread_id,thread_id,provider,provider_session_id,status,updated_at,payload_json)
    VALUES(${providerThreadId},${threadId},'codex','original-session','active',${now},${provider})`;
  yield* sql`INSERT INTO workbench_repair_generations(id,work_item_id,queue_id,task_id,task_created_at,dispatcher_thread_id,project_id,thread_id,generation,used,phase,source_run_id,created_at,updated_at)
    VALUES('generation','repair-item','repair-queue','task','2026-11-27','dispatcher','project',${threadId},1,2,'repairing','source-run',${now},${now})`;
  yield* sql`INSERT INTO workbench_repair_attempts(id,generation_id,work_item_id,queue_id,ordinal,state,source_run_id,failure_evidence_json,thread_id,project_id,claim_id,wake_generation,attention_epoch,command_id,message_id,provider_instance_id,provider_thread_id,provider_session_id,native_driver,native_thread_id,worktree_path,worktree_real_path,branch,git_common_dir,model_selection_json,runtime_mode,interaction_mode,request_json,created_at,updated_at)
    VALUES('third','generation','repair-item','repair-queue',3,'reserved','source-run','[]',${threadId},'project','claim',2,0,${command.commandId},${command.messageId},${providerInstanceId},${providerThreadId},'original-session','codex','original-native-thread','/workspace','/workspace','feature','/repo/.git',${yield* encodeJson(modelSelection)},'full-access','default',${yield* encodeCommand(command)},${now},${now})`;
});

const planned = Effect.fn("WorkbenchRepairGuard.test.planned")(function* () {
  const now = yield* DateTime.now;
  const runId = RunId.make("third-run");
  return {
    commandId: command.commandId,
    commandType: command.type,
    threadId,
    executionCommand: command,
    acceptedAt: now,
    events: [
      {
        id: EventId.make("third-run-created"),
        type: "run.created" as const,
        threadId,
        runId,
        occurredAt: now,
        payload: {
          id: runId,
          threadId,
          ordinal: 2,
          providerInstanceId,
          modelSelection,
          providerThreadId,
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
});

it.effect("admits the exact third repair and blocks altered or unregistered sends", () =>
  Effect.gen(function* () {
    yield* seed();
    const guard = yield* Guard.WorkbenchAttentionGuard;
    yield* guard.assertCommand(command);
    for (const candidate of [
      { ...command, text: "Different task" },
      { ...command, commandId: CommandId.make("raw-agent-send"), messageId: MessageId.make("raw") },
      { type: "queue.resume" as const, threadId, commandId: CommandId.make("raw-resume") },
    ]) {
      expect((yield* guard.assertCommand(candidate).pipe(Effect.result))._tag).toBe("Failure");
    }
    expect(
      (yield* guard.originalResume({ threadId, commandId: command.commandId }))?.nativeThreadId,
    ).toBe("original-native-thread");
  }).pipe(Effect.provide(layer)),
);

it.effect.each(["claim", "wake", "epoch", "hold", "model", "provider", "generation"] as const)(
  "rejects repair admission when its original %s fence changed",
  (drift) =>
    Effect.gen(function* () {
      yield* seed();
      const sql = yield* SqlClient.SqlClient;
      const guard = yield* Guard.WorkbenchAttentionGuard;
      if (drift === "claim") yield* sql`UPDATE workbench_queue_entries SET claim_id='other'`;
      if (drift === "wake") yield* sql`UPDATE workbench_queue_entries SET wake_generation=3`;
      if (drift === "epoch") yield* sql`UPDATE workbench_work_items SET attention_epoch=1`;
      if (drift === "hold") yield* sql`UPDATE workbench_work_items SET attention_hold=1`;
      if (drift === "model")
        yield* sql`UPDATE orchestration_v2_projection_threads SET payload_json=json_set(payload_json,'$.modelSelection.model','other')`;
      if (drift === "provider")
        yield* sql`UPDATE orchestration_v2_projection_provider_threads SET provider_session_id='other'`;
      if (drift === "generation")
        yield* sql`UPDATE workbench_repair_generations SET superseded_at='2026-11-27'`;
      expect((yield* guard.assertCommand(command).pipe(Effect.result))._tag).toBe("Failure");
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "charges acceptance once with run creation and admits the charged third run at used three",
  () =>
    Effect.gen(function* () {
      yield* seed();
      const sink = yield* EventSink.EventSinkV2;
      const guard = yield* Guard.WorkbenchAttentionGuard;
      const sql = yield* SqlClient.SqlClient;
      const input = yield* planned();
      expect((yield* sink.commitCommand(input)).committed).toBe(true);
      expect((yield* sink.commitCommand(input)).committed).toBe(false);
      expect(yield* sql`SELECT used FROM workbench_repair_generations`).toEqual([{ used: 3 }]);
      expect(yield* sql`SELECT charged,state,run_id FROM workbench_repair_attempts`).toEqual([
        { charged: 1, state: "accepted", run_id: "third-run" },
      ]);
      yield* guard.assertAdmission({ threadId, runId: RunId.make("third-run"), expectedEpoch: 0 });
      expect(
        (yield* guard.originalResume({ threadId, runId: RunId.make("third-run") }))?.nativeThreadId,
      ).toBe("original-native-thread");
      for (const effectType of ["provider-turn.restart", "provider-turn.steer"]) {
        let called = false;
        expect(
          (yield* guard
            .withAdmission(
              {
                threadId,
                runId: RunId.make("third-run"),
                commandId: command.commandId,
                expectedEpoch: 0,
                effectType,
              },
              Effect.sync(() => {
                called = true;
              }),
            )
            .pipe(Effect.result))._tag,
        ).toBe("Failure");
        expect(called).toBe(false);
      }
      expect(
        (yield* guard
          .assertCommand({ ...command, commandId: CommandId.make("fourth") })
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
    }).pipe(Effect.provide(layer)),
);

it.effect("rejects an unregistered raw command at the event sink and never creates its run", () =>
  Effect.gen(function* () {
    yield* seed();
    const sink = yield* EventSink.EventSinkV2;
    const sql = yield* SqlClient.SqlClient;
    const input = yield* planned();
    const raw = {
      ...command,
      commandId: CommandId.make("raw-agent"),
      messageId: MessageId.make("raw-message"),
    };
    expect(
      (yield* sink
        .commitCommand({
          ...input,
          commandId: raw.commandId,
          executionCommand: raw,
        })
        .pipe(Effect.result))._tag,
    ).toBe("Failure");
    expect(
      yield* sql`SELECT 1 FROM orchestration_v2_projection_runs WHERE run_id='third-run'`,
    ).toHaveLength(0);
    expect(yield* sql`SELECT used FROM workbench_repair_generations`).toEqual([{ used: 2 }]);
  }).pipe(Effect.provide(layer)),
);

it.effect.each([false, true])(
  "keeps delegation within its charged repair and fences descendants after a hold (answered: %s)",
  (answered) =>
    Effect.gen(function* () {
      yield* seed();
      const sink = yield* EventSink.EventSinkV2;
      const guard = yield* Guard.WorkbenchAttentionGuard;
      const sql = yield* SqlClient.SqlClient;
      yield* sink.commitCommand(yield* planned());
      yield* sql`UPDATE orchestration_v2_projection_runs SET status='running',payload_json=json_set(payload_json,'$.status','running') WHERE run_id='third-run'`;
      yield* sql`INSERT INTO orchestration_v2_projection_nodes(node_id,thread_id,run_id,root_node_id,kind,status,provider_thread_id,payload_json)
      VALUES('repair-parent-node',${threadId},'third-run','repair-parent-node','root_turn','running',${providerThreadId},'{}')`;
      let parentRunId = RunId.make("third-run");
      let parentNodeId = NodeId.make("repair-parent-node");
      if (answered) {
        parentRunId = RunId.make("native-answer-run");
        parentNodeId = NodeId.make("native-answer-node");
        yield* sql`UPDATE orchestration_v2_projection_runs SET status='completed' WHERE run_id='third-run'`;
        yield* sql`INSERT INTO orchestration_v2_projection_runtime_requests(runtime_request_id,thread_id,node_id,kind,status,created_at,payload_json)
        VALUES('question',${threadId},'repair-parent-node','user_input','resolved','2026-11-27','{"responseCapability":{"type":"message"}}')`;
        yield* sql`INSERT INTO orchestration_v2_projection_runs(run_id,thread_id,ordinal,provider,status,requested_at,payload_json)
        VALUES(${parentRunId},${threadId},3,'codex','running','2026-11-27',${yield* encodeJson({ userMessageId: "async-answer:question", providerInstanceId, providerThreadId, modelSelection })})`;
        yield* sql`INSERT INTO orchestration_v2_projection_nodes(node_id,thread_id,run_id,root_node_id,kind,status,provider_thread_id,payload_json)
        VALUES(${parentNodeId},${threadId},${parentRunId},${parentNodeId},'root_turn','running',${providerThreadId},'{}')`;
        yield* sql`INSERT INTO orchestration_v2_projection_messages(message_id,thread_id,run_id,role,streaming,created_at,updated_at,payload_json)
        VALUES('async-answer:question',${threadId},${parentRunId},'user',0,'2026-11-27','2026-11-27','{"creationSource":"server"}')`;
        yield* sql`INSERT INTO orchestration_command_receipts(command_id,aggregate_kind,aggregate_id,command_type,accepted_at,result_sequence,status)
        VALUES('native-answer','thread',${threadId},'runtime-request.respond','2026-11-27',2,'accepted')`;
        yield* sql`INSERT INTO workbench_repair_native_answers(command_id,generation_id,work_item_id,queue_id,repair_attempt_id,claim_id,wake_generation,attention_epoch,thread_id,request_id,source_run_id,message_id,run_id,created_at)
        VALUES('native-answer','generation','repair-item','repair-queue','third','claim',2,0,${threadId},'question','third-run','async-answer:question',${parentRunId},'2026-11-27')`;
        yield* guard.assertAdmission({ threadId, runId: parentRunId });
        expect(
          (yield* guard.originalResume({ threadId, runId: parentRunId }))?.nativeThreadId,
        ).toBe("original-native-thread");
      }
      const delegation = {
        type: "delegated_task.request" as const,
        commandId: CommandId.make("repair-child-task"),
        parentThreadId: threadId,
        parentRunId,
        parentNodeId,
        task: "Investigate this check within the current repair.",
        modelSelection,
        runtimeMode: "full-access" as const,
        interactionMode: "default" as const,
        createdBy: "agent" as const,
        creationSource: "server" as const,
      };
      yield* guard.assertCommand(delegation);
      expect(yield* sql`SELECT used FROM workbench_repair_generations`).toEqual([{ used: 3 }]);
      expect(
        (yield* guard
          .assertCommand({ ...delegation, parentRunId: RunId.make("different-run") })
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
      yield* sql`INSERT INTO orchestration_v2_projection_subagents(subagent_id,thread_id,run_id,parent_node_id,provider,child_thread_id,origin,status,updated_at,payload_json)
      VALUES('repair-task',${threadId},${parentRunId},${parentNodeId},'codex','repair-grandchild','app_owned','running','2026-11-27','{}')`;
      const child = { threadId: ThreadId.make("repair-grandchild"), expectedEpoch: 0 };
      yield* guard.assertAdmission(child);
      yield* sql`UPDATE workbench_work_items SET attention_hold=1,attention_epoch=1 WHERE id='repair-item'`;
      let called = false;
      expect(
        (yield* guard
          .withAdmission(
            child,
            Effect.sync(() => {
              called = true;
            }),
          )
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
      expect(called).toBe(false);
    }).pipe(Effect.provide(layer)),
);

it.effect("preserves a registered original run's delegation while watching its checks", () =>
  Effect.gen(function* () {
    yield* seed();
    const guard = yield* Guard.WorkbenchAttentionGuard;
    const sql = yield* SqlClient.SqlClient;
    yield* sql`UPDATE workbench_repair_generations SET phase='watching'`;
    yield* guard.assertCommand({
      type: "delegated_task.request",
      commandId: CommandId.make("original-run-delegation"),
      parentThreadId: threadId,
      parentRunId: RunId.make("source-run"),
      parentNodeId: NodeId.make("original-node"),
      task: "Continue the original task.",
      modelSelection,
      runtimeMode: "full-access",
      interactionMode: "default",
      createdBy: "agent",
      creationSource: "server",
    });
  }).pipe(Effect.provide(layer)),
);

it.effect.each([false, true])(
  "admits only the durable original delegation's prepared run before observation catches up (linked: %s)",
  (linked) =>
    Effect.gen(function* () {
      yield* seed();
      const sql = yield* SqlClient.SqlClient;
      const guard = yield* Guard.WorkbenchAttentionGuard;
      const requestId = CommandId.make("original-delegation");
      const childId = derive.delegatedTaskThread({ commandId: requestId });
      const runId = derive.run({ threadId: childId, ordinal: 1 });
      const taskId = derive.delegatedTaskNode({ commandId: requestId });
      const messageId = derive.delegatedTaskMessage({ commandId: requestId });
      const stamp = DateTime.formatIso(yield* DateTime.now);
      yield* sql`DELETE FROM workbench_repair_attempts`;
      yield* sql`UPDATE workbench_repair_generations SET phase='watching',used=0,thread_id=${childId},source_run_id=NULL`;
      yield* sql`UPDATE workbench_work_items SET request_id=${requestId},work_thread_id=${linked ? childId : null},run_id=NULL,accepted_run_id=NULL`;
      yield* sql`UPDATE orchestration_v2_projection_threads SET thread_id=${childId},payload_json=json_set(payload_json,
      '$.lineage.parentThreadId','dispatcher','$.lineage.relationshipToParent','subagent',
      '$.forkedFrom.type','node','$.forkedFrom.nodeId',${taskId}) WHERE thread_id=${threadId}`;
      yield* sql`INSERT INTO orchestration_v2_projection_threads(thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,payload_json)
      VALUES('dispatcher','project','Dispatcher','codex','full-access','default',${stamp},${stamp},'{}')`;
      yield* sql`INSERT INTO orchestration_v2_projection_runs(run_id,thread_id,ordinal,provider,status,requested_at,payload_json)
      VALUES(${runId},${childId},1,'codex','preparing',${stamp},${yield* encodeJson({ userMessageId: messageId, delegatedTaskId: taskId })})`;
      yield* sql`INSERT INTO orchestration_v2_projection_messages(message_id,thread_id,run_id,role,streaming,created_at,updated_at,payload_json)
      VALUES(${messageId},${childId},${runId},'user',0,${stamp},${stamp},'{}')`;
      yield* sql`INSERT INTO orchestration_v2_projection_subagents(subagent_id,thread_id,run_id,parent_node_id,provider,child_thread_id,origin,status,updated_at,payload_json)
      VALUES(${taskId},'dispatcher','dispatcher-run','dispatcher-node','codex',${childId},'app_owned','pending',${stamp},'{}')`;
      yield* sql`INSERT INTO orchestration_command_receipts(command_id,aggregate_kind,aggregate_id,command_type,status,accepted_at,result_sequence)
      VALUES(${requestId},'thread','dispatcher','delegated_task.request','accepted',${stamp},1)`;
      const release = {
        type: "prepared-run.release" as const,
        commandId: CommandId.make("original-delegation:release"),
        threadId: childId,
        runId,
      };
      yield* guard.assertCommand(release);
      expect(
        (yield* guard
          .assertCommand({
            ...command,
            threadId: childId,
            commandId: CommandId.make("raw-before-observation"),
          })
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
      expect(
        (yield* guard
          .assertCommand({ ...release, runId: RunId.make("unregistered-run") })
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
      yield* sql`DELETE FROM orchestration_events`;
      yield* guard.assertCommand(release);
      yield* sql`UPDATE orchestration_command_receipts SET status='rejected' WHERE command_id=${requestId}`;
      expect((yield* guard.assertCommand(release).pipe(Effect.result))._tag).toBe("Failure");
      expect(yield* sql`SELECT used FROM workbench_repair_generations`).toEqual([{ used: 0 }]);
    }).pipe(Effect.provide(layer)),
);

it.effect.each([false, true])(
  "rechecks repair fences in the accepted-command transaction without charging a refusal (missing command: %s)",
  (missingCommand) =>
    Effect.gen(function* () {
      yield* seed();
      const guard = yield* Guard.WorkbenchAttentionGuard;
      const sink = yield* EventSink.EventSinkV2;
      const sql = yield* SqlClient.SqlClient;
      yield* guard.assertCommand(command);
      const { executionCommand, ...rest } = yield* planned();
      yield* sql`UPDATE workbench_queue_entries SET wake_generation=3`;
      expect(
        (yield* sink
          .commitCommand({
            ...rest,
            ...(missingCommand ? {} : { executionCommand }),
          })
          .pipe(Effect.result))._tag,
      ).toBe("Failure");
      expect(yield* sql`SELECT used FROM workbench_repair_generations`).toEqual([{ used: 2 }]);
      expect(yield* sql`SELECT charged,run_id FROM workbench_repair_attempts`).toEqual([
        { charged: 0, run_id: null },
      ]);
      expect(
        yield* sql`SELECT 1 FROM orchestration_command_receipts WHERE command_id=${command.commandId}`,
      ).toHaveLength(0);
      expect(
        yield* sql`SELECT 1 FROM orchestration_v2_projection_runs WHERE run_id='third-run'`,
      ).toHaveLength(0);
    }).pipe(Effect.provide(layer)),
);
