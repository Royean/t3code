import { expect, it } from "@effect/vitest";
import {
  CommandId,
  EventId,
  MessageId,
  OrchestrationV2Command,
  OrchestrationV2RuntimeRequest,
  RunId,
  RuntimeRequestId,
  ThreadId,
} from "@t3tools/contracts";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as Fiber from "effect/Fiber";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as Outbox from "./EffectOutbox.ts";
import * as Guard from "./WorkbenchAttentionGuard.ts";
import * as EventSink from "./EventSink.ts";
import * as EventStore from "./EventStore.ts";
import * as ProjectionStore from "./ProjectionStore.ts";

const threadId = ThreadId.make("attention-child");
const encodeCommand = Schema.encodeEffect(Schema.fromJsonString(OrchestrationV2Command));
const encodeRun = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const decodeRequest = Schema.decodeEffect(
  Schema.fromJsonString(Schema.toCodecJson(OrchestrationV2RuntimeRequest)),
);
const command = {
  type: "message.dispatch" as const,
  commandId: CommandId.make("attention-resume-command"),
  messageId: MessageId.make("attention-resume-message"),
  threadId,
  text: "Continue with the recorded human result.",
  attachments: [],
  createdBy: "agent" as const,
  creationSource: "server" as const,
  dispatchMode: { type: "start_immediately" as const },
};
const seed = Effect.fn("WorkbenchAttentionGuard.test.seed")(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`INSERT INTO workbench_queues VALUES('queue','controller','project')`;
  yield* sql`INSERT INTO workbench_queue_bindings(task_id,task_created_at,queue_id,project_id,dispatcher_thread_id)
    VALUES('task','2026-11-26','queue','project','dispatcher')`;
  yield* sql`INSERT INTO workbench_work_items(id,task_id,task_created_at,project_id,business_delivery_id,dispatcher_thread_id,event_json,work_thread_id,created_at,updated_at)
    VALUES('item','task','2026-11-26','project','event','dispatcher','{}',${threadId},'2026-11-26','2026-11-26')`;
  yield* sql`INSERT INTO workbench_queue_entries(work_item_id,queue_id,state,claim_id,created_at,updated_at)
    VALUES('item','queue','occupied','claim','2026-11-26','2026-11-26')`;
});
const layer = Layer.mergeAll(Guard.layer, Outbox.layer).pipe(
  Layer.provideMerge(SqlitePersistence.layerMemory),
);
const eventLayer = EventSink.layer.pipe(
  Layer.provideMerge(
    Layer.merge(EventStore.layer, ProjectionStore.layer).pipe(Layer.provideMerge(layer)),
  ),
);
const seedResponse = Effect.fn("WorkbenchAttentionGuard.test.seedResponse")(function* () {
  yield* seed();
  const sql = yield* SqlClient.SqlClient;
  const responseCommand = {
    type: "runtime-request.respond" as const,
    commandId: CommandId.make("recorded-response"),
    threadId,
    requestId: RuntimeRequestId.make("original-request"),
    decision: "accept" as const,
  };
  const pending = yield* encodeRun({
    id: responseCommand.requestId,
    nodeId: "node",
    kind: "command",
    status: "pending",
    providerTurnId: null,
    nativeRequestRef: null,
    createdAt: DateTime.formatIso(yield* DateTime.now),
    resolvedAt: null,
    responseCapability: { type: "live", providerSessionId: "session" },
  });
  const item = yield* encodeRun({
    id: "card",
    requestId: responseCommand.requestId,
    type: "approval_request",
  });
  const source = {
    id: "source",
    version: 1,
    fingerprint: "original",
    queue_id: "queue",
    work_item_id: "item",
    cycle_number: 1,
    batch_id: null,
    thread_id: threadId,
    source_run_id: "source-run",
    source_attempt_id: null,
    node_id: "node",
    provider_thread_id: "provider-thread",
    provider_turn_id: null,
    runtime_request_id: responseCommand.requestId,
    provider_session_id: "session",
    request_json: pending,
    turn_item_id: "card",
    turn_item_json: item,
    capability: "live",
    provider_instance_id: "provider",
    model_selection_json: '{"model":"model"}',
    runtime_mode: "full-access",
    interaction_mode: "default",
    native_driver: "codex",
    native_thread_id: "native-thread",
    worktree_path: "/workspace",
    worktree_real_path: "/workspace",
    branch: "feature",
    git_common_dir: "/repo/.git",
  };
  const policy = yield* encodeRun({
    providerInstanceId: "provider",
    modelSelection: { model: "model" },
    runtimeMode: "full-access",
    interactionMode: "default",
  });
  yield* sql`INSERT INTO orchestration_v2_projection_threads(thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,payload_json)
    VALUES(${threadId},'project','Child','codex','full-access','default','2026-11-27','2026-11-27',${policy})`;
  yield* sql`INSERT INTO orchestration_v2_projection_runs(run_id,thread_id,ordinal,provider,status,requested_at,payload_json)
    VALUES('source-run',${threadId},1,'codex','waiting','2026-11-27','{}')`;
  yield* sql`UPDATE workbench_work_items SET run_id='source-run' WHERE id='item'`;
  yield* sql`INSERT INTO orchestration_v2_projection_nodes(node_id,thread_id,run_id,root_node_id,kind,status,provider_thread_id,payload_json)
    VALUES('node',${threadId},'source-run','node','approval','waiting','provider-thread','{}')`;
  yield* sql`INSERT INTO orchestration_v2_projection_provider_threads(provider_thread_id,thread_id,provider,provider_session_id,status,updated_at,payload_json)
    VALUES('provider-thread',${threadId},'codex','session','active','2026-11-27','{"nativeThreadRef":{"driver":"codex","nativeId":"native-thread"}}')`;
  yield* sql`INSERT INTO orchestration_v2_projection_runtime_requests(runtime_request_id,thread_id,node_id,kind,status,created_at,payload_json)
    VALUES(${responseCommand.requestId},${threadId},'node','command','pending','2026-11-27',${pending})`;
  yield* sql`INSERT INTO orchestration_v2_projection_turn_items(turn_item_id,thread_id,node_id,ordinal,type,status,updated_at,payload_json)
    VALUES('card',${threadId},'node',1,'approval_request','pending','2026-11-27',${item})`;
  yield* sql`INSERT INTO workbench_runtime_request_sources ${sql.insert({ ...source, status: "pending", summary_json: "{}", created_at: "2026-11-27", updated_at: "2026-11-27" })}`;
  yield* sql`INSERT INTO workbench_runtime_responses(id,source_id,source_version,source_json,queue_id,work_item_id,attention_epoch,claim_id,command_id,request_json,state,payload_sha,actor_json,native_principal_json,created_at,updated_at)
    VALUES('response','source',1,${yield* encodeRun(source)},'queue','item',0,'claim',${responseCommand.commandId},${yield* encodeCommand(responseCommand)},'queued','sha','{}','{}','2026-11-27','2026-11-27')`;
  return responseCommand;
});

it.effect(
  "admits only the recorded answer with the original source and occupied claim while held",
  () =>
    Effect.gen(function* () {
      const command = yield* seedResponse();
      const sql = yield* SqlClient.SqlClient;
      const guard = yield* Guard.WorkbenchAttentionGuard;
      const nativeCommand = { ...command, commandId: CommandId.make("original-ui-answer") };
      yield* guard.assertCommand(nativeCommand);
      yield* sql`UPDATE workbench_work_items SET attention_hold=1 WHERE id='item'`;
      expect((yield* guard.assertCommand(nativeCommand).pipe(Effect.flip)).cause).toMatchObject({
        reason: "claim-stale",
      });
      yield* guard.assertCommand(command);
      yield* guard.assertAdmission({ threadId, commandId: command.commandId, expectedEpoch: 0 });
      expect(
        (yield* guard.assertCommand({ ...command, decision: "acceptAlways" }).pipe(Effect.flip))
          .reason,
      ).toBe("response-mismatch");
      yield* sql`UPDATE workbench_runtime_request_sources SET version=2 WHERE id='source'`;
      expect((yield* guard.assertCommand(command).pipe(Effect.flip)).cause).toMatchObject({
        reason: "source-changed",
      });
      yield* sql`UPDATE workbench_runtime_request_sources SET version=1 WHERE id='source'`;
      yield* sql`UPDATE workbench_queue_entries SET claim_id='other-claim' WHERE work_item_id='item'`;
      expect((yield* guard.assertCommand(command).pipe(Effect.flip)).cause).toMatchObject({
        reason: "claim-stale",
      });
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["superseded", "uncharged", "claim", "epoch", "wake", "finished"] as const)(
  "admits a current repair's native answer but rejects its stale %s provenance",
  (drift) =>
    Effect.gen(function* () {
      const recorded = yield* seedResponse();
      const sql = yield* SqlClient.SqlClient;
      const guard = yield* Guard.WorkbenchAttentionGuard;
      yield* sql`UPDATE workbench_work_items SET run_id='original-work',accepted_run_id='original-work' WHERE id='item'`;
      yield* sql`INSERT INTO workbench_repair_generations(id,work_item_id,queue_id,task_id,task_created_at,dispatcher_thread_id,project_id,thread_id,generation,used,phase,reason,source_run_id,created_at,updated_at)
      VALUES('response-generation','item','queue','task','2026-11-26','dispatcher','project',${threadId},1,1,'awaiting_user','user_input','original-work','2026-11-27','2026-11-27')`;
      yield* sql`INSERT INTO workbench_repair_attempts(id,generation_id,work_item_id,queue_id,ordinal,charged,state,source_run_id,failure_evidence_json,thread_id,project_id,claim_id,wake_generation,attention_epoch,command_id,message_id,run_id,provider_instance_id,provider_thread_id,provider_session_id,native_driver,native_thread_id,worktree_path,worktree_real_path,branch,git_common_dir,model_selection_json,runtime_mode,interaction_mode,request_json,created_at,updated_at)
      VALUES('response-repair','response-generation','item','queue',1,1,'accepted','original-work','[]',${threadId},'project','claim',0,0,'repair-command','repair-message','source-run','provider','provider-thread','session','codex','native-thread','/workspace','/workspace','feature','/repo/.git','{"model":"model"}','full-access','default','{}','2026-11-27','2026-11-27')`;
      const native = { ...recorded, commandId: CommandId.make("repair-original-ui-answer") };
      yield* guard.assertCommand(native);
      yield* sql`UPDATE orchestration_v2_projection_runtime_requests SET status='resolved',payload_json=json_set(payload_json,'$.status','resolved','$.decision','accept') WHERE runtime_request_id=${native.requestId}`;
      yield* guard.assertResponseExecution({
        threadId,
        commandId: native.commandId,
        requestId: native.requestId,
        providerSessionId: "session",
        decision: "accept",
      });
      yield* sql`UPDATE orchestration_v2_projection_runtime_requests SET status='pending',payload_json=json_remove(json_set(payload_json,'$.status','pending'),'$.decision') WHERE runtime_request_id=${native.requestId}`;
      if (drift === "superseded")
        yield* sql`UPDATE workbench_repair_generations SET superseded_at='2026-11-27'`;
      if (drift === "uncharged") yield* sql`UPDATE workbench_repair_attempts SET charged=0`;
      if (drift === "claim") yield* sql`UPDATE workbench_queue_entries SET claim_id='other-claim'`;
      if (drift === "epoch") yield* sql`UPDATE workbench_work_items SET attention_epoch=1`;
      if (drift === "wake") yield* sql`UPDATE workbench_queue_entries SET wake_generation=1`;
      if (drift === "finished") yield* sql`UPDATE workbench_repair_attempts SET state='succeeded'`;
      expect((yield* guard.assertCommand(native).pipe(Effect.flip)).cause).toMatchObject({
        reason: "claim-stale",
      });
      expect(yield* sql`SELECT used FROM workbench_repair_generations`).toEqual([{ used: 1 }]);
    }).pipe(Effect.provide(layer)),
);

it.effect.each([false, true])(
  "rechecks a changed source in the command transaction after planning (missing command: %s)",
  (missing) =>
    Effect.gen(function* () {
      const command = yield* seedResponse();
      const sql = yield* SqlClient.SqlClient;
      const guard = yield* Guard.WorkbenchAttentionGuard;
      const sink = yield* EventSink.EventSinkV2;
      yield* guard.assertCommand(command);
      const [raw] = yield* sql<{
        payload_json: string;
      }>`SELECT payload_json FROM orchestration_v2_projection_runtime_requests WHERE runtime_request_id=${command.requestId}`;
      const request = yield* decodeRequest(raw!.payload_json);
      const now = yield* DateTime.now;
      // Provider ingestion and source refresh can race a plan without taking Orchestrator's command lock.
      yield* sql`UPDATE workbench_runtime_request_sources SET version=2 WHERE id='source'`;
      const rejected = yield* sink
        .commitCommand({
          commandId: command.commandId,
          commandType: command.type,
          threadId,
          acceptedAt: now,
          ...(missing ? {} : { responseCommand: command }),
          events: [
            {
              id: EventId.make("answer-resolved"),
              type: "runtime-request.updated",
              threadId,
              occurredAt: now,
              payload: { ...request, status: "resolved", decision: "accept", resolvedAt: now },
            },
          ],
          effects: [],
        })
        .pipe(Effect.flip);
      expect(rejected.cause).toMatchObject({
        reason: missing ? "command-mismatch" : "source-changed",
      });
      expect(
        yield* sql`SELECT 1 FROM orchestration_command_receipts WHERE command_id=${command.commandId}`,
      ).toHaveLength(0);
      expect(
        yield* sql`SELECT status FROM orchestration_v2_projection_runtime_requests WHERE runtime_request_id=${command.requestId}`,
      ).toEqual([{ status: "pending" }]);
    }).pipe(Effect.provide(eventLayer)),
);

it.effect(
  "requires an accepted running effect before handing the recorded answer to the live runtime",
  () =>
    Effect.gen(function* () {
      const command = yield* seedResponse();
      const sql = yield* SqlClient.SqlClient;
      const guard = yield* Guard.WorkbenchAttentionGuard;
      const sink = yield* EventSink.EventSinkV2;
      const outbox = yield* Outbox.EffectOutboxV2;
      const input = {
        threadId,
        commandId: command.commandId,
        responseId: "response",
        requestId: command.requestId,
        providerSessionId: "session",
        decision: "accept",
      };
      yield* sql`UPDATE orchestration_v2_projection_runtime_requests SET status='resolved',payload_json=json_set(payload_json,'$.status','resolved','$.decision','accept') WHERE runtime_request_id=${command.requestId}`;
      expect((yield* guard.assertResponseExecution(input).pipe(Effect.flip)).cause).toMatchObject({
        reason: "command-mismatch",
      });
      yield* sql`UPDATE orchestration_v2_projection_runtime_requests SET status='pending',payload_json=(SELECT request_json FROM workbench_runtime_request_sources WHERE id='source') WHERE runtime_request_id=${command.requestId}`;
      const [raw] = yield* sql<{
        payload_json: string;
      }>`SELECT payload_json FROM orchestration_v2_projection_runtime_requests WHERE runtime_request_id=${command.requestId}`;
      const request = yield* decodeRequest(raw!.payload_json);
      const now = yield* DateTime.now;
      yield* sink.commitCommand({
        commandId: command.commandId,
        commandType: command.type,
        threadId,
        responseCommand: command,
        acceptedAt: now,
        events: [
          {
            id: EventId.make("live-answer"),
            type: "runtime-request.updated",
            threadId,
            occurredAt: now,
            payload: { ...request, status: "resolved", decision: "accept", resolvedAt: now },
          },
        ],
        effects: [
          {
            id: "live-answer-effect",
            commandId: command.commandId,
            threadId,
            request: {
              type: "runtime-request.respond",
              providerSessionId: "session" as never,
              responseId: "response",
              requestId: command.requestId,
              decision: "accept",
            },
          },
        ],
      });
      expect((yield* guard.assertResponseExecution(input).pipe(Effect.flip)).cause).toMatchObject({
        reason: "command-mismatch",
      });
      const effect = Option.getOrThrow(yield* outbox.claimNext({ workerId: "worker" }));
      yield* sql`UPDATE workbench_work_items SET attention_hold=1 WHERE id='item'`;
      yield* sql`UPDATE workbench_runtime_request_sources SET version=2 WHERE id='source'`;
      yield* guard.assertResponseExecution(input);
      expect(
        (yield* guard
          .assertResponseExecution({ ...input, decision: "acceptAlways" })
          .pipe(Effect.flip)).cause,
      ).toMatchObject({ reason: "command-mismatch" });
      yield* outbox.cancelUnsettled({
        threadId,
        effectTypes: [effect.request.type],
        reason: "pause",
      });
      expect((yield* guard.assertResponseExecution(input).pipe(Effect.flip)).cause).toMatchObject({
        reason: "claim-stale",
      });
    }).pipe(Effect.provide(eventLayer)),
);

it.effect(
  "archives a successful live handoff without claiming provider delivery, including after retention",
  () =>
    Effect.gen(function* () {
      const command = yield* seedResponse();
      const sql = yield* SqlClient.SqlClient;
      const outbox = yield* Outbox.EffectOutboxV2;
      yield* outbox.enqueue([
        {
          id: "answer-effect",
          commandId: command.commandId,
          threadId,
          request: {
            type: "runtime-request.respond",
            responseId: "response",
            providerSessionId: "session" as never,
            requestId: command.requestId,
            decision: "accept",
          },
        },
      ]);
      yield* sql`UPDATE workbench_runtime_responses SET state='submitted',effect_id='answer-effect' WHERE id='response'`;
      const effect = Option.getOrThrow(yield* outbox.claimNext({ workerId: "worker" }));
      yield* outbox.succeed({ effectId: effect.id, workerId: "worker" });
      const [receipt] =
        yield* sql`SELECT state,evidence_level,delivered_at FROM workbench_runtime_responses WHERE id='response'`;
      expect(receipt).toMatchObject({
        state: "runtime_accepted",
        evidence_level: "runtime_handoff",
        delivered_at: null,
      });
      yield* sql`UPDATE orchestration_v2_effect_outbox SET completed_at='1960-01-01T00:00:00.000Z' WHERE effect_id='answer-effect'`;
      // Recover a crash between transport settlement and evidence publication before pruning its record.
      yield* sql`UPDATE workbench_runtime_responses SET state='submitted',evidence_level='command_receipt',effect_state=NULL WHERE id='response'`;
      yield* outbox.pruneSettled;
      expect(Option.isNone(yield* outbox.get(effect.id))).toBe(true);
      const [retained] =
        yield* sql`SELECT state,evidence_level,effect_state FROM workbench_runtime_responses WHERE id='response'`;
      expect(retained).toMatchObject({
        state: "runtime_accepted",
        evidence_level: "runtime_handoff",
        effect_state: "succeeded",
      });
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "requires an occupied original cycle or the durable claimed workbench response intent",
  () =>
    Effect.gen(function* () {
      yield* seed();
      const guard = yield* Guard.WorkbenchAttentionGuard;
      const sql = yield* SqlClient.SqlClient;
      yield* sql`UPDATE workbench_queue_entries SET claim_id=NULL WHERE work_item_id='item'`;
      const failure = yield* guard
        .assertCommand({
          type: "runtime-request.respond",
          commandId: CommandId.make("unregistered-response"),
          threadId,
          requestId: RuntimeRequestId.make("original-request"),
          decision: "accept",
        })
        .pipe(Effect.flip);
      expect(failure.reason).toBe("response-mismatch");
      expect(failure.cause).toMatchObject({ reason: "claim-stale" });
      expect(
        (yield* guard
          .assertCommand({
            type: "runtime-request.respond",
            commandId: CommandId.make("workbench-response:missing"),
            threadId,
            requestId: RuntimeRequestId.make("original-request"),
            decision: "accept",
          })
          .pipe(Effect.flip)).cause,
      ).toMatchObject({ reason: "intent-missing" });
    }).pipe(Effect.provide(layer)),
);

it.effect.each(["user_input", "command"] as const)(
  "protects a pending %s request from the private automatic Stop while preserving explicit user Stop",
  (kind) =>
    Effect.gen(function* () {
      yield* seed();
      const sql = yield* SqlClient.SqlClient;
      const guard = yield* Guard.WorkbenchAttentionGuard;
      const stop = {
        type: "thread.stop" as const,
        threadId,
        commandId: CommandId.make("private-pause-command"),
      };
      yield* sql`UPDATE workbench_work_items SET attention_hold=1,attention_epoch=1 WHERE id='item'`;
      yield* sql`INSERT INTO workbench_attention_items(id,queue_id,work_item_id,phase,reason_json,priority,pause_command_id,pause_wake_generation,epoch,created_at,updated_at)
      VALUES('attention','queue','item','pausing','{}',0,${stop.commandId},0,1,'2026-11-26','2026-11-26')`;
      yield* guard.assertCommand(stop);
      yield* sql`INSERT INTO orchestration_v2_projection_runtime_requests(runtime_request_id,thread_id,node_id,kind,status,created_at,payload_json)
      VALUES('late-pending',${threadId},'node',${kind},'pending','2026-11-26','{}')`;
      const refusal = yield* guard.assertCommand(stop).pipe(Effect.flip);
      expect(refusal.reason).toBe("runtime-request-pending");
      expect(refusal.runtimeRequestId).toBe("late-pending");
      yield* guard.assertCommand({ ...stop, commandId: CommandId.make("explicit-user-stop") });
      yield* sql`UPDATE workbench_attention_items SET observation_only=1,phase='blocked' WHERE id='attention'`;
      yield* sql`UPDATE workbench_work_items SET attention_hold=0 WHERE id='item'`;
      expect((yield* guard.assertCommand(stop).pipe(Effect.flip)).reason).toBe("stale");
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "holds ordinary commands and rejects old durable effects after the hold is released",
  () =>
    Effect.gen(function* () {
      yield* seed();
      const sql = yield* SqlClient.SqlClient;
      const guard = yield* Guard.WorkbenchAttentionGuard;
      const outbox = yield* Outbox.EffectOutboxV2;
      yield* guard.assertCommand(command);
      yield* outbox.enqueue([
        {
          id: "old-start",
          commandId: command.commandId,
          threadId,
          request: { type: "provider-turn.start", runId: RunId.make("old-run") },
        },
      ]);
      const effect = Option.getOrThrow(yield* outbox.get("old-start"));
      expect(effect.attentionEpoch).toBe(0);
      yield* sql`UPDATE workbench_work_items SET attention_hold=1,attention_epoch=1 WHERE id='item'`;
      for (const input of [
        command,
        { type: "queue.resume" as const, commandId: command.commandId, threadId },
      ]) {
        expect((yield* guard.assertCommand(input).pipe(Effect.flip)).reason).toBe("held");
      }
      yield* sql`UPDATE workbench_work_items SET attention_hold=0 WHERE id='item'`;
      for (const expectedEpoch of [effect.attentionEpoch ?? null, null]) {
        expect(
          (yield* guard.assertAdmission({ threadId, expectedEpoch }).pipe(Effect.flip)).reason,
        ).toBe("stale");
      }
    }).pipe(Effect.provide(layer)),
);

it.effect(
  "requires the claimed exact resume and finds it before the HTTP observer records its run",
  () =>
    Effect.gen(function* () {
      yield* seed();
      const sql = yield* SqlClient.SqlClient;
      const guard = yield* Guard.WorkbenchAttentionGuard;
      const requestJson = yield* encodeCommand(command);
      yield* sql`UPDATE workbench_work_items SET attention_hold=1,attention_epoch=1 WHERE id='item'`;
      yield* sql`INSERT INTO workbench_attention_resume_intents(id,attention_id,work_item_id,queue_id,attention_epoch,thread_id,project_id,command_id,message_id,provider_instance_id,provider_session_id,provider_thread_id,native_driver,native_thread_id,worktree_path,worktree_real_path,branch,git_common_dir,request_json,state,created_at,updated_at)
    VALUES('resume','attention','item','queue',1,${threadId},'project',${command.commandId},${command.messageId},'provider','session','provider-thread','codex','native-thread','/workspace','/workspace','feature','/repo/.git',${requestJson},'pending','2026-11-26','2026-11-26')`;
      expect((yield* guard.assertCommand(command).pipe(Effect.flip)).reason).toBe("stale");
      yield* sql`UPDATE workbench_attention_resume_intents SET claim_id='claim' WHERE id='resume'`;
      yield* guard.assertCommand(command);
      expect(
        (yield* guard.assertCommand({ ...command, text: "Changed" }).pipe(Effect.flip)).reason,
      ).toBe("resume-mismatch");
      const runId = RunId.make("accepted-resume");
      const runJson = yield* encodeRun({
        userMessageId: command.messageId,
      });
      yield* sql`INSERT INTO orchestration_v2_projection_runs(run_id,thread_id,ordinal,provider,status,requested_at,payload_json)
    VALUES(${runId},${threadId},1,'codex','starting','2026-11-26',${runJson})`;
      expect((yield* guard.originalResume({ threadId, runId }))?.nativeThreadId).toBe(
        "native-thread",
      );
      yield* sql`UPDATE workbench_work_items SET attention_epoch=2,attention_hold=0 WHERE id='item'`;
      expect((yield* guard.originalResume({ threadId, runId }).pipe(Effect.flip)).reason).toBe(
        "stale",
      );
    }).pipe(Effect.provide(layer)),
);

it.effect("waits for an admitted external call to leave after the durable fence commits", () =>
  Effect.gen(function* () {
    yield* seed();
    const sql = yield* SqlClient.SqlClient;
    const guard = yield* Guard.WorkbenchAttentionGuard;
    const entered = yield* Deferred.make<void>();
    const release = yield* Deferred.make<void>();
    const call = yield* guard
      .withAdmission(
        { threadId, expectedEpoch: 0 },
        Deferred.succeed(entered, undefined).pipe(Effect.andThen(Deferred.await(release))),
      )
      .pipe(Effect.forkChild({ startImmediately: true }));
    yield* Deferred.await(entered);
    yield* sql`UPDATE workbench_work_items SET attention_hold=1,attention_epoch=1 WHERE id='item'`;
    const waiting = yield* guard
      .awaitAdmissions(threadId)
      .pipe(Effect.forkChild({ startImmediately: true }));
    expect(waiting.pollUnsafe()).toBeUndefined();
    yield* Deferred.succeed(release, undefined);
    yield* Fiber.join(call);
    yield* Fiber.join(waiting);
  }).pipe(Effect.provide(layer)),
);
