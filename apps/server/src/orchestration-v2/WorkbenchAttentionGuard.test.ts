import { expect, it } from "@effect/vitest";
import { CommandId, MessageId, OrchestrationV2Command, RunId, ThreadId } from "@t3tools/contracts";
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

const threadId = ThreadId.make("attention-child");
const encodeCommand = Schema.encodeEffect(Schema.fromJsonString(OrchestrationV2Command));
const encodeRun = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
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
  yield* sql`INSERT INTO workbench_queue_bindings VALUES('task','2026-11-26','queue','project','dispatcher')`;
  yield* sql`INSERT INTO workbench_work_items(id,task_id,task_created_at,project_id,business_delivery_id,dispatcher_thread_id,event_json,work_thread_id,created_at,updated_at)
    VALUES('item','task','2026-11-26','project','event','dispatcher','{}',${threadId},'2026-11-26','2026-11-26')`;
  yield* sql`INSERT INTO workbench_queue_entries(work_item_id,queue_id,state,claim_id,created_at,updated_at)
    VALUES('item','queue','occupied','claim','2026-11-26','2026-11-26')`;
});
const layer = Layer.mergeAll(Guard.layer, Outbox.layer).pipe(
  Layer.provideMerge(SqlitePersistence.layerMemory),
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
        {
          type: "runtime-request.respond" as const,
          commandId: command.commandId,
          threadId,
          requestId: "request" as never,
        },
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
