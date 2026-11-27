import { OrchestrationV2Command, WorkbenchQueueError } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Scope from "effect/Scope";
import * as SqlClient from "effect/sql/SqlClient";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as Engine from "../orchestration-v2/Orchestrator.ts";
import { forkParked } from "../serverActivation.ts";
import * as Attention from "./WorkbenchAttention.ts";
import * as Repairs from "./WorkbenchRepairs.ts";

const decodeCommand = Schema.decodeEffect(Schema.fromJsonString(OrchestrationV2Command));
export class WorkbenchRepairReactor extends Context.Service<
  WorkbenchRepairReactor,
  {
    readonly reconcile: (workItemId: string) => Effect.Effect<void, WorkbenchQueueError>;
    readonly start: () => Effect.Effect<void>;
  }
>()("t3/scheduledTasks/WorkbenchRepairReactor") {}
const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient,
    engine = yield* Engine.OrchestratorV2,
    repairs = yield* Repairs.WorkbenchRepairs,
    attention = yield* Attention.WorkbenchAttention;
  const locks = yield* KeyedLock.make<string>();
  const scope = yield* Scope.Scope;
  const reconcile = Effect.fn("WorkbenchRepairReactor.reconcile")(
    function* (workItemId: string) {
      const result = yield* repairs.observe(workItemId);
      if (result.kind === "attention")
        return yield* attention.pauseSystem(workItemId, result.generationId);
      if (result.kind !== "repair") return;
      const [intent] = yield* sql<{
        request_json: string;
      }>`SELECT request_json FROM workbench_repair_attempts WHERE id=${result.attemptId} AND state='reserved'`;
      if (!intent) return;
      const command = yield* decodeCommand(intent.request_json);
      if (command.type !== "message.dispatch")
        return yield* new WorkbenchQueueError({ reason: "stale_state" });
      yield* engine.dispatch(command);
    },
    (effect, id) => locks.withLock(id, effect),
    Effect.mapError((cause) =>
      Object.defineProperty(new WorkbenchQueueError({ reason: "unavailable" }), "cause", {
        value: cause,
      }),
    ),
  );
  const report = Effect.fn("WorkbenchRepairReactor.report")(function* (workItemId: string) {
    yield* reconcile(workItemId).pipe(
      Effect.catchCause((cause) =>
        Effect.logWarning("Workbench repair observation requires recovery", { workItemId, cause }),
      ),
    );
  });
  const start = Effect.fn("WorkbenchRepairReactor.start")(
    function* () {
      yield* forkParked(
        Stream.runForEach(engine.streamDomainEvents, (event) => {
          if (
            ![
              "run.created",
              "run.updated",
              "run-attempt.updated",
              "turn-item.updated",
              "runtime-request.updated",
              "subagent.updated",
            ].includes(event.type)
          )
            return Effect.void;
          return Effect.gen(function* () {
            const rows = yield* sql<{
              id: string;
            }>`SELECT id FROM workbench_work_items WHERE work_thread_id=${event.threadId} AND business_state!='completed'`;
            yield* Effect.forEach(rows, (row) => report(row.id), { discard: true });
          });
        }).pipe(
          Effect.catchCause((cause) =>
            Effect.logWarning("Workbench repair event stream failed", { cause }),
          ),
        ),
      );
      const rows = yield* sql<{
        id: string;
      }>`SELECT id FROM workbench_work_items WHERE work_thread_id IS NOT NULL AND business_state!='completed'`;
      yield* Effect.forEach(rows, (row) => report(row.id), { discard: true });
    },
    Effect.provideService(Scope.Scope, scope),
    Effect.catchCause((cause) =>
      Effect.logWarning("Workbench repair startup recovery failed", { cause }),
    ),
  );
  return WorkbenchRepairReactor.of({ reconcile, start });
});
export const layer = Layer.effect(WorkbenchRepairReactor, make);
