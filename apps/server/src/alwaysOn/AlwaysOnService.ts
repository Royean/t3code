import {
  ALWAYS_ON_PROVIDER_POLICY,
  AlwaysOnAgentId,
  AlwaysOnModelDecision,
  AlwaysOnAgentSummary,
  AlwaysOnApprovalId,
  AlwaysOnCancelInput,
  AlwaysOnCreateInput,
  AlwaysOnCreateResult,
  AlwaysOnDetail,
  AlwaysOnError,
  AlwaysOnEventId,
  AlwaysOnEventKind,
  AlwaysOnGetInput,
  AlwaysOnLoadedContext,
  AlwaysOnMemoryId,
  AlwaysOnMemoryRecord,
  AlwaysOnPhase,
  AlwaysOnReadMemoryInput,
  AlwaysOnResolveApprovalInput,
  AlwaysOnRunId,
  AlwaysOnRunOutcome,
  AlwaysOnRunStatus,
  AlwaysOnSessionId,
  AlwaysOnSetEnabledInput,
  AlwaysOnSnapshot,
  AlwaysOnSubmitEventInput,
  AlwaysOnSubmitEventResult,
  ProjectId,
  type AlwaysOnEffectAction,
  type AlwaysOnLoadedContext as AlwaysOnLoadedContextType,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import * as Context from "effect/Context";
import * as Option from "effect/Option";
import * as SecureRandom from "effect/crypto/SecureRandom";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as PubSub from "effect/PubSub";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as Stream from "effect/Stream";
import * as SqlClient from "effect/sql/SqlClient";
import * as SqlError from "effect/sql/SqlError";

import * as Deferred from "effect/Deferred";

import * as Scheduler from "../scheduling/Scheduler.ts";
import * as AlwaysOnModel from "./AlwaysOnModel.ts";
import {
  alwaysOnPrompt,
  containsCredential,
  nextRecurringDue,
  selectInboxBatch,
} from "./alwaysOnLogic.ts";

const LEASE_MS = 30_000;
const MAX_PASSES = 20;
const MAX_BATCH = 20;
const OUTBOX_ATTEMPTS = 5;

const decodeDecision = Schema.decodeUnknownEffect(AlwaysOnModelDecision);
const MODEL_TIMEOUT = "20 seconds";
const decodeContext = Schema.decodeUnknownSync(AlwaysOnLoadedContext);
export class AlwaysOnDeliveryFailed extends Schema.TaggedError<AlwaysOnDeliveryFailed>()(
  "AlwaysOnDeliveryFailed",
  {},
) {}

/** Test seam for the crash window after a run is durable and before its effects. */
export class AlwaysOnRunGate extends Context.Reference<{
  readonly afterClaim: Effect.Effect<void>;
  readonly afterDecision?: Effect.Effect<void>;
  readonly afterUncertain: Effect.Effect<void>;
}>("t3/alwaysOn/AlwaysOnRunGate", {
  defaultValue: () => ({
    afterClaim: Effect.void,
    afterUncertain: Effect.void,
  }),
}) {}

/** Test seam for outbox delivery. Production delivery writes the T3 session feed. */
class AlwaysOnDeliveryAttempt extends Context.Reference<
  (input: { readonly outboxId: string }) => Effect.Effect<void, AlwaysOnDeliveryFailed>
>("t3/alwaysOn/AlwaysOnDeliveryAttempt", {
  defaultValue: () => () => Effect.void,
}) {}

class ClaimAborted extends Schema.TaggedError<ClaimAborted>()("ClaimAborted", {}) {}

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const decodeJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));
const newId = (prefix: string) => `${prefix}:${SecureRandom.uuidV4Unsafe()}`;

const stamp = (value: DateTime.DateTime) => DateTime.formatIso(value);

const rejected = () =>
  new AlwaysOnError({
    message: "Credential-shaped text was rejected and not stored.",
    reason: "rejected",
  });

const notFound = (message: string) => new AlwaysOnError({ message, reason: "not_found" });

const denied = () =>
  new AlwaysOnError({
    message: "That record belongs to another always-on agent.",
    reason: "denied",
  });

interface AgentRow {
  readonly agent_id: string;
  readonly project_id: string;
  readonly title: string;
  readonly enabled: number;
  readonly user_session_id: string | null;
  readonly lease_expires_at: string | null;
  readonly lease_run_id: string | null;
  readonly provider_kind: "textGenerator" | "claudeAgent";
  readonly model_instance_id: string;
  readonly model: string;
  readonly created_at: string;
  readonly updated_at: string;
}

interface InboxRow {
  readonly event_id: string;
  readonly kind: AlwaysOnEventKind;
  readonly related_session_id: string | null;
  readonly body_text: string;
  readonly source: string;
  readonly source_event_id: string;
  readonly status: "pending" | "consumed" | "cancelled";
  readonly session_policy: "continue" | "fresh";
  readonly created_at: string;
}

const emptyContext: AlwaysOnLoadedContextType = {
  instructions: "",
  noteIndex: [],
  progressIndex: [],
  recentResults: [],
};

const phaseFor = (
  enabled: boolean,
  needsConfirmation: boolean,
  leaseExpiresAt: string | null,
  now: string,
): AlwaysOnPhase => {
  if (needsConfirmation) return "needs_confirmation";
  if (!enabled) return "paused";
  if (leaseExpiresAt !== null && leaseExpiresAt > now) return "running";
  return "sleeping";
};

export class AlwaysOnService extends Context.Service<
  AlwaysOnService,
  {
    readonly subscribe: () => Stream.Stream<AlwaysOnSnapshot, AlwaysOnError | SqlError.SqlError>;
    readonly create: (
      input: AlwaysOnCreateInput,
    ) => Effect.Effect<AlwaysOnCreateResult, AlwaysOnError | SqlError.SqlError>;
    readonly setEnabled: (
      input: AlwaysOnSetEnabledInput,
    ) => Effect.Effect<AlwaysOnAgentSummary, AlwaysOnError | SqlError.SqlError>;
    readonly submitEvent: (
      input: AlwaysOnSubmitEventInput,
    ) => Effect.Effect<AlwaysOnSubmitEventResult, AlwaysOnError | SqlError.SqlError>;
    readonly get: (
      input: AlwaysOnGetInput,
    ) => Effect.Effect<AlwaysOnDetail, AlwaysOnError | SqlError.SqlError>;
    readonly readMemory: (
      input: AlwaysOnReadMemoryInput,
    ) => Effect.Effect<AlwaysOnMemoryRecord, AlwaysOnError | SqlError.SqlError>;
    readonly resolveApproval: (
      input: AlwaysOnResolveApprovalInput,
    ) => Effect.Effect<AlwaysOnSubmitEventResult, AlwaysOnError | SqlError.SqlError>;
    readonly confirmUnknown: (input: {
      readonly agentId: AlwaysOnAgentId;
    }) => Effect.Effect<AlwaysOnAgentSummary, AlwaysOnError | SqlError.SqlError>;
    readonly cancel: (
      input: AlwaysOnCancelInput,
    ) => Effect.Effect<AlwaysOnAgentSummary, AlwaysOnError | SqlError.SqlError>;
    readonly drain: () => Effect.Effect<void>;
  }
>()("t3/alwaysOn/AlwaysOnService") {}

const make = Effect.fn("AlwaysOnService.make")(function* (wake: "eager" | "manual") {
  const sql = yield* SqlClient.SqlClient;
  const scheduler = yield* Scheduler.Scheduler;
  const model = yield* AlwaysOnModel.AlwaysOnModel;
  const cancelSignals = yield* Ref.make(new Map<string, Deferred.Deferred<void>>());
  const gate = yield* AlwaysOnRunGate;
  const deliverAttempt = yield* AlwaysOnDeliveryAttempt;
  const scope = yield* Effect.scope;
  const permit = yield* Semaphore.make(1);
  const revision = yield* Ref.make(0);
  const changes = yield* PubSub.unbounded<AlwaysOnSnapshot>();

  const clock = Effect.map(DateTime.now, (value) => ({
    value,
    iso: stamp(value),
  }));

  const countByAgent = (
    rows: ReadonlyArray<{ readonly agent_id: string; readonly count: number }>,
  ) => new Map(rows.map((row) => [row.agent_id, row.count]));

  const listSummaries = Effect.fnUntraced(function* (now: string) {
    const agents =
      yield* sql<AgentRow>`SELECT * FROM always_on_agent ORDER BY updated_at, agent_id`;
    const pending = countByAgent(
      yield* sql<{ readonly agent_id: string; readonly count: number }>`
          SELECT agent_id, COUNT(*) AS count FROM always_on_inbox WHERE status = 'pending' GROUP BY agent_id
        `,
    );
    const approvals = countByAgent(
      yield* sql<{ readonly agent_id: string; readonly count: number }>`
          SELECT agent_id, COUNT(*) AS count FROM always_on_approval WHERE status = 'pending' GROUP BY agent_id
        `,
    );
    const outbox = countByAgent(
      yield* sql<{ readonly agent_id: string; readonly count: number }>`
          SELECT agent_id, COUNT(*) AS count FROM always_on_outbox WHERE status = 'pending' GROUP BY agent_id
        `,
    );
    const unknown = countByAgent(
      yield* sql<{ readonly agent_id: string; readonly count: number }>`
          SELECT agent_id, COUNT(*) AS count FROM always_on_run WHERE status = 'unknown' GROUP BY agent_id
        `,
    );
    return agents.map((agent) =>
      summaryOf(agent, now, {
        pending: pending.get(agent.agent_id) ?? 0,
        approvals: approvals.get(agent.agent_id) ?? 0,
        outbox: outbox.get(agent.agent_id) ?? 0,
        unknown: (unknown.get(agent.agent_id) ?? 0) > 0,
      }),
    );
  });

  const snapshot = Effect.gen(function* () {
    const now = yield* clock;
    return {
      revision: yield* Ref.get(revision),
      agents: yield* listSummaries(now.iso),
      providers: ALWAYS_ON_PROVIDER_POLICY,
    };
  });

  const publish = Effect.gen(function* () {
    yield* Ref.update(revision, (current) => current + 1);
    yield* PubSub.publish(changes, yield* snapshot);
  });

  const requireAgent = Effect.fnUntraced(function* (agentId: AlwaysOnAgentId) {
    const [agent] = yield* sql<AgentRow>`SELECT * FROM always_on_agent WHERE agent_id = ${agentId}`;
    if (agent === undefined) return yield* notFound("Always-on agent not found.");
    return agent;
  });

  const summaryFor = Effect.fnUntraced(function* (agentId: string) {
    const now = yield* clock;
    const agents = yield* listSummaries(now.iso);
    const agent = agents.find((entry) => entry.id === agentId);
    if (agent === undefined) return yield* notFound("Always-on agent not found.");
    return agent;
  });

  const loadContext = Effect.fnUntraced(function* (agentId: string) {
    const instructions = yield* sql<{ readonly body: string | null }>`
        SELECT body FROM always_on_memory
        WHERE agent_id = ${agentId} AND kind = 'instruction'
        ORDER BY created_at
      `;
    const notes = yield* sql<{
      readonly memory_id: string;
      readonly title: string;
      readonly pointer: string | null;
    }>`
        SELECT memory_id, title, pointer FROM always_on_memory
        WHERE agent_id = ${agentId} AND kind = 'note'
        ORDER BY updated_at
      `;
    const progress = yield* sql<{
      readonly memory_id: string;
      readonly title: string;
      readonly task_key: string | null;
      readonly pointer: string | null;
    }>`
        SELECT memory_id, title, task_key, pointer FROM always_on_memory
        WHERE agent_id = ${agentId} AND kind = 'task_progress'
        ORDER BY updated_at
      `;
    const results = yield* sql<{
      readonly memory_id: string;
      readonly title: string;
      readonly body: string | null;
    }>`
        SELECT memory_id, title, body FROM always_on_memory
        WHERE agent_id = ${agentId} AND kind = 'sent_result'
        ORDER BY updated_at DESC
        LIMIT 3
      `;
    return {
      instructions: instructions.map((row) => row.body ?? "").join("\n"),
      noteIndex: notes.map((row) => ({
        id: AlwaysOnMemoryId.make(row.memory_id),
        title: row.title,
        pointer: row.pointer,
      })),
      progressIndex: progress.map((row) => ({
        id: AlwaysOnMemoryId.make(row.memory_id),
        title: row.title,
        taskKey: row.task_key,
        pointer: row.pointer,
      })),
      recentResults: [...results].reverse().map((row) => ({
        id: AlwaysOnMemoryId.make(row.memory_id),
        title: row.title,
        body: row.body ?? "",
      })),
    } satisfies AlwaysOnLoadedContextType;
  });

  const recover = Effect.fnUntraced(function* (startup: boolean) {
    const now = yield* clock;
    const runs = yield* sql<{
      readonly run_id: string;
      readonly agent_id: string;
      readonly uncertain: number;
      readonly cancel_requested: number;
      readonly decision_json: string | null;
    }>`
        SELECT run_id, agent_id, uncertain, cancel_requested, decision_json
        FROM always_on_run WHERE status = 'running'
      `;
    for (const run of runs) {
      const [agent] = yield* sql<{
        readonly lease_expires_at: string | null;
        readonly lease_run_id: string | null;
      }>`SELECT lease_expires_at, lease_run_id FROM always_on_agent WHERE agent_id = ${run.agent_id}`;
      const holds = agent?.lease_run_id === run.run_id;
      const expired = holds && (agent?.lease_expires_at ?? "") <= now.iso;
      if (!startup && !expired) continue;
      const uncertainEffects = yield* sql`
          SELECT 1 FROM always_on_effect
          WHERE run_id = ${run.run_id} AND status = 'uncertain' LIMIT 1
        `;
      const uncertain = run.uncertain === 1 || uncertainEffects.length > 0;
      if (uncertain) {
        yield* sql`UPDATE always_on_run SET status = 'unknown', finished_at = ${now.iso} WHERE run_id = ${run.run_id}`;
      } else if (run.cancel_requested === 1) {
        yield* sql`UPDATE always_on_run SET status = 'cancelled', finished_at = ${now.iso} WHERE run_id = ${run.run_id}`;
      } else if (run.decision_json !== null) {
        // The model already answered. Replaying that call could repeat a paid request.
        yield* sql`
          UPDATE always_on_run SET status = 'running', finished_at = NULL WHERE run_id = ${run.run_id}
        `;
      } else {
        yield* sql`UPDATE always_on_run SET status = 'interrupted', finished_at = ${now.iso} WHERE run_id = ${run.run_id}`;
        yield* sql`
            UPDATE always_on_inbox
            SET status = 'pending', run_id = NULL
            WHERE run_id = ${run.run_id} AND status = 'consumed'
          `;
      }
      if (holds) {
        yield* sql`
            UPDATE always_on_agent
            SET lease_expires_at = NULL, lease_run_id = NULL, updated_at = ${now.iso}
            WHERE agent_id = ${run.agent_id} AND lease_run_id = ${run.run_id}
          `;
      }
    }
  });

  // A restarted process owns this database alone, so a leftover lease belongs to a dead process.
  yield* recover(true);

  const markProgress = (runId: string, at: string) =>
    sql`UPDATE always_on_run SET last_progress_at = ${at} WHERE run_id = ${runId}`;

  const rememberEffect = (agentId: string, key: string, runId: string, status: string) =>
    sql<{ readonly action_key: string }>`
        INSERT INTO always_on_effect (agent_id, action_key, run_id, status)
        VALUES (${agentId}, ${key}, ${runId}, ${status})
        ON CONFLICT (agent_id, action_key) DO NOTHING
        RETURNING action_key
      `;

  const finishRun = (
    runId: string,
    agentId: string,
    status: AlwaysOnRunStatus,
    outcome: AlwaysOnRunOutcome | null,
    at: string,
  ) =>
    sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`
            UPDATE always_on_run
            SET status = ${status}, outcome = ${outcome}, finished_at = ${at}
            WHERE run_id = ${runId}
          `;
        yield* sql`
            UPDATE always_on_agent
            SET lease_expires_at = NULL, lease_run_id = NULL, updated_at = ${at}
            WHERE agent_id = ${agentId} AND lease_run_id = ${runId}
          `;
      }),
    );

  const applyEffect = Effect.fn("AlwaysOnService.applyEffect")(function* (
    agentId: string,
    runId: string,
    sessionId: string,
    action: AlwaysOnEffectAction,
    at: string,
  ) {
    const claimed = yield* rememberEffect(agentId, action.key, runId, "applied");
    if (claimed.length === 0) return;
    switch (action.type) {
      case "silent":
        return;
      case "remember_note":
      case "remember_result": {
        const memoryId = newId("always-on-memory");
        yield* sql`
              INSERT INTO always_on_memory (
                memory_id, agent_id, kind, title, body, pointer, task_key, created_at, updated_at
              ) VALUES (
                ${memoryId},
                ${agentId},
                ${action.type === "remember_note" ? "note" : "sent_result"},
                ${action.title},
                ${action.type === "remember_note" ? (action.body ?? null) : action.body},
                ${action.type === "remember_note" ? (action.pointer ?? null) : null},
                ${null},
                ${at},
                ${at}
              )
            `;
        yield* markProgress(runId, at);
        return;
      }
      case "record_progress": {
        const body = [action.done, action.left].filter((part) => part !== undefined).join("\n");
        const [existing] = yield* sql<{ readonly memory_id: string }>`
              SELECT memory_id FROM always_on_memory
              WHERE agent_id = ${agentId} AND task_key = ${action.taskKey}
            `;
        if (existing === undefined) {
          yield* sql`
                INSERT INTO always_on_memory (
                  memory_id, agent_id, kind, title, body, pointer, task_key, created_at, updated_at
                ) VALUES (
                  ${newId("always-on-memory")},
                  ${agentId},
                  ${"task_progress"},
                  ${action.title},
                  ${body.length === 0 ? null : body},
                  ${action.pointer ?? null},
                  ${action.taskKey},
                  ${at},
                  ${at}
                )
              `;
        } else {
          yield* sql`
                UPDATE always_on_memory
                SET title = ${action.title},
                    body = ${body.length === 0 ? null : body},
                    pointer = ${action.pointer ?? null},
                    updated_at = ${at}
                WHERE memory_id = ${existing.memory_id} AND agent_id = ${agentId}
              `;
        }
        yield* markProgress(runId, at);
        return;
      }
      case "set_timer": {
        const everyMs = action.timerKind === "recurring" ? action.everyMs : null;
        yield* sql`
              INSERT INTO always_on_timer (
                timer_id, agent_id, kind, due_at, every_ms, status, fire_count, action_key
              ) VALUES (
                ${newId("always-on-timer")},
                ${agentId},
                ${action.timerKind},
                ${action.dueAt},
                ${everyMs},
                ${"armed"},
                ${0},
                ${action.key}
              )
              ON CONFLICT (agent_id, action_key) DO UPDATE SET
                kind = excluded.kind,
                due_at = excluded.due_at,
                every_ms = excluded.every_ms,
                status = 'armed'
            `;
        yield* markProgress(runId, at);
        return;
      }
      case "deliver": {
        yield* sql`
              INSERT INTO always_on_outbox (
                outbox_id, agent_id, run_id, session_id, dedupe_key, body, status, attempts,
                next_attempt_at, created_at, delivered_at
              ) VALUES (
                ${newId("always-on-outbox")},
                ${agentId},
                ${runId},
                ${sessionId},
                ${action.dedupeKey},
                ${action.body},
                ${"pending"},
                ${0},
                ${null},
                ${at},
                ${null}
              )
              ON CONFLICT (agent_id, dedupe_key) DO NOTHING
            `;
        yield* markProgress(runId, at);
        return;
      }
    }
  });

  const applyRun = Effect.fnUntraced(function* (claimed: {
    readonly agentId: string;
    readonly runId: string;
    readonly sessionId: string;
    readonly events: readonly { readonly kind: string; readonly text: string }[];
    readonly providerKind: "textGenerator" | "claudeAgent";
    readonly instanceId: string;
    readonly model: string;
  }) {
    const now = yield* clock;
    const expires = stamp(DateTime.add(now.value, { milliseconds: LEASE_MS }));
    // Lease renewal is a heartbeat. It is not progress.
    yield* sql`
        UPDATE always_on_agent SET lease_expires_at = ${expires}
        WHERE agent_id = ${claimed.agentId} AND lease_run_id = ${claimed.runId}
      `;
    yield* gate.afterClaim;
    const [cancel] = yield* sql<{ readonly cancel_requested: number }>`
        SELECT cancel_requested FROM always_on_run WHERE run_id = ${claimed.runId}
      `;
    if (cancel?.cancel_requested === 1) {
      yield* finishRun(claimed.runId, claimed.agentId, "cancelled", null, now.iso);
      return;
    }
    const context = yield* loadContext(claimed.agentId);
    const prompt = alwaysOnPrompt({
      instructions: context.instructions,
      notes: context.noteIndex,
      progress: context.progressIndex,
      recentResults: context.recentResults,
      events: claimed.events,
    });
    const cancelSignal = yield* Deferred.make<void>();
    yield* Ref.update(cancelSignals, (current) =>
      new Map(current).set(claimed.runId, cancelSignal),
    );
    const decided = yield* model
      .decide({
        providerKind: claimed.providerKind,
        instanceId: claimed.instanceId,
        model: claimed.model,
        prompt,
      })
      .pipe(
        Effect.timeoutOrElse({
          duration: MODEL_TIMEOUT,
          orElse: () =>
            Effect.fail(new AlwaysOnModel.AlwaysOnModelError({ detail: "The model timed out." })),
        }),
        Effect.raceFirst(
          Deferred.await(cancelSignal).pipe(
            Effect.andThen(
              Effect.fail(
                new AlwaysOnModel.AlwaysOnModelError({ detail: "The run was cancelled." }),
              ),
            ),
          ),
        ),
        Effect.ensuring(
          Ref.update(cancelSignals, (current) => {
            const next = new Map(current);
            next.delete(claimed.runId);
            return next;
          }),
        ),
        Effect.exit,
      );
    if (decided._tag === "Failure") {
      const failed = Cause.findErrorOption(decided.cause);
      const detail = Option.isSome(failed) ? failed.value.detail : "The model call failed.";
      const interrupted = detail === "The run was cancelled.";
      yield* sql`
        UPDATE always_on_run SET model_error = ${detail} WHERE run_id = ${claimed.runId}
      `;
      yield* finishRun(
        claimed.runId,
        claimed.agentId,
        interrupted ? "cancelled" : "failed",
        null,
        (yield* clock).iso,
      );
      return;
    }
    const decoded = yield* decodeDecision(decided.value).pipe(Effect.exit);
    if (decoded._tag === "Failure" || containsCredential(decoded.value)) {
      yield* sql`
        UPDATE always_on_run
        SET model_error = ${"The model returned an action this server will not run."}
        WHERE run_id = ${claimed.runId}
      `;
      yield* finishRun(claimed.runId, claimed.agentId, "failed", null, (yield* clock).iso);
      return;
    }
    const decision = decoded.value;
    yield* sql`
      UPDATE always_on_run
      SET decision_json = ${encodeJson(decision)}, model_summary = ${decision.summary}
      WHERE run_id = ${claimed.runId}
    `;
    if (gate.afterDecision) yield* gate.afterDecision;
    let outcome: AlwaysOnRunOutcome = "silent";
    const executed: { readonly type: string; readonly key: string }[] = [];
    for (const action of decision.actions) {
      const [still] = yield* sql<{ readonly cancel_requested: number }>`
          SELECT cancel_requested FROM always_on_run WHERE run_id = ${claimed.runId}
        `;
      if (still?.cancel_requested === 1) {
        yield* sql`
            UPDATE always_on_run SET executed_actions_json = ${encodeJson(executed)}
            WHERE run_id = ${claimed.runId}
          `;
        yield* finishRun(claimed.runId, claimed.agentId, "cancelled", null, (yield* clock).iso);
        return;
      }
      executed.push({ type: action.type, key: action.key });
      if (action.type === "record_uncertain") {
        const recorded = yield* rememberEffect(
          claimed.agentId,
          action.key,
          claimed.runId,
          "uncertain",
        );
        yield* sql`UPDATE always_on_run SET uncertain = 1 WHERE run_id = ${claimed.runId}`;
        if (recorded.length > 0) yield* gate.afterUncertain;
        yield* sql`
            UPDATE always_on_effect SET status = 'withheld'
            WHERE agent_id = ${claimed.agentId}
              AND action_key = ${action.key}
              AND status = 'uncertain'
          `;
        const remaining = yield* sql`
            SELECT 1 FROM always_on_effect
            WHERE run_id = ${claimed.runId} AND status = 'uncertain' LIMIT 1
          `;
        if (remaining.length === 0) {
          yield* sql`UPDATE always_on_run SET uncertain = 0 WHERE run_id = ${claimed.runId}`;
        }
        continue;
      }
      if (action.type === "request_approval") {
        const at = (yield* clock).iso;
        yield* rememberEffect(claimed.agentId, action.key, claimed.runId, "applied");
        yield* sql`
            INSERT INTO always_on_approval (
              approval_id, agent_id, run_id, session_id, summary, on_approve_json, status,
              created_at, resolved_at
            ) VALUES (
              ${newId("always-on-approval")},
              ${claimed.agentId},
              ${claimed.runId},
              ${claimed.sessionId},
              ${action.summary},
              ${encodeJson(action.onApprove)},
              ${"pending"},
              ${at},
              ${null}
            )
          `;
        yield* sql`
            UPDATE always_on_session SET status = 'suspended'
            WHERE session_id = ${claimed.sessionId} AND agent_id = ${claimed.agentId}
          `;
        yield* sql`
            UPDATE always_on_run SET executed_actions_json = ${encodeJson(executed)}
            WHERE run_id = ${claimed.runId}
          `;
        yield* finishRun(
          claimed.runId,
          claimed.agentId,
          "awaiting_approval",
          "awaiting_approval",
          at,
        );
        return;
      }
      const at = (yield* clock).iso;
      yield* applyEffect(claimed.agentId, claimed.runId, claimed.sessionId, action, at);
      if (action.type === "deliver") outcome = "deliver";
    }
    yield* sql`
        UPDATE always_on_run SET executed_actions_json = ${encodeJson(executed)}
        WHERE run_id = ${claimed.runId}
      `;
    yield* finishRun(claimed.runId, claimed.agentId, "completed", outcome, (yield* clock).iso);
  });

  const claimAgent = Effect.fnUntraced(function* (agentId: string) {
    return yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const now = yield* clock;
          const [agent] = yield* sql<AgentRow>`
              SELECT * FROM always_on_agent WHERE agent_id = ${agentId}
            `;
          if (agent === undefined || agent.enabled !== 1) return yield* new ClaimAborted();
          if (agent.lease_expires_at !== null && agent.lease_expires_at > now.iso) {
            return yield* new ClaimAborted();
          }
          const unknown = yield* sql`
              SELECT 1 FROM always_on_run WHERE agent_id = ${agentId} AND status = 'unknown' LIMIT 1
            `;
          if (unknown.length > 0) return yield* new ClaimAborted();
          const pending = yield* sql<InboxRow>`
              SELECT event_id, kind, related_session_id, body_text, source, source_event_id,
                     status, session_policy, created_at
              FROM always_on_inbox
              WHERE agent_id = ${agentId} AND status = 'pending'
              ORDER BY created_at, event_id
            `;
          const batch = selectInboxBatch(
            pending.map((row) => ({ ...row, relatedSessionId: row.related_session_id })),
          ).slice(0, MAX_BATCH);
          const first = batch[0];
          if (first === undefined) return yield* new ClaimAborted();
          let sessionId: string;
          if (first.kind === "user_reply") {
            sessionId = agent.user_session_id ?? newId("always-on-session");
            if (agent.user_session_id === null) {
              yield* sql`
                  INSERT INTO always_on_session (session_id, agent_id, kind, status, created_at)
                  VALUES (${sessionId}, ${agentId}, ${"user"}, ${"open"}, ${now.iso})
                `;
              yield* sql`
                  UPDATE always_on_agent SET user_session_id = ${sessionId}
                  WHERE agent_id = ${agentId}
                `;
            }
          } else if (first.kind === "approval") {
            if (first.related_session_id === null) return yield* new ClaimAborted();
            sessionId = first.related_session_id;
            yield* sql`
                UPDATE always_on_session SET status = 'open'
                WHERE session_id = ${sessionId} AND agent_id = ${agentId}
              `;
          } else {
            sessionId = newId("always-on-session");
            yield* sql`
                INSERT INTO always_on_session (session_id, agent_id, kind, status, created_at)
                VALUES (${sessionId}, ${agentId}, ${"task"}, ${"open"}, ${now.iso})
              `;
          }
          const runId = newId("always-on-run");
          const events: { readonly kind: string; readonly text: string }[] = [];
          for (const event of batch) {
            const updated = yield* sql<{ readonly event_id: string }>`
                UPDATE always_on_inbox
                SET status = 'consumed', run_id = ${runId}
                WHERE event_id = ${event.event_id} AND status = 'pending'
                RETURNING event_id
              `;
            if (updated.length === 0) return yield* new ClaimAborted();
            events.push({ kind: event.kind, text: event.body_text });
          }
          const context = yield* loadContext(agentId);
          const expires = stamp(DateTime.add(now.value, { milliseconds: LEASE_MS }));
          yield* sql`
              INSERT INTO always_on_run (
                run_id, agent_id, session_id, status, outcome, uncertain, cancel_requested,
                context_json, event_ids_json, last_progress_at, started_at, finished_at
              ) VALUES (
                ${runId},
                ${agentId},
                ${sessionId},
                ${"running"},
                ${null},
                ${0},
                ${0},
                ${encodeJson(context)},
                ${encodeJson(batch.map((event) => event.event_id))},
                ${null},
                ${now.iso},
                ${null}
              )
            `;
          yield* sql`
              UPDATE always_on_agent
              SET lease_expires_at = ${expires}, lease_run_id = ${runId}, updated_at = ${now.iso}
              WHERE agent_id = ${agentId}
            `;
          return {
            agentId,
            runId,
            sessionId,
            events,
            providerKind: agent.provider_kind,
            instanceId: agent.model_instance_id,
            model: agent.model,
          };
        }),
      )
      .pipe(Effect.catchTags({ ClaimAborted: () => Effect.succeed(null) }));
  });

  const fireTimers = Effect.fnUntraced(function* () {
    const now = yield* clock;
    const due = yield* sql<{
      readonly timer_id: string;
      readonly agent_id: string;
      readonly kind: "deadline" | "recurring";
      readonly due_at: string;
      readonly every_ms: number | null;
      readonly fire_count: number;
    }>`
        SELECT t.timer_id, t.agent_id, t.kind, t.due_at, t.every_ms, t.fire_count
        FROM always_on_timer t
        JOIN always_on_agent a ON a.agent_id = t.agent_id
        WHERE t.status = 'armed' AND t.due_at <= ${now.iso} AND a.enabled = 1
      `;
    let fired = false;
    for (const timer of due) {
      const did = yield* sql.withTransaction(
        Effect.gen(function* () {
          const fireCount = timer.fire_count + 1;
          const sourceEventId = `${timer.timer_id}:${fireCount}`;
          const dueMs = Date.parse(timer.due_at);
          const nextDue =
            timer.kind === "recurring" && timer.every_ms !== null && Number.isFinite(dueMs)
              ? DateTime.formatIso(
                  DateTime.makeUnsafe(
                    nextRecurringDue(dueMs, timer.every_ms, DateTime.toEpochMillis(now.value)),
                  ),
                )
              : timer.due_at;
          const status = timer.kind === "deadline" ? "fired" : "armed";
          const updated = yield* sql<{ readonly timer_id: string }>`
              UPDATE always_on_timer
              SET fire_count = ${fireCount}, status = ${status}, due_at = ${nextDue}
              WHERE timer_id = ${timer.timer_id} AND status = 'armed'
              RETURNING timer_id
            `;
          if (updated.length === 0) return false;
          yield* sql`
              INSERT INTO always_on_inbox (
                event_id, agent_id, source, source_event_id, kind, session_policy,
                related_session_id, body_text, actions_json, status, run_id, created_at
              ) VALUES (
                ${newId("always-on-event")},
                ${timer.agent_id},
                ${"timer"},
                ${sourceEventId},
                ${"timer"},
                ${"fresh"},
                ${null},
                ${"Timer due."},
                ${"[]"},
                ${"pending"},
                ${null},
                ${now.iso}
              )
            `;
          return true;
        }),
      );
      fired ||= did;
    }
    return fired;
  });

  const dispatchOutbox = Effect.fnUntraced(function* () {
    const now = yield* clock;
    const pending = yield* sql<{
      readonly outbox_id: string;
      readonly agent_id: string;
      readonly session_id: string;
      readonly body: string;
      readonly attempts: number;
    }>`
        SELECT outbox_id, agent_id, session_id, body, attempts
        FROM always_on_outbox
        WHERE status = 'pending' AND (next_attempt_at IS NULL OR next_attempt_at <= ${now.iso})
        ORDER BY created_at
      `;
    let attempted = false;
    for (const row of pending) {
      attempted = true;
      const result = yield* deliverAttempt({ outboxId: row.outbox_id }).pipe(Effect.exit);
      if (result._tag === "Success") {
        const at = (yield* clock).iso;
        yield* sql.withTransaction(
          Effect.gen(function* () {
            yield* sql`
                INSERT INTO always_on_delivery (
                  delivery_id, outbox_id, agent_id, session_id, body, delivered_at
                ) VALUES (
                  ${newId("always-on-delivery")},
                  ${row.outbox_id},
                  ${row.agent_id},
                  ${row.session_id},
                  ${row.body},
                  ${at}
                )
                ON CONFLICT (outbox_id) DO NOTHING
              `;
            yield* sql`
                UPDATE always_on_outbox
                SET status = 'delivered', delivered_at = ${at}, attempts = ${row.attempts + 1}
                WHERE outbox_id = ${row.outbox_id} AND status = 'pending'
              `;
          }),
        );
      } else {
        const attempts = row.attempts + 1;
        const status = attempts >= OUTBOX_ATTEMPTS ? "failed" : "pending";
        const next = stamp(DateTime.add((yield* clock).value, { seconds: 1 }));
        yield* sql`
            UPDATE always_on_outbox
            SET attempts = ${attempts}, status = ${status}, next_attempt_at = ${next}
            WHERE outbox_id = ${row.outbox_id} AND status = 'pending'
          `;
      }
    }
    return attempted;
  });

  const startRuns = Effect.fnUntraced(function* () {
    const agents = yield* sql<{ readonly agent_id: string }>`
        SELECT agent_id FROM always_on_agent WHERE enabled = 1 ORDER BY agent_id
      `;
    let started = false;
    for (const agent of agents) {
      const claimed = yield* claimAgent(agent.agent_id).pipe(
        Effect.catch((cause) =>
          Effect.logWarning("Always-on claim failed", { agentId: agent.agent_id, cause }).pipe(
            Effect.as(null),
          ),
        ),
      );
      if (claimed === null) continue;
      started = true;
      yield* applyRun(claimed).pipe(
        Effect.catch((cause) =>
          Effect.gen(function* () {
            yield* Effect.logWarning("Always-on run failed", {
              agentId: claimed.agentId,
              runId: claimed.runId,
              cause,
            });
            yield* finishRun(
              claimed.runId,
              claimed.agentId,
              "failed",
              "silent",
              (yield* clock).iso,
            );
          }),
        ),
      );
    }
    return started;
  });

  const resumeStored = Effect.fnUntraced(function* () {
    const rows = yield* sql<{
      readonly run_id: string;
      readonly agent_id: string;
      readonly session_id: string;
      readonly decision_json: string;
    }>`
      SELECT r.run_id, r.agent_id, r.session_id, r.decision_json
      FROM always_on_run r
      JOIN always_on_agent a ON a.agent_id = r.agent_id
      WHERE r.status = 'running'
        AND r.decision_json IS NOT NULL
        AND (a.lease_run_id IS NULL OR a.lease_run_id <> r.run_id)
    `;
    let resumed = false;
    for (const row of rows) {
      resumed = true;
      const decoded = yield* decodeDecision(decodeJson(row.decision_json)).pipe(Effect.exit);
      if (decoded._tag === "Failure") {
        yield* finishRun(row.run_id, row.agent_id, "failed", null, (yield* clock).iso);
        continue;
      }
      const pendingApproval = yield* sql`
          SELECT 1 FROM always_on_approval
          WHERE run_id = ${row.run_id} AND status = 'pending' LIMIT 1
        `;
      const executed = decoded.value.actions.map((action) => ({
        type: action.type,
        key: action.key,
      }));
      yield* sql`
        UPDATE always_on_run
        SET executed_actions_json = ${encodeJson(executed)},
            model_summary = ${decoded.value.summary}
        WHERE run_id = ${row.run_id}
      `;
      if (pendingApproval.length > 0) {
        yield* finishRun(
          row.run_id,
          row.agent_id,
          "awaiting_approval",
          "awaiting_approval",
          (yield* clock).iso,
        );
        continue;
      }
      let awaiting = false;
      for (const action of decoded.value.actions) {
        if (action.type === "request_approval") {
          const existing = yield* sql`
              SELECT 1 FROM always_on_approval
              WHERE run_id = ${row.run_id} AND agent_id = ${row.agent_id} LIMIT 1
            `;
          if (existing.length === 0) {
            const at = (yield* clock).iso;
            yield* rememberEffect(row.agent_id, action.key, row.run_id, "applied");
            yield* sql`
                INSERT INTO always_on_approval (
                  approval_id, agent_id, run_id, session_id, summary, on_approve_json, status,
                  created_at, resolved_at
                ) VALUES (
                  ${newId("always-on-approval")},
                  ${row.agent_id},
                  ${row.run_id},
                  ${row.session_id},
                  ${action.summary},
                  ${encodeJson(action.onApprove)},
                  ${"pending"},
                  ${at},
                  ${null}
                )
              `;
            yield* sql`
                UPDATE always_on_session SET status = 'suspended'
                WHERE session_id = ${row.session_id} AND agent_id = ${row.agent_id}
              `;
          }
          awaiting = true;
          break;
        }
        if (action.type === "record_uncertain") {
          const recorded = yield* rememberEffect(row.agent_id, action.key, row.run_id, "uncertain");
          if (recorded.length > 0) {
            yield* sql`UPDATE always_on_run SET uncertain = 1 WHERE run_id = ${row.run_id}`;
            yield* sql`
                UPDATE always_on_effect SET status = 'withheld'
                WHERE agent_id = ${row.agent_id}
                  AND action_key = ${action.key}
                  AND status = 'uncertain'
              `;
          }
          continue;
        }
        yield* applyEffect(row.agent_id, row.run_id, row.session_id, action, (yield* clock).iso);
      }
      if (awaiting) {
        yield* finishRun(
          row.run_id,
          row.agent_id,
          "awaiting_approval",
          "awaiting_approval",
          (yield* clock).iso,
        );
        continue;
      }
      const outcome = decoded.value.actions.some((action) => action.type === "deliver")
        ? "deliver"
        : "silent";
      yield* finishRun(row.run_id, row.agent_id, "completed", outcome, (yield* clock).iso);
    }
    return resumed;
  });

  const drainBody = Effect.gen(function* () {
    yield* recover(false);
    let worked = false;
    for (let pass = 0; pass < MAX_PASSES; pass++) {
      const timers = yield* fireTimers();
      const resumed = yield* resumeStored();
      const runs = yield* startRuns();
      const outbox = yield* dispatchOutbox();
      if (!timers && !resumed && !runs && !outbox) break;
      worked = true;
    }
    if (worked) yield* publish;
  }).pipe(Effect.catch((cause) => Effect.logWarning("Always-on drain failed", { cause })));

  const drainEffect = permit.withPermits(1)(drainBody);
  const drain = () => drainEffect;
  const kick =
    wake === "eager" ? drainEffect.pipe(Effect.forkIn(scope), Effect.asVoid) : Effect.void;

  yield* scheduler.register("always-on", drainEffect);

  const create = Effect.fn("AlwaysOnService.create")(function* (input: AlwaysOnCreateInput) {
    if (containsCredential(input)) return yield* rejected();
    const now = yield* clock;
    const agentId = newId("always-on-agent");
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`
            INSERT INTO always_on_agent (
              agent_id, project_id, title, enabled, user_session_id, lease_expires_at,
              lease_run_id, provider_kind, model_instance_id, model, created_at, updated_at
            ) VALUES (
              ${agentId}, ${input.projectId}, ${input.title}, ${1}, ${null}, ${null}, ${null},
              ${input.providerKind}, ${input.instanceId}, ${input.model}, ${now.iso}, ${now.iso}
            )
          `;
        yield* sql`
            INSERT INTO always_on_memory (
              memory_id, agent_id, kind, title, body, pointer, task_key, created_at, updated_at
            ) VALUES (
              ${newId("always-on-memory")}, ${agentId}, ${"instruction"}, ${"Instructions"},
              ${input.instructions}, ${null}, ${null}, ${now.iso}, ${now.iso}
            )
          `;
      }),
    );
    yield* publish;
    return { agent: yield* summaryFor(agentId) };
  });

  const setEnabled = Effect.fn("AlwaysOnService.setEnabled")(function* (
    input: AlwaysOnSetEnabledInput,
  ) {
    yield* requireAgent(input.agentId);
    const now = yield* clock;
    yield* sql`
        UPDATE always_on_agent SET enabled = ${input.enabled ? 1 : 0}, updated_at = ${now.iso}
        WHERE agent_id = ${input.agentId}
      `;
    yield* publish;
    if (input.enabled) yield* kick;
    return yield* summaryFor(input.agentId);
  });

  const submitEvent = Effect.fn("AlwaysOnService.submitEvent")(function* (
    input: AlwaysOnSubmitEventInput,
  ) {
    if (containsCredential(input)) return yield* rejected();
    yield* requireAgent(input.agentId);
    const now = yield* clock;
    const eventId = newId("always-on-event");
    const policy = input.kind === "user_reply" ? "continue" : "fresh";
    const [agent] =
      yield* sql<AgentRow>`SELECT * FROM always_on_agent WHERE agent_id = ${input.agentId}`;
    const related = input.kind === "user_reply" ? (agent?.user_session_id ?? null) : null;
    const inserted = yield* sql<{ readonly event_id: string }>`
        INSERT INTO always_on_inbox (
          event_id, agent_id, source, source_event_id, kind, session_policy, related_session_id,
          body_text, actions_json, status, run_id, created_at
        ) VALUES (
          ${eventId},
          ${input.agentId},
          ${input.source},
          ${input.sourceEventId},
          ${input.kind},
          ${policy},
          ${related},
          ${input.text},
          ${"[]"},
          ${"pending"},
          ${null},
          ${now.iso}
        )
        ON CONFLICT (agent_id, source, source_event_id) DO NOTHING
        RETURNING event_id
      `;
    const stored = inserted[0];
    if (stored === undefined) {
      const [existing] = yield* sql<{ readonly event_id: string }>`
          SELECT event_id FROM always_on_inbox
          WHERE agent_id = ${input.agentId}
            AND source = ${input.source}
            AND source_event_id = ${input.sourceEventId}
        `;
      if (existing === undefined) return yield* notFound("Always-on event not found.");
      return {
        eventId: AlwaysOnEventId.make(existing.event_id),
        duplicate: true,
      };
    }
    yield* publish;
    yield* kick;
    return { eventId: AlwaysOnEventId.make(stored.event_id), duplicate: false };
  });

  const get = Effect.fn("AlwaysOnService.get")(function* (input: AlwaysOnGetInput) {
    const agent = yield* summaryFor(input.agentId);
    const [instruction] = yield* sql<{ readonly body: string | null }>`
        SELECT body FROM always_on_memory
        WHERE agent_id = ${input.agentId} AND kind = 'instruction'
        ORDER BY created_at LIMIT 1
      `;
    const inbox = yield* sql<InboxRow>`
        SELECT event_id, source, source_event_id, kind, status, session_policy, created_at,
               related_session_id, body_text
        FROM always_on_inbox
        WHERE agent_id = ${input.agentId}
        ORDER BY created_at DESC
        LIMIT 50
      `;
    const runs = yield* sql<{
      readonly run_id: string;
      readonly session_id: string;
      readonly status: AlwaysOnRunStatus;
      readonly outcome: AlwaysOnRunOutcome | null;
      readonly uncertain: number;
      readonly context_json: string;
      readonly event_ids_json: string;
      readonly last_progress_at: string | null;
      readonly model_summary: string | null;
      readonly model_error: string | null;
      readonly executed_actions_json: string;
      readonly started_at: string;
      readonly finished_at: string | null;
    }>`
        SELECT * FROM always_on_run
        WHERE agent_id = ${input.agentId}
        ORDER BY started_at DESC
        LIMIT 20
      `;
    const memory = yield* sql<{
      readonly memory_id: string;
      readonly kind: AlwaysOnMemoryRecord["kind"];
      readonly title: string;
      readonly body: string | null;
      readonly pointer: string | null;
      readonly task_key: string | null;
    }>`
        SELECT memory_id, kind, title, body, pointer, task_key
        FROM always_on_memory WHERE agent_id = ${input.agentId}
        ORDER BY updated_at
      `;
    const approvals = yield* sql<{
      readonly approval_id: string;
      readonly run_id: string;
      readonly session_id: string;
      readonly status: "pending" | "approved" | "rejected" | "cancelled";
      readonly summary: string;
      readonly created_at: string;
    }>`
        SELECT approval_id, run_id, session_id, status, summary, created_at
        FROM always_on_approval WHERE agent_id = ${input.agentId}
        ORDER BY (status = 'pending') DESC, created_at DESC, approval_id LIMIT 20
      `;
    const deliveries = yield* sql<{
      readonly outbox_id: string;
      readonly session_id: string;
      readonly body: string;
      readonly status: "pending" | "delivered" | "failed";
      readonly attempts: number;
      readonly dedupe_key: string;
    }>`
        SELECT outbox_id, session_id, body, status, attempts, dedupe_key
        FROM always_on_outbox WHERE agent_id = ${input.agentId}
        ORDER BY created_at DESC LIMIT 20
      `;
    const timers = yield* sql<{
      readonly timer_id: string;
      readonly kind: "deadline" | "recurring";
      readonly due_at: string;
      readonly every_ms: number | null;
      readonly status: "armed" | "fired" | "cancelled";
    }>`
        SELECT timer_id, kind, due_at, every_ms, status
        FROM always_on_timer WHERE agent_id = ${input.agentId}
        ORDER BY due_at
      `;
    return {
      agent,
      instructions: instruction?.body ?? "",
      inbox: [...inbox].reverse().map((row) => ({
        id: AlwaysOnEventId.make(row.event_id),
        source: row.source,
        sourceEventId: row.source_event_id,
        kind: row.kind,
        status: row.status,
        sessionPolicy: row.session_policy,
        createdAt: row.created_at,
      })),
      runs: [...runs].reverse().map((row) => ({
        id: AlwaysOnRunId.make(row.run_id),
        sessionId: AlwaysOnSessionId.make(row.session_id),
        status: row.status,
        outcome: row.outcome,
        uncertain: row.uncertain === 1 || row.status === "unknown",
        startedAt: row.started_at,
        finishedAt: row.finished_at,
        lastProgressAt: row.last_progress_at,
        eventIds: stringArray(decodeJson(row.event_ids_json)).map((id) => AlwaysOnEventId.make(id)),
        loadedContext: decodeStoredContext(row.context_json),
        modelSummary: row.model_summary,
        modelError: row.model_error,
        executedActions: executedActions(decodeJson(row.executed_actions_json)),
      })),
      memoryIndex: memory.map((row) => ({
        id: AlwaysOnMemoryId.make(row.memory_id),
        kind: row.kind,
        title: row.title,
        taskKey: row.task_key,
        pointer: row.pointer,
        hasBody: row.body !== null && row.body.length > 0,
      })),
      approvals: [...approvals].reverse().map((row) => ({
        id: AlwaysOnApprovalId.make(row.approval_id),
        runId: AlwaysOnRunId.make(row.run_id),
        sessionId: AlwaysOnSessionId.make(row.session_id),
        status: row.status,
        summary: row.summary,
        createdAt: row.created_at,
      })),
      deliveries: [...deliveries].reverse().map((row) => ({
        id: row.outbox_id,
        sessionId: AlwaysOnSessionId.make(row.session_id),
        body: row.body,
        status: row.status,
        attempts: row.attempts,
        dedupeKey: row.dedupe_key,
      })),
      timers: timers.map((row) => ({
        id: row.timer_id,
        kind: row.kind,
        dueAt: row.due_at,
        everyMs: row.every_ms,
        status: row.status,
      })),
    };
  });

  const readMemory = Effect.fn("AlwaysOnService.readMemory")(function* (
    input: AlwaysOnReadMemoryInput,
  ) {
    yield* requireAgent(input.agentId);
    const [row] = yield* sql<{
      readonly memory_id: string;
      readonly agent_id: string;
      readonly kind: AlwaysOnMemoryRecord["kind"];
      readonly title: string;
      readonly body: string | null;
      readonly pointer: string | null;
      readonly task_key: string | null;
    }>`SELECT * FROM always_on_memory WHERE memory_id = ${input.memoryId}`;
    if (row === undefined) return yield* notFound("Always-on memory not found.");
    if (row.agent_id !== input.agentId) return yield* denied();
    return {
      id: AlwaysOnMemoryId.make(row.memory_id),
      agentId: AlwaysOnAgentId.make(row.agent_id),
      kind: row.kind,
      title: row.title,
      body: row.body,
      pointer: row.pointer,
      taskKey: row.task_key,
    };
  });

  const resolveApproval = Effect.fn("AlwaysOnService.resolveApproval")(function* (
    input: AlwaysOnResolveApprovalInput,
  ) {
    const [approval] = yield* sql<{
      readonly approval_id: string;
      readonly agent_id: string;
      readonly session_id: string;
      readonly status: string;
      readonly summary: string;
    }>`
        SELECT approval_id, agent_id, session_id, status, summary
        FROM always_on_approval WHERE approval_id = ${input.approvalId}
      `;
    if (approval === undefined || approval.status !== "pending") {
      return yield* notFound("Always-on approval not found.");
    }
    if (approval.agent_id !== input.agentId) return yield* denied();
    const text =
      input.decision === "approved"
        ? `The user approved: ${approval.summary}`
        : `The user rejected: ${approval.summary}. Do not perform the action.`;
    const now = yield* clock;
    const eventId = newId("always-on-event");
    yield* sql
      .withTransaction(
        Effect.gen(function* () {
          const updated = yield* sql<{ readonly approval_id: string }>`
            UPDATE always_on_approval
            SET status = ${input.decision}, resolved_at = ${now.iso}
            WHERE approval_id = ${approval.approval_id} AND status = 'pending'
            RETURNING approval_id
          `;
          if (updated.length === 0) return yield* new ClaimAborted();
          yield* sql`
            INSERT INTO always_on_inbox (
              event_id, agent_id, source, source_event_id, kind, session_policy,
              related_session_id, body_text, actions_json, status, run_id, created_at
            ) VALUES (
              ${eventId},
              ${approval.agent_id},
              ${"approval"},
              ${`${approval.approval_id}:${input.decision}`},
              ${"approval"},
              ${"continue"},
              ${approval.session_id},
              ${text},
              ${"[]"},
              ${"pending"},
              ${null},
              ${now.iso}
            )
          `;
        }),
      )
      .pipe(
        Effect.catchTags({
          ClaimAborted: () => Effect.fail(notFound("Always-on approval not found.")),
        }),
      );
    yield* publish;
    yield* kick;
    return { eventId: AlwaysOnEventId.make(eventId), duplicate: false };
  });

  const confirmUnknown = Effect.fn("AlwaysOnService.confirmUnknown")(function* (input: {
    readonly agentId: AlwaysOnAgentId;
  }) {
    yield* requireAgent(input.agentId);
    const now = yield* clock;
    const unknown = yield* sql<{ readonly run_id: string }>`
        SELECT run_id FROM always_on_run WHERE agent_id = ${input.agentId} AND status = 'unknown'
      `;
    if (unknown.length === 0) {
      return yield* new AlwaysOnError({
        message: "This agent has no run waiting for confirmation.",
        reason: "conflict",
      });
    }
    yield* sql.withTransaction(
      Effect.gen(function* () {
        yield* sql`
            UPDATE always_on_run
            SET status = 'completed', outcome = 'silent', uncertain = 0, finished_at = ${now.iso}
            WHERE agent_id = ${input.agentId} AND status = 'unknown'
          `;
        yield* sql`
            UPDATE always_on_effect SET status = 'withheld'
            WHERE agent_id = ${input.agentId} AND status = 'uncertain'
          `;
      }),
    );
    yield* publish;
    return yield* summaryFor(input.agentId);
  });

  const cancel = Effect.fn("AlwaysOnService.cancel")(function* (input: AlwaysOnCancelInput) {
    yield* requireAgent(input.agentId);
    const now = yield* clock;
    const [run] = yield* sql<{ readonly status: string }>`
        SELECT status FROM always_on_run
        WHERE run_id = ${input.runId} AND agent_id = ${input.agentId}
      `;
    if (run === undefined) return yield* notFound("Always-on run not found.");
    if (run.status === "awaiting_approval") {
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`
              UPDATE always_on_run
              SET status = 'cancelled', cancel_requested = 1, finished_at = ${now.iso}
              WHERE run_id = ${input.runId}
            `;
          yield* sql`
              UPDATE always_on_approval
              SET status = 'cancelled', resolved_at = ${now.iso}
              WHERE run_id = ${input.runId} AND status = 'pending'
            `;
        }),
      );
    } else if (run.status === "running") {
      const signal = (yield* Ref.get(cancelSignals)).get(input.runId);
      yield* sql`
          UPDATE always_on_run SET cancel_requested = 1
          WHERE run_id = ${input.runId} AND status = 'running'
        `;
      if (signal !== undefined) yield* Deferred.succeed(signal, undefined);
    } else {
      return yield* new AlwaysOnError({
        message: "This run can no longer be cancelled.",
        reason: "conflict",
      });
    }
    yield* publish;
    return yield* summaryFor(input.agentId);
  });

  const subscribe = () =>
    Stream.unwrap(
      Effect.gen(function* () {
        const subscription = yield* PubSub.subscribe(changes);
        const initial = yield* snapshot;
        return Stream.concat(Stream.make(initial), Stream.fromSubscription(subscription));
      }),
    );

  return AlwaysOnService.of({
    subscribe,
    create,
    setEnabled,
    submitEvent,
    get,
    readMemory,
    resolveApproval,
    confirmUnknown,
    cancel,
    drain,
  });
});

export const layer = Layer.effect(AlwaysOnService, make("eager"));
export const layerManual = Layer.effect(AlwaysOnService, make("manual"));

const summaryOf = (
  agent: AgentRow,
  now: string,
  counts: {
    readonly pending: number;
    readonly approvals: number;
    readonly outbox: number;
    readonly unknown: boolean;
  },
): AlwaysOnAgentSummary => ({
  id: AlwaysOnAgentId.make(agent.agent_id),
  projectId: ProjectId.make(agent.project_id),
  title: agent.title,
  enabled: agent.enabled === 1,
  phase: phaseFor(agent.enabled === 1, counts.unknown, agent.lease_expires_at, now),
  userSessionId:
    agent.user_session_id === null ? null : AlwaysOnSessionId.make(agent.user_session_id),
  pendingInboxCount: counts.pending,
  pendingApprovalCount: counts.approvals,
  pendingOutboxCount: counts.outbox,
  updatedAt: agent.updated_at,
});

const stringArray = (value: unknown): readonly string[] =>
  Array.isArray(value) ? value.filter((item): item is string => typeof item === "string") : [];

const executedActions = (value: unknown) => {
  if (!Array.isArray(value)) return [];
  return value.flatMap((item) => {
    if (item === null || typeof item !== "object") return [];
    const record = item as { readonly type?: unknown; readonly key?: unknown };
    return typeof record.type === "string" && typeof record.key === "string"
      ? [{ type: record.type, key: record.key }]
      : [];
  });
};

const decodeStoredContext = (value: string) => {
  try {
    return decodeContext(decodeJson(value));
  } catch {
    return emptyContext;
  }
};
