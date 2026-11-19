import * as Schema from "effect/Schema";

import { IsoDateTime, ProjectId, TrimmedNonEmptyString } from "./baseSchemas.ts";

const Identifier = TrimmedNonEmptyString.check(Schema.isMaxLength(160));
const Summary = TrimmedNonEmptyString.check(Schema.isMaxLength(2_000));
const Instructions = TrimmedNonEmptyString.check(Schema.isMaxLength(8_000));

const MIN_ALWAYS_ON_TIMER_MS = 60_000;

export const AlwaysOnAgentId = TrimmedNonEmptyString.pipe(Schema.brand("AlwaysOnAgentId"));
export type AlwaysOnAgentId = typeof AlwaysOnAgentId.Type;
export const AlwaysOnSessionId = TrimmedNonEmptyString.pipe(Schema.brand("AlwaysOnSessionId"));
export type AlwaysOnSessionId = typeof AlwaysOnSessionId.Type;
export const AlwaysOnRunId = TrimmedNonEmptyString.pipe(Schema.brand("AlwaysOnRunId"));
export type AlwaysOnRunId = typeof AlwaysOnRunId.Type;
export const AlwaysOnEventId = TrimmedNonEmptyString.pipe(Schema.brand("AlwaysOnEventId"));
export type AlwaysOnEventId = typeof AlwaysOnEventId.Type;
export const AlwaysOnMemoryId = TrimmedNonEmptyString.pipe(Schema.brand("AlwaysOnMemoryId"));
export type AlwaysOnMemoryId = typeof AlwaysOnMemoryId.Type;
export const AlwaysOnApprovalId = TrimmedNonEmptyString.pipe(Schema.brand("AlwaysOnApprovalId"));
export type AlwaysOnApprovalId = typeof AlwaysOnApprovalId.Type;

export const AlwaysOnPhase = Schema.Literals([
  "sleeping",
  "running",
  "paused",
  "needs_confirmation",
]);
export type AlwaysOnPhase = typeof AlwaysOnPhase.Type;

export const AlwaysOnEventKind = Schema.Literals([
  "user_reply",
  "timer",
  "approval",
  "external",
  "system",
]);
export type AlwaysOnEventKind = typeof AlwaysOnEventKind.Type;

export const AlwaysOnRunStatus = Schema.Literals([
  "running",
  "completed",
  "awaiting_approval",
  "interrupted",
  "unknown",
  "cancelled",
  "failed",
]);
export type AlwaysOnRunStatus = typeof AlwaysOnRunStatus.Type;

export const AlwaysOnRunOutcome = Schema.Literals(["silent", "deliver", "awaiting_approval"]);
export type AlwaysOnRunOutcome = typeof AlwaysOnRunOutcome.Type;

const ActionKey = Identifier;

const AlwaysOnNoteAction = Schema.Struct({
  type: Schema.Literal("remember_note"),
  key: ActionKey,
  title: Summary,
  body: Schema.optional(Summary),
  pointer: Schema.optional(Summary),
});

const AlwaysOnProgressAction = Schema.Struct({
  type: Schema.Literal("record_progress"),
  key: ActionKey,
  title: Summary,
  taskKey: Identifier,
  done: Schema.optional(Summary),
  left: Schema.optional(Summary),
  pointer: Schema.optional(Summary),
});

const AlwaysOnResultAction = Schema.Struct({
  type: Schema.Literal("remember_result"),
  key: ActionKey,
  title: Summary,
  body: Summary,
});

const AlwaysOnTimerAction = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("set_timer"),
    key: ActionKey,
    timerKind: Schema.Literal("deadline"),
    dueAt: IsoDateTime,
  }),
  Schema.Struct({
    type: Schema.Literal("set_timer"),
    key: ActionKey,
    timerKind: Schema.Literal("recurring"),
    dueAt: IsoDateTime,
    everyMs: Schema.Int.check(Schema.isGreaterThanOrEqualTo(MIN_ALWAYS_ON_TIMER_MS)),
  }),
]);

const AlwaysOnDeliverAction = Schema.Struct({
  type: Schema.Literal("deliver"),
  key: ActionKey,
  dedupeKey: Identifier,
  body: Summary,
});

const AlwaysOnSilentAction = Schema.Struct({
  type: Schema.Literal("silent"),
  key: ActionKey,
});

/** Actions the restricted executor can apply without a provider process. */
export const AlwaysOnEffectAction = Schema.Union([
  AlwaysOnNoteAction,
  AlwaysOnProgressAction,
  AlwaysOnResultAction,
  AlwaysOnTimerAction,
  AlwaysOnDeliverAction,
  AlwaysOnSilentAction,
]);
export type AlwaysOnEffectAction = typeof AlwaysOnEffectAction.Type;

const AlwaysOnApprovalAction = Schema.Struct({
  type: Schema.Literal("request_approval"),
  key: ActionKey,
  summary: Summary,
  onApprove: Schema.Array(AlwaysOnEffectAction).check(Schema.isMaxLength(4)),
});

/**
 * Records an external effect this runtime will not perform. A crash while it is
 * unresolved becomes `unknown` and is not replayed.
 */
const AlwaysOnUncertainAction = Schema.Struct({
  type: Schema.Literal("record_uncertain"),
  key: ActionKey,
  summary: Summary,
});

export const AlwaysOnAction = Schema.Union([
  AlwaysOnEffectAction,
  AlwaysOnApprovalAction,
  AlwaysOnUncertainAction,
]);
export type AlwaysOnAction = typeof AlwaysOnAction.Type;

export const AlwaysOnMemoryKind = Schema.Literals([
  "instruction",
  "note",
  "sent_result",
  "task_progress",
]);
export type AlwaysOnMemoryKind = typeof AlwaysOnMemoryKind.Type;

export const AlwaysOnMemoryIndexEntry = Schema.Struct({
  id: AlwaysOnMemoryId,
  kind: AlwaysOnMemoryKind,
  title: Schema.String,
  taskKey: Schema.NullOr(Schema.String),
  pointer: Schema.NullOr(Schema.String),
  hasBody: Schema.Boolean,
});
export type AlwaysOnMemoryIndexEntry = typeof AlwaysOnMemoryIndexEntry.Type;

export const AlwaysOnLoadedContext = Schema.Struct({
  instructions: Schema.String,
  noteIndex: Schema.Array(
    Schema.Struct({
      id: AlwaysOnMemoryId,
      title: Schema.String,
      pointer: Schema.NullOr(Schema.String),
    }),
  ),
  progressIndex: Schema.Array(
    Schema.Struct({
      id: AlwaysOnMemoryId,
      title: Schema.String,
      taskKey: Schema.NullOr(Schema.String),
      pointer: Schema.NullOr(Schema.String),
    }),
  ),
  recentResults: Schema.Array(
    Schema.Struct({ id: AlwaysOnMemoryId, title: Schema.String, body: Schema.String }),
  ),
});
export type AlwaysOnLoadedContext = typeof AlwaysOnLoadedContext.Type;

export const AlwaysOnAgentSummary = Schema.Struct({
  id: AlwaysOnAgentId,
  projectId: ProjectId,
  title: Schema.String,
  enabled: Schema.Boolean,
  phase: AlwaysOnPhase,
  userSessionId: Schema.NullOr(AlwaysOnSessionId),
  pendingInboxCount: Schema.Int,
  pendingApprovalCount: Schema.Int,
  pendingOutboxCount: Schema.Int,
  updatedAt: IsoDateTime,
});
export type AlwaysOnAgentSummary = typeof AlwaysOnAgentSummary.Type;

export const AlwaysOnSnapshot = Schema.Struct({
  revision: Schema.Number,
  agents: Schema.Array(AlwaysOnAgentSummary),
});
export type AlwaysOnSnapshot = typeof AlwaysOnSnapshot.Type;

export const AlwaysOnInboxEntry = Schema.Struct({
  id: AlwaysOnEventId,
  source: Schema.String,
  sourceEventId: Schema.String,
  kind: AlwaysOnEventKind,
  status: Schema.Literals(["pending", "consumed", "cancelled"]),
  sessionPolicy: Schema.Literals(["continue", "fresh"]),
  createdAt: IsoDateTime,
});
export type AlwaysOnInboxEntry = typeof AlwaysOnInboxEntry.Type;

export const AlwaysOnRunEntry = Schema.Struct({
  id: AlwaysOnRunId,
  sessionId: AlwaysOnSessionId,
  status: AlwaysOnRunStatus,
  outcome: Schema.NullOr(AlwaysOnRunOutcome),
  uncertain: Schema.Boolean,
  startedAt: IsoDateTime,
  finishedAt: Schema.NullOr(IsoDateTime),
  lastProgressAt: Schema.NullOr(IsoDateTime),
  eventIds: Schema.Array(AlwaysOnEventId),
  loadedContext: AlwaysOnLoadedContext,
});
export type AlwaysOnRunEntry = typeof AlwaysOnRunEntry.Type;

export const AlwaysOnApprovalEntry = Schema.Struct({
  id: AlwaysOnApprovalId,
  runId: AlwaysOnRunId,
  sessionId: AlwaysOnSessionId,
  status: Schema.Literals(["pending", "approved", "rejected", "cancelled"]),
  summary: Schema.String,
  createdAt: IsoDateTime,
});
export type AlwaysOnApprovalEntry = typeof AlwaysOnApprovalEntry.Type;

export const AlwaysOnDeliveryEntry = Schema.Struct({
  id: Identifier,
  sessionId: AlwaysOnSessionId,
  body: Schema.String,
  status: Schema.Literals(["pending", "delivered", "failed"]),
  attempts: Schema.Int,
  dedupeKey: Schema.String,
});
export type AlwaysOnDeliveryEntry = typeof AlwaysOnDeliveryEntry.Type;

export const AlwaysOnTimerEntry = Schema.Struct({
  id: Identifier,
  kind: Schema.Literals(["deadline", "recurring"]),
  dueAt: IsoDateTime,
  everyMs: Schema.NullOr(Schema.Int),
  status: Schema.Literals(["armed", "fired", "cancelled"]),
});
export type AlwaysOnTimerEntry = typeof AlwaysOnTimerEntry.Type;

export const AlwaysOnDetail = Schema.Struct({
  agent: AlwaysOnAgentSummary,
  instructions: Schema.String,
  inbox: Schema.Array(AlwaysOnInboxEntry),
  runs: Schema.Array(AlwaysOnRunEntry),
  memoryIndex: Schema.Array(AlwaysOnMemoryIndexEntry),
  approvals: Schema.Array(AlwaysOnApprovalEntry),
  deliveries: Schema.Array(AlwaysOnDeliveryEntry),
  timers: Schema.Array(AlwaysOnTimerEntry),
});
export type AlwaysOnDetail = typeof AlwaysOnDetail.Type;

export const AlwaysOnMemoryRecord = Schema.Struct({
  id: AlwaysOnMemoryId,
  agentId: AlwaysOnAgentId,
  kind: AlwaysOnMemoryKind,
  title: Schema.String,
  body: Schema.NullOr(Schema.String),
  pointer: Schema.NullOr(Schema.String),
  taskKey: Schema.NullOr(Schema.String),
});
export type AlwaysOnMemoryRecord = typeof AlwaysOnMemoryRecord.Type;

export const AlwaysOnCreateInput = Schema.Struct({
  projectId: ProjectId,
  title: Identifier,
  instructions: Instructions,
});
export type AlwaysOnCreateInput = typeof AlwaysOnCreateInput.Type;

export const AlwaysOnCreateResult = Schema.Struct({
  agent: AlwaysOnAgentSummary,
});
export type AlwaysOnCreateResult = typeof AlwaysOnCreateResult.Type;

export const AlwaysOnSetEnabledInput = Schema.Struct({
  agentId: AlwaysOnAgentId,
  enabled: Schema.Boolean,
});
export type AlwaysOnSetEnabledInput = typeof AlwaysOnSetEnabledInput.Type;

export const AlwaysOnSubmitKind = Schema.Literals(["user_reply", "external", "system"]);
export type AlwaysOnSubmitKind = typeof AlwaysOnSubmitKind.Type;

export const AlwaysOnSubmitEventInput = Schema.Struct({
  agentId: AlwaysOnAgentId,
  source: Identifier,
  sourceEventId: Identifier,
  kind: AlwaysOnSubmitKind,
  actions: Schema.Array(AlwaysOnAction).check(Schema.isMaxLength(8)),
});
export type AlwaysOnSubmitEventInput = typeof AlwaysOnSubmitEventInput.Type;

export const AlwaysOnSubmitEventResult = Schema.Struct({
  eventId: AlwaysOnEventId,
  duplicate: Schema.Boolean,
});
export type AlwaysOnSubmitEventResult = typeof AlwaysOnSubmitEventResult.Type;

export const AlwaysOnGetInput = Schema.Struct({
  agentId: AlwaysOnAgentId,
  /** Ignored by the server. Clients change it to refetch after a snapshot revision. */
  revision: Schema.optional(Schema.Number),
});
export type AlwaysOnGetInput = typeof AlwaysOnGetInput.Type;

export const AlwaysOnReadMemoryInput = Schema.Struct({
  agentId: AlwaysOnAgentId,
  memoryId: AlwaysOnMemoryId,
});
export type AlwaysOnReadMemoryInput = typeof AlwaysOnReadMemoryInput.Type;

export const AlwaysOnResolveApprovalInput = Schema.Struct({
  agentId: AlwaysOnAgentId,
  approvalId: AlwaysOnApprovalId,
  decision: Schema.Literals(["approved", "rejected"]),
});
export type AlwaysOnResolveApprovalInput = typeof AlwaysOnResolveApprovalInput.Type;

export const AlwaysOnAgentInput = Schema.Struct({
  agentId: AlwaysOnAgentId,
});
export type AlwaysOnAgentInput = typeof AlwaysOnAgentInput.Type;

export const AlwaysOnCancelInput = Schema.Struct({
  agentId: AlwaysOnAgentId,
  runId: AlwaysOnRunId,
});
export type AlwaysOnCancelInput = typeof AlwaysOnCancelInput.Type;

export class AlwaysOnError extends Schema.TaggedError<AlwaysOnError>()("AlwaysOnError", {
  message: Schema.String,
  reason: Schema.Literals(["not_found", "denied", "rejected", "conflict", "unavailable"]),
}) {}
