import * as Schema from "effect/Schema";

import {
  CommandId,
  IsoDateTime,
  ProjectId,
  ThreadId,
  TrimmedNonEmptyString,
} from "./baseSchemas.ts";
import { ModelSelection } from "./modelSelection.ts";
import { ProviderInteractionMode, RuntimeMode } from "./providerPolicy.ts";

const Identifier = TrimmedNonEmptyString.check(Schema.isMaxLength(160));
const Summary = TrimmedNonEmptyString.check(Schema.isMaxLength(8_000));
const Evidence = Schema.Array(Summary).check(Schema.isMinLength(1));

export const EventOrchestrationConfig = Schema.Struct({
  enabled: Schema.Boolean,
  projectId: ProjectId,
  coordinatorExecutorId: Identifier,
  executors: Schema.Array(Schema.Struct({ id: Identifier, modelSelection: ModelSelection })),
  roles: Schema.Array(
    Schema.Struct({
      id: Identifier,
      title: Identifier,
      instructionsPath: TrimmedNonEmptyString,
      executorIds: Schema.Array(Identifier).check(Schema.isMinLength(1)),
    }),
  ),
  allowedRepositories: Schema.Array(Identifier),
  allowedEventTypes: Schema.Array(Identifier),
  runtimeMode: RuntimeMode,
  interactionMode: ProviderInteractionMode,
});
export type EventOrchestrationConfig = typeof EventOrchestrationConfig.Type;

export const GitHubEventSubmission = Schema.Struct({
  source: Schema.Literal("github"),
  deliveryId: Identifier,
  repository: Identifier,
  eventType: Identifier,
  action: Schema.optional(Identifier),
  subject: Schema.optional(Summary),
  text: Summary,
  url: Schema.optional(TrimmedNonEmptyString),
});
export type GitHubEventSubmission = typeof GitHubEventSubmission.Type;

export const EventOrchestrationAdmission = Schema.Struct({
  jobId: Identifier,
  accepted: Schema.Literal(true),
});

export const EventOrchestrationAssignInput = Schema.Struct({
  jobId: Identifier,
  taskId: Identifier,
  roleId: Identifier,
  objective: Summary,
  executorId: Schema.optional(Identifier),
  dependsOn: Schema.optional(Schema.Array(Identifier)),
});
export type EventOrchestrationAssignInput = typeof EventOrchestrationAssignInput.Type;

export const EventOrchestrationFailureKind = Schema.Literals([
  "transient",
  "executor",
  "authentication",
  "authorization",
  "requirements",
  "permanent",
]);
export type EventOrchestrationFailureKind = typeof EventOrchestrationFailureKind.Type;

export const EventOrchestrationReportInput = Schema.Union([
  Schema.Struct({
    kind: Schema.Literal("progress"),
    jobId: Identifier,
    attemptId: Identifier,
    summary: Summary,
  }),
  Schema.Struct({
    kind: Schema.Literal("waiting"),
    jobId: Identifier,
    attemptId: Identifier,
    summary: Summary,
  }),
  Schema.Struct({
    kind: Schema.Literal("completed"),
    jobId: Identifier,
    attemptId: Identifier,
    summary: Summary,
    evidence: Evidence,
  }),
  Schema.Struct({
    kind: Schema.Literal("failed"),
    jobId: Identifier,
    attemptId: Identifier,
    failureKind: EventOrchestrationFailureKind,
    summary: Summary,
  }),
  Schema.Struct({
    kind: Schema.Literal("accept"),
    jobId: Identifier,
    taskId: Identifier,
    evidence: Evidence,
  }),
  Schema.Struct({
    kind: Schema.Literal("retry"),
    jobId: Identifier,
    taskId: Identifier,
    executorId: Schema.optional(Identifier),
  }),
  Schema.Struct({ kind: Schema.Literal("job_complete"), jobId: Identifier, evidence: Evidence }),
  Schema.Struct({ kind: Schema.Literal("job_resume"), jobId: Identifier, evidence: Evidence }),
  Schema.Struct({ kind: Schema.Literal("job_blocked"), jobId: Identifier, summary: Summary }),
]);
export type EventOrchestrationReportInput = typeof EventOrchestrationReportInput.Type;

export const EventOrchestrationAttempt = Schema.Struct({
  id: Identifier,
  ordinal: Schema.Int,
  executorId: Identifier,
  commandId: CommandId,
  threadId: ThreadId,
  status: Schema.Literals([
    "queued",
    "running",
    "waiting",
    "stalled",
    "unknown",
    "awaiting_stop",
    "completed",
    "failed",
    "cancelled",
  ]),
  failureKind: Schema.NullOr(EventOrchestrationFailureKind),
  waitingReason: Schema.NullOr(Schema.String),
  summary: Schema.NullOr(Schema.String),
  evidence: Schema.Array(Schema.String),
  stopped: Schema.Boolean,
  lastHeartbeatAt: Schema.NullOr(IsoDateTime),
  lastProgressAt: Schema.NullOr(IsoDateTime),
  lastEventSequence: Schema.Number,
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
});
export type EventOrchestrationAttempt = typeof EventOrchestrationAttempt.Type;

export const EventOrchestrationTask = Schema.Struct({
  id: Identifier,
  jobId: Identifier,
  roleId: Identifier,
  objective: Schema.String,
  dependsOn: Schema.Array(Identifier),
  status: Schema.Literals([
    "queued",
    "running",
    "waiting",
    "stalled",
    "unknown",
    "awaiting_review",
    "succeeded",
    "blocked",
    "failed",
    "cancelled",
  ]),
  attempts: Schema.Array(EventOrchestrationAttempt),
  acceptanceEvidence: Schema.Array(Schema.String),
});
export type EventOrchestrationTask = typeof EventOrchestrationTask.Type;

export const EventOrchestrationJob = Schema.Struct({
  id: Identifier,
  event: GitHubEventSubmission,
  coordinatorThreadId: ThreadId,
  coordinatorCommandId: CommandId,
  coordinatorLaunchMode: Schema.Literals(["launch", "resume"]),
  coordinatorLaunchCount: Schema.Int,
  coordinatorStatus: Schema.Literals([
    "queued",
    "running",
    "waiting",
    "stalled",
    "unknown",
    "completed",
    "failed",
    "cancelled",
  ]),
  coordinatorFailureKind: Schema.NullOr(EventOrchestrationFailureKind),
  coordinatorStopped: Schema.Boolean,
  coordinatorLastHeartbeatAt: Schema.NullOr(IsoDateTime),
  coordinatorLastProgressAt: Schema.NullOr(IsoDateTime),
  status: Schema.Literals([
    "queued",
    "running",
    "blocked",
    "succeeded",
    "failed",
    "cancelling",
    "cancelled",
  ]),
  summary: Schema.NullOr(Schema.String),
  evidence: Schema.Array(Schema.String),
  createdAt: IsoDateTime,
  updatedAt: IsoDateTime,
});
export type EventOrchestrationJob = typeof EventOrchestrationJob.Type;
export const EventOrchestrationJobSummary = EventOrchestrationJob;
export type EventOrchestrationJobSummary = typeof EventOrchestrationJobSummary.Type;
export const EventOrchestrationJobDetail = Schema.Struct({
  ...EventOrchestrationJob.fields,
  tasks: Schema.Array(EventOrchestrationTask),
});
export type EventOrchestrationJobDetail = typeof EventOrchestrationJobDetail.Type;

export const EventOrchestrationChange = Schema.Struct({
  cursor: Schema.Number,
  jobId: Identifier,
  taskId: Schema.NullOr(Identifier),
  attemptId: Schema.NullOr(Identifier),
  kind: Schema.Literals(["job", "task", "attempt", "checkpoint"]),
  status: Schema.String,
  at: IsoDateTime,
});
export type EventOrchestrationChange = typeof EventOrchestrationChange.Type;
export const EventOrchestrationChanges = Schema.Struct({
  changes: Schema.Array(EventOrchestrationChange),
  nextCursor: Schema.Number,
});
export type EventOrchestrationChanges = typeof EventOrchestrationChanges.Type;
export const EventOrchestrationCancelInput = Schema.Struct({ jobId: Identifier });
export type EventOrchestrationCancelInput = typeof EventOrchestrationCancelInput.Type;
