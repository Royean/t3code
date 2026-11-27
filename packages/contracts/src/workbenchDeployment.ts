import * as Schema from "effect/Schema";
import { ProjectId, ScheduledTaskId, ThreadId } from "./baseSchemas.ts";

const DeploymentIso = Schema.String.check(
  Schema.isMaxLength(64),
  Schema.makeFilter((value) => {
    const m =
      /^(\d{4})-(\d{2})-(\d{2})T([01]\d|2[0-3]):[0-5]\d:[0-5]\d(?:\.\d+)?(?:Z|[+-](?:[01]\d|2[0-3]):[0-5]\d)$/.exec(
        value,
      );
    if (!m || !Number.isFinite(Date.parse(value))) return false;
    const year = Number(m[1]),
      month = Number(m[2]),
      day = Number(m[3]);
    const days = [
      31,
      year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0) ? 29 : 28,
      31,
      30,
      31,
      30,
      31,
      31,
      30,
      31,
      30,
      31,
    ];
    return month >= 1 && month <= 12 && day >= 1 && day <= days[month - 1]!;
  }),
);
const Id = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(200),
  Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._:/-]*$/),
);
const Hash = Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/));
const GitSha = Schema.String.check(Schema.isPattern(/^(?:[a-f0-9]{40}|[a-f0-9]{64})$/));
const Count = Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: Number.MAX_SAFE_INTEGER }));
const Positive = Schema.Int.check(
  Schema.isBetween({ minimum: 1, maximum: Number.MAX_SAFE_INTEGER }),
);
const Text = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(1024),
  Schema.isPattern(/^[^\x00-\x1f\x7f]+$/),
);
const Blob = Schema.Struct({
  sha256: Hash,
  bytes: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 64 * 1024 * 1024 })),
});
const Action = Schema.Struct({ actionId: Id, planDigest: Hash });
const Lock = Schema.Struct({ epoch: Positive, nonce: Hash });
export const WorkbenchDeploymentBinding = Schema.Struct({
  environmentId: Id,
  trackingId: Id,
  queueId: Id,
  controllerId: Id,
  projectId: ProjectId,
  workItemId: Id,
  taskId: ScheduledTaskId,
  taskCreatedAt: DeploymentIso,
  dispatcherThreadId: ThreadId,
  workThreadId: ThreadId,
  cycleNumber: Positive,
  workItemVersion: Count,
  latestEventSequence: Count,
  processedThroughSequence: Count,
});
export const WorkbenchDeploymentBasis = Schema.Struct({
  serviceId: Id,
  version: Schema.String.check(
    Schema.isMinLength(1),
    Schema.isMaxLength(200),
    Schema.isPattern(/^[^\x00-\x1f\x7f]+$/),
  ),
  sourceSha: GitSha,
  artifactSha256: Hash,
});
const Baseline = Schema.Struct({ basis: WorkbenchDeploymentBasis, revision: Count });
export const WorkbenchDeploymentPlan = Schema.Struct({
  adapterId: Id,
  adapterVersion: Positive,
  generationId: Id,
  serviceKey: Id,
  coordinator: Schema.Struct({ id: Hash, origin: Text }),
  deploy: Action,
  rollback: Schema.Struct({
    ...Action.fields,
    verification: Schema.Literal("fixed_target_and_adapter"),
  }),
});
export type WorkbenchDeploymentPlan = typeof WorkbenchDeploymentPlan.Type;
export const WorkbenchDeploymentHostPolicy = Schema.Struct({
  adapter: Schema.Struct({
    id: Id,
    version: Positive,
    generationId: Id,
    origin: Text,
    pathPrefix: Text,
    credentialRef: Id,
  }),
  serviceKey: Id,
  coordinator: Schema.Struct({
    id: Hash,
    origin: Text,
    publicKey: Schema.String.check(Schema.isMaxLength(4096)),
  }),
  deploy: Action,
  rollback: Action,
});
export type WorkbenchDeploymentHostPolicy = typeof WorkbenchDeploymentHostPolicy.Type;
export const WorkbenchDeploymentDescriptor = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  adapterId: Id,
  adapterVersion: Positive,
  generationId: Id,
  serviceKey: Id,
  serviceId: Id,
  coordinatorId: Hash,
  deploy: Action,
  rollback: Action,
});
export type WorkbenchDeploymentDescriptor = typeof WorkbenchDeploymentDescriptor.Type;
export const WorkbenchDeploymentTarget = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  adapterId: Id,
  adapterVersion: Positive,
  generationId: Id,
  serviceKey: Id,
  serviceId: Id,
  revision: Count,
  lastOperationId: Schema.NullOr(Id),
  state: Schema.Literals(["consistent", "partial", "unknown"]),
  basis: Schema.NullOr(WorkbenchDeploymentBasis),
  observedAt: DeploymentIso,
});
export type WorkbenchDeploymentTarget = typeof WorkbenchDeploymentTarget.Type;
export const WorkbenchDeploymentPreflightInput = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  id: Id,
  inputDigest: Hash,
  descriptorDigest: Hash,
  serviceKey: Id,
  sourceSha: GitSha,
  artifact: Blob,
  baseline: Schema.Struct({ ...Baseline.fields, artifact: Blob }),
  deployPlanDigest: Hash,
  rollbackPlanDigest: Hash,
});
export type WorkbenchDeploymentPreflightInput = typeof WorkbenchDeploymentPreflightInput.Type;
const OperationState = Schema.Literals(["queued", "running", "succeeded", "failed", "unknown"]);
export const WorkbenchDeploymentPreflight = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  adapterId: Id,
  adapterVersion: Positive,
  generationId: Id,
  serviceKey: Id,
  id: Id,
  inputDigest: Hash,
  revision: Positive,
  state: OperationState,
  result: Schema.NullOr(
    Schema.Struct({ exitCode: Schema.Int, outputDigest: Hash, completedAt: DeploymentIso }),
  ),
  createdAt: DeploymentIso,
  updatedAt: DeploymentIso,
});
export type WorkbenchDeploymentPreflight = typeof WorkbenchDeploymentPreflight.Type;
export const WorkbenchDeploymentEvidence = Schema.Struct({
  plan: WorkbenchDeploymentPlan,
  desired: WorkbenchDeploymentBasis,
  preflight: WorkbenchDeploymentPreflight,
  baselineRevision: Count,
});
export type WorkbenchDeploymentEvidence = typeof WorkbenchDeploymentEvidence.Type;
export const WorkbenchDeploymentPayload = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  deploymentId: Id,
  nativeDeploymentId: Id,
  ownerId: Id,
  candidateId: Id,
  decisionId: Id,
  decidedAt: Count,
  packetDigest: Hash,
  preparationId: Id,
  evidenceDigest: Hash,
  profileDigest: Hash,
  binding: WorkbenchDeploymentBinding,
  sourceSha: GitSha,
  mainSha: GitSha,
  artifact: Blob,
  rollbackArtifact: Blob,
  plan: WorkbenchDeploymentPlan,
  desired: WorkbenchDeploymentBasis,
  baseline: Baseline,
  deployOperationId: Id,
  rollbackOperationId: Id,
  lock: Lock,
  issuer: Schema.Struct({ id: Hash, origin: Text }),
  claimTokenDigest: Hash,
  createdAt: Count,
});
export type WorkbenchDeploymentPayload = typeof WorkbenchDeploymentPayload.Type;
export const WorkbenchDeploymentSigned = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  kind: Schema.Literals(["reservation", "commit"]),
  keyId: Hash,
  authorizationDigest: Hash,
  payload: WorkbenchDeploymentPayload,
  committedAt: Schema.NullOr(Count),
  signature: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9_-]{86}$/)),
});
export type WorkbenchDeploymentSigned = typeof WorkbenchDeploymentSigned.Type;
export const WorkbenchDeploymentOperation = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  adapterId: Id,
  adapterVersion: Positive,
  generationId: Id,
  serviceKey: Id,
  serviceId: Id,
  id: Id,
  kind: Schema.Literals(["deploy", "rollback"]),
  inputDigest: Hash,
  authorizationDigest: Hash,
  externalExecutionId: Id,
  revision: Positive,
  state: OperationState,
  result: Schema.NullOr(
    Schema.Struct({
      effect: Schema.Literals(["unchanged", "changed", "partial"]),
      writtenRevision: Schema.NullOr(Count),
      exitCode: Schema.NullOr(Schema.Int),
      outputDigest: Hash,
      completedAt: DeploymentIso,
    }),
  ),
  createdAt: DeploymentIso,
  updatedAt: DeploymentIso,
});
export type WorkbenchDeploymentOperation = typeof WorkbenchDeploymentOperation.Type;
export const WorkbenchDeploymentReason = Schema.Literals([
  "not_configured",
  "target_not_registered",
  "coordinator_not_configured",
  "coordinator_identity_mismatch",
  "coordinator_unavailable",
  "coordinator_forbidden",
  "permit_invalid",
  "invalid_binding",
  "conflict",
  "identity_mismatch",
  "source_changed",
  "profile_changed",
  "material_changed",
  "artifact_unavailable",
  "artifact_invalid",
  "evidence_unavailable",
  "preflight_failed",
  "adapter_unavailable",
  "adapter_identity_mismatch",
  "adapter_conflict",
  "target_unavailable",
  "target_mismatch",
  "target_changed",
  "target_changed_unrelated",
  "deployment_failed",
  "rollback_failed",
  "rollback_unknown",
  "lock_conflict",
  "service_busy",
  "native_unavailable",
  "state_unknown",
  "not_started",
  "claim_withdrawn",
  "registration_changed",
]);
export type WorkbenchDeploymentReason = typeof WorkbenchDeploymentReason.Type;
export const WorkbenchEvidenceVerification = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  semanticsVersion: Schema.Literal(1),
  ref: Id,
  manifestDigest: Hash,
  archive: Schema.Struct({
    sha256: Hash,
    bytes: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 268435456 })),
  }),
  sourceTreeSha: GitSha,
  policyDigest: Hash,
});
export type WorkbenchEvidenceVerification = typeof WorkbenchEvidenceVerification.Type;
export const WorkbenchAcceptancePolicy = Schema.Struct({
  semanticsVersion: Schema.Literal(1),
  sampling: Schema.Literal("health_then_core"),
  durationSeconds: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 86400 })),
  intervalSeconds: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 3600 })),
  consecutiveSuccesses: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 100 })),
  maxGapSeconds: Positive,
  maxSamples: Schema.Literal(131072),
});
export type WorkbenchAcceptancePolicy = typeof WorkbenchAcceptancePolicy.Type;
export const WorkbenchOnlineReceipt = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  id: Id,
  sampleId: Id,
  executionId: Id,
  role: Schema.Literals(["health", "core"]),
  scriptId: Id,
  definitionHash: Hash,
  phase: Schema.Literal("online"),
  subject: Schema.Literal("deployed_artifact"),
  authorizationDigest: Hash,
  materialDigest: Hash,
  candidate: WorkbenchDeploymentBasis,
  activationRevision: Count,
  runnerEpoch: Id,
  state: Schema.Literals(["succeeded", "failed", "unknown"]),
  exitCode: Schema.NullOr(Schema.Int),
  outputDigest: Schema.NullOr(Hash),
  startedAt: DeploymentIso,
  completedAt: Schema.NullOr(DeploymentIso),
  durationMilliseconds: Schema.NullOr(Count),
});
export type WorkbenchOnlineReceipt = typeof WorkbenchOnlineReceipt.Type;
export const WorkbenchAcceptanceSample = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  id: Id,
  ordinal: Positive,
  runnerEpoch: Id,
  inputDigest: Hash,
  state: Schema.Literals(["succeeded", "failed", "unknown"]),
  candidate: WorkbenchDeploymentBasis,
  activationRevision: Count,
  startedAt: DeploymentIso,
  completedAt: Schema.NullOr(DeploymentIso),
  health: Schema.NullOr(WorkbenchOnlineReceipt),
  core: Schema.NullOr(WorkbenchOnlineReceipt),
  targetBefore: Schema.NullOr(WorkbenchDeploymentTarget),
  targetAfter: Schema.NullOr(WorkbenchDeploymentTarget),
  versionObservation: Schema.NullOr(
    Schema.Struct({ basis: WorkbenchDeploymentBasis, observedAt: DeploymentIso }),
  ),
  gapMilliseconds: Schema.NullOr(Count),
  creditedElapsedMilliseconds: Count,
  digest: Hash,
  wallGapMilliseconds: Schema.NullOr(Count),
});
export type WorkbenchAcceptanceSample = typeof WorkbenchAcceptanceSample.Type;
export const WorkbenchAcceptanceReason = Schema.Literals([
  "material_missing",
  "material_invalid",
  "health_failed",
  "core_failed",
  "observation_failed",
  "check_unknown",
  "window_gap",
  "clock_changed",
  "native_restarted",
  "target_changed",
  "evidence_limit",
]);
export type WorkbenchAcceptanceReason = typeof WorkbenchAcceptanceReason.Type;
export const WorkbenchAcceptance = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  id: Id,
  authorizationDigest: Hash,
  materialDigest: Schema.NullOr(Hash),
  policy: WorkbenchAcceptancePolicy,
  candidate: WorkbenchDeploymentBasis,
  activationRevision: Count,
  deployOperationId: Id,
  version: Positive,
  state: Schema.Literals(["pending", "checking", "observing", "passed", "failed", "unknown"]),
  reasons: Schema.Array(WorkbenchAcceptanceReason).check(Schema.isMaxLength(11)),
  runnerEpoch: Schema.NullOr(Id),
  firstSample: Schema.NullOr(WorkbenchAcceptanceSample),
  latestSample: Schema.NullOr(WorkbenchAcceptanceSample),
  window: Schema.Struct({
    id: Schema.NullOr(Id),
    runnerEpoch: Schema.NullOr(Id),
    startedAt: Schema.NullOr(DeploymentIso),
    endedAt: Schema.NullOr(DeploymentIso),
    elapsedMilliseconds: Count,
    successfulSamples: Count,
    maxGapMilliseconds: Count,
    resets: Count,
    resetReason: Schema.NullOr(
      Schema.Literals([
        "window_gap",
        "clock_changed",
        "native_restarted",
        "check_unknown",
        "target_changed",
      ]),
    ),
    maxWallGapMilliseconds: Count,
  }),
  ledger: Schema.Struct({
    ref: Id,
    count: Schema.Int.check(Schema.isBetween({ minimum: 0, maximum: 131072 })),
    digest: Hash,
  }),
  failure: Schema.NullOr(
    Schema.Struct({
      id: Id,
      reason: Schema.Literals(["health_failed", "core_failed", "observation_failed"]),
      role: Schema.Literals(["health", "core"]),
      sampleId: Id,
      receiptId: Id,
      outputDigest: Hash,
      occurredAt: DeploymentIso,
    }),
  ),
  createdAt: DeploymentIso,
  updatedAt: DeploymentIso,
  observedAt: Schema.NullOr(DeploymentIso),
  completedAt: Schema.NullOr(DeploymentIso),
  activeSample: Schema.NullOr(
    Schema.Struct({
      id: Id,
      ordinal: Positive,
      runnerEpoch: Id,
      role: Schema.Literals(["health", "core"]),
      startedAt: DeploymentIso,
    }),
  ),
});
export type WorkbenchAcceptance = typeof WorkbenchAcceptance.Type;
export const WorkbenchDeployment = Schema.Struct({
  acceptance: Schema.optional(Schema.NullOr(WorkbenchAcceptance)),
  id: Id,
  authorizationDigest: Hash,
  preparationId: Id,
  evidenceDigest: Hash,
  profileDigest: Hash,
  binding: WorkbenchDeploymentBinding,
  plan: WorkbenchDeploymentPlan,
  desired: WorkbenchDeploymentBasis,
  baseline: Baseline,
  lock: Lock,
  version: Positive,
  state: Schema.Literals([
    "queued",
    "staging",
    "awaiting_claim",
    "deploying",
    "deployed",
    "deploy_unknown",
    "rolling_back",
    "rolled_back",
    "failed_unchanged",
    "rejected",
    "attention",
    "rollback_unknown",
  ]),
  reasons: Schema.Array(WorkbenchDeploymentReason).check(Schema.isMaxLength(34)),
  claimState: Schema.Literals(["reserved", "committed", "withdrawn", "unknown"]),
  deployOperation: Schema.NullOr(WorkbenchDeploymentOperation),
  rollbackOperation: Schema.NullOr(WorkbenchDeploymentOperation),
  target: Schema.NullOr(WorkbenchDeploymentTarget),
  versionObservation: Schema.NullOr(
    Schema.Struct({ basis: WorkbenchDeploymentBasis, observedAt: DeploymentIso }),
  ),
  createdAt: DeploymentIso,
  updatedAt: DeploymentIso,
  observedAt: Schema.NullOr(DeploymentIso),
});
export type WorkbenchDeployment = typeof WorkbenchDeployment.Type;
const Scope = { controllerId: Id, projectId: ProjectId, expectedTrackingId: Id };
export const WorkbenchDeploymentQuery = Schema.Struct(Scope);
export const WorkbenchDeploymentAdmitInput = Schema.Struct({
  ...Scope,
  deploymentId: Id,
  preparationId: Id,
  expectedEvidenceDigest: Hash,
  reservation: WorkbenchDeploymentSigned,
  claimToken: Hash,
});
export type WorkbenchDeploymentAdmitInput = typeof WorkbenchDeploymentAdmitInput.Type;
export const WorkbenchDeploymentResult = Schema.Struct({
  environmentId: Id,
  trackingId: Id,
  queueId: Id,
  controllerId: Id,
  projectId: ProjectId,
  deployment: WorkbenchDeployment,
});
export type WorkbenchDeploymentResult = typeof WorkbenchDeploymentResult.Type;
export class WorkbenchDeploymentError extends Schema.TaggedError<WorkbenchDeploymentError>()(
  "WorkbenchDeploymentError",
  {
    reason: Schema.Literals([
      "invalid_request",
      "not_found",
      "permit_invalid",
      "coordinator_forbidden",
      "conflict",
      "invalid_binding",
      "stale_state",
      "evidence_invalid",
      "unavailable",
      "not_configured",
    ]),
  },
) {}
