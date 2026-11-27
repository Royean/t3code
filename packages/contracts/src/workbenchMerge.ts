import * as Schema from "effect/Schema";
import {
  ReleaseIso as Iso,
  ReleaseId as Id,
  ReleaseHash as Hash,
  ReleaseGitSha as Sha,
  ReleaseCount as Count,
  ReleasePositive as Positive,
  ReleaseText as Text,
} from "./workbenchReleasePrimitives.ts";

const ProviderId = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(200),
  Schema.isPattern(/^[^\x00-\x1f\x7f]+$/),
);
const Host = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(253),
  Schema.makeFilter((value) => {
    try {
      const url = new URL(`https://${value}`);
      return (
        url.host === value &&
        url.hostname.length > 0 &&
        url.username === "" &&
        url.password === "" &&
        url.pathname === "/" &&
        url.search === "" &&
        url.hash === ""
      );
    } catch {
      return false;
    }
  }),
);
const Ref = Text.check(
  Schema.isMaxLength(500),
  Schema.isPattern(/^refs\/heads\/[A-Za-z0-9][A-Za-z0-9._/-]*$/),
  Schema.makeFilter(
    (value) =>
      !value.includes("..") &&
      !value.endsWith("/") &&
      !value.endsWith(".") &&
      !value.includes("//") &&
      !value.split("/").some((part) => part.startsWith(".") || part.endsWith(".lock")),
  ),
);
export const WorkbenchMergePlan = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  kind: Schema.Literal("github_exact_ff"),
  host: Host,
  repository: Schema.String.check(
    Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9-]{0,38}\/[A-Za-z0-9_.-]{1,100}$/),
    Schema.makeFilter((value) => !value.split("/").some((p) => p === "." || p === "..")),
  ),
  repositoryId: ProviderId,
  ref: Ref,
  requiredChecks: Schema.Array(
    Schema.Struct({ context: Text.check(Schema.isMaxLength(255)), appId: Positive }),
  ).check(
    Schema.isMinLength(1),
    Schema.isMaxLength(20),
    Schema.makeFilter(
      (checks) => new Set(checks.map((c) => `${c.context}\0${c.appId}`)).size === checks.length,
    ),
  ),
});
export type WorkbenchMergePlan = typeof WorkbenchMergePlan.Type;
export const WorkbenchMergeSubject = Schema.Struct({
  baseSha: Sha,
  sourceSha: Sha,
  sourceTreeSha: Sha,
});
export type WorkbenchMergeSubject = typeof WorkbenchMergeSubject.Type;
export const WorkbenchMergeRemoteReason = Schema.Literals([
  "unsupported",
  "unavailable",
  "identity_mismatch",
  "actor_changed",
  "policy_changed",
  "main_changed",
  "candidate_unavailable",
  "candidate_changed",
  "checks_pending",
  "checks_failed",
  "checks_missing",
  "protected_ref_rejected",
  "mutation_unknown",
  "invalid_response",
]);
export type WorkbenchMergeRemoteReason = typeof WorkbenchMergeRemoteReason.Type;
export const WorkbenchMergeRequiredResult = Schema.Struct({
  context: Text.check(Schema.isMaxLength(255)),
  appId: Positive,
  headSha: Sha,
  checkId: Schema.NullOr(ProviderId),
  state: Schema.Literals(["passed", "failed", "pending", "missing"]),
  conclusion: Schema.NullOr(
    Schema.Literals([
      "success",
      "failure",
      "cancelled",
      "timed_out",
      "action_required",
      "neutral",
      "skipped",
      "stale",
      "startup_failure",
    ]),
  ),
  receiptDigest: Hash,
});
export type WorkbenchMergeRequiredResult = typeof WorkbenchMergeRequiredResult.Type;
export const WorkbenchMergeObservation = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  repository: Schema.Struct({
    host: Host,
    name: WorkbenchMergePlan.fields.repository,
    id: ProviderId,
  }),
  actor: Schema.Struct({
    id: ProviderId,
    login: Text.check(
      Schema.isMaxLength(100),
      Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9-]{0,38}(?:\[bot\])?$/),
    ),
  }),
  ref: Ref,
  main: Schema.NullOr(Schema.Struct({ sha: Sha, treeSha: Sha })),
  candidate: Schema.NullOr(Schema.Struct({ sha: Sha, treeSha: Sha, baseAncestor: Schema.Boolean })),
  requiredResults: Schema.Array(WorkbenchMergeRequiredResult).check(Schema.isMaxLength(20)),
  protection: Schema.Struct({
    state: Schema.Literals(["supported", "unsupported", "unavailable"]),
    policyDigest: Schema.NullOr(Hash),
    reasons: Schema.Array(WorkbenchMergeRemoteReason).check(Schema.isMaxLength(14)),
  }),
  observedAt: Iso,
});
export type WorkbenchMergeObservation = typeof WorkbenchMergeObservation.Type;
export const WorkbenchMergeEvidence = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  planDigest: Hash,
  preflight: WorkbenchMergeObservation,
});
export type WorkbenchMergeEvidence = typeof WorkbenchMergeEvidence.Type;
export const WorkbenchMergeReason = Schema.Literals([
  "source_changed",
  "profile_changed",
  "work_changed",
  "main_changed",
  "candidate_unavailable",
  "candidate_changed",
  "actor_changed",
  "policy_changed",
  "unsupported",
  "checks_pending",
  "checks_missing",
  "checks_failed",
  "health_failed",
  "core_failed",
  "check_unknown",
  "target_changed",
  "target_unknown",
  "coordinator_unavailable",
  "coordinator_forbidden",
  "lock_conflict",
  "merge_rejected",
  "merge_unknown",
  "result_mismatch",
  "completion_changed",
  "state_unknown",
  "verification_limit",
]);
export type WorkbenchMergeReason = typeof WorkbenchMergeReason.Type;
export const WorkbenchMergeKnownFailureReason = Schema.Literals([
  "source_changed",
  "profile_changed",
  "work_changed",
  "main_changed",
  "candidate_unavailable",
  "candidate_changed",
  "actor_changed",
  "policy_changed",
  "unsupported",
  "checks_failed",
  "health_failed",
  "core_failed",
  "coordinator_forbidden",
  "merge_rejected",
  "result_mismatch",
  "completion_changed",
  "target_changed",
]);
export type WorkbenchMergeKnownFailureReason = typeof WorkbenchMergeKnownFailureReason.Type;
export const WorkbenchMergeInput = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  deploymentId: Id,
  authorizationDigest: Hash,
  acceptanceDigest: Hash,
  plan: WorkbenchMergePlan,
  subject: WorkbenchMergeSubject,
  lock: Schema.Struct({ epoch: Positive, nonce: Hash }),
  deployOperationId: Id,
  activationRevision: Count,
});
export type WorkbenchMergeInput = typeof WorkbenchMergeInput.Type;
export const WorkbenchMergeClaimInput = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  mergeId: Id,
  input: WorkbenchMergeInput,
  inputDigest: Hash,
});
export type WorkbenchMergeClaimInput = typeof WorkbenchMergeClaimInput.Type;
export const WorkbenchMergePermit = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  kind: Schema.Literal("merge_commit"),
  keyId: Hash,
  authorizationDigest: Hash,
  payload: WorkbenchMergeClaimInput,
  committedAt: Count,
  signature: Schema.String.check(Schema.isPattern(/^[A-Za-z0-9_-]{86}$/)),
});
export type WorkbenchMergePermit = typeof WorkbenchMergePermit.Type;
export const WorkbenchDeliveryReceipt = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  id: Id,
  mergeId: Id,
  mergeDigest: Hash,
  authorizationDigest: Hash,
  workItemVersionBefore: Count,
  workItemVersionAfter: Positive,
  latestEventSequence: Count,
  processedThroughSequence: Count,
  handoffRef: Id,
  preservedRef: Text.check(
    Schema.isPattern(/^refs\/t3\/workbench-handoffs\/[A-Za-z0-9._/-]+$/),
    Schema.makeFilter((value) => !value.includes("..")),
  ),
  completedAt: Iso,
  digest: Hash,
});
export type WorkbenchDeliveryReceipt = typeof WorkbenchDeliveryReceipt.Type;
export const workbenchMergeSummary = <R extends Schema.Top>(receipt: R) =>
  Schema.Struct({
    schemaVersion: Schema.Literal(1),
    id: Id,
    input: WorkbenchMergeInput,
    inputDigest: Hash,
    version: Positive,
    state: Schema.Literals([
      "pending",
      "checking",
      "awaiting_claim",
      "merging",
      "verifying",
      "completed",
      "failed",
      "unknown",
    ]),
    reasons: Schema.Array(WorkbenchMergeReason).check(Schema.isMaxLength(26)),
    claim: Schema.Struct({
      state: Schema.Literals(["none", "committed"]),
      digest: Schema.NullOr(Hash),
    }),
    freshReceipts: Schema.Array(receipt).check(Schema.isMaxLength(2)),
    observation: Schema.NullOr(WorkbenchMergeObservation),
    mutation: Schema.Struct({
      state: Schema.Literals(["not_sent", "intent", "acknowledged", "rejected", "unknown"]),
      ackDigest: Schema.NullOr(Hash),
    }),
    result: Schema.NullOr(
      Schema.Struct({
        kind: Schema.Literals(["acknowledged", "observed_matching"]),
        commitSha: Sha,
        treeSha: Sha,
        ackDigest: Schema.NullOr(Hash),
        observation: WorkbenchMergeObservation,
        observedAt: Iso,
        digest: Hash,
      }),
    ),
    verification: Schema.NullOr(
      Schema.Struct({
        attempt: Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 128 })),
        runnerEpoch: Id,
        state: Schema.Literals(["checking", "succeeded", "failed", "unknown"]),
        startedAt: Iso,
        completedAt: Schema.NullOr(Iso),
        digest: Schema.NullOr(Hash),
      }),
    ),
    failure: Schema.NullOr(
      Schema.Struct({
        id: Id,
        mergeInputDigest: Hash,
        reason: WorkbenchMergeKnownFailureReason,
        stage: Schema.Literals(["eligibility", "mutation", "finalization"]),
        basis: Schema.Struct({
          kind: Schema.Literals([
            "online_receipt",
            "remote_observation",
            "local_fence",
            "mutation_rejection",
          ]),
          ref: Schema.NullOr(Id),
          digest: Hash,
        }),
        occurredAt: Iso,
        digest: Hash,
      }),
    ),
    recovery: Schema.NullOr(
      Schema.Struct({
        schemaVersion: Schema.Literal(1),
        kind: Schema.Literal("merge_failure"),
        id: Id,
        reason: WorkbenchMergeKnownFailureReason,
        mergeInputDigest: Hash,
        failureDigest: Hash,
        occurredAt: Iso,
      }),
    ),
    createdAt: Iso,
    updatedAt: Iso,
  });
