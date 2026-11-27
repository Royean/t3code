import type {
  WorkbenchMergePlan,
  WorkbenchMergeSubject,
  WorkbenchMergeObservation,
  WorkbenchMergeRemoteReason,
} from "@t3tools/contracts";
import { canonicalReleaseJson, releaseDigest } from "./workbenchReleaseProfile.ts";

export const sameMergeValue = (a: unknown, b: unknown) =>
  canonicalReleaseJson(a) === canonicalReleaseJson(b);
export function mergeObservationReason(
  plan: WorkbenchMergePlan,
  subject: WorkbenchMergeSubject,
  observed: WorkbenchMergeObservation,
  pinned?: WorkbenchMergeObservation,
  after = false,
): WorkbenchMergeRemoteReason | null {
  if (
    observed.repository.host !== plan.host ||
    observed.repository.name.toLowerCase() !== plan.repository.toLowerCase() ||
    observed.repository.id !== plan.repositoryId ||
    observed.ref !== plan.ref
  )
    return "identity_mismatch";
  if (pinned && observed.actor.id !== pinned.actor.id) return "actor_changed";
  if (pinned && observed.protection.policyDigest !== pinned.protection.policyDigest)
    return "policy_changed";
  if (observed.protection.state !== "supported" || !observed.protection.policyDigest)
    return observed.protection.state === "unavailable" ? "unavailable" : "unsupported";
  if (!observed.candidate) return "candidate_unavailable";
  if (
    observed.candidate.sha !== subject.sourceSha ||
    observed.candidate.treeSha !== subject.sourceTreeSha ||
    !observed.candidate.baseAncestor
  )
    return "candidate_changed";
  if (
    observed.main?.sha !== (after ? subject.sourceSha : subject.baseSha) ||
    (after && observed.main.treeSha !== subject.sourceTreeSha)
  )
    return "main_changed";
  if (observed.requiredResults.length !== plan.requiredChecks.length) return "checks_missing";
  for (const required of plan.requiredChecks) {
    const found = observed.requiredResults.filter(
      (result) => result.context === required.context && result.appId === required.appId,
    );
    if (found.length !== 1) return "checks_missing";
    const result = found[0]!,
      { receiptDigest, ...value } = result;
    if (result.headSha !== subject.sourceSha || releaseDigest(value) !== receiptDigest)
      return "invalid_response";
    if (result.state === "missing") return "checks_missing";
    if (result.state === "pending") return "checks_pending";
    if (
      result.state === "failed" &&
      result.checkId &&
      result.conclusion &&
      result.conclusion !== "success"
    )
      return "checks_failed";
    if (result.state !== "passed" || !result.checkId || result.conclusion !== "success")
      return "invalid_response";
  }
  return null;
}
