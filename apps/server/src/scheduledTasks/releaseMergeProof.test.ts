import { describe, expect, it } from "@effect/vitest";
import type { WorkbenchMergeObservation, WorkbenchMergePlan } from "@t3tools/contracts";
import { mergeObservationReason } from "./releaseMergeProof.ts";
import { releaseDigest } from "./workbenchReleaseProfile.ts";

const plan: WorkbenchMergePlan = {
  schemaVersion: 1,
  kind: "github_exact_ff",
  host: "github.com",
  repository: "example/project",
  repositoryId: "repository=",
  ref: "refs/heads/main",
  requiredChecks: [{ context: "CI", appId: 1 }],
};
const subject = {
  baseSha: "a".repeat(40),
  sourceSha: "c".repeat(40),
  sourceTreeSha: "d".repeat(40),
};
const check = {
  context: "CI",
  appId: 1,
  headSha: subject.sourceSha,
  checkId: "check=",
  state: "passed" as const,
  conclusion: "success" as const,
};
const pin: WorkbenchMergeObservation = {
  schemaVersion: 1,
  repository: { host: plan.host, name: plan.repository, id: plan.repositoryId },
  actor: { id: "actor=", login: "release" },
  ref: plan.ref,
  main: { sha: subject.baseSha, treeSha: "a".repeat(40) },
  candidate: { sha: subject.sourceSha, treeSha: subject.sourceTreeSha, baseAncestor: true },
  requiredResults: [{ ...check, receiptDigest: releaseDigest(check) }],
  protection: { state: "supported", policyDigest: "1".repeat(64), reasons: [] },
  observedAt: "2026-11-27T00:00:00.000Z",
};

describe("merge protection evidence", () => {
  it.each([
    ["unavailable", null, "unavailable"],
    ["unsupported", null, "unsupported"],
    ["supported", null, "invalid_response"],
    ["supported", "2".repeat(64), "policy_changed"],
    ["supported", pin.protection.policyDigest, null],
  ] as const)(
    "classifies %s rules without inventing a policy change",
    (state, policyDigest, expected) => {
      expect(
        mergeObservationReason(
          plan,
          subject,
          { ...pin, protection: { state, policyDigest, reasons: [] } },
          pin,
        ),
      ).toBe(expected);
    },
  );
});
