import {
  WorkbenchDeployment as DeploymentSchema,
  WorkbenchDeploymentAdmitInput,
  WorkbenchReleaseHostProfile,
  WorkbenchMergeKnownFailureReason,
  type WorkbenchDeployment,
  type WorkbenchMergeSummary,
  type WorkbenchMergeObservation,
  type WorkbenchMergeReason,
  WorkbenchMergePermit,
  type WorkbenchDeliveryReceipt,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Random from "effect/Random";
import * as Option from "effect/Option";
import * as SqlClient from "effect/sql/SqlClient";
import type * as SqlError from "effect/sql/SqlError";
import * as Evidence from "./WorkbenchReleaseEvidence.ts";
import * as Adapter from "./WorkbenchReleaseAdapter.ts";
import * as Refs from "./WorkbenchReleaseRefs.ts";
import { completeAcceptedDelivery } from "./WorkbenchWorkItems.ts";
import {
  auditIso,
  clockPair,
  logicalReleaseIso,
  VerificationError,
} from "./releaseVerification.ts";
import { canonicalReleaseJson, releaseDigest, releaseHash } from "./workbenchReleaseProfile.ts";
import { mergeObservationReason, sameMergeValue as same } from "./releaseMergeProof.ts";

type Merge = WorkbenchMergeSummary;
type Runtime = {
  permit_json: string | null;
  mutation_intent: number;
  mutation_outcome_json: string | null;
  verification_intent_json: string | null;
};
class MergeStop extends Schema.TaggedError<MergeStop>()("ReleaseMergeStop", {
  reason: Schema.String,
  known: Schema.Boolean,
  basis: Schema.Unknown,
  kind: Schema.Literals([
    "online_receipt",
    "remote_observation",
    "local_fence",
    "mutation_rejection",
  ]),
  ref: Schema.NullOr(Schema.String),
  occurredAt: Schema.NullOr(Schema.String),
  cause: Schema.optional(Schema.Defect()),
}) {
  override get message() {
    return "Accepted merge was blocked.";
  }
}
const isStop = Schema.is(MergeStop),
  isKnown = Schema.is(WorkbenchMergeKnownFailureReason);
const decodeD = Schema.decodeEffect(Schema.fromJsonString(DeploymentSchema)),
  decodeI = Schema.decodeEffect(Schema.fromJsonString(WorkbenchDeploymentAdmitInput)),
  decodeP = Schema.decodeEffect(Schema.fromJsonString(WorkbenchReleaseHostProfile));
const decodeJson = Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown)),
  decodePermit = Schema.decodeEffect(Schema.fromJsonString(WorkbenchMergePermit));
const decodeIntent = Schema.decodeEffect(
  Schema.fromJsonString(
    Schema.Struct({
      attempt: Schema.Int,
      runnerEpoch: Schema.String,
      healthId: Schema.String,
      coreId: Schema.String,
    }),
  ),
);
const opaque = (kind: string, value: string) => `${kind}:${releaseHash(value)}`;
const request = (m: Merge) => ({
  schemaVersion: 1 as const,
  mergeId: m.id,
  input: m.input,
  inputDigest: m.inputDigest,
});
const terminalVerification = (
  m: Merge,
  state: "succeeded" | "failed" | "unknown",
  completedAt: string,
) => {
  const { digest: _digest, ...metadata } = { ...m.verification!, state, completedAt };
  return {
    ...metadata,
    digest: releaseDigest({
      mergeId: m.id,
      mergeInputDigest: m.inputDigest,
      verification: metadata,
      freshReceipts: m.freshReceipts,
    }),
  };
};
const stop = (
  reason: WorkbenchMergeReason,
  known: boolean,
  basis: unknown,
  kind: MergeStop["kind"] = "local_fence",
  ref: string | null = null,
  occurredAt: string | null = null,
  cause?: unknown,
) =>
  new MergeStop({
    reason,
    known,
    basis,
    kind,
    ref,
    occurredAt,
    ...(cause === undefined ? {} : { cause }),
  });
const remoteReason = (reason: string): WorkbenchMergeReason =>
  reason === "unavailable" || reason === "invalid_response"
    ? "state_unknown"
    : reason === "identity_mismatch"
      ? "candidate_changed"
      : reason === "protected_ref_rejected"
        ? "merge_rejected"
        : reason === "mutation_unknown"
          ? "merge_unknown"
          : isKnown(reason) || ["checks_pending", "checks_missing"].includes(reason)
            ? (reason as WorkbenchMergeReason)
            : "state_unknown";

export const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient,
    evidence = yield* Evidence.WorkbenchReleaseEvidence,
    adapter = yield* Adapter.WorkbenchReleaseAdapter,
    refs = yield* Effect.serviceOption(Refs.WorkbenchReleaseRefs);
  const epoch = opaque(
    "merge-runner",
    `${yield* Random.nextInt}:${yield* Random.nextInt}:${yield* Random.nextInt}:${yield* Random.nextInt}`,
  );
  const read = Effect.fn("ReleaseMerge.read")(function* (id: string) {
    const [row] = yield* sql<{
      deployment_json: string;
      profile_json: string;
      input_json: string;
    }>`SELECT deployment_json,profile_json,input_json FROM workbench_deployments WHERE id=${id}`;
    if (!row) return yield* stop("state_unknown", false, { stage: "missing_job" });
    return {
      d: yield* decodeD(row.deployment_json),
      profile: yield* decodeP(row.profile_json),
      input: yield* decodeI(row.input_json),
    };
  });
  const runtime = Effect.fn("ReleaseMerge.runtime")(function* (id: string) {
    yield* sql`INSERT INTO workbench_merge_runtime(deployment_id) VALUES(${id}) ON CONFLICT(deployment_id) DO NOTHING`;
    return (yield* sql<Runtime>`SELECT * FROM workbench_merge_runtime WHERE deployment_id=${id}`)[0]!;
  });
  const persist = Effect.fn("ReleaseMerge.persist")(function* (
    id: string,
    m: Merge,
    patch: Partial<WorkbenchDeployment> = {},
    write?: Effect.Effect<unknown, SqlError.SqlError>,
  ) {
    const { d } = yield* read(id),
      p = yield* clockPair;
    const next = {
      ...d,
      ...patch,
      merge: {
        ...m,
        version: (d.merge?.version ?? 0) + 1,
        updatedAt: logicalReleaseIso(d.merge?.updatedAt ?? d.updatedAt, p.wall),
      },
      version: d.version + 1,
      updatedAt: logicalReleaseIso(d.updatedAt, p.wall),
    };
    yield* sql.withTransaction(
      Effect.gen(function* () {
        if (write) yield* write;
        const changed =
          yield* sql`UPDATE workbench_deployments SET deployment_json=${canonicalReleaseJson(next)},state=${next.state},updated_at=${next.updatedAt} WHERE id=${id} AND deployment_json=${canonicalReleaseJson(d)} RETURNING id`;
        if (changed.length !== 1) return yield* stop("state_unknown", false, { stage: "job_cas" });
      }),
    );
    return next;
  });
  const scope = (d: WorkbenchDeployment) => ({
    controllerId: d.binding.controllerId,
    projectId: d.binding.projectId,
    expectedTrackingId: d.binding.trackingId,
    expectedEvidenceDigest: d.evidenceDigest,
  });
  const material = Effect.fn("ReleaseMerge.material")(function* (
    d: WorkbenchDeployment,
    phase: "admission" | "finalization" | "recovery",
  ) {
    return yield* evidence
      .mergeMaterial(d.binding.queueId, d.preparationId, scope(d), phase)
      .pipe(
        Effect.mapError((cause) =>
          stop(
            remoteReason(cause.reason),
            cause.known,
            { stage: "material", phase, reason: cause.reason, expected: d.binding },
            "local_fence",
            null,
            null,
            cause,
          ),
        ),
      );
  });
  const observation = Effect.fn("ReleaseMerge.observation")(function* (
    d: WorkbenchDeployment,
    cwd: string,
    after = false,
  ) {
    if (Option.isNone(refs)) return yield* stop("unsupported", true, { stage: "capability" });
    const frozen = yield* material(d, "recovery"),
      m = d.merge!,
      pin = frozen.evidence.merge!.preflight;
    const value = yield* refs.value
      .inspect({
        cwd,
        plan: m.input.plan,
        subject: m.input.subject,
        expectedActorId: pin.actor.id,
        expectedPolicyDigest: pin.protection.policyDigest!,
      })
      .pipe(
        Effect.mapError((cause) =>
          stop(
            remoteReason(cause.reason),
            false,
            { stage: "remote", reason: cause.reason },
            "local_fence",
            null,
            null,
            cause,
          ),
        ),
      );
    const reason = mergeObservationReason(m.input.plan, m.input.subject, value, pin, after);
    if (reason) {
      const mapped = remoteReason(reason);
      return yield* stop(
        mapped,
        isKnown(mapped),
        value,
        "remote_observation",
        null,
        value.observedAt,
      );
    }
    return value;
  });
  const owns = (d: WorkbenchDeployment, t: NonNullable<WorkbenchDeployment["target"]>) =>
    t.state === "consistent" &&
    same(t.basis, d.desired) &&
    t.revision === d.acceptance!.activationRevision &&
    (d.deployOperation?.result?.effect === "unchanged"
      ? same(d.desired, d.baseline.basis)
      : t.lastOperationId === d.deployOperation?.id);
  const currentTarget = Effect.fn("ReleaseMerge.currentTarget")(function* (
    d: WorkbenchDeployment,
    profile: WorkbenchReleaseHostProfile,
    independent: boolean,
  ) {
    if (!independent) {
      const t = yield* adapter
        .readTarget(profile)
        .pipe(
          Effect.mapError((cause) =>
            stop("target_unknown", false, { stage: "target" }, "local_fence", null, null, cause),
          ),
        );
      if (!owns(d, t)) return yield* stop("target_changed", true, t);
      return { target: t };
    }
    const observed = yield* adapter
      .observe(profile)
      .pipe(
        Effect.mapError((cause) =>
          stop("target_unknown", false, { stage: "version" }, "local_fence", null, null, cause),
        ),
      );
    if (!owns(d, observed.target) || !same(observed.versionObservation.basis, d.desired))
      return yield* stop("target_changed", true, observed);
    return observed;
  });
  const fail = Effect.fn("ReleaseMerge.fail")(function* (id: string, error: MergeStop) {
    const { d } = yield* read(id),
      m = d.merge;
    if (!m || m.failure || d.state === "delivered") return;
    const uncertain =
      m.mutation.state !== "not_sent" && m.mutation.state !== "rejected" && !m.result;
    if (!error.known || !isKnown(error.reason) || uncertain) {
      let next = {
        ...m,
        state: "unknown" as const,
        reasons: [uncertain ? ("merge_unknown" as const) : remoteReason(error.reason)],
      };
      if (next.verification?.state === "checking")
        next = {
          ...next,
          verification: terminalVerification(next, "unknown", auditIso((yield* clockPair).wall)),
        };
      yield* persist(id, next);
      return;
    }
    const p = yield* clockPair,
      occurredAt = error.occurredAt ?? auditIso(p.wall),
      auditId = opaque("merge-audit", `${m.id}:${m.version}:${error.reason}`),
      basisDigest = releaseDigest(error.basis);
    const unsigned = {
      id: opaque("merge-failure", auditId),
      mergeInputDigest: m.inputDigest,
      reason: error.reason,
      stage: m.result
        ? ("finalization" as const)
        : m.mutation.state === "rejected"
          ? ("mutation" as const)
          : ("eligibility" as const),
      basis: {
        kind: error.kind,
        ref:
          error.ref ??
          (error.kind === "local_fence" || error.kind === "mutation_rejection" ? auditId : null),
        digest: basisDigest,
      },
      occurredAt,
    };
    const failure = { ...unsigned, digest: releaseDigest(unsigned) },
      recovery = {
        schemaVersion: 1 as const,
        kind: "merge_failure" as const,
        id: opaque("merge-recovery", failure.id),
        reason: failure.reason,
        mergeInputDigest: m.inputDigest,
        failureDigest: failure.digest,
        occurredAt,
      };
    yield* persist(
      id,
      {
        ...m,
        state: "failed",
        reasons: [failure.reason],
        failure,
        recovery,
        ...(error.kind === "remote_observation"
          ? { observation: error.basis as WorkbenchMergeObservation }
          : {}),
      },
      {},
      sql`INSERT INTO workbench_merge_audits(id,deployment_id,kind,digest,body_json,created_at) VALUES(${auditId},${id},${error.kind},${basisDigest},${canonicalReleaseJson(error.basis)},${occurredAt})`,
    );
  });
  const failureRequested = Effect.fn("ReleaseMerge.failureRequested")(function* (
    d: WorkbenchDeployment,
  ) {
    const m = d.merge,
      f = m?.failure,
      r = m?.recovery;
    if (
      !m ||
      m.state !== "failed" ||
      !f ||
      !r ||
      r.failureDigest !== f.digest ||
      r.mergeInputDigest !== m.inputDigest ||
      m.input.acceptanceDigest !== releaseDigest(d.acceptance)
    )
      return false;
    const { digest, ...unsigned } = f;
    if (releaseDigest(unsigned) !== digest) return false;
    const [audit] = yield* sql<{
      body_json: string;
      digest: string;
    }>`SELECT body_json,digest FROM workbench_merge_audits WHERE deployment_id=${d.id} AND digest=${f.basis.digest} LIMIT 1`;
    return !!audit && audit.digest === releaseDigest(yield* decodeJson(audit.body_json));
  });
  const execute = Effect.fn("ReleaseMerge.execute")(function* (id: string) {
    let { d, profile, input } = yield* read(id);
    if (d.state === "delivered" || d.acceptance?.state !== "passed" || d.rollbackOperation) return;
    const frozen = yield* material(d, "recovery");
    if (!profile.policy.merge || !frozen.evidence.merge) return;
    let m = d.merge;
    if (!m) {
      const p = yield* clockPair,
        mergeInput = {
          schemaVersion: 1 as const,
          deploymentId: d.id,
          authorizationDigest: d.authorizationDigest,
          acceptanceDigest: releaseDigest(d.acceptance),
          plan: profile.policy.merge,
          subject: {
            baseSha: frozen.evidence.source.main.commitSha,
            sourceSha: frozen.evidence.source.commitSha,
            sourceTreeSha: frozen.evidence.source.treeSha,
          },
          lock: d.lock,
          deployOperationId: d.deployOperation!.id,
          activationRevision: d.acceptance.activationRevision,
        };
      m = {
        schemaVersion: 1,
        id: opaque("merge", `${id}:${d.authorizationDigest}`),
        input: mergeInput,
        inputDigest: releaseDigest(mergeInput),
        version: 1,
        state: "pending",
        reasons: [],
        claim: { state: "none", digest: null },
        freshReceipts: [],
        observation: null,
        mutation: { state: "not_sent", ackDigest: null },
        result: null,
        recovery: null,
        verification: null,
        failure: null,
        createdAt: logicalReleaseIso(d.updatedAt, p.wall),
        updatedAt: logicalReleaseIso(d.updatedAt, p.wall),
      };
      d = yield* persist(id, m);
      m = d.merge!;
    }
    if (m.failure) return;
    const r = yield* runtime(id);
    if (m.result) {
      // A known result survives restart, including the B==C no-write case.
    } else if (m.mutation.state !== "not_sent") {
      if (!m.result) {
        const observed = yield* observation(d, frozen.cwd, true);
        const unsigned = {
          kind: "observed_matching" as const,
          commitSha: m.input.subject.sourceSha,
          treeSha: m.input.subject.sourceTreeSha,
          ackDigest: null,
          observation: observed,
          observedAt: observed.observedAt,
        };
        d = yield* persist(id, {
          ...m,
          state: "verifying",
          reasons: [],
          observation: observed,
          result: { ...unsigned, digest: releaseDigest(unsigned) },
        });
        m = d.merge!;
      }
    } else {
      const durableFailed = m.freshReceipts.find((receipt) => receipt.state === "failed");
      if (durableFailed)
        return yield* stop(
          durableFailed.role === "health" ? "health_failed" : "core_failed",
          true,
          durableFailed,
          "online_receipt",
          durableFailed.id,
          durableFailed.completedAt,
        );
      const before = yield* material(d, "admission");
      let observed = yield* observation(d, before.cwd);
      yield* currentTarget(d, profile, false);
      const privateIntent = r.verification_intent_json
        ? yield* decodeIntent(r.verification_intent_json)
        : null;
      const attempt = Math.max(m.verification?.attempt ?? 0, privateIntent?.attempt ?? 0) + 1;
      if (attempt > 128) return yield* stop("verification_limit", false, { stage: "budget" });
      const healthId = opaque("merge-execution", `${m.id}:${epoch}:${attempt}:health`),
        coreId = opaque("merge-execution", `${m.id}:${epoch}:${attempt}:core`);
      yield* sql.withTransaction(
        Effect.gen(function* () {
          yield* sql`UPDATE workbench_merge_executions SET state='unknown' WHERE deployment_id=${id} AND state='running'`;
          yield* sql`UPDATE workbench_merge_runtime SET verification_intent_json=${canonicalReleaseJson({ attempt, runnerEpoch: epoch, healthId, coreId })} WHERE deployment_id=${id}`;
          yield* sql`INSERT INTO workbench_merge_executions(execution_id,deployment_id,attempt,role,state,input_json) VALUES(${healthId},${id},${attempt},'health','running',${canonicalReleaseJson({ mergeId: m!.id, inputDigest: m!.inputDigest })})`;
        }),
      );
      for (const role of ["health", "core"] as const) {
        const executionId = role === "health" ? healthId : coreId;
        if (role === "core")
          yield* sql`INSERT INTO workbench_merge_executions(execution_id,deployment_id,attempt,role,state,input_json) VALUES(${coreId},${id},${attempt},'core','running',${canonicalReleaseJson({ mergeId: m.id, inputDigest: m.inputDigest })})`;
        const receipt = yield* evidence
          .runOnlineCheck(d.binding.queueId, d.preparationId, {
            ...scope(d),
            sampleId: m.id,
            executionId,
            role,
            authorizationDigest: d.authorizationDigest,
            runnerEpoch: epoch,
            candidate: d.desired,
            activationRevision: m.input.activationRevision,
            ...(role === "health"
              ? {
                  onStarted: Effect.fn("ReleaseMerge.beginVerification")(
                    function* (p: { wall: number; mono: bigint }) {
                      d = yield* persist(id, {
                        ...m!,
                        state: "checking",
                        reasons: [],
                        freshReceipts: [],
                        verification: {
                          attempt,
                          runnerEpoch: epoch,
                          state: "checking",
                          startedAt: auditIso(p.wall),
                          completedAt: null,
                          digest: null,
                        },
                      });
                      m = d.merge!;
                    },
                    Effect.mapError(
                      (cause) =>
                        new VerificationError({
                          reason: "material_invalid",
                          stage: "invocation_intent",
                          cause,
                        }),
                    ),
                  ),
                }
              : {}),
          })
          .pipe(
            Effect.mapError((cause) =>
              stop(
                "check_unknown",
                false,
                { stage: "execution", role },
                "local_fence",
                null,
                null,
                cause,
              ),
            ),
          );
        m = { ...m, freshReceipts: [...m.freshReceipts, receipt] };
        const completed = auditIso((yield* clockPair).wall);
        if (receipt.state !== "succeeded")
          m = {
            ...m,
            verification: terminalVerification(
              m,
              receipt.state === "failed" ? "failed" : "unknown",
              completed,
            ),
            state: "unknown",
            reasons: ["check_unknown"],
          };
        else if (role === "core") {
          const [health, core] = m.freshReceipts;
          const consistent =
            [health!, core!].every(
              (item) =>
                item.completedAt !== null &&
                item.durationMilliseconds !== null &&
                Date.parse(item.completedAt) - Date.parse(item.startedAt) >= 0 &&
                Math.abs(
                  Date.parse(item.completedAt) -
                    Date.parse(item.startedAt) -
                    item.durationMilliseconds,
                ) <= 2,
            ) &&
            Date.parse(health!.completedAt!) <= Date.parse(core!.startedAt) &&
            Date.parse(core!.completedAt!) <= Date.parse(completed);
          m = {
            ...m,
            state: "unknown",
            verification: terminalVerification(m, consistent ? "succeeded" : "unknown", completed),
            reasons: consistent ? [] : ["check_unknown"],
          };
        }
        d = yield* persist(
          id,
          m,
          {},
          sql`UPDATE workbench_merge_executions SET state=${receipt.state},receipt_json=${canonicalReleaseJson(receipt)} WHERE execution_id=${executionId}`,
        );
        m = d.merge!;
        if (receipt.state === "failed")
          return yield* stop(
            role === "health" ? "health_failed" : "core_failed",
            true,
            receipt,
            "online_receipt",
            receipt.id,
            receipt.completedAt,
          );
        if (receipt.state !== "succeeded" || m.verification?.state === "unknown")
          return yield* stop("check_unknown", false, { stage: "receipt" });
      }
      const live = yield* currentTarget(d, profile, true);
      yield* material(d, "admission");
      observed = yield* observation(d, before.cwd);
      d = yield* persist(
        id,
        { ...m, state: "awaiting_claim", reasons: [], observation: observed },
        { ...live, state: "deployed" },
      );
      m = d.merge!;
      let permit: WorkbenchMergePermit;
      if (r.permit_json) {
        permit = yield* decodePermit(r.permit_json);
        if (!Adapter.verifyMergePermit(permit, profile, request(m)))
          return yield* stop("state_unknown", false, { stage: "stored_permit" });
      } else {
        permit = yield* adapter
          .claimMerge(profile, input.reservation, input.claimToken, request(m))
          .pipe(
            Effect.mapError((cause) =>
              stop(
                cause.reason === "coordinator_forbidden"
                  ? "coordinator_forbidden"
                  : "coordinator_unavailable",
                cause.reason === "coordinator_forbidden",
                { stage: "merge_claim", reason: cause.reason },
                "local_fence",
                null,
                null,
                cause,
              ),
            ),
          );
        d = yield* persist(
          id,
          { ...m, claim: { state: "committed", digest: releaseDigest(permit) } },
          {},
          sql`UPDATE workbench_merge_runtime SET permit_json=${canonicalReleaseJson(permit)} WHERE deployment_id=${id}`,
        );
        m = d.merge!;
      }
      yield* material(d, "admission");
      observed = yield* observation(d, before.cwd);
      const finalTarget = yield* currentTarget(d, profile, true);
      d = yield* persist(
        id,
        { ...m, state: "merging", reasons: [], observation: observed },
        { ...finalTarget, state: "deployed" },
      );
      m = d.merge!;
      if (Option.isNone(refs)) return yield* stop("merge_unknown", false, { stage: "capability" });
      let callbackFailure: MergeStop | undefined;
      const beforeMutation = Effect.gen(function* () {
        yield* material(d, "admission");
        const latestTarget = yield* currentTarget(d, profile, true);
        d = yield* persist(
          id,
          { ...m!, state: "merging", mutation: { state: "intent", ackDigest: null } },
          { ...latestTarget, state: "deployed" },
          sql`UPDATE workbench_merge_runtime SET mutation_intent=1 WHERE deployment_id=${id}`,
        );
        m = d.merge!;
      }).pipe(
        Effect.mapError((cause) => {
          callbackFailure = isStop(cause)
            ? cause
            : stop(
                "state_unknown",
                false,
                { stage: "before_mutation" },
                "local_fence",
                null,
                null,
                cause,
              );
          return new Refs.ReleaseRefsError({
            reason: "unavailable",
            stage: "before_mutation",
            cause,
          });
        }),
      );
      const attemptResult = yield* refs.value
        .exactFF({
          cwd: before.cwd,
          plan: m.input.plan,
          subject: m.input.subject,
          expectedActorId: frozen.evidence.merge.preflight.actor.id,
          expectedPolicyDigest: frozen.evidence.merge.preflight.protection.policyDigest!,
          operationId: m.id,
          beforeMutation,
        })
        .pipe(
          Effect.mapError(
            (cause) =>
              callbackFailure ??
              stop(
                "merge_unknown",
                false,
                { stage: "mutation", reason: cause.reason },
                "local_fence",
                null,
                null,
                cause,
              ),
          ),
        );
      const ack =
        attemptResult.ack?.clientMutationId === m.id
          ? releaseDigest({
              operationId: m.id,
              repositoryId: m.input.plan.repositoryId,
              ref: m.input.plan.ref,
              baseSha: m.input.subject.baseSha,
              sourceSha: m.input.subject.sourceSha,
              ack: attemptResult.ack,
            })
          : null;
      const rejected = attemptResult.kind === "rejected";
      if (rejected && !attemptResult.sent && m.mutation.state === "not_sent") {
        yield* sql`UPDATE workbench_merge_runtime SET mutation_outcome_json=${canonicalReleaseJson(attemptResult)} WHERE deployment_id=${id}`;
        const reason = remoteReason(attemptResult.reason ?? "invalid_response");
        return yield* stop(
          reason,
          isKnown(reason),
          attemptResult.observation ?? attemptResult,
          attemptResult.observation ? "remote_observation" : "local_fence",
          null,
          attemptResult.observation?.observedAt ?? null,
        );
      }
      d = yield* persist(
        id,
        {
          ...m,
          state: "unknown",
          mutation: {
            state: rejected
              ? "rejected"
              : ack
                ? "acknowledged"
                : !attemptResult.sent && m.mutation.state === "not_sent"
                  ? "not_sent"
                  : "unknown",
            ackDigest: ack,
          },
          reasons: ["merge_unknown"],
        },
        {},
        sql`UPDATE workbench_merge_runtime SET mutation_outcome_json=${canonicalReleaseJson(attemptResult)} WHERE deployment_id=${id}`,
      );
      m = d.merge!;
      if (rejected) return yield* stop("merge_rejected", true, attemptResult, "mutation_rejection");
      if (!attemptResult.observation)
        return yield* stop("merge_unknown", false, { stage: "mutation_outcome" });
      const reason = mergeObservationReason(
        m.input.plan,
        m.input.subject,
        attemptResult.observation,
        frozen.evidence.merge.preflight,
        true,
      );
      if (reason) return yield* stop("merge_unknown", false, { stage: "readback", reason });
      const unsigned = {
        kind: ack ? ("acknowledged" as const) : ("observed_matching" as const),
        commitSha: m.input.subject.sourceSha,
        treeSha: m.input.subject.sourceTreeSha,
        ackDigest: ack,
        observation: attemptResult.observation,
        observedAt: attemptResult.observation.observedAt,
      };
      d = yield* persist(id, {
        ...m,
        state: "verifying",
        reasons: [],
        observation: attemptResult.observation,
        result: { ...unsigned, digest: releaseDigest(unsigned) },
      });
      m = d.merge!;
    }
    yield* material(d, "finalization");
    const observed = yield* observation(d, frozen.cwd, true),
      live = yield* currentTarget(d, profile, true);
    const preservedRef = `refs/t3/workbench-handoffs/${releaseHash(m.id)}`;
    yield* evidence
      .preserveMergeSource(d.binding.queueId, d.preparationId, scope(d), preservedRef)
      .pipe(
        Effect.mapError((cause) =>
          stop(
            "completion_changed",
            cause.known,
            { stage: "preservation", reason: cause.reason },
            "local_fence",
            null,
            null,
            cause,
          ),
        ),
      );
    yield* material(d, "finalization");
    const pair = yield* clockPair,
      updated = logicalReleaseIso(m.updatedAt, pair.wall),
      completedMerge: Merge = {
        ...m,
        state: "completed",
        reasons: [],
        observation: observed,
        version: m.version + 1,
        updatedAt: updated,
      };
    const unsigned = {
      schemaVersion: 1 as const,
      id: opaque("delivery", m.id),
      mergeId: m.id,
      mergeDigest: releaseDigest(completedMerge),
      authorizationDigest: d.authorizationDigest,
      workItemVersionBefore: d.binding.workItemVersion,
      workItemVersionAfter: d.binding.workItemVersion + 1,
      latestEventSequence: d.binding.latestEventSequence,
      processedThroughSequence: d.binding.processedThroughSequence,
      handoffRef: opaque("handoff", m.id),
      preservedRef,
      completedAt: auditIso(pair.wall),
    };
    const delivery: WorkbenchDeliveryReceipt = { ...unsigned, digest: releaseDigest(unsigned) },
      next = {
        ...d,
        ...live,
        state: "delivered" as const,
        reasons: [],
        merge: completedMerge,
        delivery,
        version: d.version + 1,
        updatedAt: logicalReleaseIso(d.updatedAt, pair.wall),
      };
    yield* completeAcceptedDelivery(d, next).pipe(
      Effect.provideService(SqlClient.SqlClient, sql),
      Effect.mapError((cause) =>
        stop(
          "completion_changed",
          true,
          { stage: "completion", binding: d.binding },
          "local_fence",
          null,
          null,
          cause,
        ),
      ),
    );
  });
  const reconcile = Effect.fn("ReleaseMerge.reconcile")((id: string) =>
    execute(id).pipe(
      Effect.catch((cause) =>
        Effect.gen(function* () {
          yield* Effect.logWarning("Accepted merge blocked", cause);
          const error = isStop(cause)
            ? cause
            : stop(
                "state_unknown",
                false,
                { stage: "unexpected" },
                "local_fence",
                null,
                null,
                cause,
              );
          yield* fail(id, error);
        }),
      ),
    ),
  );
  return { reconcile, failureRequested };
});
