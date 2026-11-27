// @effect-diagnostics nodeBuiltinImport:off -- Each scoped runner needs a fresh opaque epoch even when a test clock restarts.
import * as NodeCrypto from "node:crypto";
import {
  WorkbenchDeployment,
  WorkbenchDeploymentAdmitInput,
  WorkbenchReleaseHostProfile,
  type WorkbenchAcceptance,
  type WorkbenchAcceptanceSample,
  type WorkbenchAcceptanceReason,
  type WorkbenchDeploymentTarget,
  type WorkbenchOnlineReceipt,
} from "@t3tools/contracts";
import * as Deferred from "effect/Deferred";
import * as Fiber from "effect/Fiber";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as Semaphore from "effect/Semaphore";
import * as SqlClient from "effect/sql/SqlClient";
import type * as SqlError from "effect/sql/SqlError";
import * as Evidence from "./WorkbenchReleaseEvidence.ts";
import * as Adapter from "./WorkbenchReleaseAdapter.ts";
import { acceptancePolicy, clockPair, auditIso, VerificationError } from "./releaseVerification.ts";
import { canonicalReleaseJson, releaseDigest, releaseHash } from "./workbenchReleaseProfile.ts";

type Lock = <A, E, R>(id: string, effect: Effect.Effect<A, E, R>) => Effect.Effect<A, E, R>;
type RuntimeRow = {
  deployment_id: string;
  runner_epoch: string | null;
  last_mono: string | null;
  last_wall: number | null;
  intent_json: string | null;
  recovery_json: string | null;
};
type Intent = {
  sample: WorkbenchAcceptanceSample;
  startedAt: string;
  role: "health" | "core";
  executionId: string;
  acceptanceVersion: number;
};
const decodeDeployment = Schema.decodeEffect(Schema.fromJsonString(WorkbenchDeployment));
const decodeInput = Schema.decodeEffect(Schema.fromJsonString(WorkbenchDeploymentAdmitInput));
const decodeProfile = Schema.decodeEffect(Schema.fromJsonString(WorkbenchReleaseHostProfile));
const decodeJson = Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown));
const isVerificationError = Schema.is(VerificationError);
const same = (a: unknown, b: unknown) => canonicalReleaseJson(a) === canonicalReleaseJson(b);
const opaque = (kind: string, value: string) => `${kind}:${releaseHash(value)}`;
const logicalIso = (previous: string, wall: number) =>
  auditIso(Math.max(Date.parse(previous), wall));
const hardStop = (a: WorkbenchAcceptance) =>
  a.state === "passed" ||
  a.state === "failed" ||
  a.failure !== null ||
  a.reasons.some((r) => ["material_missing", "material_invalid", "evidence_limit"].includes(r));
const sampleDigest = (sample: WorkbenchAcceptanceSample) => {
  const { digest: _digest, ...unsigned } = sample;
  return releaseDigest(unsigned);
};
const terminalFailure = (sample: WorkbenchAcceptanceSample) =>
  [sample.health, sample.core].find(
    (receipt) =>
      receipt?.state === "failed" &&
      receipt.exitCode !== null &&
      receipt.exitCode !== 0 &&
      receipt.outputDigest !== null &&
      receipt.completedAt !== null,
  ) ?? null;
const failedAcceptance = (
  a: WorkbenchAcceptance,
  sample: WorkbenchAcceptanceSample,
  failed: WorkbenchOnlineReceipt,
): WorkbenchAcceptance => {
  const reason =
    a.window.successfulSamples > 0
      ? "observation_failed"
      : failed.role === "health"
        ? "health_failed"
        : "core_failed";
  return {
    ...a,
    state: "failed",
    reasons: [reason],
    activeSample: null,
    observedAt: sample.completedAt,
    completedAt: sample.completedAt,
    failure: {
      id: opaque("failure", failed.id),
      reason,
      role: failed.role,
      sampleId: sample.id,
      receiptId: failed.id,
      outputDigest: failed.outputDigest!,
      occurredAt: failed.completedAt!,
    },
  };
};
const recoveryIntent = (a: WorkbenchAcceptance, sample: WorkbenchAcceptanceSample) => ({
  authorizationDigest: a.authorizationDigest,
  materialDigest: a.materialDigest,
  failureDigest: releaseDigest(a.failure),
  sampleId: sample.id,
});
const emptyWindow = () =>
  ({
    id: null,
    runnerEpoch: null,
    startedAt: null,
    endedAt: null,
    elapsedMilliseconds: 0,
    successfulSamples: 0,
    maxGapMilliseconds: 0,
    resets: 0,
    resetReason: null,
    maxWallGapMilliseconds: 0,
  }) satisfies WorkbenchAcceptance["window"];
class AcceptanceOperationError extends Schema.TaggedError<AcceptanceOperationError>()(
  "AcceptanceOperationError",
  { stage: Schema.String, cause: Schema.Defect() },
) {
  override get message() {
    return `Acceptance operation failed (${this.stage}).`;
  }
}

export const make = Effect.fn("ReleaseAcceptance.make")(function* (withLock: Lock) {
  const sql = yield* SqlClient.SqlClient,
    evidence = yield* Evidence.WorkbenchReleaseEvidence,
    adapter = yield* Adapter.WorkbenchReleaseAdapter,
    scope = yield* Effect.scope;
  const started = yield* clockPair,
    epoch = opaque(
      "runner",
      `${started.wall}:${started.mono}:${yield* Effect.sync(() => NodeCrypto.randomUUID())}`,
    );
  const workers = yield* Semaphore.make(2),
    jobs = new Map<string, Deferred.Deferred<void>>();
  const dueReady = new Map<string, number>();
  const waiters = new Map<string, Array<{ count: number; done: Deferred.Deferred<void> }>>();
  const read = Effect.fn("ReleaseAcceptance.read")(function* (id: string) {
    const [row] = yield* sql<{
      deployment_json: string;
      profile_json: string;
      input_json: string;
      commit_json: string | null;
    }>`SELECT deployment_json,profile_json,input_json,commit_json FROM workbench_deployments WHERE id=${id}`;
    if (!row)
      return yield* new VerificationError({ reason: "material_invalid", stage: "deployment" });
    return {
      row,
      d: yield* decodeDeployment(row.deployment_json),
      profile: yield* decodeProfile(row.profile_json),
      input: yield* decodeInput(row.input_json),
    };
  });
  const runtime = Effect.fn("ReleaseAcceptance.runtime")(function* (id: string) {
    yield* sql`INSERT INTO workbench_acceptance_runtime(deployment_id) VALUES(${id}) ON CONFLICT(deployment_id) DO NOTHING`;
    const [row] =
      yield* sql<RuntimeRow>`SELECT * FROM workbench_acceptance_runtime WHERE deployment_id=${id}`;
    return row!;
  });
  const notify = Effect.fn("ReleaseAcceptance.notify")(function* (d: WorkbenchDeployment) {
    const list = waiters.get(d.id) ?? [],
      rest = [] as typeof list;
    for (const waiter of list) {
      if (
        d.acceptance &&
        ((d.acceptance.ledger.count >= waiter.count &&
          (dueReady.get(d.id) ?? -1) >= d.acceptance.ledger.count) ||
          hardStop(d.acceptance))
      )
        yield* Deferred.succeed(waiter.done, undefined);
      else rest.push(waiter);
    }
    waiters.set(d.id, rest);
  });
  const persist = Effect.fn("ReleaseAcceptance.persist")(function* (
    d: WorkbenchDeployment,
    a: WorkbenchAcceptance,
    write?: Effect.Effect<unknown, SqlError.SqlError>,
  ) {
    const pair = yield* clockPair;
    const acceptance: WorkbenchAcceptance = {
      ...a,
      version: (d.acceptance?.version ?? 0) + 1,
      updatedAt: logicalIso(d.acceptance?.updatedAt ?? d.updatedAt, pair.wall),
    };
    const next = {
      ...d,
      acceptance,
      version: d.version + 1,
      updatedAt: logicalIso(d.updatedAt, pair.wall),
    };
    yield* sql.withTransaction(
      Effect.gen(function* () {
        if (write) yield* write;
        const rows =
          yield* sql`UPDATE workbench_deployments SET deployment_json=${canonicalReleaseJson(next)},updated_at=${next.updatedAt} WHERE id=${d.id} AND deployment_json=${canonicalReleaseJson(d)} RETURNING id`;
        if (rows.length !== 1)
          return yield* new VerificationError({
            reason: "material_invalid",
            stage: "stale_completion",
          });
      }),
    );
    yield* notify(next);
    return acceptance;
  });
  const append = Effect.fn("ReleaseAcceptance.append")(function* (
    d: WorkbenchDeployment,
    a: WorkbenchAcceptance,
    sample: WorkbenchAcceptanceSample,
    mono: bigint | null,
    wall: number | null,
    recovery: unknown | null,
    completion?: { wall: number; mono: bigint },
  ) {
    const previous = yield* runtime(d.id),
      finished = completion ?? (yield* clockPair),
      audit = canonicalReleaseJson({
        previousRunnerEpoch: previous.runner_epoch,
        previousMonotonicNanos: previous.last_mono,
        previousWallMilliseconds: previous.last_wall,
        finalizationRunnerEpoch: epoch,
        completionMonotonicNanos: String(finished.mono),
        completionWallMilliseconds: finished.wall,
      });
    const finalized = { ...sample, digest: sampleDigest(sample) };
    const next = {
      ...a,
      latestSample: finalized,
      activeSample: null,
      ledger: {
        ...a.ledger,
        count: finalized.ordinal,
        digest: releaseDigest({
          previous: a.ledger.digest,
          ordinal: finalized.ordinal,
          sampleDigest: finalized.digest,
        }),
      },
    };
    if (next.firstSample?.id === finalized.id) next.firstSample = finalized;
    yield* persist(
      d,
      next,
      Effect.gen(function* () {
        yield* sql`UPDATE workbench_acceptance_executions SET state='unknown' WHERE deployment_id=${d.id} AND sample_id=${finalized.id} AND state='running'`;
        yield* sql`INSERT INTO workbench_acceptance_samples(deployment_id,ordinal,sample_id,input_digest,sample_digest,sample_json,audit_json) VALUES(${d.id},${finalized.ordinal},${finalized.id},${finalized.inputDigest},${finalized.digest},${canonicalReleaseJson(finalized)},${audit})`;
        yield* sql`UPDATE workbench_acceptance_runtime SET runner_epoch=${epoch},last_mono=${mono === null ? null : String(mono)},last_wall=${wall},intent_json=NULL,recovery_json=COALESCE(recovery_json,${recovery === null ? null : canonicalReleaseJson(recovery)}) WHERE deployment_id=${d.id}`;
      }),
    );
  });
  const reset = (
    a: WorkbenchAcceptance,
    reason: NonNullable<WorkbenchAcceptance["window"]["resetReason"]>,
  ) => ({ ...emptyWindow(), resets: a.window.resets + 1, resetReason: reason });
  const initialize = Effect.fn("ReleaseAcceptance.initialize")(function* (id: string) {
    const { d, profile } = yield* read(id),
      r = yield* runtime(id);
    if (d.acceptance && hardStop(d.acceptance)) return d.acceptance;
    const p = yield* clockPair;
    if (!d.acceptance) {
      const material = yield* evidence.admittedMaterial(d.binding.queueId, d.preparationId, {
        controllerId: d.binding.controllerId,
        projectId: d.binding.projectId,
        expectedTrackingId: d.binding.trackingId,
        expectedEvidenceDigest: d.evidenceDigest,
      });
      const materialDigest = material.evidence.verification?.manifestDigest ?? null,
        policy = acceptancePolicy(profile.policy.observation),
        revision = d.deployOperation?.result?.writtenRevision ?? d.baseline.revision;
      const a: WorkbenchAcceptance = {
        schemaVersion: 1,
        id: opaque("acceptance", d.id),
        authorizationDigest: d.authorizationDigest,
        materialDigest,
        policy,
        candidate: d.desired,
        activationRevision: revision,
        deployOperationId: d.deployOperation!.id,
        version: 1,
        state: materialDigest ? "pending" : "unknown",
        reasons: materialDigest ? [] : ["material_missing"],
        runnerEpoch: epoch,
        firstSample: null,
        latestSample: null,
        activeSample: null,
        window: emptyWindow(),
        ledger: {
          ref: opaque("ledger", d.id),
          count: 0,
          digest: releaseDigest({
            schemaVersion: 1,
            deploymentId: d.id,
            authorizationDigest: d.authorizationDigest,
            materialDigest,
            policyDigest: releaseDigest(policy),
          }),
        },
        failure: null,
        createdAt: logicalIso(d.updatedAt, p.wall),
        updatedAt: logicalIso(d.updatedAt, p.wall),
        observedAt: null,
        completedAt: null,
      };
      return yield* persist(
        d,
        a,
        sql`UPDATE workbench_acceptance_runtime SET runner_epoch=${epoch} WHERE deployment_id=${id}`,
      );
    }
    const a = d.acceptance;
    if (r.intent_json) {
      const intent = (yield* decodeJson(r.intent_json)) as Intent;
      const failed = terminalFailure(intent.sample);
      if (failed) {
        const [execution] = yield* sql<{
          state: string;
          receipt_json: string | null;
        }>`SELECT state,receipt_json FROM workbench_acceptance_executions WHERE execution_id=${failed.executionId} AND deployment_id=${id} AND sample_id=${intent.sample.id}`;
        if (
          !execution ||
          execution.state !== "failed" ||
          execution.receipt_json !== canonicalReleaseJson(failed)
        )
          return yield* new VerificationError({
            reason: "material_invalid",
            stage: "failure_journal",
          });
        const sample = {
          ...intent.sample,
          state: "failed" as const,
          completedAt: auditIso(p.wall),
        };
        const next = failedAcceptance(a, sample, failed);
        yield* append(d, next, sample, null, null, recoveryIntent(next, sample));
        return (yield* read(id)).d.acceptance!;
      }
      const sample = { ...intent.sample, state: "unknown" as const, completedAt: null };
      const next: WorkbenchAcceptance = {
        ...a,
        state: "unknown",
        runnerEpoch: epoch,
        reasons: ["native_restarted"],
        firstSample: null,
        activeSample: null,
        window: reset(a, "native_restarted"),
        observedAt: auditIso(p.wall),
      };
      yield* append(d, next, sample, null, null, null);
      return (yield* read(id)).d.acceptance!;
    }
    if (a.runnerEpoch !== epoch)
      return yield* persist(
        d,
        {
          ...a,
          state: "pending",
          runnerEpoch: epoch,
          reasons: ["native_restarted"],
          firstSample: null,
          activeSample: null,
          window: reset(a, "native_restarted"),
        },
        sql`UPDATE workbench_acceptance_runtime SET runner_epoch=${epoch},last_mono=NULL,last_wall=NULL WHERE deployment_id=${id}`,
      );
    return a;
  });
  const owns = (d: WorkbenchDeployment, t: WorkbenchDeploymentTarget) =>
    t.revision === (d.deployOperation?.result?.writtenRevision ?? d.baseline.revision) &&
    (d.deployOperation?.result?.effect === "unchanged"
      ? same(d.desired, d.baseline.basis)
      : t.lastOperationId === d.deployOperation?.id);
  const current = Effect.fn("ReleaseAcceptance.current")(function* (
    id: string,
    sampleId: string,
    version: number,
  ) {
    const snapshot = yield* read(id),
      r = yield* runtime(id),
      a = snapshot.d.acceptance;
    if (
      !a ||
      a.runnerEpoch !== epoch ||
      a.version !== version ||
      a.activeSample?.id !== sampleId ||
      a.state === "failed" ||
      a.state === "passed" ||
      a.failure !== null ||
      snapshot.d.rollbackOperation !== null ||
      snapshot.d.claimState !== "committed" ||
      snapshot.d.deployOperation?.state !== "succeeded" ||
      r.runner_epoch !== epoch ||
      r.recovery_json !== null ||
      !r.intent_json
    )
      return null;
    const intent = (yield* decodeJson(r.intent_json)) as Intent;
    if (
      intent.sample.id !== sampleId ||
      intent.sample.runnerEpoch !== epoch ||
      intent.acceptanceVersion !== version ||
      intent.sample.ordinal !== a.ledger.count + 1 ||
      a.activeSample.ordinal !== intent.sample.ordinal ||
      a.activeSample.runnerEpoch !== epoch ||
      a.activeSample.role !== intent.role ||
      a.activeSample.startedAt !== intent.startedAt ||
      a.authorizationDigest !== snapshot.d.authorizationDigest ||
      a.deployOperationId !== snapshot.d.deployOperation.id ||
      a.activationRevision !==
        (snapshot.d.deployOperation.result?.writtenRevision ?? snapshot.d.baseline.revision) ||
      !same(a.candidate, snapshot.d.desired) ||
      !same(a.policy, acceptancePolicy(snapshot.profile.policy.observation)) ||
      !same(intent.sample.candidate, a.candidate) ||
      intent.sample.activationRevision !== a.activationRevision ||
      intent.sample.inputDigest !==
        releaseDigest({
          authorizationDigest: snapshot.d.authorizationDigest,
          materialDigest: a.materialDigest,
          sampleId,
          ordinal: intent.sample.ordinal,
          runnerEpoch: epoch,
          candidate: a.candidate,
          activationRevision: a.activationRevision,
          policyDigest: releaseDigest(a.policy),
        })
    )
      return null;
    return { ...snapshot, a, r, intent };
  });
  const runSample = Effect.fn("ReleaseAcceptance.runSample")(function* (id: string) {
    const launched = yield* withLock(
      id,
      Effect.gen(function* () {
        const { d, profile } = yield* read(id),
          a = d.acceptance;
        if (
          !a ||
          hardStop(a) ||
          a.activeSample ||
          d.claimState !== "committed" ||
          d.deployOperation?.state !== "succeeded" ||
          d.rollbackOperation
        )
          return null;
        if (a.ledger.count >= a.policy.maxSamples) {
          yield* persist(d, {
            ...a,
            state: "unknown",
            reasons: ["evidence_limit"],
            activeSample: null,
          });
          return null;
        }
        const pair = yield* clockPair,
          ordinal = a.ledger.count + 1,
          sampleId = opaque("sample", `${id}:${epoch}:${ordinal}`),
          executionId = opaque("execution", `${sampleId}:health`);
        const sample: WorkbenchAcceptanceSample = {
          schemaVersion: 1,
          id: sampleId,
          ordinal,
          runnerEpoch: epoch,
          inputDigest: releaseDigest({
            authorizationDigest: d.authorizationDigest,
            materialDigest: a.materialDigest,
            sampleId,
            ordinal,
            runnerEpoch: epoch,
            candidate: a.candidate,
            activationRevision: a.activationRevision,
            policyDigest: releaseDigest(a.policy),
          }),
          state: "unknown",
          candidate: a.candidate,
          activationRevision: a.activationRevision,
          startedAt: auditIso(pair.wall),
          completedAt: null,
          health: null,
          core: null,
          targetBefore: null,
          targetAfter: null,
          versionObservation: null,
          gapMilliseconds: null,
          creditedElapsedMilliseconds: 0,
          wallGapMilliseconds: null,
          digest: "0".repeat(64),
        };
        const next = {
          ...a,
          state: "checking" as const,
          reasons: [],
          activeSample: {
            id: sampleId,
            ordinal,
            runnerEpoch: epoch,
            role: "health" as const,
            startedAt: sample.startedAt,
          },
        };
        const intent: Intent = {
          sample,
          startedAt: sample.startedAt,
          role: "health",
          executionId,
          acceptanceVersion: a.version + 1,
        };
        yield* persist(
          d,
          next,
          Effect.gen(function* () {
            yield* sql`UPDATE workbench_acceptance_runtime SET intent_json=${canonicalReleaseJson(intent)} WHERE deployment_id=${id}`;
            yield* sql`INSERT INTO workbench_acceptance_executions(execution_id,deployment_id,sample_id,role,state,input_json) VALUES(${executionId},${id},${sampleId},'health','running',${canonicalReleaseJson({ sampleId, runnerEpoch: epoch })})`;
          }),
        );
        return { d, profile, intent };
      }),
    );
    if (!launched) return;
    let { intent } = launched;
    const { d, profile } = launched,
      sample = { ...intent.sample };
    let reason: WorkbenchAcceptanceReason = "check_unknown";
    const before = yield* adapter.readTarget(profile).pipe(Effect.result);
    if (before._tag === "Success") sample.targetBefore = before.success;
    const mayRun =
      sample.targetBefore &&
      owns(d, sample.targetBefore) &&
      sample.targetBefore.state === "consistent" &&
      same(sample.targetBefore.basis, d.desired);
    if (mayRun) {
      for (const role of ["health", "core"] as const) {
        const outcome = yield* evidence
          .runOnlineCheck(d.binding.queueId, d.preparationId, {
            controllerId: d.binding.controllerId,
            projectId: d.binding.projectId,
            expectedTrackingId: d.binding.trackingId,
            expectedEvidenceDigest: d.evidenceDigest,
            sampleId: sample.id,
            executionId: intent.executionId,
            role,
            authorizationDigest: d.authorizationDigest,
            runnerEpoch: epoch,
            candidate: d.desired,
            activationRevision: sample.activationRevision,
          })
          .pipe(Effect.result);
        if (outcome._tag === "Failure") {
          yield* Effect.logWarning(
            "Acceptance check failed",
            new AcceptanceOperationError({ stage: "execution", cause: outcome.failure }),
          );
          reason = isVerificationError(outcome.failure) ? outcome.failure.reason : "check_unknown";
          break;
        }
        sample[role] = outcome.success;
        if (role === "health") sample.startedAt = outcome.success.startedAt;
        const nextIntent = yield* withLock(
          id,
          Effect.gen(function* () {
            const live = yield* current(id, sample.id, intent.acceptanceVersion);
            if (
              !live ||
              live.intent.executionId !== intent.executionId ||
              live.intent.role !== role
            )
              return null;
            const move = role === "health" && outcome.success.state === "succeeded";
            const updated: Intent = {
              sample,
              startedAt: intent.startedAt,
              role: move ? "core" : role,
              executionId: move ? opaque("execution", `${sample.id}:core`) : intent.executionId,
              acceptanceVersion: live.a.version + 1,
            };
            yield* persist(
              live.d,
              {
                ...live.a,
                activeSample: {
                  ...live.a.activeSample!,
                  role: updated.role,
                  startedAt: live.a.activeSample!.startedAt,
                },
              },
              Effect.gen(function* () {
                yield* sql`UPDATE workbench_acceptance_executions SET state=${outcome.success.state},receipt_json=${canonicalReleaseJson(outcome.success)} WHERE execution_id=${intent.executionId}`;
                yield* sql`UPDATE workbench_acceptance_runtime SET intent_json=${canonicalReleaseJson(updated)} WHERE deployment_id=${id}`;
                if (move)
                  yield* sql`INSERT INTO workbench_acceptance_executions(execution_id,deployment_id,sample_id,role,state,input_json) VALUES(${updated.executionId},${id},${sample.id},'core','running',${canonicalReleaseJson({ sampleId: sample.id, runnerEpoch: epoch })})`;
              }),
            );
            return updated;
          }),
        );
        if (!nextIntent) {
          yield* sql`UPDATE workbench_acceptance_executions SET receipt_json=COALESCE(receipt_json,${canonicalReleaseJson(outcome.success)}) WHERE execution_id=${intent.executionId} AND deployment_id=${id} AND sample_id=${sample.id}`;
          return "stale" as const;
        }
        intent = nextIntent;
        if (outcome.success.state !== "succeeded") break;
      }
    } else reason = "target_changed";
    const after = yield* adapter.readTarget(profile).pipe(Effect.result);
    if (after._tag === "Success") sample.targetAfter = after.success;
    const failed = terminalFailure(sample);
    if (failed) sample.state = "failed";
    else if (sample.health?.state === "succeeded" && sample.core?.state === "succeeded") {
      const observed = yield* adapter.observe(profile).pipe(Effect.result);
      if (observed._tag === "Success") {
        sample.targetAfter = observed.success.target;
        sample.versionObservation = observed.success.versionObservation;
        if (
          sample.targetBefore &&
          owns(d, sample.targetBefore) &&
          owns(d, sample.targetAfter) &&
          sample.targetBefore.state === "consistent" &&
          sample.targetAfter.state === "consistent" &&
          same(sample.targetBefore.basis, d.desired) &&
          same(sample.targetAfter.basis, d.desired) &&
          same(sample.versionObservation.basis, d.desired)
        )
          sample.state = "succeeded";
        else reason = "target_changed";
      }
      if (
        sample.state === "succeeded" &&
        ([sample.health, sample.core].some(
          (r) =>
            !r.completedAt ||
            r.durationMilliseconds === null ||
            Date.parse(r.completedAt) - Date.parse(r.startedAt) < 0 ||
            Math.abs(Date.parse(r.completedAt) - Date.parse(r.startedAt) - r.durationMilliseconds) >
              2,
        ) ||
          Date.parse(sample.health.completedAt!) > Date.parse(sample.core.startedAt))
      ) {
        sample.state = "unknown";
        reason = "clock_changed";
      }
    }
    const completed = yield* clockPair;
    sample.completedAt = auditIso(completed.wall);
    if (
      sample.state === "succeeded" &&
      (completed.wall < Date.parse(sample.core!.completedAt!) ||
        !sample.versionObservation ||
        Date.parse(sample.versionObservation.observedAt) < Date.parse(sample.core!.completedAt!) ||
        Date.parse(sample.versionObservation.observedAt) > completed.wall)
    ) {
      sample.state = "unknown";
      reason = "clock_changed";
    }
    return yield* withLock(
      id,
      Effect.gen(function* () {
        const live = yield* current(id, sample.id, intent.acceptanceVersion);
        if (!live) return "stale" as const;
        let next = {
          ...live.a,
          activeSample: null,
          observedAt: sample.completedAt,
        } as WorkbenchAcceptance;
        let mono: bigint | null = null,
          wall: number | null = null,
          recovery: unknown | null = null;
        if (failed) {
          next = failedAcceptance(next, sample, failed);
          recovery = recoveryIntent(next, sample);
        } else if (sample.state === "succeeded") {
          const rawGap =
              live.r.last_mono === null ? null : completed.mono - BigInt(live.r.last_mono),
            rawWall = live.r.last_wall === null ? null : completed.wall - live.r.last_wall,
            interval = BigInt(next.policy.intervalSeconds) * 1000000000n,
            maxGap = interval * 2n;
          let restart: WorkbenchAcceptance["window"]["resetReason"] = null;
          if (rawGap !== null && rawGap < 0n) {
            sample.state = "unknown";
            reason = "clock_changed";
          } else if (rawGap !== null && rawGap < interval) {
            sample.state = "unknown";
            reason = "check_unknown";
          } else if (
            rawWall !== null &&
            (rawWall < 0 || rawWall > next.policy.maxGapSeconds * 1000)
          )
            restart = "clock_changed";
          else if (rawGap !== null && rawGap > maxGap) restart = "window_gap";
          if (sample.state === "succeeded") {
            const fresh =
              !next.firstSample ||
              live.r.runner_epoch !== epoch ||
              rawGap === null ||
              restart !== null;
            if (fresh) {
              next = {
                ...next,
                firstSample: sample,
                window: {
                  ...emptyWindow(),
                  id: opaque("window", sample.id),
                  runnerEpoch: epoch,
                  startedAt: sample.completedAt,
                  endedAt: sample.completedAt,
                  successfulSamples: 1,
                  resets: next.window.resets + (restart ? 1 : 0),
                  resetReason: restart ?? next.window.resetReason,
                },
              };
            } else {
              sample.gapMilliseconds = Number(rawGap! / 1000000n);
              sample.wallGapMilliseconds = rawWall;
              next = {
                ...next,
                window: {
                  ...next.window,
                  endedAt: sample.completedAt,
                  elapsedMilliseconds: next.window.elapsedMilliseconds + sample.gapMilliseconds,
                  successfulSamples: next.window.successfulSamples + 1,
                  maxGapMilliseconds: Math.max(
                    next.window.maxGapMilliseconds,
                    sample.gapMilliseconds,
                  ),
                  maxWallGapMilliseconds: Math.max(
                    next.window.maxWallGapMilliseconds,
                    rawWall ?? 0,
                  ),
                },
              };
              sample.creditedElapsedMilliseconds = next.window.elapsedMilliseconds;
            }
            const passed =
              next.window.elapsedMilliseconds >= next.policy.durationSeconds * 1000 &&
              next.window.successfulSamples >= next.policy.consecutiveSuccesses;
            next = {
              ...next,
              state: passed ? "passed" : "observing",
              reasons: [],
              completedAt: passed ? sample.completedAt : null,
            };
            mono = completed.mono;
            wall = completed.wall;
          }
        }
        if (sample.state === "unknown")
          next = {
            ...next,
            state: "unknown",
            reasons: [reason],
            firstSample: null,
            completedAt: null,
            window: reset(
              next,
              reason === "clock_changed"
                ? "clock_changed"
                : reason === "target_changed"
                  ? "target_changed"
                  : "check_unknown",
            ),
          };
        yield* append(live.d, next, sample, mono, wall, recovery, completed);
      }),
    );
  });
  const run = Effect.fn("ReleaseAcceptance.run")(function* (id: string) {
    let a = yield* withLock(id, initialize(id));
    while (!hardStop(a)) {
      if ((yield* workers.withPermit(runSample(id))) === "stale") return;
      const d = (yield* withLock(id, read(id))).d;
      if (!d.acceptance) return;
      a = d.acceptance;
      if (
        hardStop(a) ||
        d.rollbackOperation ||
        ["rolled_back", "failed_unchanged", "rejected"].includes(d.state)
      )
        return;
      const waiting = yield* Effect.sleep(`${a.policy.intervalSeconds} seconds`).pipe(
        Effect.forkChild({ startImmediately: true }),
      );
      dueReady.set(id, a.ledger.count);
      yield* notify(d);
      yield* Fiber.join(waiting);
    }
  });
  const schedule = Effect.fn("ReleaseAcceptance.schedule")(function* (id: string) {
    if (jobs.has(id)) return;
    const done = yield* Deferred.make<void>();
    jobs.set(id, done);
    yield* run(id).pipe(
      Effect.catch((cause) =>
        Effect.logWarning(
          "Acceptance worker failed",
          new AcceptanceOperationError({ stage: "worker", cause }),
        ),
      ),
      Effect.ensuring(Effect.sync(() => jobs.delete(id))),
      Effect.ensuring(Deferred.succeed(done, undefined)),
      Effect.forkIn(scope),
    );
  });
  const awaitSample = Effect.fn("ReleaseAcceptance.awaitSample")(function* (
    id: string,
    count: number,
  ) {
    const waiting = yield* withLock(
      id,
      Effect.gen(function* () {
        const d = (yield* read(id)).d;
        if (
          d.acceptance &&
          ((d.acceptance.ledger.count >= count &&
            (dueReady.get(id) ?? -1) >= d.acceptance.ledger.count) ||
            hardStop(d.acceptance))
        )
          return null;
        const done = yield* Deferred.make<void>();
        waiters.set(id, [...(waiters.get(id) ?? []), { count, done }]);
        return done;
      }),
    );
    if (waiting) yield* Deferred.await(waiting);
  });
  const failureRequested = Effect.fn("ReleaseAcceptance.failureRequested")(function* (
    d: WorkbenchDeployment,
  ) {
    if (d.acceptance?.state !== "failed" || !d.acceptance.failure) return false;
    const [r] =
      yield* sql<RuntimeRow>`SELECT * FROM workbench_acceptance_runtime WHERE deployment_id=${d.id}`;
    if (!r?.recovery_json) return false;
    const recovery = (yield* decodeJson(r.recovery_json)) as {
      authorizationDigest: string;
      materialDigest: string;
      failureDigest: string;
      sampleId: string;
    };
    const [stored] = yield* sql<{
      sample_json: string;
      sample_digest: string;
    }>`SELECT sample_json,sample_digest FROM workbench_acceptance_samples WHERE deployment_id=${d.id} AND sample_id=${recovery.sampleId}`;
    if (!stored) return false;
    const sample = (yield* decodeJson(stored.sample_json)) as WorkbenchAcceptanceSample,
      failure = d.acceptance.failure,
      receipt = sample[failure.role];
    return (
      recovery.authorizationDigest === d.authorizationDigest &&
      recovery.materialDigest === d.acceptance.materialDigest &&
      recovery.failureDigest === releaseDigest(failure) &&
      sample.digest === stored.sample_digest &&
      sampleDigest(sample) === sample.digest &&
      sample.state === "failed" &&
      receipt?.state === "failed" &&
      receipt.exitCode !== null &&
      receipt.exitCode !== 0 &&
      receipt.id === failure.receiptId &&
      receipt.outputDigest === failure.outputDigest &&
      receipt.authorizationDigest === d.authorizationDigest
    );
  });
  return { schedule, awaitSample, failureRequested };
});
