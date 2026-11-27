import * as Acceptance from "./releaseAcceptance.ts";
import {
  WorkbenchDeployment as DeploymentSchema,
  WorkbenchDeploymentAdmitInput,
  WorkbenchDeploymentError,
  WorkbenchReleaseHostProfile,
  WorkbenchDeploymentSigned,
  type WorkbenchDeployment as DeploymentRecord,
  type WorkbenchDeploymentPayload,
  type WorkbenchDeploymentOperation,
  type WorkbenchDeploymentTarget,
  type WorkbenchDeploymentResult,
  type WorkbenchDeploymentReason,
  type WorkbenchAttentionScope,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";
import * as Schedule from "effect/Schedule";
import * as SqlClient from "effect/sql/SqlClient";
import * as KeyedLock from "@t3tools/shared/KeyedLock";
import * as Evidence from "./WorkbenchReleaseEvidence.ts";
import * as Adapter from "./WorkbenchReleaseAdapter.ts";
import * as Environment from "../environment/ServerEnvironment.ts";
import {
  canonicalReleaseJson,
  releaseDigest,
  releaseHash,
  publicDeploymentPlan,
} from "./workbenchReleaseProfile.ts";
import { logicalReleaseIso } from "./releaseVerification.ts";
import * as MergeLifecycle from "./releaseMerge.ts";

type Scope = typeof WorkbenchAttentionScope.Type;
type Row = {
  id: string;
  queue_id: string;
  controller_id: string;
  project_id: string;
  tracking_id: string;
  input_json: string;
  profile_json: string;
  commit_json: string | null;
  claim_intent: number;
  settled_state: string | null;
  deployment_json: string;
  state: string;
};
const same = (a: unknown, b: unknown) => canonicalReleaseJson(a) === canonicalReleaseJson(b);
const terminal = (o: WorkbenchDeploymentOperation) =>
  o.state === "succeeded" || o.state === "failed";
class DeploymentOperationError extends Schema.TaggedError<DeploymentOperationError>()(
  "DeploymentOperationError",
  {
    stage: Schema.String,
    cause: Schema.Defect(),
  },
) {
  override get message() {
    return `Deployment operation failed (${this.stage}).`;
  }
}
const decodeAdmit = Schema.decodeEffect(WorkbenchDeploymentAdmitInput);
const isAdapterError = Schema.is(Adapter.ReleaseAdapterError);
const isPublicError = Schema.is(WorkbenchDeploymentError);
const failure = Effect.fn("WorkbenchDeployment.failure")(function* (stage: string, cause: unknown) {
  if (isPublicError(cause)) return yield* cause;
  yield* Effect.logWarning(
    "Deployment operation failed",
    new DeploymentOperationError({ stage, cause }),
  );
  return yield* new WorkbenchDeploymentError({ reason: "unavailable" });
});
export class WorkbenchDeployment extends Context.Service<
  WorkbenchDeployment,
  {
    readonly admit: (
      queueId: string,
      input: WorkbenchDeploymentAdmitInput,
    ) => Effect.Effect<WorkbenchDeploymentResult, WorkbenchDeploymentError>;
    readonly get: (
      queueId: string,
      id: string,
      scope: Scope,
    ) => Effect.Effect<WorkbenchDeploymentResult, WorkbenchDeploymentError>;
    readonly awaitAcceptanceSample: (
      queueId: string,
      id: string,
      scope: Scope,
      count: number,
    ) => Effect.Effect<WorkbenchDeploymentResult, WorkbenchDeploymentError>;
    readonly sync: Effect.Effect<void>;
  }
>()("t3/scheduledTasks/WorkbenchDeployment") {}

const make = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient,
    evidence = yield* Evidence.WorkbenchReleaseEvidence,
    adapter = yield* Adapter.WorkbenchReleaseAdapter,
    environment = yield* Environment.ServerEnvironmentIdentity,
    serviceScope = yield* Effect.scope,
    locks = yield* KeyedLock.make<string>();
  const now = DateTime.now.pipe(Effect.map(DateTime.formatIso));
  const acceptance = yield* Acceptance.make((id, effect) => locks.withLock(id, effect));
  const merge = yield* MergeLifecycle.make;
  const decodeInput = Schema.decodeEffect(Schema.fromJsonString(WorkbenchDeploymentAdmitInput));
  const decodeDeployment = Schema.decodeEffect(Schema.fromJsonString(DeploymentSchema));
  const decodeProfile = Schema.decodeEffect(Schema.fromJsonString(WorkbenchReleaseHostProfile));
  const decodeCommit = Schema.decodeEffect(Schema.fromJsonString(WorkbenchDeploymentSigned));
  const envelope = Effect.fn("WorkbenchDeployment.envelope")(function* (
    d: DeploymentRecord,
  ): Effect.fn.Return<WorkbenchDeploymentResult, WorkbenchDeploymentError> {
    if (d.binding.environmentId !== (yield* environment.getEnvironmentId))
      return yield* new WorkbenchDeploymentError({ reason: "invalid_binding" });
    return {
      environmentId: d.binding.environmentId,
      trackingId: d.binding.trackingId,
      queueId: d.binding.queueId,
      controllerId: d.binding.controllerId,
      projectId: d.binding.projectId,
      deployment: d,
    };
  });
  const read = Effect.fn("WorkbenchDeployment.read")(function* (
    queueId: string,
    id: string,
    scope: Scope,
  ) {
    const [row] =
      yield* sql<Row>`SELECT * FROM workbench_deployments WHERE id=${id} AND queue_id=${queueId} AND controller_id=${scope.controllerId} AND project_id=${scope.projectId} AND tracking_id=${scope.expectedTrackingId}`;
    if (!row) return yield* new WorkbenchDeploymentError({ reason: "not_found" });
    return row;
  });
  const get = Effect.fn("WorkbenchDeployment.get")(
    function* (queueId: string, id: string, scope: Scope) {
      return yield* envelope(
        yield* decodeDeployment((yield* read(queueId, id, scope)).deployment_json),
      );
    },
    Effect.catch((cause) => failure("get", cause)),
  );
  const matches = (p: WorkbenchDeploymentPayload, m: Evidence.ReleaseDeploymentMaterial) => {
    const e = m.evidence,
      d = e.deployment;
    return (
      d &&
      m.profile.policy.deployment &&
      p.preparationId === e.preparationId &&
      p.evidenceDigest === e.digest &&
      p.profileDigest === e.profile.digest &&
      same(p.binding, e.binding) &&
      p.sourceSha === e.source.commitSha &&
      p.mainSha === e.source.main.commitSha &&
      same(p.artifact, { sha256: e.artifact.sha256, bytes: e.artifact.bytes }) &&
      same(p.rollbackArtifact, {
        sha256: e.rollback.artifact.sha256,
        bytes: e.rollback.artifact.bytes,
      }) &&
      same(p.plan, d.plan) &&
      same(p.plan, publicDeploymentPlan(m.profile.policy.deployment)) &&
      same(p.desired, d.desired) &&
      same(p.baseline, {
        basis: { serviceId: e.target.serviceId, ...e.target.basis },
        revision: d.baselineRevision,
      }) &&
      p.desired.serviceId === e.target.serviceId &&
      p.desired.sourceSha === e.source.commitSha &&
      p.desired.artifactSha256 === e.artifact.sha256 &&
      p.desired.version === `sha256-${e.artifact.sha256}` &&
      p.deployOperationId !== p.rollbackOperationId &&
      p.createdAt >= p.decidedAt
    );
  };
  const admit = Effect.fn("WorkbenchDeployment.admit")(
    function* (queueId: string, input: WorkbenchDeploymentAdmitInput) {
      const decoded = yield* decodeAdmit(input, {
        onExcessProperty: "error",
      }).pipe(Effect.mapError(() => new WorkbenchDeploymentError({ reason: "invalid_request" })));
      return yield* locks.withLock(
        decoded.deploymentId,
        Effect.gen(function* () {
          const [prior] =
            yield* sql<Row>`SELECT * FROM workbench_deployments WHERE id=${decoded.deploymentId}`;
          if (prior) {
            if (prior.queue_id !== queueId || prior.input_json !== canonicalReleaseJson(decoded))
              return yield* new WorkbenchDeploymentError({ reason: "conflict" });
            return yield* envelope(yield* decodeDeployment(prior.deployment_json));
          }
          const reservation = decoded.reservation,
            p = reservation.payload;
          const material = yield* evidence
            .admittedMaterial(queueId, decoded.preparationId, {
              ...decoded,
              expectedEvidenceDigest: decoded.expectedEvidenceDigest,
            })
            .pipe(
              Effect.mapError(() => new WorkbenchDeploymentError({ reason: "evidence_invalid" })),
            );
          if (
            p.nativeDeploymentId !== decoded.deploymentId ||
            p.preparationId !== decoded.preparationId ||
            p.evidenceDigest !== decoded.expectedEvidenceDigest ||
            p.binding.queueId !== queueId ||
            p.binding.controllerId !== decoded.controllerId ||
            p.binding.projectId !== decoded.projectId ||
            p.binding.trackingId !== decoded.expectedTrackingId ||
            p.binding.environmentId !== (yield* environment.getEnvironmentId)
          )
            return yield* new WorkbenchDeploymentError({ reason: "invalid_binding" });
          if (
            !matches(p, material) ||
            releaseHash(decoded.claimToken) !== p.claimTokenDigest ||
            !Adapter.verifyDeploymentSignature(
              reservation,
              material.profile.policy.deployment!,
              "reservation",
            )
          )
            return yield* new WorkbenchDeploymentError({ reason: "permit_invalid" });
          const stamp = yield* now;
          const d: DeploymentRecord = {
            id: decoded.deploymentId,
            authorizationDigest: reservation.authorizationDigest,
            preparationId: p.preparationId,
            evidenceDigest: p.evidenceDigest,
            profileDigest: p.profileDigest,
            binding: p.binding,
            plan: p.plan,
            desired: p.desired,
            baseline: p.baseline,
            lock: p.lock,
            version: 1,
            state: "queued",
            reasons: [],
            claimState: "reserved",
            deployOperation: null,
            rollbackOperation: null,
            target: null,
            versionObservation: null,
            createdAt: stamp,
            updatedAt: stamp,
            observedAt: null,
          };
          yield* sql`INSERT INTO workbench_deployments(id,queue_id,controller_id,project_id,tracking_id,input_json,profile_json,deployment_json,state,created_at,updated_at) VALUES(${d.id},${queueId},${decoded.controllerId},${decoded.projectId},${decoded.expectedTrackingId},${canonicalReleaseJson(decoded)},${canonicalReleaseJson(material.profile)},${canonicalReleaseJson(d)},${d.state},${stamp},${stamp})`;
          yield* sync.pipe(Effect.forkIn(serviceScope));
          return yield* envelope(d);
        }),
      );
    },
    Effect.catch((cause) => failure("admit", cause)),
  );
  const validOperation = (
    o: WorkbenchDeploymentOperation,
    previous: WorkbenchDeploymentOperation | null,
    p: WorkbenchDeploymentPayload,
    authorizationDigest: string,
    kind: "deploy" | "rollback",
  ) => {
    const id = kind === "deploy" ? p.deployOperationId : p.rollbackOperationId;
    return (
      o.id === id &&
      o.kind === kind &&
      o.authorizationDigest === authorizationDigest &&
      o.inputDigest === releaseDigest({ authorizationDigest, kind, operationId: id }) &&
      o.adapterId === p.plan.adapterId &&
      o.adapterVersion === p.plan.adapterVersion &&
      o.generationId === p.plan.generationId &&
      o.serviceKey === p.plan.serviceKey &&
      o.serviceId === p.desired.serviceId &&
      Date.parse(o.updatedAt) >= Date.parse(o.createdAt) &&
      (terminal(o)
        ? o.result !== null && Date.parse(o.result.completedAt) >= Date.parse(o.createdAt)
        : o.result === null) &&
      (!previous ||
        (o.externalExecutionId === previous.externalExecutionId &&
          o.revision >= previous.revision &&
          (!terminal(previous) || terminal(o)) &&
          (o.revision !== previous.revision || same(o, previous)) &&
          (!terminal(previous) || same(o, previous))))
    );
  };
  const step = Effect.fn("WorkbenchDeployment.step")(function* (row: Row) {
    const input = yield* decodeInput(row.input_json),
      profile = yield* decodeProfile(row.profile_json),
      reservation = input.reservation,
      p = reservation.payload;
    let d = yield* decodeDeployment(row.deployment_json),
      claimIntent = row.claim_intent === 1;
    const alreadyDeployed = row.settled_state === "deployed" || d.state === "deployed";
    const physicallySettled = row.settled_state !== null;
    let proof = row.commit_json ? yield* decodeCommit(row.commit_json) : null;
    const save = Effect.fn("WorkbenchDeployment.save")(function* (
      patch: Partial<DeploymentRecord>,
    ) {
      const currentStamp = yield* now;
      const next = {
        ...d,
        ...patch,
        version: d.version + 1,
        updatedAt: logicalReleaseIso(d.updatedAt, Date.parse(currentStamp)),
      };
      const changed =
        yield* sql`UPDATE workbench_deployments SET deployment_json=${canonicalReleaseJson(next)},state=${next.state},updated_at=${next.updatedAt},settled_state=CASE WHEN settled_state IS NULL AND ${["deployed", "rolled_back", "failed_unchanged"].includes(next.state)} THEN ${next.state} ELSE settled_state END WHERE id=${d.id} AND deployment_json=${canonicalReleaseJson(d)} RETURNING id`;
      if (changed.length !== 1)
        return yield* new WorkbenchDeploymentError({ reason: "stale_state" });
      d = next;
    });
    const scope = {
      controllerId: d.binding.controllerId,
      projectId: d.binding.projectId,
      expectedTrackingId: d.binding.trackingId,
      expectedEvidenceDigest: d.evidenceDigest,
    };
    const material = yield* evidence
      .admittedMaterial(d.binding.queueId, d.preparationId, scope)
      .pipe(
        Effect.mapError(
          (cause) =>
            new Adapter.ReleaseAdapterError({
              reason: "material_changed",
              stage: "material",
              cause,
            }),
        ),
      );
    if (
      !matches(p, material) ||
      !same(profile, material.profile) ||
      !Adapter.verifyDeploymentSignature(reservation, profile.policy.deployment!, "reservation")
    )
      return yield* new Adapter.ReleaseAdapterError({
        reason: "material_changed",
        stage: "material",
      });
    if (!proof) {
      const claimed = yield* adapter.readClaim(profile, reservation, input.claimToken);
      if (claimed.state === "withdrawn") {
        yield* save({ state: "rejected", claimState: "withdrawn", reasons: ["claim_withdrawn"] });
        return;
      }
      if (claimed.state === "committed") proof = claimed.authorization;
      else {
        yield* save({ state: "staging", reasons: [] });
        yield* adapter.stage(profile, material.artifact, material.rollback);
        const fresh = yield* evidence
          .admissionMaterial(d.binding.queueId, d.preparationId, scope)
          .pipe(Effect.result);
        if (fresh._tag === "Failure") {
          yield* Effect.logWarning(
            "Deployment admission failed",
            new DeploymentOperationError({ stage: "admission", cause: fresh.failure }),
          );
          yield* save({
            state: claimIntent ? "deploy_unknown" : "rejected",
            claimState: claimIntent ? "unknown" : "reserved",
            reasons: ["source_changed"],
          });
          return;
        }
        if (!matches(p, fresh.success))
          return yield* new Adapter.ReleaseAdapterError({
            reason: "material_changed",
            stage: "admission",
          });
        yield* save({ state: "awaiting_claim", reasons: [] });
        yield* sql`UPDATE workbench_deployments SET claim_intent=1 WHERE id=${d.id}`;
        claimIntent = true;
        const committed = yield* adapter.claim(profile, reservation, input.claimToken);
        if (committed.state === "withdrawn") {
          yield* save({ state: "rejected", claimState: "withdrawn", reasons: ["claim_withdrawn"] });
          return;
        }
        if (committed.state !== "committed")
          return yield* new Adapter.ReleaseAdapterError({
            reason: "state_unknown",
            stage: "claim",
          });
        proof = committed.authorization;
      }
      if (!proof)
        return yield* new Adapter.ReleaseAdapterError({ reason: "permit_invalid", stage: "claim" });
      yield* sql`UPDATE workbench_deployments SET commit_json=${canonicalReleaseJson(proof)} WHERE id=${d.id}`;
      yield* save({ claimState: "committed", state: "deploying", reasons: [] });
    }
    if (
      !Adapter.verifyDeploymentSignature(proof, profile.policy.deployment!, "commit") ||
      proof.authorizationDigest !== d.authorizationDigest ||
      !same(proof.payload, p)
    )
      return yield* new Adapter.ReleaseAdapterError({ reason: "permit_invalid", stage: "commit" });
    if (d.claimState !== "committed") yield* save({ claimState: "committed" });
    const releaseFailure =
      (yield* acceptance.failureRequested(d)) || (yield* merge.failureRequested(d));
    const operation = Effect.fn("WorkbenchDeployment.operation")(function* (
      kind: "deploy" | "rollback",
      allowPut: boolean,
    ) {
      const id = kind === "deploy" ? p.deployOperationId : p.rollbackOperationId,
        previous = kind === "deploy" ? d.deployOperation : d.rollbackOperation;
      let observed = yield* adapter.readOperation(profile, id);
      if (!observed && allowPut) {
        const inputDigest = releaseDigest({
          authorizationDigest: d.authorizationDigest,
          kind,
          operationId: id,
        });
        yield* sql`INSERT INTO workbench_deployment_actions(deployment_id,kind,operation_id,input_digest,state) VALUES(${d.id},${kind},${id},${inputDigest},'intent') ON CONFLICT(deployment_id,kind) DO NOTHING`;
        observed = yield* adapter.putOperation(profile, proof!, kind);
      }
      if (!observed || !validOperation(observed, previous, p, d.authorizationDigest, kind))
        return yield* new Adapter.ReleaseAdapterError({ reason: "identity_mismatch", stage: kind });
      yield* sql`UPDATE workbench_deployment_actions SET state=${observed.state},receipt_json=${canonicalReleaseJson(observed)} WHERE deployment_id=${d.id} AND kind=${kind}`;
      yield* save(
        kind === "deploy" ? { deployOperation: observed } : { rollbackOperation: observed },
      );
      return observed;
    });
    const deploy = yield* operation(
      "deploy",
      !physicallySettled &&
        !alreadyDeployed &&
        d.state !== "rolled_back" &&
        d.state !== "failed_unchanged",
    );
    if (!terminal(deploy)) {
      yield* save({
        state: deploy.state === "unknown" ? "deploy_unknown" : "deploying",
        reasons: deploy.state === "unknown" ? ["state_unknown"] : [],
      });
      return;
    }
    const attributable = (target: WorkbenchDeploymentTarget) =>
      deploy.result?.writtenRevision !== null &&
      deploy.result?.writtenRevision !== undefined &&
      target.lastOperationId === deploy.id &&
      target.revision === deploy.result.writtenRevision;
    const reconcileRollback = Effect.fn("WorkbenchDeployment.reconcileRollback")(function* (
      before: WorkbenchDeploymentTarget,
    ) {
      yield* save({ state: "rolling_back", reasons: ["deployment_failed"] });
      const rollback = yield* operation(
        "rollback",
        (!physicallySettled || releaseFailure) && attributable(before),
      );
      if (!terminal(rollback)) {
        yield* save({
          state: rollback.state === "unknown" ? "rollback_unknown" : "rolling_back",
          reasons: rollback.state === "unknown" ? ["rollback_unknown"] : [],
        });
        return;
      }
      const final = yield* adapter.observe(profile);
      if (final.target.revision < before.revision)
        return yield* new Adapter.ReleaseAdapterError({
          reason: "state_unknown",
          stage: "rollback_revision",
        });
      const restored =
        rollback.state === "succeeded" &&
        rollback.result?.effect === "changed" &&
        rollback.result.writtenRevision !== null &&
        rollback.result.writtenRevision > 0 &&
        final.target.state === "consistent" &&
        final.target.lastOperationId === rollback.id &&
        final.target.revision === rollback.result.writtenRevision &&
        same(final.target.basis, p.baseline.basis) &&
        same(final.versionObservation.basis, p.baseline.basis);
      yield* save({
        ...final,
        observedAt: yield* now,
        state: restored ? "rolled_back" : "attention",
        reasons: restored ? [] : ["rollback_failed"],
      });
    });
    const target = yield* adapter.readTarget(profile);
    if (d.target && target.revision < d.target.revision)
      return yield* new Adapter.ReleaseAdapterError({
        reason: "state_unknown",
        stage: "target_revision",
      });
    yield* save({ target, observedAt: yield* now });
    const [rollbackAction] =
      yield* sql`SELECT operation_id FROM workbench_deployment_actions WHERE deployment_id=${d.id} AND kind='rollback'`;
    if (
      d.rollbackOperation ||
      rollbackAction ||
      d.state === "rolling_back" ||
      d.state === "rollback_unknown"
    ) {
      yield* reconcileRollback(target);
      return;
    }
    if (
      (!alreadyDeployed || releaseFailure) &&
      attributable(target) &&
      (releaseFailure ||
        deploy.state === "failed" ||
        deploy.result?.effect === "partial" ||
        target.state !== "consistent" ||
        !same(target.basis, p.desired))
    ) {
      yield* reconcileRollback(target);
      return;
    }
    const candidateActivation =
      target.state === "consistent" &&
      same(target.basis, p.desired) &&
      (deploy.result?.effect === "changed"
        ? attributable(target)
        : deploy.result?.effect === "unchanged" &&
          same(p.desired, p.baseline.basis) &&
          target.revision === p.baseline.revision);
    // The independent endpoint may be broken by the candidate. Durable failure
    // recovery and real probes must still run; only their successful coverage needs it.
    if (deploy.state === "succeeded" && (d.acceptance || candidateActivation))
      yield* acceptance.schedule(d.id);
    if (
      !releaseFailure &&
      profile.policy.merge &&
      material.evidence.merge &&
      d.acceptance?.state === "passed"
    ) {
      yield* merge.reconcile(d.id);
      return;
    }
    const observation = yield* adapter.observe(profile);
    if (observation.target.revision < target.revision)
      return yield* new Adapter.ReleaseAdapterError({
        reason: "state_unknown",
        stage: "target_revision",
      });
    yield* save({ ...observation, observedAt: yield* now });
    const verifiedTarget = observation.target,
      version = observation.versionObservation.basis;
    const baseline =
      verifiedTarget.state === "consistent" &&
      same(verifiedTarget.basis, p.baseline.basis) &&
      same(version, p.baseline.basis);
    const desired =
      verifiedTarget.state === "consistent" &&
      same(verifiedTarget.basis, p.desired) &&
      same(version, p.desired);
    const own = attributable(verifiedTarget);
    if (
      releaseFailure &&
      deploy.result?.effect === "unchanged" &&
      baseline &&
      verifiedTarget.revision === p.baseline.revision
    ) {
      yield* save({ state: "failed_unchanged", reasons: ["deployment_failed"] });
      return;
    }
    if (
      !releaseFailure &&
      deploy.state === "succeeded" &&
      desired &&
      (deploy.result?.effect === "changed"
        ? own
        : deploy.result?.effect === "unchanged" &&
          same(p.desired, p.baseline.basis) &&
          verifiedTarget.revision === p.baseline.revision)
    ) {
      yield* save({ state: "deployed", reasons: [] });
      return;
    }
    if (
      deploy.result?.effect === "unchanged" &&
      deploy.result.writtenRevision === null &&
      baseline &&
      verifiedTarget.revision === p.baseline.revision
    ) {
      yield* save({ state: "failed_unchanged", reasons: ["deployment_failed"] });
      return;
    }
    if ((!alreadyDeployed || releaseFailure) && own) {
      yield* reconcileRollback(verifiedTarget);
      return;
    }
    yield* save({ state: "attention", reasons: ["target_changed_unrelated"] });
  });
  const sync = Effect.gen(function* () {
    const rows =
      yield* sql<Row>`SELECT * FROM workbench_deployments WHERE state NOT IN ('rejected','rolled_back','failed_unchanged','delivered') ORDER BY created_at,id`;
    for (const candidate of rows)
      yield* locks.withLock(
        candidate.id,
        Effect.gen(function* () {
          const [row] =
            yield* sql<Row>`SELECT * FROM workbench_deployments WHERE id=${candidate.id}`;
          if (
            !row ||
            ["rejected", "rolled_back", "failed_unchanged", "delivered"].includes(row.state)
          )
            return;
          yield* step(row).pipe(
            Effect.catch((cause) =>
              Effect.gen(function* () {
                yield* Effect.logWarning(
                  "Deployment reconciliation failed",
                  new DeploymentOperationError({ stage: "reconcile", cause }),
                );
                const [latest] =
                  yield* sql<Row>`SELECT * FROM workbench_deployments WHERE id=${row.id}`;
                if (!latest) return;
                const d = yield* decodeDeployment(latest.deployment_json);
                const reason: WorkbenchDeploymentReason = isAdapterError(cause)
                  ? cause.reason
                  : "state_unknown";
                const observedStamp = yield* now;
                const next: DeploymentRecord = {
                  ...d,
                  state:
                    d.rollbackOperation ||
                    d.state === "rolling_back" ||
                    d.state === "rollback_unknown"
                      ? "rollback_unknown"
                      : d.claimState === "committed" || latest.claim_intent
                        ? "deploy_unknown"
                        : "attention",
                  reasons: [reason],
                  claimState:
                    d.claimState === "committed"
                      ? "committed"
                      : latest.claim_intent
                        ? "unknown"
                        : d.claimState,
                  version: d.version + 1,
                  updatedAt: logicalReleaseIso(d.updatedAt, Date.parse(observedStamp)),
                };
                yield* sql`UPDATE workbench_deployments SET deployment_json=${canonicalReleaseJson(next)},state=${next.state},updated_at=${next.updatedAt} WHERE id=${d.id} AND deployment_json=${latest.deployment_json}`;
              }),
            ),
          );
        }),
      );
  }).pipe(
    Effect.catch((cause) =>
      Effect.logWarning(
        "Deployment background reconciliation failed",
        new DeploymentOperationError({ stage: "sync", cause }),
      ),
    ),
  );
  const cached = yield* sql<Row>`SELECT * FROM workbench_deployments WHERE state='deployed'`;
  for (const row of cached) {
    const d = yield* decodeDeployment(row.deployment_json),
      wall = yield* now;
    const next: DeploymentRecord = {
      ...d,
      state: "deploy_unknown",
      reasons: ["state_unknown"],
      version: d.version + 1,
      updatedAt: logicalReleaseIso(d.updatedAt, Date.parse(wall)),
    };
    yield* sql`UPDATE workbench_deployments SET deployment_json=${canonicalReleaseJson(next)},state=${next.state},updated_at=${next.updatedAt} WHERE id=${d.id} AND deployment_json=${row.deployment_json}`;
  }
  yield* sync.pipe(Effect.repeat(Schedule.spaced("2 seconds")), Effect.forkIn(serviceScope));
  const awaitAcceptanceSample = Effect.fn("WorkbenchDeployment.awaitAcceptanceSample")(
    function* (queueId: string, id: string, scope: Scope, count: number) {
      yield* read(queueId, id, scope);
      yield* acceptance.awaitSample(id, count);
      return yield* get(queueId, id, scope);
    },
    Effect.catch((cause) => failure("get", cause)),
  );
  return WorkbenchDeployment.of({ admit, get, sync, awaitAcceptanceSample });
});
export const layer = Layer.effect(WorkbenchDeployment, make);
