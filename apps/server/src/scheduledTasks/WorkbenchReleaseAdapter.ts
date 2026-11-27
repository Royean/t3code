// @effect-diagnostics nodeBuiltinImport:off -- Ed25519 verifies the pinned coordinator authority without a private signing key.
import * as NodeCrypto from "node:crypto";
import {
  WorkbenchDeploymentDescriptor,
  WorkbenchDeploymentTarget,
  WorkbenchDeploymentPreflight,
  WorkbenchDeploymentOperation,
  WorkbenchDeploymentSigned,
  WorkbenchDeploymentBasis,
  WorkbenchDeploymentReason,
  WorkbenchMergePermit,
  type WorkbenchMergeClaimInput,
  type WorkbenchDeploymentHostPolicy,
  type WorkbenchDeploymentPlan,
  type WorkbenchDeploymentPreflightInput,
  type WorkbenchDeploymentEvidence,
  type WorkbenchReleaseHostProfile,
} from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Config from "../config.ts";
import {
  releaseDigest,
  releaseHash,
  canonicalReleaseJson,
  publicDeploymentPlan,
} from "./workbenchReleaseProfile.ts";

export class ReleaseAdapterError extends Schema.TaggedError<ReleaseAdapterError>()(
  "ReleaseAdapterError",
  {
    reason: WorkbenchDeploymentReason,
    stage: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message() {
    return `Release adapter operation failed (${this.stage}).`;
  }
}
const isAdapterError = Schema.is(ReleaseAdapterError);
const Credentials = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  credentials: Schema.Record(Schema.String, Schema.String),
});
const decodeCredentials = Schema.decodeEffect(Schema.fromJsonString(Credentials));
const decodeJson = Schema.decodeEffect(Schema.fromJsonString(Schema.Unknown));
const Artifact = Schema.Struct({ sha256: Schema.String, bytes: Schema.Int });
const Identity = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  configured: Schema.Boolean,
  id: Schema.NullOr(Schema.String),
  origin: Schema.NullOr(Schema.String),
  publicKey: Schema.NullOr(Schema.String),
});
const Claim = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  deploymentId: Schema.String,
  authorizationDigest: Schema.String,
  state: Schema.Literals(["reserved", "committed", "withdrawn"]),
  authorization: Schema.NullOr(WorkbenchDeploymentSigned),
});
type Claim = typeof Claim.Type;
const same = (a: unknown, b: unknown) => canonicalReleaseJson(a) === canonicalReleaseJson(b);
function deploymentKeyId(publicKey: string) {
  const key = NodeCrypto.createPublicKey(publicKey);
  if (key.asymmetricKeyType !== "ed25519") throw new TypeError("Unsupported release authority key");
  return releaseHash(key.export({ type: "spki", format: "der" }));
}
export function verifyDeploymentSignature(
  signed: WorkbenchDeploymentSigned,
  policy: WorkbenchDeploymentHostPolicy,
  kind: "reservation" | "commit",
) {
  const { signature, ...unsigned } = signed;
  try {
    return (
      signed.kind === kind &&
      signed.keyId === policy.coordinator.id &&
      deploymentKeyId(policy.coordinator.publicKey) === policy.coordinator.id &&
      signed.authorizationDigest === releaseDigest(signed.payload) &&
      same(signed.payload.issuer, {
        id: policy.coordinator.id,
        origin: policy.coordinator.origin,
      }) &&
      (kind === "reservation"
        ? signed.committedAt === null
        : signed.committedAt !== null && signed.committedAt >= signed.payload.createdAt) &&
      Buffer.from(signature, "base64url").length === 64 &&
      Buffer.from(signature, "base64url").toString("base64url") === signature &&
      NodeCrypto.verify(
        null,
        Buffer.from(releaseDigest(unsigned), "utf8"),
        policy.coordinator.publicKey,
        Buffer.from(signature, "base64url"),
      )
    );
  } catch {
    return false;
  }
}
export function verifyMergePermit(
  signed: WorkbenchMergePermit,
  profile: WorkbenchReleaseHostProfile,
  input: WorkbenchMergeClaimInput,
) {
  const policy = profile.policy.deployment;
  if (!policy) return false;
  const { signature, ...unsigned } = signed;
  try {
    return (
      signed.kind === "merge_commit" &&
      signed.keyId === policy.coordinator.id &&
      signed.authorizationDigest === input.input.authorizationDigest &&
      same(signed.payload, input) &&
      deploymentKeyId(policy.coordinator.publicKey) === signed.keyId &&
      NodeCrypto.verify(
        null,
        Buffer.from(releaseDigest(unsigned), "utf8"),
        policy.coordinator.publicKey,
        Buffer.from(signature, "base64url"),
      )
    );
  } catch {
    return false;
  }
}
function deploymentDescriptor(
  plan: WorkbenchDeploymentPlan,
  serviceId: string,
): WorkbenchDeploymentDescriptor {
  return {
    schemaVersion: 1,
    adapterId: plan.adapterId,
    adapterVersion: plan.adapterVersion,
    generationId: plan.generationId,
    serviceKey: plan.serviceKey,
    serviceId,
    coordinatorId: plan.coordinator.id,
    deploy: plan.deploy,
    rollback: { actionId: plan.rollback.actionId, planDigest: plan.rollback.planDigest },
  };
}
export function deploymentPreflightInput(
  id: string,
  plan: WorkbenchDeploymentPlan,
  sourceSha: string,
  artifact: { sha256: string; bytes: number },
  baseline: {
    basis: typeof WorkbenchDeploymentBasis.Type;
    revision: number;
    artifact: { sha256: string; bytes: number };
  },
): WorkbenchDeploymentPreflightInput {
  const unsigned = {
    schemaVersion: 1 as const,
    id,
    descriptorDigest: releaseDigest(deploymentDescriptor(plan, baseline.basis.serviceId)),
    serviceKey: plan.serviceKey,
    sourceSha,
    artifact,
    baseline,
    deployPlanDigest: plan.deploy.planDigest,
    rollbackPlanDigest: plan.rollback.planDigest,
  };
  return { ...unsigned, inputDigest: releaseDigest(unsigned) };
}
type Observation = {
  target: WorkbenchDeploymentTarget;
  versionObservation: { basis: typeof WorkbenchDeploymentBasis.Type; observedAt: string };
};
export class WorkbenchReleaseAdapter extends Context.Service<
  WorkbenchReleaseAdapter,
  {
    readonly inspect: (
      profile: WorkbenchReleaseHostProfile,
    ) => Effect.Effect<
      { descriptor: WorkbenchDeploymentDescriptor; target: WorkbenchDeploymentTarget },
      ReleaseAdapterError
    >;
    readonly prepare: (
      profile: WorkbenchReleaseHostProfile,
      id: string,
      sourceSha: string,
      artifact: Uint8Array,
      rollback: Uint8Array,
      baseline: typeof WorkbenchDeploymentBasis.Type,
    ) => Effect.Effect<WorkbenchDeploymentEvidence, ReleaseAdapterError>;
    readonly verify: (
      profile: WorkbenchReleaseHostProfile,
      evidence: WorkbenchDeploymentEvidence,
      input: WorkbenchDeploymentPreflightInput,
    ) => Effect.Effect<void, ReleaseAdapterError>;
    readonly stage: (
      profile: WorkbenchReleaseHostProfile,
      artifact: Uint8Array,
      rollback: Uint8Array,
    ) => Effect.Effect<void, ReleaseAdapterError>;
    readonly readTarget: (
      profile: WorkbenchReleaseHostProfile,
    ) => Effect.Effect<WorkbenchDeploymentTarget, ReleaseAdapterError>;
    readonly observe: (
      profile: WorkbenchReleaseHostProfile,
    ) => Effect.Effect<Observation, ReleaseAdapterError>;
    readonly readOperation: (
      profile: WorkbenchReleaseHostProfile,
      id: string,
    ) => Effect.Effect<WorkbenchDeploymentOperation | null, ReleaseAdapterError>;
    readonly putOperation: (
      profile: WorkbenchReleaseHostProfile,
      authorization: WorkbenchDeploymentSigned,
      kind: "deploy" | "rollback",
    ) => Effect.Effect<WorkbenchDeploymentOperation, ReleaseAdapterError>;
    readonly readClaim: (
      profile: WorkbenchReleaseHostProfile,
      reservation: WorkbenchDeploymentSigned,
      token: string,
    ) => Effect.Effect<Claim, ReleaseAdapterError>;
    readonly claim: (
      profile: WorkbenchReleaseHostProfile,
      reservation: WorkbenchDeploymentSigned,
      token: string,
    ) => Effect.Effect<Claim, ReleaseAdapterError>;
    readonly claimMerge: (
      profile: WorkbenchReleaseHostProfile,
      reservation: WorkbenchDeploymentSigned,
      token: string,
      input: WorkbenchMergeClaimInput,
    ) => Effect.Effect<WorkbenchMergePermit, ReleaseAdapterError>;
  }
>()("t3/scheduledTasks/WorkbenchReleaseAdapter") {}

const make = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem,
    path = yield* Path.Path,
    config = yield* Config.ServerConfig,
    sql = yield* SqlClient.SqlClient;
  const policy = (profile: WorkbenchReleaseHostProfile) => profile.policy.deployment;
  const credential = Effect.fn("WorkbenchReleaseAdapter.credential")(
    function* (p: WorkbenchDeploymentHostPolicy) {
      const file = path.join(config.stateDir, "workbench-release-adapter-credentials.json");
      const stat = yield* fs.stat(file);
      if (stat.type !== "File" || stat.size > 1024 * 1024)
        return yield* new ReleaseAdapterError({ reason: "not_configured", stage: "credential" });
      const decoded = yield* decodeCredentials(yield* fs.readFileString(file));
      const token = decoded.credentials[p.adapter.credentialRef];
      if (!token || token.length > 4096 || /[^\x21-\x7e]/.test(token))
        return yield* new ReleaseAdapterError({ reason: "not_configured", stage: "credential" });
      return token;
    },
    Effect.mapError((cause) =>
      isAdapterError(cause)
        ? cause
        : new ReleaseAdapterError({ reason: "not_configured", stage: "credential", cause }),
    ),
  );
  const rpc = Effect.fn("WorkbenchReleaseAdapter.rpc")(
    function* <A>(
      schema: Schema.Codec<A, unknown>,
      origin: string,
      route: string,
      stage: string,
      options: {
        token?: string;
        method?: string;
        body?: string | Uint8Array;
        binary?: boolean;
        missing?: boolean;
      } = {},
    ) {
      const result = yield* Effect.tryPromise({
        try: async (signal) => {
          // @effect-diagnostics-next-line globalFetchInEffect:off -- Fixed-origin adapter disables redirects, bounds bytes and forwards scoped cancellation.
          const response = await fetch(new URL(route, origin), {
            method: options.method ?? "GET",
            redirect: "error",
            credentials: "omit",
            signal,
            headers: {
              accept: "application/json",
              ...(options.token ? { authorization: `Bearer ${options.token}` } : {}),
              ...(options.body
                ? {
                    "content-type": options.binary
                      ? "application/octet-stream"
                      : "application/json",
                  }
                : {}),
            },
            ...(options.body
              ? {
                  body: typeof options.body === "string" ? options.body : Buffer.from(options.body),
                }
              : {}),
          });
          const reader = response.body?.getReader();
          let bytes = 0;
          const chunks: Uint8Array[] = [];
          if (reader) {
            try {
              while (true) {
                const chunk = await reader.read();
                if (chunk.done) break;
                bytes += chunk.value.length;
                if (bytes > 256 * 1024) throw new RangeError("Adapter response limit");
                chunks.push(chunk.value);
              }
            } finally {
              await reader.cancel();
            }
          }
          return { status: response.status, text: Buffer.concat(chunks).toString("utf8") };
        },
        catch: (cause) =>
          new ReleaseAdapterError({
            reason: stage.startsWith("coordinator")
              ? "coordinator_unavailable"
              : "adapter_unavailable",
            stage,
            cause,
          }),
      }).pipe(Effect.timeout(options.binary ? "120 seconds" : "10 seconds"));
      const body = yield* decodeJson(result.text);
      if (
        result.status === 404 &&
        options.missing &&
        typeof body === "object" &&
        body !== null &&
        (body as { _tag?: string })._tag === "ReleaseAdapterError" &&
        (body as { reason?: string }).reason === "not_found"
      )
        return null;
      if (result.status !== 200 && result.status !== 201 && result.status !== 202)
        return yield* new ReleaseAdapterError({
          reason:
            result.status === 403
              ? stage.startsWith("coordinator")
                ? "coordinator_forbidden"
                : "permit_invalid"
              : result.status === 409
                ? "adapter_conflict"
                : stage.startsWith("coordinator")
                  ? "coordinator_unavailable"
                  : "adapter_unavailable",
          stage,
        });
      return yield* Schema.decodeUnknownEffect(schema)(body, { onExcessProperty: "error" }).pipe(
        Effect.mapError(
          (cause) =>
            new ReleaseAdapterError({
              reason: stage.startsWith("coordinator")
                ? "coordinator_identity_mismatch"
                : "adapter_identity_mismatch",
              stage,
              cause,
            }),
        ),
      );
    },
    Effect.mapError((cause) =>
      isAdapterError(cause)
        ? cause
        : new ReleaseAdapterError({ reason: "adapter_unavailable", stage: "rpc", cause }),
    ),
  );
  const adapterRpc = Effect.fn("WorkbenchReleaseAdapter.adapterRpc")(function* <A>(
    profile: WorkbenchReleaseHostProfile,
    schema: Schema.Codec<A, unknown>,
    route: string,
    options: {
      method?: string;
      body?: string | Uint8Array;
      binary?: boolean;
      missing?: boolean;
    } = {},
  ) {
    const p = policy(profile);
    if (!p) return yield* new ReleaseAdapterError({ reason: "not_configured", stage: "adapter" });
    const token = yield* credential(p);
    return yield* rpc(schema, p.adapter.origin, `${p.adapter.pathPrefix}${route}`, "adapter", {
      ...options,
      token,
    });
  });
  const inspectAdapter = Effect.fn("WorkbenchReleaseAdapter.inspectAdapter")(function* (
    profile: WorkbenchReleaseHostProfile,
  ) {
    const p = policy(profile);
    if (!p) return yield* new ReleaseAdapterError({ reason: "not_configured", stage: "inspect" });
    const descriptor = yield* adapterRpc(profile, WorkbenchDeploymentDescriptor, "/descriptor"),
      target = yield* adapterRpc(profile, WorkbenchDeploymentTarget, "/target");
    const expected = deploymentDescriptor(publicDeploymentPlan(p), profile.policy.target.serviceId);
    if (
      !descriptor ||
      !same(descriptor, expected) ||
      !target ||
      target.adapterId !== expected.adapterId ||
      target.adapterVersion !== expected.adapterVersion ||
      target.generationId !== expected.generationId ||
      target.serviceKey !== expected.serviceKey ||
      target.serviceId !== expected.serviceId
    )
      return yield* new ReleaseAdapterError({
        reason: "adapter_identity_mismatch",
        stage: "inspect",
      });
    return { descriptor, target };
  });
  const inspect = Effect.fn("WorkbenchReleaseAdapter.inspect")(
    function* (profile: WorkbenchReleaseHostProfile) {
      const p = policy(profile);
      if (!p) return yield* new ReleaseAdapterError({ reason: "not_configured", stage: "inspect" });
      const identity = yield* rpc(
        Identity,
        p.coordinator.origin,
        "/.well-known/workbench-release-coordinator",
        "coordinator_identity",
      );
      const identityPublicKey = identity?.publicKey;
      const identityKeyId = identityPublicKey
        ? yield* Effect.try({
            try: () => deploymentKeyId(identityPublicKey),
            catch: (cause) =>
              new ReleaseAdapterError({
                reason: "coordinator_identity_mismatch",
                stage: "identity",
                cause,
              }),
          })
        : null;
      if (
        !identity ||
        !identity.configured ||
        identity.id !== p.coordinator.id ||
        identity.origin !== p.coordinator.origin ||
        !identity.publicKey ||
        identityKeyId !== p.coordinator.id
      )
        return yield* new ReleaseAdapterError({
          reason: "coordinator_identity_mismatch",
          stage: "inspect",
        });
      return yield* inspectAdapter(profile);
    },
    Effect.mapError((cause) =>
      isAdapterError(cause)
        ? cause
        : new ReleaseAdapterError({
            reason: "coordinator_identity_mismatch",
            stage: "inspect",
            cause,
          }),
    ),
  );
  const artifact = Effect.fn("WorkbenchReleaseAdapter.artifact")(function* (
    profile: WorkbenchReleaseHostProfile,
    sha256: string,
    bytes: number,
    data?: Uint8Array,
  ) {
    let read = yield* adapterRpc(profile, Artifact, `/artifacts/${sha256}`, { missing: true });
    if (!read && data)
      read = yield* adapterRpc(profile, Artifact, `/artifacts/${sha256}`, {
        method: "PUT",
        body: data,
        binary: true,
      });
    if (!read || read.sha256 !== sha256 || read.bytes !== bytes)
      return yield* new ReleaseAdapterError({ reason: "artifact_invalid", stage: "artifact" });
  });
  const stage = Effect.fn("WorkbenchReleaseAdapter.stage")(function* (
    profile: WorkbenchReleaseHostProfile,
    candidate: Uint8Array,
    rollback: Uint8Array,
  ) {
    yield* artifact(profile, releaseHash(candidate), candidate.length, candidate);
    yield* artifact(profile, releaseHash(rollback), rollback.length, rollback);
  });
  const checkPreflight = (
    p: WorkbenchDeploymentPreflight,
    input: WorkbenchDeploymentPreflightInput,
    profile: WorkbenchReleaseHostProfile,
  ) => {
    const policy = profile.policy.deployment!;
    return (
      p.id === input.id &&
      p.inputDigest === input.inputDigest &&
      p.adapterId === policy.adapter.id &&
      p.adapterVersion === policy.adapter.version &&
      p.generationId === policy.adapter.generationId &&
      p.serviceKey === policy.serviceKey &&
      (p.state === "succeeded" || p.state === "failed"
        ? p.result !== null && Date.parse(p.result.completedAt) >= Date.parse(p.createdAt)
        : p.result === null)
    );
  };
  const prepare = Effect.fn("WorkbenchReleaseAdapter.prepare")(
    function* (
      profile: WorkbenchReleaseHostProfile,
      id: string,
      sourceSha: string,
      candidate: Uint8Array,
      rollback: Uint8Array,
      baseline: typeof WorkbenchDeploymentBasis.Type,
    ) {
      const inspected = yield* inspect(profile),
        plan = publicDeploymentPlan(profile.policy.deployment!);
      if (inspected.target.state !== "consistent" || !same(inspected.target.basis, baseline))
        return yield* new ReleaseAdapterError({ reason: "target_changed", stage: "preflight" });
      const input = deploymentPreflightInput(
        `preflight:${releaseHash(id)}`,
        plan,
        sourceSha,
        { sha256: releaseHash(candidate), bytes: candidate.length },
        {
          basis: baseline,
          revision: inspected.target.revision,
          artifact: { sha256: releaseHash(rollback), bytes: rollback.length },
        },
      );
      yield* sql`INSERT INTO workbench_release_adapter_preflights(preparation_id,input_json,state) VALUES(${id},${canonicalReleaseJson(input)},'intent') ON CONFLICT(preparation_id) DO NOTHING`;
      const [prior] = yield* sql<{
        input_json: string;
      }>`SELECT input_json FROM workbench_release_adapter_preflights WHERE preparation_id=${id}`;
      if (prior?.input_json !== canonicalReleaseJson(input))
        return yield* new ReleaseAdapterError({ reason: "conflict", stage: "preflight" });
      yield* stage(profile, candidate, rollback);
      let preflight = yield* adapterRpc(
        profile,
        WorkbenchDeploymentPreflight,
        `/preflights/${encodeURIComponent(input.id)}`,
        { missing: true },
      );
      if (!preflight)
        preflight = yield* adapterRpc(
          profile,
          WorkbenchDeploymentPreflight,
          `/preflights/${encodeURIComponent(input.id)}`,
          { method: "PUT", body: canonicalReleaseJson(input) },
        );
      while (preflight && ["queued", "running"].includes(preflight.state)) {
        if (!checkPreflight(preflight, input, profile))
          return yield* new ReleaseAdapterError({ reason: "preflight_failed", stage: "preflight" });
        yield* Effect.sleep("250 millis");
        preflight = yield* adapterRpc(
          profile,
          WorkbenchDeploymentPreflight,
          `/preflights/${encodeURIComponent(input.id)}`,
        );
      }
      if (
        !preflight ||
        !checkPreflight(preflight, input, profile) ||
        preflight.state !== "succeeded" ||
        preflight.result?.exitCode !== 0
      )
        return yield* new ReleaseAdapterError({ reason: "preflight_failed", stage: "preflight" });
      yield* sql`UPDATE workbench_release_adapter_preflights SET state='completed',receipt_json=${canonicalReleaseJson(preflight)} WHERE preparation_id=${id}`;
      return {
        plan,
        desired: {
          serviceId: baseline.serviceId,
          version: `sha256-${releaseHash(candidate)}`,
          sourceSha,
          artifactSha256: releaseHash(candidate),
        },
        preflight,
        baselineRevision: inspected.target.revision,
      };
    },
    Effect.timeout("20 minutes"),
    Effect.mapError((cause) =>
      isAdapterError(cause)
        ? cause
        : new ReleaseAdapterError({ reason: "preflight_failed", stage: "preflight", cause }),
    ),
  );
  const verify = Effect.fn("WorkbenchReleaseAdapter.verify")(function* (
    profile: WorkbenchReleaseHostProfile,
    evidence: WorkbenchDeploymentEvidence,
    input: WorkbenchDeploymentPreflightInput,
  ) {
    const current = yield* inspect(profile);
    if (
      !same(evidence.plan, publicDeploymentPlan(profile.policy.deployment!)) ||
      current.target.revision !== evidence.baselineRevision ||
      current.target.state !== "consistent" ||
      !same(current.target.basis, input.baseline.basis)
    )
      return yield* new ReleaseAdapterError({ reason: "target_changed", stage: "verify" });
    yield* artifact(profile, input.artifact.sha256, input.artifact.bytes);
    yield* artifact(profile, input.baseline.artifact.sha256, input.baseline.artifact.bytes);
    const preflight = yield* adapterRpc(
      profile,
      WorkbenchDeploymentPreflight,
      `/preflights/${encodeURIComponent(input.id)}`,
    );
    if (
      !preflight ||
      !checkPreflight(preflight, input, profile) ||
      !same(preflight, evidence.preflight) ||
      preflight.state !== "succeeded" ||
      preflight.result?.exitCode !== 0
    )
      return yield* new ReleaseAdapterError({ reason: "preflight_failed", stage: "verify" });
  });
  const readTarget = Effect.fn("WorkbenchReleaseAdapter.readTarget")(function* (
    profile: WorkbenchReleaseHostProfile,
  ) {
    return (yield* inspectAdapter(profile)).target;
  });
  const observe = Effect.fn("WorkbenchReleaseAdapter.observe")(function* (
    profile: WorkbenchReleaseHostProfile,
  ) {
    const target = yield* readTarget(profile),
      basis = yield* rpc(
        WorkbenchDeploymentBasis,
        profile.policy.target.origin,
        profile.policy.target.versionPath,
        "target",
      );
    if (!basis || basis.serviceId !== profile.policy.target.serviceId)
      return yield* new ReleaseAdapterError({ reason: "target_mismatch", stage: "target" });
    return {
      target,
      versionObservation: { basis, observedAt: DateTime.formatIso(yield* DateTime.now) },
    };
  });
  const readOperation = Effect.fn("WorkbenchReleaseAdapter.readOperation")(function* (
    profile: WorkbenchReleaseHostProfile,
    id: string,
  ) {
    return yield* adapterRpc(
      profile,
      WorkbenchDeploymentOperation,
      `/operations/${encodeURIComponent(id)}`,
      { missing: true },
    );
  });
  const putOperation = Effect.fn("WorkbenchReleaseAdapter.putOperation")(function* (
    profile: WorkbenchReleaseHostProfile,
    authorization: WorkbenchDeploymentSigned,
    kind: "deploy" | "rollback",
  ) {
    const p = policy(profile);
    if (!p || !verifyDeploymentSignature(authorization, p, "commit"))
      return yield* new ReleaseAdapterError({ reason: "permit_invalid", stage: "operation" });
    yield* inspectAdapter(profile);
    const operationId =
      kind === "deploy"
        ? authorization.payload.deployOperationId
        : authorization.payload.rollbackOperationId;
    const operation = yield* adapterRpc(
      profile,
      WorkbenchDeploymentOperation,
      `/operations/${encodeURIComponent(operationId)}`,
      {
        method: "PUT",
        body: canonicalReleaseJson({
          schemaVersion: 1,
          kind,
          inputDigest: releaseDigest({
            authorizationDigest: authorization.authorizationDigest,
            kind,
            operationId,
          }),
          authorization,
        }),
      },
    );
    if (!operation)
      return yield* new ReleaseAdapterError({ reason: "state_unknown", stage: "operation" });
    return operation;
  });
  const claimRpc = Effect.fn("WorkbenchReleaseAdapter.claim")(function* (
    profile: WorkbenchReleaseHostProfile,
    reservation: WorkbenchDeploymentSigned,
    token: string,
    commit: boolean,
  ) {
    const p = policy(profile);
    if (!p)
      return yield* new ReleaseAdapterError({
        reason: "not_configured",
        stage: "coordinator_claim",
      });
    const claim = yield* rpc(
      Claim,
      p.coordinator.origin,
      `/api/release-coordinator/v1/claims/${encodeURIComponent(reservation.payload.deploymentId)}`,
      "coordinator_claim",
      {
        token,
        ...(commit
          ? {
              method: "POST",
              body: canonicalReleaseJson({
                nativeDeploymentId: reservation.payload.nativeDeploymentId,
                authorizationDigest: reservation.authorizationDigest,
              }),
            }
          : {}),
      },
    );
    if (
      !claim ||
      claim.deploymentId !== reservation.payload.deploymentId ||
      claim.authorizationDigest !== reservation.authorizationDigest ||
      (claim.state === "committed"
        ? !claim.authorization ||
          claim.authorization.authorizationDigest !== reservation.authorizationDigest ||
          !same(claim.authorization.payload, reservation.payload) ||
          !verifyDeploymentSignature(claim.authorization, p, "commit")
        : claim.authorization !== null)
    )
      return yield* new ReleaseAdapterError({
        reason: "permit_invalid",
        stage: "coordinator_claim",
      });
    return claim;
  });
  return WorkbenchReleaseAdapter.of({
    inspect,
    prepare,
    verify,
    stage,
    observe,
    readTarget,
    readOperation,
    putOperation,
    readClaim: Effect.fn("WorkbenchReleaseAdapter.readClaim")((p, r, t) =>
      claimRpc(p, r, t, false),
    ),
    claim: Effect.fn("WorkbenchReleaseAdapter.commitClaim")((p, r, t) => claimRpc(p, r, t, true)),
    claimMerge: Effect.fn("WorkbenchReleaseAdapter.claimMerge")(
      function* (profile, reservation, token, input) {
        const p = policy(profile);
        if (!p)
          return yield* new ReleaseAdapterError({
            reason: "not_configured",
            stage: "coordinator_merge",
          });
        const permit = yield* rpc(
          WorkbenchMergePermit,
          p.coordinator.origin,
          `/api/release-coordinator/v1/claims/${encodeURIComponent(reservation.payload.deploymentId)}/merge`,
          "coordinator_merge",
          { token, method: "POST", body: canonicalReleaseJson(input) },
        );
        if (
          !permit ||
          !verifyMergePermit(permit, profile, input) ||
          permit.authorizationDigest !== reservation.authorizationDigest
        )
          return yield* new ReleaseAdapterError({
            reason: "permit_invalid",
            stage: "coordinator_merge",
          });
        return permit;
      },
    ),
  });
});
export const layer = Layer.effect(WorkbenchReleaseAdapter, make);
