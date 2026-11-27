// @effect-diagnostics nodeBuiltinImport:off -- Sealed source uses streamed Node digests and the host's fixed Node executable.
import * as NodeCrypto from "node:crypto";
import {
  type WorkbenchAcceptancePolicy,
  type WorkbenchEvidenceVerification,
  type WorkbenchOnlineReceipt,
  type WorkbenchReleaseHostProfile,
  type WorkbenchReleaseEvidence,
  type WorkbenchDeploymentBasis,
} from "@t3tools/contracts";
import * as Clock from "effect/Clock";
import * as DateTime from "effect/DateTime";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as Stream from "effect/Stream";
import * as Runner from "../processRunner.ts";
import {
  canonicalReleaseJson,
  releaseDigest,
  releaseHash,
  releaseBlobHash,
  releaseScriptHash,
  releaseCommandArgv,
  safeReleasePath,
} from "./workbenchReleaseProfile.ts";

export const acceptancePolicy = (
  o: WorkbenchReleaseHostProfile["policy"]["observation"],
): WorkbenchAcceptancePolicy => ({
  semanticsVersion: 1,
  sampling: "health_then_core",
  ...o,
  maxGapSeconds: o.intervalSeconds * 2,
  maxSamples: 131072,
});
export class VerificationError extends Schema.TaggedError<VerificationError>()(
  "VerificationError",
  {
    reason: Schema.Literals(["material_missing", "material_invalid"]),
    stage: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message() {
    return `Verification material operation failed (${this.stage}).`;
  }
}
const isVerificationError = Schema.is(VerificationError);
const Script = Schema.Struct({
  scriptId: Schema.String,
  definitionHash: Schema.String,
  command: Schema.String.check(Schema.isMaxLength(4096)),
});
const Manifest = Schema.Struct({
  schemaVersion: Schema.Literal(1),
  semanticsVersion: Schema.Literal(1),
  sourceSha: Schema.String,
  treeSha: Schema.String,
  archive: Schema.Struct({ sha256: Schema.String, bytes: Schema.Int }),
  tracked: Schema.Array(
    Schema.Struct({
      path: Schema.String,
      mode: Schema.Literals(["100644", "100755"]),
      blobSha: Schema.String,
    }),
  ).check(Schema.isMaxLength(20000)),
  health: Script,
  core: Script,
  policyDigest: Schema.String,
  execution: Schema.Struct({
    envVersion: Schema.Literal(1),
    timeoutSeconds: Schema.Literal(120),
    maxOutputBytes: Schema.Literal(1048576),
  }),
});
const decodeManifest = Schema.decodeEffect(Schema.fromJsonString(Manifest));
type Tracked = (typeof Manifest.Type)["tracked"];
export type OnlineCheckInput = {
  sampleId: string;
  executionId: string;
  role: "health" | "core";
  authorizationDigest: string;
  runnerEpoch: string;
  candidate: typeof WorkbenchDeploymentBasis.Type;
  activationRevision: number;
};
export const clockPair = Effect.gen(function* () {
  const clock = yield* Clock.Clock;
  return yield* Effect.sync(() => ({
    wall: clock.currentTimeMillisUnsafe(),
    mono: clock.monotonicTimeNanosUnsafe(),
  }));
});
export const auditIso = (millis: number) => DateTime.formatIso(DateTime.makeUnsafe(millis));

export const make = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem,
    path = yield* Path.Path,
    runner = yield* Runner.ProcessRunner;
  const regular = Effect.fn("ReleaseVerification.regular")(function* (
    root: string,
    relative: string,
    limit: number,
  ) {
    if (!safeReleasePath(relative))
      return yield* new VerificationError({ reason: "material_invalid", stage: "path" });
    let file = root;
    for (const component of relative.split("/")) {
      file = path.join(file, component);
      const info = yield* fs.stat(file);
      if (info.type === "SymbolicLink" || (yield* fs.realPath(file)) !== file)
        return yield* new VerificationError({ reason: "material_invalid", stage: "path" });
    }
    const info = yield* fs.stat(file);
    if (info.type !== "File" || info.size > limit)
      return yield* new VerificationError({ reason: "material_invalid", stage: "size" });
    return { file, size: Number(info.size) };
  });
  const hashFile = Effect.fn("ReleaseVerification.hashFile")(function* (
    root: string,
    relative: string,
    limit: number,
  ) {
    const info = yield* regular(root, relative, limit),
      hash = NodeCrypto.createHash("sha256");
    let bytes = 0;
    yield* Stream.runForEach(fs.stream(info.file), (chunk) =>
      Effect.sync(() => {
        bytes += chunk.length;
        hash.update(chunk);
      }),
    );
    if (bytes !== info.size || bytes > limit)
      return yield* new VerificationError({ reason: "material_invalid", stage: "bytes" });
    return { sha256: hash.digest("hex"), bytes };
  });
  const seal = Effect.fn("ReleaseVerification.seal")(function* (input: {
    directory: string;
    preparationId: string;
    sourceSha: string;
    treeSha: string;
    tracked: Tracked;
    health: { id: string; command: string };
    core: { id: string; command: string };
    profile: WorkbenchReleaseHostProfile;
    originalSourceRoots: ReadonlyArray<string>;
  }) {
    for (const script of [input.health, input.core]) {
      const argv = releaseCommandArgv(script.command);
      if (
        !argv ||
        argv.some((arg) =>
          input.originalSourceRoots.some((root) => arg === root || arg.startsWith(root + "/")),
        )
      )
        return yield* new VerificationError({ reason: "material_invalid", stage: "mutable_argv" });
    }
    const archive = yield* hashFile(input.directory, "source.tar", 268435456);
    if (archive.bytes < 1 || input.tracked.length > 20000)
      return yield* new VerificationError({ reason: "material_invalid", stage: "archive" });
    const script = (s: { id: string; command: string }) => ({
      scriptId: s.id,
      definitionHash: releaseScriptHash(s),
      command: s.command,
    });
    const policyDigest = releaseDigest(acceptancePolicy(input.profile.policy.observation));
    const manifest = {
      schemaVersion: 1,
      semanticsVersion: 1,
      sourceSha: input.sourceSha,
      treeSha: input.treeSha,
      archive,
      tracked: input.tracked,
      health: script(input.health),
      core: script(input.core),
      policyDigest,
      execution: { envVersion: 1, timeoutSeconds: 120, maxOutputBytes: 1048576 },
    };
    const json = canonicalReleaseJson(manifest);
    if (new TextEncoder().encode(json).length > 8388608)
      return yield* new VerificationError({ reason: "material_invalid", stage: "manifest" });
    yield* fs.copyFile(
      path.join(input.directory, "source.tar"),
      path.join(input.directory, "verification.tar"),
    );
    yield* fs.chmod(path.join(input.directory, "verification.tar"), 0o400);
    yield* fs.writeFileString(path.join(input.directory, "verification.json"), json, {
      mode: 0o400,
    });
    const material: WorkbenchEvidenceVerification = {
      schemaVersion: 1,
      semanticsVersion: 1,
      ref: `verification:${releaseHash(input.preparationId)}`,
      manifestDigest: releaseDigest(manifest),
      archive,
      sourceTreeSha: input.treeSha,
      policyDigest,
    };
    return material;
  });
  const validate = Effect.fn("ReleaseVerification.validate")(
    function* (
      directory: string,
      evidence: WorkbenchReleaseEvidence,
      profile: WorkbenchReleaseHostProfile,
    ) {
      const material = evidence.verification;
      if (!material)
        return yield* new VerificationError({ reason: "material_missing", stage: "manifest" });
      const info = yield* regular(directory, "verification.json", 8388608),
        manifest = yield* decodeManifest(yield* fs.readFileString(info.file));
      if (
        releaseDigest(manifest) !== material.manifestDigest ||
        manifest.sourceSha !== evidence.source.commitSha ||
        manifest.treeSha !== evidence.source.treeSha ||
        material.sourceTreeSha !== manifest.treeSha ||
        releaseDigest(acceptancePolicy(profile.policy.observation)) !== manifest.policyDigest ||
        material.policyDigest !== manifest.policyDigest ||
        canonicalReleaseJson(manifest.archive) !== canonicalReleaseJson(material.archive)
      )
        return yield* new VerificationError({ reason: "material_invalid", stage: "manifest" });
      const archive = yield* hashFile(directory, "verification.tar", 268435456);
      if (canonicalReleaseJson(archive) !== canonicalReleaseJson(material.archive))
        return yield* new VerificationError({ reason: "material_invalid", stage: "archive" });
      return manifest;
    },
    Effect.mapError((cause) =>
      isVerificationError(cause)
        ? cause
        : new VerificationError({ reason: "material_invalid", stage: "validate", cause }),
    ),
  );
  const check = Effect.fn("ReleaseVerification.check")(
    function* (
      directory: string,
      evidence: WorkbenchReleaseEvidence,
      profile: WorkbenchReleaseHostProfile,
      input: OnlineCheckInput,
    ) {
      const manifest = yield* validate(directory, evidence, profile),
        material = evidence.verification!;
      const definition = manifest[input.role],
        ref = profile.policy[input.role];
      if (
        definition.scriptId !== ref.scriptId ||
        definition.definitionHash !== ref.definitionHash ||
        releaseScriptHash({ id: definition.scriptId, command: definition.command }) !==
          ref.definitionHash
      )
        return yield* new VerificationError({ reason: "material_invalid", stage: "definition" });
      const argv = releaseCommandArgv(definition.command);
      if (!argv) return yield* new VerificationError({ reason: "material_invalid", stage: "argv" });
      const context = yield* fs.makeTempDirectoryScoped({ directory, prefix: "online-" });
      const unpack = yield* runner.run({
        command: "/usr/bin/tar",
        args: ["-xf", path.join(directory, "verification.tar"), "-C", context],
        env: { PATH: "/usr/bin:/bin" },
        extendEnv: false,
        timeout: "120 seconds",
        maxOutputBytes: 1048576,
      });
      if (unpack.code !== 0)
        return yield* new VerificationError({ reason: "material_invalid", stage: "unpack" });
      const verify = Effect.fn("ReleaseVerification.verifyCopy")(function* () {
        for (const entry of manifest.tracked) {
          const file = yield* regular(context, entry.path, 67108864),
            bytes = yield* fs.readFile(file.file);
          if (
            releaseBlobHash(bytes, entry.blobSha.length === 40 ? "sha1" : "sha256") !==
            entry.blobSha
          )
            return yield* new VerificationError({ reason: "material_invalid", stage: "tracked" });
        }
      });
      yield* verify();
      const started = yield* clockPair;
      const execution = yield* runner
        .run({
          command: argv[0]!,
          args: argv.slice(1),
          cwd: context,
          env: {
            PATH: path.dirname(process.execPath) + ":/usr/bin:/bin",
            HOME: context,
            TMPDIR: context,
            GIT_CEILING_DIRECTORIES: directory,
            GIT_CONFIG_NOSYSTEM: "1",
            GIT_CONFIG_GLOBAL: path.join(directory, "empty.gitconfig"),
            T3_RELEASE_PHASE: "online",
            T3_RELEASE_CHECK_ROLE: input.role,
            T3_RELEASE_EXECUTION_ID: input.executionId,
            T3_RELEASE_TARGET_ORIGIN: profile.policy.target.origin,
            T3_RELEASE_VERSION_PATH: profile.policy.target.versionPath,
            T3_RELEASE_EXPECTED_VERSION: input.candidate.version,
            T3_RELEASE_EXPECTED_ARTIFACT_SHA256: input.candidate.artifactSha256,
            T3_RELEASE_SOURCE_SHA: input.candidate.sourceSha,
            T3_RELEASE_ACTIVATION_REVISION: String(input.activationRevision),
            T3_RELEASE_ARTIFACT: path.join(directory, "artifact"),
            T3_RELEASE_ROLLBACK_ARTIFACT: path.join(directory, "rollback"),
          },
          extendEnv: false,
          timeout: "120 seconds",
          maxOutputBytes: 1048576,
          outputMode: "error",
        })
        .pipe(Effect.result);
      const completed = yield* clockPair;
      yield* verify();
      if (execution._tag === "Failure")
        yield* Effect.logWarning("Online check execution failed", execution.failure);
      const result = execution._tag === "Success" ? execution.success : null;
      const known =
        result &&
        result.code !== null &&
        !result.timedOut &&
        !result.stdoutTruncated &&
        !result.stderrTruncated &&
        !result.stdoutInvalidUtf8 &&
        !result.stderrInvalidUtf8;
      const receipt: WorkbenchOnlineReceipt = {
        schemaVersion: 1,
        id: `receipt:${releaseHash(input.executionId)}`,
        sampleId: input.sampleId,
        executionId: input.executionId,
        role: input.role,
        scriptId: definition.scriptId,
        definitionHash: definition.definitionHash,
        phase: "online",
        subject: "deployed_artifact",
        authorizationDigest: input.authorizationDigest,
        materialDigest: material.manifestDigest,
        candidate: input.candidate,
        activationRevision: input.activationRevision,
        runnerEpoch: input.runnerEpoch,
        state: known ? (result.code === 0 ? "succeeded" : "failed") : "unknown",
        exitCode: known ? result.code : null,
        outputDigest: known
          ? releaseDigest({ stdout: result.stdout, stderr: result.stderr })
          : null,
        startedAt: auditIso(started.wall),
        completedAt: auditIso(completed.wall),
        durationMilliseconds: Number((completed.mono - started.mono) / 1000000n),
      };
      return receipt;
    },
    Effect.scoped,
    Effect.mapError((cause) =>
      isVerificationError(cause)
        ? cause
        : new VerificationError({ reason: "material_invalid", stage: "check", cause }),
    ),
  );
  return { seal, check, validate };
});
