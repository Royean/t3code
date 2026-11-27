// @effect-diagnostics nodeBuiltinImport:off -- Content digests use Node's synchronous hash primitive.
import * as NodeCrypto from "node:crypto";
import { tokenizeCliArgs } from "@t3tools/shared/cliArgs";
import * as HostProcess from "@t3tools/shared/HostProcess";
import {
  WorkbenchReleaseProfileFile,
  WorkbenchReleaseError,
  ProjectScript,
  type WorkbenchReleaseHostProfile,
  type WorkbenchPublicReleaseProfile,
  type WorkbenchDeploymentHostPolicy,
  type WorkbenchDeploymentPlan,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Config from "../config.ts";

class ReleaseProfileOperationError extends Schema.TaggedError<ReleaseProfileOperationError>()(
  "ReleaseProfileOperationError",
  {
    stage: Schema.Literals(["decode_profile", "decode_scripts"]),
    cause: Schema.Defect(),
  },
) {
  override get message() {
    return "Release profile decoding failed.";
  }
}
const decodeProfileFile = Schema.decodeEffect(Schema.fromJsonString(WorkbenchReleaseProfileFile));
const decodeProjectScripts = Schema.decodeEffect(
  Schema.fromJsonString(Schema.Array(ProjectScript)),
);

export function releaseCommandArgv(command: string): ReadonlyArray<string> | null {
  if (command.length > 4096 || /[\x00-\x1f\x7f;&|`<>$()]/.test(command)) return null;
  let quote: "'" | '"' | undefined;
  for (let i = 0; i < command.length; i++) {
    const char = command[i]!;
    if (quote === "'") {
      if (char === "'") quote = undefined;
      continue;
    }
    if (quote === '"') {
      if (char === '"') quote = undefined;
      else if (char === "\\" && ['"', "\\"].includes(command[i + 1] ?? "")) i++;
      continue;
    }
    if (char === "'" || char === '"') quote = char;
    else if (char === "\\") {
      if (!/[ '"\t]/.test(command[i + 1] ?? "") || i + 1 === command.length) return null;
      i++;
    } else if ("?*~{}#[]".includes(char)) return null;
  }
  if (quote !== undefined) return null;
  const tokens = tokenizeCliArgs(command);
  if (
    !tokens[0] ||
    tokens[0].includes("=") ||
    [
      "!",
      "if",
      "then",
      "else",
      "elif",
      "fi",
      "case",
      "esac",
      "for",
      "while",
      "until",
      "do",
      "done",
      "function",
      "time",
    ].includes(tokens[0])
  )
    return null;
  return tokens;
}

export function canonicalReleaseJson(value: unknown): string {
  if (value === null || typeof value === "boolean" || typeof value === "string")
    return JSON.stringify(value);
  if (typeof value === "number" && Number.isSafeInteger(value)) return JSON.stringify(value);
  if (Array.isArray(value)) return `[${value.map(canonicalReleaseJson).join(",")}]`;
  if (typeof value === "object" && value !== null)
    return `{${Object.keys(value)
      .sort()
      .map(
        (key) =>
          `${JSON.stringify(key)}:${canonicalReleaseJson((value as Record<string, unknown>)[key])}`,
      )
      .join(",")}}`;
  throw new TypeError("Unsupported release value");
}
export const releaseHash = (value: string | Uint8Array) =>
  NodeCrypto.createHash("sha256").update(value).digest("hex");
export const releaseScriptHash = (script: { id: string; command: string }) =>
  releaseHash(JSON.stringify({ id: script.id, command: script.command }));
export const releaseDigest = (value: unknown) => releaseHash(canonicalReleaseJson(value));
export const releaseBlobHash = (bytes: Uint8Array, algorithm: "sha1" | "sha256") =>
  NodeCrypto.createHash(algorithm).update(`blob ${bytes.byteLength}\0`).update(bytes).digest("hex");
export function safeReleasePath(value: string) {
  return (
    value.length <= 1024 &&
    !value.startsWith("/") &&
    !/[\\\x00-\x1f\x7f:]/.test(value) &&
    value.split("/").every((p) => p !== "" && p !== "." && p !== "..")
  );
}
function validReleaseTarget(target: { origin: string; versionPath: string }) {
  try {
    const url = new URL(target.origin);
    const host = url.hostname.toLowerCase();
    const parts = host.split(".").map(Number);
    const local =
      host === "localhost" ||
      host === "127.0.0.1" ||
      host === "[::1]" ||
      host.endsWith(".ts.net") ||
      (parts.length === 4 &&
        parts[0] === 100 &&
        parts[1]! >= 64 &&
        parts[1]! <= 127 &&
        parts.every((n) => Number.isInteger(n) && n >= 0 && n <= 255));
    return (
      url.origin === target.origin &&
      !url.username &&
      !url.password &&
      !url.search &&
      !url.hash &&
      (url.protocol === "https:" || (url.protocol === "http:" && local)) &&
      target.versionPath.startsWith("/") &&
      !target.versionPath.startsWith("//") &&
      !/[\\?#\x00-\x20\x7f]/.test(target.versionPath) &&
      new URL(target.versionPath, url).origin === url.origin &&
      !decodeURIComponent(target.versionPath)
        .split("/")
        .some((p) => p === ".." || p === ".")
    );
  } catch {
    return false;
  }
}
function validDeploymentPolicy(p: WorkbenchDeploymentHostPolicy) {
  try {
    const key = NodeCrypto.createPublicKey(p.coordinator.publicKey);
    return (
      validReleaseTarget({ origin: p.adapter.origin, versionPath: p.adapter.pathPrefix }) &&
      !p.adapter.pathPrefix.endsWith("/") &&
      validReleaseTarget({ origin: p.coordinator.origin, versionPath: "/" }) &&
      key.asymmetricKeyType === "ed25519" &&
      releaseHash(key.export({ type: "spki", format: "der" })) === p.coordinator.id
    );
  } catch {
    return false;
  }
}
export function publicDeploymentPlan(p: WorkbenchDeploymentHostPolicy): WorkbenchDeploymentPlan {
  return {
    adapterId: p.adapter.id,
    adapterVersion: p.adapter.version,
    generationId: p.adapter.generationId,
    serviceKey: p.serviceKey,
    coordinator: { id: p.coordinator.id, origin: p.coordinator.origin },
    deploy: p.deploy,
    rollback: { ...p.rollback, verification: "fixed_target_and_adapter" },
  };
}
export const readReleaseProfiles = Effect.fn("WorkbenchReleaseEvidence.readProfiles")(function* () {
  const fs = yield* FileSystem.FileSystem,
    path = yield* Path.Path,
    config = yield* Config.ServerConfig;
  const file = path.join(config.stateDir, "workbench-release-profiles.json");
  if (!(yield* fs.exists(file)))
    return { configured: false, profiles: [] as WorkbenchReleaseHostProfile[] };
  const stat = yield* fs.stat(file);
  if (stat.type !== "File" || stat.size > 1024 * 1024)
    return yield* new WorkbenchReleaseError({ reason: "profile_invalid" });
  const decoded = yield* decodeProfileFile(yield* fs.readFileString(file)).pipe(
    Effect.tapError((cause) =>
      Effect.logWarning(
        "Release profile decoding failed",
        new ReleaseProfileOperationError({ stage: "decode_profile", cause }),
      ),
    ),
    Effect.mapError(() => new WorkbenchReleaseError({ reason: "profile_invalid" })),
  );
  const platform = yield* HostProcess.Platform;
  if (platform !== "darwin" && platform !== "linux" && decoded.profiles.some((p) => p.enabled))
    return yield* new WorkbenchReleaseError({ reason: "profile_invalid" });
  if (new Set(decoded.profiles.map((p) => p.id)).size !== decoded.profiles.length)
    return yield* new WorkbenchReleaseError({ reason: "profile_invalid" });
  for (const p of decoded.profiles) {
    const policy = p.policy,
      o = policy.observation;
    if (
      !validReleaseTarget(policy.target) ||
      (policy.merge !== undefined &&
        (!policy.deployment || policy.merge.ref !== policy.main.ref)) ||
      (policy.deployment !== undefined && !validDeploymentPolicy(policy.deployment)) ||
      !safeReleasePath(policy.artifact.relativePath) ||
      !safeReleasePath(policy.rollback.artifactRelativePath) ||
      !/^refs\/heads\/[A-Za-z0-9][A-Za-z0-9._/-]*$/.test(policy.main.ref) ||
      policy.main.ref.includes("..") ||
      !/^[A-Za-z0-9][A-Za-z0-9._-]*$/.test(policy.main.remote) ||
      o.intervalSeconds > o.durationSeconds ||
      o.consecutiveSuccesses > Math.floor(o.durationSeconds / o.intervalSeconds) + 1
    )
      return yield* new WorkbenchReleaseError({ reason: "profile_invalid" });
  }
  return { configured: true, profiles: decoded.profiles };
});
export const releaseProfileScripts = Effect.fn("WorkbenchReleaseEvidence.profileScripts")(
  function* (profile: WorkbenchReleaseHostProfile) {
    const sql = yield* SqlClient.SqlClient;
    const [row] = yield* sql<{
      scripts_json: string;
    }>`SELECT scripts_json FROM projection_projects WHERE project_id=${profile.projectId} AND deleted_at IS NULL`;
    if (!row) return yield* new WorkbenchReleaseError({ reason: "profile_invalid" });
    const scripts = yield* decodeProjectScripts(row.scripts_json).pipe(
      Effect.tapError((cause) =>
        Effect.logWarning(
          "Release script decoding failed",
          new ReleaseProfileOperationError({ stage: "decode_scripts", cause }),
        ),
      ),
      Effect.mapError(() => new WorkbenchReleaseError({ reason: "profile_invalid" })),
    );
    const p = profile.policy;
    const refs = [p.build, ...p.checks, p.health, p.core, p.rollback.execute, p.rollback.verify];
    const resolved = refs.map((ref) => scripts.find((s) => s.id === ref.scriptId));
    if (
      refs.some(
        (ref, i) =>
          !resolved[i] ||
          resolved[i]!.async === true ||
          resolved[i]!.command.length > 4096 ||
          releaseCommandArgv(resolved[i]!.command) === null ||
          releaseScriptHash(resolved[i]!) !== ref.definitionHash,
      )
    )
      return yield* new WorkbenchReleaseError({ reason: "profile_changed" });
    return resolved.map((s) => s!);
  },
);
export const publicReleaseProfile = Effect.fn("WorkbenchReleaseEvidence.publicProfile")(function* (
  p: WorkbenchReleaseHostProfile,
) {
  const scripts = yield* releaseProfileScripts(p),
    path = yield* Path.Path;
  const summary = (i: number) => ({
    id: scripts[i]!.id,
    name: scripts[i]!.name,
    definitionHash: releaseScriptHash(scripts[i]!),
  });
  const n = p.policy.checks.length;
  return {
    ...(p.policy.merge ? { mergePlan: p.policy.merge } : {}),
    ...(p.policy.deployment ? { deploymentPlan: publicDeploymentPlan(p.policy.deployment) } : {}),
    id: p.id,
    version: p.version,
    digest: releaseDigest(p),
    projectId: p.projectId,
    name: p.name,
    main: p.policy.main,
    target: p.policy.target,
    artifactLabel: path.basename(p.policy.artifact.relativePath),
    rollbackArtifactLabel: path.basename(p.policy.rollback.artifactRelativePath),
    scripts: {
      build: summary(0),
      checks: p.policy.checks.map((_, i) => summary(i + 1)),
      health: summary(n + 1),
      core: summary(n + 2),
      rollbackExecute: summary(n + 3),
      rollbackVerify: summary(n + 4),
    },
    observation: p.policy.observation,
  } satisfies WorkbenchPublicReleaseProfile;
});
