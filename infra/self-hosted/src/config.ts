// @effect-diagnostics-next-line nodeBuiltinImport:off -- Config loading requires O_NOFOLLOW before an Effect runtime exists.
import * as NodeFS from "node:fs";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- File descriptor ownership and mode checks precede reading secret configuration.
import * as NodeFSP from "node:fs/promises";
// @effect-diagnostics-next-line nodeBuiltinImport:off -- Config validation runs before an Effect runtime exists.
import * as NodePath from "node:path";
import * as Schema from "effect/Schema";
import { isPasswordHash } from "./password.ts";

const Identifier = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/));
const BoundedString = Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(2_048));
const Seconds = Schema.Int.check(Schema.isBetween({ minimum: 60, maximum: 7_776_000 }));

export const SelfHostedConfig = Schema.Struct({
  host: Schema.optionalKey(Schema.String.check(Schema.isMaxLength(253))),
  port: Schema.optionalKey(Schema.Int.check(Schema.isBetween({ minimum: 1, maximum: 65_535 }))),
  databasePath: BoundedString,
  account: Schema.Struct({
    accountId: Identifier,
    username: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(128)),
    passwordHash: Schema.String.check(Schema.isMaxLength(256)),
  }),
  environments: Schema.Array(
    Schema.Struct({
      environmentId: Identifier,
      label: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(160)),
      origin: BoundedString,
      pairingOrigin: BoundedString,
      credential: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(16_384)),
    }),
  ).check(Schema.isMaxLength(64)),
  allowedOrigins: Schema.optionalKey(Schema.Array(BoundedString).check(Schema.isMaxLength(32))),
  requestTimeoutMs: Schema.optionalKey(
    Schema.Int.check(Schema.isBetween({ minimum: 100, maximum: 30_000 })),
  ),
  accessTokenTtlSeconds: Schema.optionalKey(
    Schema.Int.check(Schema.isBetween({ minimum: 60, maximum: 3_600 })),
  ),
  refreshTokenTtlSeconds: Schema.optionalKey(Seconds),
});
export type SelfHostedConfig = typeof SelfHostedConfig.Type;
const decodeSelfHostedConfig = Schema.decodeUnknownSync(SelfHostedConfig);

function normalizeOrigin(value: string, allowLoopback: boolean): string {
  const url = new URL(value);
  if (
    url.username ||
    url.password ||
    url.search ||
    url.hash ||
    url.pathname !== "/" ||
    (url.protocol !== "https:" &&
      !(
        allowLoopback &&
        url.protocol === "http:" &&
        ["localhost", "127.0.0.1", "[::1]"].includes(url.hostname)
      ))
  ) {
    throw new Error("Configuration origins must use HTTPS; only local pairing/CORS may use HTTP.");
  }
  return url.origin;
}

export function decodeConfig(input: unknown): SelfHostedConfig {
  const config = decodeSelfHostedConfig(input);
  if (!NodePath.isAbsolute(config.databasePath) || !isPasswordHash(config.account.passwordHash)) {
    throw new Error("Configuration requires an absolute database path and a valid scrypt hash.");
  }
  const ids = new Set<string>();
  const environments = config.environments.map((environment) => {
    if (ids.has(environment.environmentId) || /[\r\n]/.test(environment.credential)) {
      throw new Error("Configuration contains duplicate environments or an invalid credential.");
    }
    ids.add(environment.environmentId);
    return {
      ...environment,
      origin: normalizeOrigin(environment.origin, false),
      pairingOrigin: normalizeOrigin(environment.pairingOrigin, true),
    };
  });
  return {
    ...config,
    environments,
    allowedOrigins: (config.allowedOrigins ?? []).map((origin) => normalizeOrigin(origin, true)),
  };
}

export async function loadConfig(path: string): Promise<SelfHostedConfig> {
  const file = await NodeFSP.open(path, NodeFS.constants.O_RDONLY | NodeFS.constants.O_NOFOLLOW);
  try {
    const stat = await file.stat();
    if (
      !stat.isFile() ||
      (stat.mode & 0o777) !== 0o600 ||
      stat.size > 1_048_576 ||
      (process.getuid !== undefined && stat.uid !== process.getuid())
    ) {
      throw new Error(
        "Configuration must be an owned regular file with mode 0600 (at most 1 MiB).",
      );
    }
    return decodeConfig(JSON.parse(await file.readFile("utf8")));
  } finally {
    await file.close();
  }
}
