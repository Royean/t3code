import * as Schema from "effect/Schema";

const Identifier = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9][A-Za-z0-9._-]{0,127}$/));
const Token = Schema.String.check(Schema.isPattern(/^[A-Za-z0-9_-]{43}$/));
const Timestamp = Schema.String.check(
  Schema.isPattern(/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/),
);

export const SelfHostedLoginInput = Schema.Struct({
  username: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(128)),
  password: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(1_024)),
});
export type SelfHostedLoginInput = typeof SelfHostedLoginInput.Type;

export const SelfHostedRefreshInput = Schema.Struct({ refreshToken: Token });
export type SelfHostedRefreshInput = typeof SelfHostedRefreshInput.Type;

export const SelfHostedSession = Schema.Struct({
  accountId: Identifier,
  accessToken: Token,
  refreshToken: Token,
  expiresAt: Timestamp,
});
export type SelfHostedSession = typeof SelfHostedSession.Type;

export const SelfHostedEnvironment = Schema.Struct({
  environmentId: Identifier,
  label: Schema.String.check(Schema.isMinLength(1), Schema.isMaxLength(160)),
  httpBaseUrl: Schema.String.check(
    Schema.isMaxLength(2_048),
    Schema.isPattern(/^https:\/\/[^/?#@\s]+$/),
  ),
  wsBaseUrl: Schema.String.check(
    Schema.isMaxLength(2_048),
    Schema.isPattern(/^wss:\/\/[^/?#@\s]+$/),
  ),
  available: Schema.Boolean,
});
export type SelfHostedEnvironment = typeof SelfHostedEnvironment.Type;

export const SelfHostedEnvironments = Schema.Struct({
  environments: Schema.Array(SelfHostedEnvironment),
});
export type SelfHostedEnvironments = typeof SelfHostedEnvironments.Type;

export const SelfHostedConnection = Schema.Struct({
  environmentId: Identifier,
  pairingUrl: Schema.String.check(
    Schema.isMaxLength(16_384),
    Schema.isPattern(/^https:\/\/[^/?#@\s]+\/pair#token=[^\s]+$/),
  ),
  expiresAt: Timestamp,
});
export type SelfHostedConnection = typeof SelfHostedConnection.Type;

export const SelfHostedErrorResponse = Schema.Struct({
  error: Schema.Struct({
    code: Schema.Literals([
      "invalid_request",
      "unauthorized",
      "rate_limited",
      "environment_not_found",
      "environment_unavailable",
      "upstream_timeout",
      "internal_error",
      "not_found",
    ]),
    message: Schema.String.check(Schema.isMaxLength(256)),
  }),
});
export type SelfHostedErrorResponse = typeof SelfHostedErrorResponse.Type;
