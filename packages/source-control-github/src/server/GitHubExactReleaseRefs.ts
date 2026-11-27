import {
  WorkbenchMergePlan,
  WorkbenchMergeSubject,
  WorkbenchMergeObservation,
  WorkbenchMergeRemoteReason,
  type WorkbenchMergeRequiredResult,
} from "@t3tools/contracts";
import type {
  ExactReleaseRefs,
  ExactReleaseRefsInput,
  ExactReleaseFFInput,
  ExactReleaseFFAttempt,
} from "@t3tools/source-control-core/server/driver";
import { stableStringify } from "@t3tools/shared/relaySigning";
import * as DateTime from "effect/DateTime";
import * as Digest from "effect/crypto/Digest";
import * as Effect from "effect/Effect";
import * as Result from "effect/Result";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import { FetchHttpClient } from "effect/http";
import { GraphQLClient, GraphQLClientError } from "effect/graphql";

import * as GitHubPullRequestApi from "./GitHubPullRequestApi.ts";
import * as GitHubRest from "./GitHubRest.ts";
import * as GitHubGraphQlClient from "./gitHubGraphQlClient.ts";
import * as GitHubHttpClient from "./GitHubHttpClient.ts";
import { ExactReleaseRefsGraphQLGroup } from "./exactReleaseRefs.graphql.ts";

class ExactReleaseRefsError extends Schema.TaggedError<ExactReleaseRefsError>()(
  "ExactReleaseRefsError",
  { reason: WorkbenchMergeRemoteReason },
) {}
const fail = (reason: WorkbenchMergeRemoteReason) => new ExactReleaseRefsError({ reason });
const ProviderId = Schema.String.check(
  Schema.isMinLength(1),
  Schema.isMaxLength(200),
  Schema.isPattern(/^[^\x00-\x1f\x7f]+$/),
);
const Sha = WorkbenchMergeSubject.fields.sourceSha;
const Pins = Schema.Struct({
  operationId: ProviderId,
  expectedActorId: ProviderId,
  expectedPolicyDigest: Schema.String.check(Schema.isPattern(/^[a-f0-9]{64}$/)),
});
const Commit = Schema.Struct({ sha: Sha, tree: Schema.Struct({ sha: Sha }) });
const Repository = Schema.Struct({
  node_id: ProviderId,
  full_name: Schema.String,
  html_url: Schema.String,
});
const Ref = Schema.Struct({
  ref: Schema.String,
  object: Schema.Struct({ type: Schema.Literal("commit"), sha: Sha }),
});
const Required = Schema.Struct({
  context: Schema.String,
  app_id: Schema.Int.check(Schema.isGreaterThan(0)),
});
const Enabled = Schema.Struct({ enabled: Schema.Boolean });
const Protection = Schema.Struct({
  enforce_admins: Enabled,
  allow_force_pushes: Enabled,
  allow_deletions: Enabled,
  required_status_checks: Schema.Struct({
    strict: Schema.Boolean,
    contexts: Schema.Array(Schema.String),
    checks: Schema.Array(Required),
  }),
  required_pull_request_reviews: Schema.optional(Schema.NullOr(Schema.Unknown)),
  required_signatures: Enabled,
  required_linear_history: Enabled,
  required_conversation_resolution: Enabled,
  restrictions: Schema.NullOr(Schema.Unknown),
  block_creations: Schema.Boolean,
  lock_branch: Enabled,
  allow_fork_syncing: Enabled,
});
const RuleBody = Schema.Struct({
  type: Schema.String,
  parameters: Schema.optional(Schema.Unknown),
});
const Rule = Schema.Struct({
  ...RuleBody.fields,
  ruleset_id: Schema.Int.check(Schema.isGreaterThan(0)),
});
const Ruleset = Schema.Struct({
  id: Schema.Int.check(Schema.isGreaterThan(0)),
  enforcement: Schema.Literal("active"),
  target: Schema.Literal("branch"),
  bypass_actors: Schema.Array(Schema.Unknown),
  rules: Schema.Array(RuleBody),
});
const RuleChecks = Schema.Struct({
  required_status_checks: Schema.Array(
    Schema.Struct({
      context: Schema.String,
      integration_id: Schema.Int.check(Schema.isGreaterThan(0)),
    }),
  ),
  strict_required_status_checks_policy: Schema.Literal(true),
  do_not_enforce_on_create: Schema.Literal(false),
});
const Check = Schema.Struct({
  id: Schema.Int.check(Schema.isGreaterThan(0)),
  node_id: ProviderId,
  name: Schema.String,
  app: Schema.Struct({ id: Schema.Int.check(Schema.isGreaterThan(0)) }),
  head_sha: Sha,
  status: Schema.Literals([
    "queued",
    "in_progress",
    "completed",
    "waiting",
    "requested",
    "pending",
  ]),
  conclusion: Schema.NullOr(
    Schema.Literals([
      "success",
      "failure",
      "cancelled",
      "timed_out",
      "action_required",
      "neutral",
      "skipped",
      "stale",
      "startup_failure",
    ]),
  ),
});
const Checks = Schema.Struct({
  total_count: Schema.Int.check(Schema.isGreaterThanOrEqualTo(0)),
  check_runs: Schema.Array(Check),
});
const record = (value: unknown): value is Record<string, unknown> =>
  typeof value === "object" && value !== null && !Array.isArray(value);
const knownKeys = (value: unknown, keys: ReadonlyArray<string>) =>
  record(value) && Object.keys(value).every((key) => keys.includes(key));
const legacyKeys = [...Object.keys(Protection.fields), "url"];
const rulesetKeys = [
  ...Object.keys(Ruleset.fields),
  "node_id",
  "name",
  "source_type",
  "source",
  "created_at",
  "updated_at",
  "_links",
  "conditions",
];
const isRefsError = Schema.is(
  Schema.TaggedStruct("ExactReleaseRefsError", { reason: WorkbenchMergeRemoteReason }),
);
const decodePlan = Schema.decodeEffect(WorkbenchMergePlan);
const decodeSubject = Schema.decodeEffect(WorkbenchMergeSubject);
const decodeProviderId = Schema.decodeEffect(ProviderId);
const decodeProtection = Schema.decodeUnknownEffect(Protection);
const decodeRuleset = Schema.decodeUnknownEffect(Ruleset);
const decodeRuleChecks = Schema.decodeUnknownOption(RuleChecks, { onExcessProperty: "error" });
const decodeObservation = Schema.decodeEffect(WorkbenchMergeObservation);
const decodePins = Schema.decodeEffect(Pins);

/** Captures the driver's existing authentication, quota and transports; no new host services. */
export const make = Effect.gen(function* () {
  const auth = yield* GitHubPullRequestApi.GitHubPullRequestApi;
  const rest = yield* GitHubRest.GitHubRest;
  const crypto = yield* Digest.Digest;
  const client = yield* GraphQLClient.make(
    ExactReleaseRefsGraphQLGroup.middleware(GitHubGraphQlClient.GitHubGraphQlAuth),
  ).pipe(Effect.provide(GitHubGraphQlClient.layer));
  const hash = (value: unknown) =>
    crypto.digestHex(Digest.sha256, new TextEncoder().encode(stableStringify(value)));
  const safeError = (error: unknown) => (isRefsError(error) ? error : fail("unavailable"));

  const validate = Effect.fn("GitHubExactReleaseRefs.validate")(function* (
    input: ExactReleaseRefsInput,
  ) {
    yield* decodePlan(input.plan).pipe(Effect.mapError(() => fail("identity_mismatch")));
    yield* decodeSubject(input.subject).pipe(Effect.mapError(() => fail("candidate_changed")));
  });

  const inspectAs = Effect.fn("GitHubExactReleaseRefs.inspect")(function* (
    input: ExactReleaseRefsInput,
    actor: { accountId: string; viewer: string },
  ) {
    const { plan, subject } = input;
    const root = `repos/${plan.repository.split("/").map(encodeURIComponent).join("/")}`;
    const branch = encodeURIComponent(plan.ref.slice("refs/heads/".length));
    const read = <A>(path: string, schema: Schema.Codec<A, unknown>) =>
      rest
        .rest({
          host: plan.host,
          path,
          operation: "exactReleaseRefs.inspect",
          allowReserve: false,
          maxResponseBytes: 1024 * 1024,
        })
        .pipe(
          Effect.flatMap((response) =>
            response.truncated || response.invalidUtf8
              ? Effect.fail(fail("invalid_response"))
              : Schema.decodeEffect(Schema.fromJsonString(schema))(response.body).pipe(
                  Effect.mapError(() => fail("invalid_response")),
                ),
          ),
        );
    const repository = yield* read(root, Repository);
    if (
      repository.node_id !== plan.repositoryId ||
      repository.full_name.toLowerCase() !== plan.repository.toLowerCase() ||
      repository.html_url.toLowerCase() !== `https://${plan.host}/${plan.repository}`.toLowerCase()
    )
      return yield* fail("identity_mismatch");
    yield* decodeProviderId(actor.accountId).pipe(Effect.mapError(() => fail("identity_mismatch")));
    yield* decodeProviderId(actor.viewer).pipe(Effect.mapError(() => fail("identity_mismatch")));
    const ref = yield* read(
      `${root}/git/ref/${plan.ref.slice("refs/".length).split("/").map(encodeURIComponent).join("/")}`,
      Ref,
    ).pipe(Effect.catchTags({ GitHubApiNotFoundError: () => Effect.succeed(null) }));
    if (ref !== null && ref.ref !== plan.ref) return yield* fail("identity_mismatch");
    const main = ref === null ? null : yield* read(`${root}/git/commits/${ref.object.sha}`, Commit);
    const candidate = yield* read(`${root}/git/commits/${subject.sourceSha}`, Commit).pipe(
      Effect.catchTags({ GitHubApiNotFoundError: () => Effect.succeed(null) }),
    );
    const compare =
      candidate === null
        ? null
        : yield* read(
            `${root}/compare/${subject.baseSha}...${subject.sourceSha}`,
            Schema.Struct({ merge_base_commit: Schema.Struct({ sha: Sha }) }),
          );
    const checkRuns: Array<typeof Check.Type> = [];
    let checkCount: number | undefined;
    for (let page = 1; ; page++) {
      if (candidate === null) break;
      if (page > 128) return yield* fail("invalid_response");
      const answer = yield* read(
        `${root}/commits/${subject.sourceSha}/check-runs?filter=latest&per_page=100&page=${page}`,
        Checks,
      );
      if (
        (checkCount !== undefined && checkCount !== answer.total_count) ||
        answer.check_runs.length > 100
      )
        return yield* fail("invalid_response");
      checkCount = answer.total_count;
      checkRuns.push(...answer.check_runs);
      if (
        checkRuns.length > answer.total_count ||
        new Set(checkRuns.map((check) => check.id)).size !== checkRuns.length ||
        checkRuns.some((check) => check.head_sha !== subject.sourceSha)
      )
        return yield* fail("invalid_response");
      if (checkRuns.length === answer.total_count) break;
      if (answer.check_runs.length === 0) return yield* fail("invalid_response");
    }
    const requiredResults = yield* Effect.forEach(plan.requiredChecks, (required) =>
      Effect.gen(function* () {
        const matches = checkRuns.filter(
          (check) =>
            check.name === required.context &&
            check.app.id === required.appId &&
            check.head_sha === subject.sourceSha,
        );
        if (matches.length > 1) return yield* fail("invalid_response");
        const found = matches[0];
        const result = {
          context: required.context,
          appId: required.appId,
          headSha: subject.sourceSha,
          checkId: found?.node_id ?? null,
          state:
            found === undefined
              ? "missing"
              : found.status !== "completed"
                ? "pending"
                : found.conclusion === "success"
                  ? "passed"
                  : "failed",
          conclusion: found?.status === "completed" ? found.conclusion : null,
        } as const;
        if (result.state === "failed" && result.conclusion === null)
          return yield* fail("invalid_response");
        return {
          ...result,
          receiptDigest: yield* hash(result),
        } satisfies WorkbenchMergeRequiredResult;
      }),
    );
    const policy = yield* Effect.result(
      Effect.gen(function* () {
        const legacyRaw = yield* read(
          `${root}/branches/${branch}/protection`,
          Schema.Record(Schema.String, Schema.Unknown),
        );
        const legacy = yield* decodeProtection(legacyRaw).pipe(
          Effect.mapError(() => fail("invalid_response")),
        );
        const rules: Array<typeof Rule.Type> = [];
        for (let page = 1; ; page++) {
          if (page > 128) return yield* fail("invalid_response");
          const next = yield* read(
            `${root}/rules/branches/${branch}?per_page=100&page=${page}`,
            Schema.Array(Rule),
          );
          if (next.length > 100) return yield* fail("invalid_response");
          rules.push(...next);
          if (rules.length > 1000) return yield* fail("unavailable");
          if (next.length < 100) break;
        }
        const rulesetIds = [...new Set(rules.map((rule) => rule.ruleset_id))].sort((a, b) => a - b);
        if (rulesetIds.length > 100) return yield* fail("unavailable");
        const rawRulesets = yield* Effect.forEach(rulesetIds, (id) =>
          read(
            `${root}/rulesets/${id}?includes_parents=true`,
            Schema.Record(Schema.String, Schema.Unknown),
          ),
        );
        const rulesets = yield* Effect.forEach(rawRulesets, (raw, index) =>
          decodeRuleset(raw).pipe(
            Effect.mapError(() => fail("invalid_response")),
            Effect.filterOrFail(
              (set) => set.id === rulesetIds[index],
              () => fail("invalid_response"),
            ),
          ),
        );
        const configured = legacy.required_status_checks.checks.map((check) => ({
          context: check.context,
          appId: check.app_id,
        }));
        let rulesSupported =
          knownKeys(legacyRaw, legacyKeys) &&
          rawRulesets.every((raw) => knownKeys(raw, rulesetKeys)) &&
          rulesets.every((set) => set.bypass_actors.length === 0) &&
          Object.entries(legacyRaw).every(
            ([key, value]) =>
              !(key in Protection.fields) ||
              !record(value) ||
              key === "required_status_checks" ||
              key === "required_pull_request_reviews" ||
              key === "restrictions" ||
              knownKeys(value, ["enabled", "url"]),
          ) &&
          knownKeys(legacyRaw.required_status_checks, [
            "url",
            "strict",
            "contexts",
            "checks",
            "contexts_url",
          ]) &&
          record(legacyRaw.required_status_checks) &&
          Array.isArray(legacyRaw.required_status_checks.checks) &&
          legacyRaw.required_status_checks.checks.every((check: unknown) =>
            knownKeys(check, ["context", "app_id"]),
          );
        for (const rule of [...rules, ...rulesets.flatMap((set) => set.rules)]) {
          if (rule.type === "required_status_checks") {
            const parameters = decodeRuleChecks(rule.parameters);
            if (parameters._tag === "None") rulesSupported = false;
            else
              configured.push(
                ...parameters.value.required_status_checks.map((check) => ({
                  context: check.context,
                  appId: check.integration_id,
                })),
              );
          } else if (
            !["non_fast_forward", "deletion"].includes(rule.type) ||
            (rule.parameters !== undefined && !knownKeys(rule.parameters, []))
          )
            rulesSupported = false;
        }
        const distinct = [
          ...new Map(
            configured.map((check) => [`${check.context}\0${check.appId}`, check]),
          ).values(),
        ];
        const sorted = (checks: ReadonlyArray<{ context: string; appId: number }>) =>
          [...checks].sort((a, b) =>
            a.context < b.context ? -1 : a.context > b.context ? 1 : a.appId - b.appId,
          );
        const supported =
          rulesSupported &&
          legacy.enforce_admins.enabled &&
          !legacy.allow_force_pushes.enabled &&
          !legacy.allow_deletions.enabled &&
          legacy.required_pull_request_reviews == null &&
          !legacy.required_signatures.enabled &&
          !legacy.required_linear_history.enabled &&
          !legacy.required_conversation_resolution.enabled &&
          legacy.restrictions === null &&
          !legacy.block_creations &&
          !legacy.lock_branch.enabled &&
          !legacy.allow_fork_syncing.enabled &&
          legacy.required_status_checks.strict &&
          stableStringify(sorted(distinct)) === stableStringify(sorted(plan.requiredChecks)) &&
          stableStringify([...legacy.required_status_checks.contexts].sort()) ===
            stableStringify(
              [...new Set(legacy.required_status_checks.checks.map((c) => c.context))].sort(),
            );
        const order = (left: unknown, right: unknown) =>
          stableStringify(left) < stableStringify(right)
            ? -1
            : stableStringify(left) > stableStringify(right)
              ? 1
              : 0;
        return {
          supported,
          digest: yield* hash({
            legacy: {
              ...legacy,
              required_status_checks: {
                ...legacy.required_status_checks,
                contexts: [...legacy.required_status_checks.contexts].sort(),
                checks: [...legacy.required_status_checks.checks].sort(order),
              },
            },
            rules: [...rules].sort(order),
            rulesets: rulesets.map((set) => ({ ...set, rules: [...set.rules].sort(order) })),
          }),
        };
      }),
    );
    let protection: WorkbenchMergeObservation["protection"] = Result.isFailure(policy)
      ? { state: "unavailable", policyDigest: null, reasons: ["unavailable"] }
      : {
          state: policy.success.supported ? "supported" : "unsupported",
          policyDigest: policy.success.digest,
          reasons: policy.success.supported ? [] : ["unsupported"],
        };
    if (input.expectedActorId !== undefined && actor.accountId !== input.expectedActorId) {
      protection = {
        ...protection,
        state: "unsupported",
        reasons: [...protection.reasons, "actor_changed"],
      };
    }
    if (
      input.expectedPolicyDigest !== undefined &&
      protection.policyDigest !== null &&
      protection.policyDigest !== input.expectedPolicyDigest
    ) {
      protection = {
        ...protection,
        state: "unsupported",
        reasons: [...protection.reasons, "policy_changed"],
      };
    }
    const observation = {
      schemaVersion: 1,
      repository: { host: plan.host, name: repository.full_name, id: repository.node_id },
      actor: { id: actor.accountId, login: actor.viewer },
      ref: plan.ref,
      main: main === null ? null : { sha: main.sha, treeSha: main.tree.sha },
      candidate:
        candidate === null
          ? null
          : {
              sha: candidate.sha,
              treeSha: candidate.tree.sha,
              baseAncestor: compare?.merge_base_commit.sha === subject.baseSha,
            },
      requiredResults,
      protection,
      observedAt: DateTime.formatIso(yield* DateTime.now),
    } as const;
    return yield* decodeObservation(observation).pipe(
      Effect.mapError(() => fail("invalid_response")),
    );
  });

  const ineligible = (
    input: ExactReleaseRefsInput,
    observation: WorkbenchMergeObservation,
  ): WorkbenchMergeRemoteReason | null => {
    if (observation.actor.id !== input.expectedActorId && input.expectedActorId !== undefined)
      return "actor_changed";
    if (
      input.expectedPolicyDigest !== undefined &&
      observation.protection.policyDigest !== null &&
      observation.protection.policyDigest !== input.expectedPolicyDigest
    )
      return "policy_changed";
    if (observation.protection.state !== "supported")
      return observation.protection.state === "unavailable" ? "unavailable" : "unsupported";
    if (!observation.candidate) return "candidate_unavailable";
    if (
      observation.candidate.sha !== input.subject.sourceSha ||
      observation.candidate.treeSha !== input.subject.sourceTreeSha ||
      !observation.candidate.baseAncestor
    )
      return "candidate_changed";
    for (const state of ["failed", "pending", "missing"] as const)
      if (observation.requiredResults.some((result) => result.state === state))
        return state === "failed"
          ? "checks_failed"
          : state === "pending"
            ? "checks_pending"
            : "checks_missing";
    return null;
  };

  const inspect: ExactReleaseRefs["inspect"] = (input) =>
    validate(input).pipe(
      Effect.andThen(
        auth.withVerifiedCredential(
          { cwd: input.cwd, host: input.plan.host, repository: input.plan.repository },
          (actor) => inspectAs(input, actor),
        ),
      ),
      Effect.mapError(safeError),
    );
  const exactFF: ExactReleaseRefs["exactFF"] = (input: ExactReleaseFFInput) =>
    validate(input).pipe(
      Effect.andThen(decodePins(input).pipe(Effect.mapError(() => fail("identity_mismatch")))),
      Effect.andThen(
        auth.withVerifiedCredential(
          { cwd: input.cwd, host: input.plan.host, repository: input.plan.repository },
          (actor) =>
            Effect.gen(function* () {
              const observed = yield* inspectAs(input, actor);
              const rejected = (reason: WorkbenchMergeRemoteReason): ExactReleaseFFAttempt => ({
                kind: "rejected",
                sent: false,
                reason,
                ack: null,
                observation: observed,
              });
              const reason = ineligible(input, observed);
              if (reason) return rejected(reason);
              if (
                observed.main?.sha === input.subject.sourceSha &&
                observed.main.treeSha === input.subject.sourceTreeSha
              )
                return input.subject.baseSha === input.subject.sourceSha
                  ? ({
                      kind: "observed_matching",
                      sent: false,
                      reason: null,
                      ack: null,
                      observation: observed,
                    } satisfies ExactReleaseFFAttempt)
                  : rejected("main_changed");
              if (observed.main?.sha !== input.subject.baseSha) return rejected("main_changed");
              const init = yield* Effect.serviceOption(FetchHttpClient.RequestInit);
              yield* input.beforeMutation;
              const reply = yield* Effect.result(
                client
                  .ExactReleaseFastForward({
                    input: {
                      repositoryId: input.plan.repositoryId,
                      clientMutationId: input.operationId,
                      refUpdates: [
                        {
                          name: input.plan.ref,
                          beforeOid: input.subject.baseSha,
                          afterOid: input.subject.sourceSha,
                          force: false,
                        },
                      ],
                    },
                  })
                  .pipe(
                    Effect.provideService(GitHubGraphQlClient.GitHubGraphQlTarget, {
                      host: input.plan.host,
                      operation: "exactReleaseRefs.exactFF",
                      allowReserve: false,
                    }),
                    Effect.provideService(GitHubHttpClient.NoRedirects, true),
                    Effect.provideService(FetchHttpClient.RequestInit, {
                      ...(Option.isSome(init) ? init.value : {}),
                      redirect: "manual",
                    }),
                  ),
              );
              if (Result.isFailure(reply)) {
                const error = reply.failure;
                const response =
                  GraphQLClientError.isGraphQLClientError(error) &&
                  error.reason._tag === "ResponseError"
                    ? error.reason
                    : null;
                const noMutationResult =
                  response !== null &&
                  (response.data === undefined ||
                    response.data === null ||
                    (record(response.data) &&
                      Object.keys(response.data).every((key) => key === "updateRefs") &&
                      response.data.updateRefs === null));
                const rootRefusal =
                  response !== null &&
                  [200, 403, 422].includes(response.status ?? 200) &&
                  noMutationResult &&
                  response.errors.every(
                    (item) =>
                      item.path?.length === 1 &&
                      item.path[0] === "updateRefs" &&
                      (item["type"] === "UNPROCESSABLE" || item["type"] === "FORBIDDEN"),
                  );
                const definitive =
                  error._tag === "GitHubApiAuthenticationError" ||
                  (error._tag === "GitHubApiResponseError" &&
                    [403, 409, 422].includes(error.status)) ||
                  rootRefusal;
                if (!definitive)
                  return {
                    kind: "unknown",
                    sent: true,
                    reason: "mutation_unknown",
                    ack: null,
                    observation: null,
                  } satisfies ExactReleaseFFAttempt;
                const after = yield* Effect.result(inspectAs(input, actor));
                const observation = Result.isSuccess(after) ? after.success : null;
                return {
                  kind: "rejected",
                  sent: true,
                  reason:
                    observation?.main?.sha !== undefined &&
                    observation.main.sha !== input.subject.baseSha
                      ? "main_changed"
                      : "protected_ref_rejected",
                  ack: null,
                  observation,
                } satisfies ExactReleaseFFAttempt;
              }
              const echo = reply.success.updateRefs?.clientMutationId;
              if (echo !== input.operationId)
                return {
                  kind: "unknown",
                  sent: true,
                  reason: "invalid_response",
                  ack: null,
                  observation: null,
                } satisfies ExactReleaseFFAttempt;
              const ack = { clientMutationId: echo };
              const after = yield* Effect.result(inspectAs(input, actor));
              if (Result.isFailure(after))
                return {
                  kind: "unknown",
                  sent: true,
                  reason: "unavailable",
                  ack,
                  observation: null,
                } satisfies ExactReleaseFFAttempt;
              const matching =
                after.success.main?.sha === input.subject.sourceSha &&
                after.success.main.treeSha === input.subject.sourceTreeSha;
              return {
                kind: matching ? "acknowledged" : "unknown",
                sent: true,
                reason: matching ? null : "main_changed",
                ack,
                observation: after.success,
              } satisfies ExactReleaseFFAttempt;
            }),
        ),
      ),
      Effect.mapError(safeError),
    );
  return { inspect, exactFF } satisfies ExactReleaseRefs;
});
