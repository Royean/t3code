import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { expect, it } from "@effect/vitest";
import { DEFAULT_SERVER_SETTINGS, type WorkbenchMergePlan } from "@t3tools/contracts";
import type { ExactReleaseFFInput } from "@t3tools/source-control-core/server/driver";
import * as SourceControlRateLimit from "@t3tools/source-control-core/server/SourceControlRateLimit";
import * as TestSourceControlHost from "@t3tools/source-control-testing/TestSourceControlHost";
import * as HostProcess from "@t3tools/shared/HostProcess";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import {
  HttpClient,
  HttpClientResponse,
  HttpClientError,
  type HttpClientRequest,
} from "effect/http";

import { driver } from "./driver.ts";

it.effect("persists an intent only after fresh eligibility and before the actual write", () =>
  Effect.gen(function* () {
    const f = fixture();
    const refs = (yield* f.make).exactReleaseRefs!;
    const before = yield* refs.inspect({ cwd: "/fixture", plan, subject });
    let intents = 0;
    const input: ExactReleaseFFInput = {
      cwd: "/fixture",
      plan,
      subject,
      operationId: "merge-intent",
      expectedActorId: "7",
      expectedPolicyDigest: before.protection.policyDigest!,
      beforeMutation: Effect.sync(() => {
        expect(f.requests.filter((r) => r.path === "/graphql")).toHaveLength(0);
        intents++;
      }),
    };
    f.state.checks = "pending";
    expect(yield* refs.exactFF(input)).toMatchObject({ sent: false, reason: "checks_pending" });
    expect(intents).toBe(0);
    f.state.checks = "success";
    expect(
      yield* Effect.result(
        refs.exactFF({
          ...input,
          beforeMutation: Effect.fail({ _tag: "ExactReleaseRefsError", reason: "main_changed" }),
        }),
      ),
    ).toMatchObject({ _tag: "Failure", failure: { reason: "main_changed" } });
    expect(f.requests.filter((r) => r.path === "/graphql")).toHaveLength(0);
    expect(yield* refs.exactFF(input)).toMatchObject({ kind: "acknowledged", sent: true });
    expect(intents).toBe(1);
  }).pipe(Effect.scoped),
);

// The remote fixture's candidate C includes D, which includes the frozen base B.
const B = "b".repeat(40),
  D = "a".repeat(40),
  C = "c".repeat(40),
  T = "d".repeat(40);
const plan: WorkbenchMergePlan = {
  schemaVersion: 1,
  kind: "github_exact_ff",
  host: "github.com",
  repository: "acme/web",
  repositoryId: "R_repo=",
  ref: "refs/heads/main",
  requiredChecks: [{ context: "verify", appId: 11 }],
};
const subject = { baseSha: B, sourceSha: C, sourceTreeSha: T };

function fixture(options: { app?: boolean } = {}) {
  const state = {
    main: B,
    actor: 7,
    repositoryId: plan.repositoryId,
    checks: "success",
    hiddenRules: false,
    race: false,
    dropAck: false,
    failReadback: false,
    deny: false,
    safeRules: false,
    pagedChecks: false,
    signatures: false,
    ambiguous: false,
    forbidden: false,
    unknownRule: false,
    candidateMissing: false,
    candidateTree: T,
    partialMutationError: false,
    partialMutationStatus: 200,
    mutationStatus: 200,
    baseAncestor: true,
  };
  const tokens = { "github.com": "fixture-token" };
  const commands: Array<ReadonlyArray<string>> = [];
  const requests: Array<{
    path: string;
    method: string;
    body: unknown;
    authorization: string | undefined;
  }> = [];
  const response = (request: HttpClientRequest.HttpClientRequest) =>
    Effect.try({
      try: () => {
        const url = new URL(request.url);
        const path = url.pathname;
        const body =
          request.body._tag === "Uint8Array"
            ? JSON.parse(new TextDecoder().decode(request.body.body))
            : null;
        requests.push({
          path,
          method: request.method,
          body,
          authorization: request.headers.authorization,
        });
        let value: unknown;
        let status = 200;
        if (path === "/user") value = { id: state.actor, login: "release-bot" };
        else if (path === "/user/installations")
          value = {
            total_count: 1,
            installations: [{ id: 1, repository_selection: "all", account: { login: "acme" } }],
          };
        else if (path === "/graphql") {
          const input = body.variables.input;
          const update = input.refUpdates[0];
          if (state.race) state.main = D;
          if (state.mutationStatus !== 200) {
            status = state.mutationStatus;
            value = { message: "Fixture server refused the request" };
          } else if (state.deny || state.main !== update.beforeOid) {
            value = {
              errors: [
                {
                  type: state.forbidden ? "FORBIDDEN" : "UNPROCESSABLE",
                  message: state.forbidden
                    ? "Resource not accessible by integration"
                    : "Protected ref update rejected",
                  path: ["updateRefs"],
                },
              ],
            };
          } else {
            state.main = update.afterOid;
            if (state.dropAck) throw new Error("response lost after accepted write");
            if (state.partialMutationError) status = state.partialMutationStatus;
            value = {
              data: { updateRefs: { clientMutationId: input.clientMutationId } },
              ...(state.partialMutationError
                ? {
                    errors: [
                      {
                        type: "FORBIDDEN",
                        message: "Return field unavailable",
                        path: ["updateRefs", "clientMutationId"],
                      },
                    ],
                  }
                : {}),
            };
          }
        } else if (path === "/repos/acme/web")
          value = {
            node_id: state.repositoryId,
            full_name: "acme/web",
            html_url: "https://github.com/acme/web",
          };
        else if (path.endsWith("/git/ref/heads/main")) {
          if (state.failReadback && state.main === C) throw new Error("readback unavailable");
          value = { ref: plan.ref, object: { type: "commit", sha: state.main } };
        } else if (path.includes("/git/commits/")) {
          const sha = path.split("/").at(-1);
          if (sha === C && state.candidateMissing) {
            status = 404;
            value = { message: "Commit not found" };
          } else value = { sha, tree: { sha: sha === C ? state.candidateTree : "e".repeat(40) } };
        } else if (path.includes("/compare/"))
          value = {
            merge_base_commit: {
              sha: path.endsWith(`${C}...${C}`) ? C : state.baseAncestor ? B : D,
            },
          };
        else if (path.endsWith("/check-runs")) {
          const required = {
            id: 99,
            node_id: "CR_check=",
            name: "verify",
            app: { id: state.checks === "untrusted" ? 12 : 11 },
            head_sha: C,
            status: state.checks === "pending" ? "in_progress" : "completed",
            conclusion:
              state.checks === "pending"
                ? null
                : state.checks === "untrusted"
                  ? "success"
                  : state.checks,
          };
          value =
            state.checks === "missing"
              ? { total_count: 0, check_runs: [] }
              : state.ambiguous
                ? {
                    total_count: 2,
                    check_runs: [
                      required,
                      { ...required, id: 98, node_id: "CR_older", conclusion: "failure" },
                    ],
                  }
                : {
                    total_count: state.pagedChecks ? 101 : 1,
                    check_runs:
                      state.pagedChecks && url.searchParams.get("page") === "1"
                        ? Array.from({ length: 100 }, (_, index) => ({
                            ...required,
                            id: 1000 + index,
                            node_id: `CR_${index}`,
                            name: `unrelated-${index}`,
                          }))
                        : [required],
                  };
        } else if (path.endsWith("/protection"))
          value = {
            enforce_admins: { enabled: true },
            allow_force_pushes: { enabled: false },
            allow_deletions: { enabled: false },
            required_status_checks: {
              strict: true,
              contexts: ["verify"],
              checks: [{ context: "verify", app_id: 11 }],
            },
            required_pull_request_reviews: null,
            required_signatures: { enabled: state.signatures },
            required_linear_history: { enabled: false },
            required_conversation_resolution: { enabled: false },
            restrictions: null,
            block_creations: false,
            lock_branch: { enabled: false },
            allow_fork_syncing: { enabled: false },
            ...(state.unknownRule ? { future_rule: { enabled: true } } : {}),
          };
        else if (path.includes("/rules/branches/"))
          value =
            state.hiddenRules || state.safeRules
              ? [{ type: "non_fast_forward", ruleset_id: 1 }]
              : [];
        else if (path.endsWith("/rulesets/1"))
          value = {
            id: 1,
            enforcement: "active",
            target: "branch",
            rules: [{ type: "non_fast_forward" }],
            ...(state.hiddenRules ? {} : { bypass_actors: [] }),
          };
        else {
          status = 404;
          value = { message: "fixture endpoint absent" };
        }
        return HttpClientResponse.fromWeb(
          request,
          new Response(JSON.stringify(value), {
            status,
            headers: { "content-type": "application/json" },
          }),
        );
      },
      catch: (cause) =>
        new HttpClientError.HttpClientError({
          reason: new HttpClientError.TransportError({ request, cause }),
        }),
    });
  const host = TestSourceControlHost.layer({
    settings: {
      ...DEFAULT_SERVER_SETTINGS,
      sourceControlHosts: {
        github: {
          tokens: options.app ? {} : tokens,
          hosts: {},
          signIns: options.app
            ? {
                "github.com": JSON.stringify({
                  accessToken: "fixture-app-token",
                  accessTokenExpiresAt: null,
                  refreshToken: null,
                  refreshTokenExpiresAt: null,
                }),
              }
            : {},
        },
      },
    },
    process: {
      run: (input) => {
        commands.push(input.args);
        return Effect.succeed(
          TestSourceControlHost.processOutput(
            input.args[0] === "auth" ? "fixture-personal-token" : "",
          ),
        );
      },
    },
  });
  const layer = Layer.mergeAll(
    host,
    NodeServices.layer,
    NodeCrypto.layer,
    SourceControlRateLimit.layer,
    Layer.succeed(HttpClient.HttpClient, HttpClient.make(response)),
  );
  const make = driver.make.pipe(
    Effect.provide(layer),
    Effect.provideService(
      HostProcess.Environment,
      options.app ? { T3CODE_GITHUB_APP_CLIENT_ID: "fixture-client" } : {},
    ),
  );
  return {
    state,
    requests,
    commands,
    make,
    changeActor: () => {
      tokens["github.com"] = "rotated-token";
      state.actor = 8;
    },
  };
}

it.effect(
  "fast-forwards only the inspected protected candidate using the driver's shared authenticated service",
  () =>
    Effect.gen(function* () {
      const f = fixture();
      const instance = yield* f.make;
      expect(instance.exactReleaseRefs).toBeDefined();
      const refs = instance.exactReleaseRefs!;
      const observed = yield* refs.inspect({ cwd: "/fixture", plan, subject });
      expect(observed).toMatchObject({
        main: { sha: B },
        candidate: { sha: C, treeSha: T, baseAncestor: true },
        protection: { state: "supported" },
        actor: { id: "7" },
      });
      expect(observed.requiredResults[0]?.receiptDigest).toBe(
        "0b3df7e10be94f2f2e5b278b5399e212f050b545e65a16aa59b54fba70b65c10",
      );
      const input: ExactReleaseFFInput = {
        cwd: "/fixture",
        plan,
        subject,
        beforeMutation: Effect.void,
        operationId: "merge-one",
        expectedActorId: "7",
        expectedPolicyDigest: observed.protection.policyDigest!,
      };
      const result = yield* refs.exactFF(input);
      expect(result).toMatchObject({
        kind: "acknowledged",
        sent: true,
        ack: { clientMutationId: "merge-one" },
        observation: { main: { sha: C, treeSha: T } },
      });
      const writes = f.requests.filter((r) => r.path === "/graphql");
      expect(writes).toHaveLength(1);
      expect(writes[0]?.body).toMatchObject({
        variables: {
          input: {
            repositoryId: "R_repo=",
            clientMutationId: "merge-one",
            refUpdates: [{ name: plan.ref, beforeOid: B, afterOid: C, force: false }],
          },
        },
      });
      expect(new Set(f.requests.map((r) => r.authorization))).toEqual(
        new Set(["Bearer fixture-token"]),
      );
    }).pipe(Effect.scoped),
);

it.effect("refuses ambiguous latest checks instead of assuming provider IDs encode freshness", () =>
  Effect.gen(function* () {
    const f = fixture();
    const refs = (yield* f.make).exactReleaseRefs!;
    const before = yield* refs.inspect({ cwd: "/fixture", plan, subject });
    f.state.ambiguous = true;
    expect(
      yield* Effect.result(
        refs.exactFF({
          cwd: "/fixture",
          plan,
          subject,
          beforeMutation: Effect.void,
          operationId: "merge-ambiguous",
          expectedActorId: "7",
          expectedPolicyDigest: before.protection.policyDigest!,
        }),
      ),
    ).toMatchObject({ _tag: "Failure" });
    expect(f.requests.filter((r) => r.path === "/graphql")).toHaveLength(0);
  }).pipe(Effect.scoped),
);

it.effect.each(["actor", "repository", "policy", "hidden"] as const)(
  "does not write after the signed %s changes",
  (mode) =>
    Effect.gen(function* () {
      const f = fixture();
      const refs = (yield* f.make).exactReleaseRefs!;
      const before = yield* refs.inspect({ cwd: "/fixture", plan, subject });
      if (mode === "actor") f.changeActor();
      if (mode === "repository") f.state.repositoryId = "R_other=";
      if (mode === "policy") f.state.signatures = true;
      if (mode === "hidden") f.state.hiddenRules = true;
      const result = yield* Effect.result(
        refs.exactFF({
          cwd: "/fixture",
          plan,
          subject,
          beforeMutation: Effect.void,
          operationId: "merge-identity",
          expectedActorId: "7",
          expectedPolicyDigest: before.protection.policyDigest!,
        }),
      );
      if (mode === "repository")
        expect(result).toMatchObject({ _tag: "Failure", failure: { reason: "identity_mismatch" } });
      else
        expect(result).toMatchObject({
          _tag: "Success",
          success: {
            kind: "rejected",
            sent: false,
            reason:
              mode === "actor"
                ? "actor_changed"
                : mode === "policy"
                  ? "policy_changed"
                  : "unavailable",
          },
        });
      expect(f.requests.filter((r) => r.path === "/graphql")).toHaveLength(0);
      expect(f.state.main).toBe(B);
    }).pipe(Effect.scoped),
);

it.effect("never hands an App-forbidden exact write to the personal credential", () =>
  Effect.gen(function* () {
    const f = fixture({ app: true });
    const refs = (yield* f.make).exactReleaseRefs!;
    const before = yield* refs.inspect({ cwd: "/fixture", plan, subject });
    f.state.deny = true;
    f.state.forbidden = true;
    const attempt = yield* refs.exactFF({
      cwd: "/fixture",
      plan,
      subject,
      beforeMutation: Effect.void,
      operationId: "merge-app",
      expectedActorId: "7",
      expectedPolicyDigest: before.protection.policyDigest!,
    });
    expect(attempt.kind).toBe("rejected");
    expect(f.requests.filter((r) => r.path === "/graphql")).toHaveLength(1);
    expect(new Set(f.requests.map((r) => r.authorization))).toEqual(
      new Set(["Bearer fixture-app-token"]),
    );
    expect(f.commands.filter((args) => args[0] === "auth")).toHaveLength(0);
  }).pipe(Effect.scoped),
);

it.effect("does not silently discard an unknown protection rule", () =>
  Effect.gen(function* () {
    const f = fixture();
    const refs = (yield* f.make).exactReleaseRefs!;
    const before = yield* refs.inspect({ cwd: "/fixture", plan, subject });
    f.state.unknownRule = true;
    expect(
      yield* refs.exactFF({
        cwd: "/fixture",
        plan,
        subject,
        beforeMutation: Effect.void,
        operationId: "merge-new-rule",
        expectedActorId: "7",
        expectedPolicyDigest: before.protection.policyDigest!,
      }),
    ).toMatchObject({ kind: "rejected", sent: false });
    expect(f.requests.filter((r) => r.path === "/graphql")).toHaveLength(0);
  }).pipe(Effect.scoped),
);

it.effect("requires a new main=C observed before dispatch to be the original B=C no-op", () =>
  Effect.gen(function* () {
    const f = fixture();
    const refs = (yield* f.make).exactReleaseRefs!;
    const before = yield* refs.inspect({ cwd: "/fixture", plan, subject });
    f.state.main = C;
    let intents = 0;
    const input = {
      cwd: "/fixture",
      plan,
      subject,
      beforeMutation: Effect.sync(() => {
        intents++;
      }),
      operationId: "merge-already",
      expectedActorId: "7",
      expectedPolicyDigest: before.protection.policyDigest!,
    };
    expect(yield* refs.exactFF(input)).toMatchObject({
      kind: "rejected",
      sent: false,
      reason: "main_changed",
      ack: null,
    });
    expect(yield* refs.exactFF({ ...input, subject: { ...subject, baseSha: C } })).toMatchObject({
      kind: "observed_matching",
      sent: false,
      ack: null,
    });
    expect(f.requests.filter((r) => r.path === "/graphql")).toHaveLength(0);
    expect(intents).toBe(0);
  }).pipe(Effect.scoped),
);

it.effect(
  "reads complete check pages and visible active no-bypass rulesets before allowing the fixed candidate",
  () =>
    Effect.gen(function* () {
      const f = fixture();
      f.state.safeRules = true;
      f.state.pagedChecks = true;
      const refs = (yield* f.make).exactReleaseRefs!;
      const observed = yield* refs.inspect({ cwd: "/fixture", plan, subject });
      expect(observed.protection.state).toBe("supported");
      expect(observed.requiredResults).toMatchObject([
        { state: "passed", checkId: "CR_check=", headSha: C },
      ]);
      expect(f.requests.some((r) => r.path.endsWith("/rulesets/1"))).toBe(true);
    }).pipe(Effect.scoped),
);

it.effect.each(["race", "deny"] as const)(
  "retains GitHub's definitive %s rejection without retrying the mutation",
  (mode) =>
    Effect.gen(function* () {
      const f = fixture();
      const refs = (yield* f.make).exactReleaseRefs!;
      const before = yield* refs.inspect({ cwd: "/fixture", plan, subject });
      f.state[mode] = true;
      const result = yield* refs.exactFF({
        cwd: "/fixture",
        plan,
        subject,
        beforeMutation: Effect.void,
        operationId: "merge-race",
        expectedActorId: "7",
        expectedPolicyDigest: before.protection.policyDigest!,
      });
      expect(result).toMatchObject({
        kind: "rejected",
        sent: true,
        ack: null,
        reason: mode === "race" ? "main_changed" : "protected_ref_rejected",
      });
      expect(f.requests.filter((r) => r.path === "/graphql")).toHaveLength(1);
      expect(f.state.main).toBe(mode === "race" ? D : B);
    }).pipe(Effect.scoped),
);

it.effect.each(["missing", "tree", "base"] as const)(
  "retains actual %s candidate counterevidence without a write",
  (mode) =>
    Effect.gen(function* () {
      const f = fixture();
      const refs = (yield* f.make).exactReleaseRefs!;
      const before = yield* refs.inspect({ cwd: "/fixture", plan, subject });
      if (mode === "missing") f.state.candidateMissing = true;
      else if (mode === "tree") f.state.candidateTree = "f".repeat(40);
      else f.state.baseAncestor = false;
      const attempt = yield* refs.exactFF({
        cwd: "/fixture",
        plan,
        subject,
        beforeMutation: Effect.void,
        operationId: "merge-candidate",
        expectedActorId: "7",
        expectedPolicyDigest: before.protection.policyDigest!,
      });
      expect(attempt).toMatchObject({
        kind: "rejected",
        sent: false,
        reason: mode === "missing" ? "candidate_unavailable" : "candidate_changed",
        observation: {
          candidate:
            mode === "missing"
              ? null
              : mode === "tree"
                ? { treeSha: "f".repeat(40) }
                : { baseAncestor: false },
        },
      });
      expect(f.requests.filter((r) => r.path === "/graphql")).toHaveLength(0);
    }).pipe(Effect.scoped),
);

it.effect.each([429, 503])(
  "never retries an uncertain HTTP %s mutation even while main is still B",
  (status) =>
    Effect.gen(function* () {
      const f = fixture();
      const refs = (yield* f.make).exactReleaseRefs!;
      const before = yield* refs.inspect({ cwd: "/fixture", plan, subject });
      f.state.mutationStatus = status;
      expect(
        yield* refs.exactFF({
          cwd: "/fixture",
          plan,
          subject,
          beforeMutation: Effect.void,
          operationId: "merge-http",
          expectedActorId: "7",
          expectedPolicyDigest: before.protection.policyDigest!,
        }),
      ).toMatchObject({ kind: "unknown", sent: true, ack: null });
      expect(f.state.main).toBe(B);
      expect(f.requests.filter((r) => r.path === "/graphql")).toHaveLength(1);
    }).pipe(Effect.scoped),
);

it.effect.each(["dropAck", "failReadback"] as const)(
  "keeps an uncertain %s write query-only and retains only a real ACK",
  (mode) =>
    Effect.gen(function* () {
      const f = fixture();
      const refs = (yield* f.make).exactReleaseRefs!;
      const before = yield* refs.inspect({ cwd: "/fixture", plan, subject });
      f.state[mode] = true;
      const result = yield* refs.exactFF({
        cwd: "/fixture",
        plan,
        subject,
        beforeMutation: Effect.void,
        operationId: "merge-unknown",
        expectedActorId: "7",
        expectedPolicyDigest: before.protection.policyDigest!,
      });
      expect(result).toMatchObject({
        kind: "unknown",
        sent: true,
        ack: mode === "dropAck" ? null : { clientMutationId: "merge-unknown" },
      });
      f.state[mode] = false;
      const recovered = yield* refs.inspect({ cwd: "/fixture", plan, subject });
      expect(recovered.main).toEqual({ sha: C, treeSha: T });
      expect(f.requests.filter((r) => r.path === "/graphql")).toHaveLength(1);
    }).pipe(Effect.scoped),
);

it.effect.each([200, 403, 422])(
  "never treats an HTTP %s error resolving a mutation result as proof that its write did not happen",
  (status) =>
    Effect.gen(function* () {
      const f = fixture();
      const refs = (yield* f.make).exactReleaseRefs!;
      const before = yield* refs.inspect({ cwd: "/fixture", plan, subject });
      f.state.partialMutationError = true;
      f.state.partialMutationStatus = status;
      expect(
        yield* refs.exactFF({
          cwd: "/fixture",
          plan,
          subject,
          beforeMutation: Effect.void,
          operationId: "merge-partial",
          expectedActorId: "7",
          expectedPolicyDigest: before.protection.policyDigest!,
        }),
      ).toMatchObject({ kind: "unknown", sent: true, ack: null });
      expect(f.state.main).toBe(C);
      expect(f.requests.filter((r) => r.path === "/graphql")).toHaveLength(1);
    }).pipe(Effect.scoped),
);

it.effect.each(["failure", "pending", "neutral", "skipped", "missing", "untrusted"])(
  "never writes a candidate with %s required checks",
  (state) =>
    Effect.gen(function* () {
      const f = fixture();
      const refs = (yield* f.make).exactReleaseRefs!;
      const before = yield* refs.inspect({ cwd: "/fixture", plan, subject });
      f.state.checks = state;
      const result = yield* Effect.result(
        refs.exactFF({
          cwd: "/fixture",
          plan,
          subject,
          beforeMutation: Effect.void,
          operationId: "merge-checks",
          expectedActorId: "7",
          expectedPolicyDigest: before.protection.policyDigest!,
        }),
      );
      expect(result).toMatchObject({
        _tag: "Success",
        success: {
          sent: false,
          reason:
            state === "pending"
              ? "checks_pending"
              : state === "missing" || state === "untrusted"
                ? "checks_missing"
                : "checks_failed",
        },
      });
      expect(f.requests.filter((r) => r.path === "/graphql")).toHaveLength(0);
      expect(f.state.main).toBe(B);
    }).pipe(Effect.scoped),
);

it.effect.each(["expectedActorId", "expectedPolicyDigest"] as const)(
  "rejects a runtime call without its signed %s pin",
  (field) =>
    Effect.gen(function* () {
      const f = fixture();
      const refs = (yield* f.make).exactReleaseRefs!;
      const before = yield* refs.inspect({ cwd: "/fixture", plan, subject });
      const input = {
        cwd: "/fixture",
        plan,
        subject,
        beforeMutation: Effect.void,
        operationId: "merge-invalid",
        expectedActorId: "7",
        expectedPolicyDigest: before.protection.policyDigest!,
        [field]: undefined,
      } as unknown as ExactReleaseFFInput;
      expect(yield* Effect.result(refs.exactFF(input))).toMatchObject({
        _tag: "Failure",
        failure: { _tag: "ExactReleaseRefsError", cause: { _tag: "SchemaError" } },
      });
      expect(f.requests.filter((r) => r.path === "/graphql")).toHaveLength(0);
    }).pipe(Effect.scoped),
);
