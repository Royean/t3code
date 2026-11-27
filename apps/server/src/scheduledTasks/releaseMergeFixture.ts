import type {
  ExactReleaseRefs,
  ExactReleaseFFAttempt,
  ExactReleaseRefsError,
} from "@t3tools/source-control-core/server/driver";
import type { WorkbenchMergeObservation, WorkbenchMergeRequiredResult } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as DateTime from "effect/DateTime";
import * as Deferred from "effect/Deferred";
import * as Runner from "../processRunner.ts";
import { ReleaseRefsError } from "./WorkbenchReleaseRefs.ts";
import { releaseDigest } from "./workbenchReleaseProfile.ts";

export const mergeFixture = Effect.fn("ReleaseMergeFixture.acquire")(function* () {
  const runner = yield* Runner.ProcessRunner;
  let failed = false,
    lost = false,
    attempts = 0,
    uncertainWithoutWrite = false;
  type Gate = {
    point: "after_intent" | "after_apply";
    reached: Deferred.Deferred<void>;
    resume: Deferred.Deferred<void>;
  };
  let nextGate: Gate | undefined;
  const waitAt = Effect.fn("ReleaseMergeFixture.waitAt")(function* (point: Gate["point"]) {
    if (nextGate?.point !== point) return;
    const gate = nextGate;
    nextGate = undefined;
    yield* Deferred.succeed(gate.reached, undefined);
    yield* Deferred.await(gate.resume);
  });
  const git = (cwd: string, args: ReadonlyArray<string>) =>
    runner.run({
      command: "/usr/bin/git",
      args,
      cwd,
      env: { PATH: "/usr/bin:/bin", GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null" },
      extendEnv: false,
      timeout: "10 seconds",
    });
  const inspect: ExactReleaseRefs["inspect"] = Effect.fn("ReleaseMergeFixture.inspect")(
    function* (input) {
      const cwd = input.cwd,
        remote = (yield* git(cwd, ["config", "remote.origin.url"])).stdout.trim();
      const read = (args: string[]) => git(cwd, ["--git-dir", remote, ...args]);
      const main = (yield* read(["rev-parse", input.plan.ref])).stdout.trim();
      const tree = (yield* read(["rev-parse", `${main}^{tree}`])).stdout.trim();
      const candidate = yield* read(["rev-parse", "--verify", `${input.subject.sourceSha}^{tree}`]);
      const ancestor = yield* read([
        "merge-base",
        "--is-ancestor",
        input.subject.baseSha,
        input.subject.sourceSha,
      ]);
      const check = yield* runner.run({
        command: process.execPath,
        args: [
          "-e",
          "const c=require('node:child_process');const v=c.execFileSync('/usr/bin/git',['--git-dir',process.argv[1],'show',process.argv[2]+':input.txt'],{encoding:'utf8'});console.log(v);process.exit(v==='candidate'&&process.argv[3]==='ok'?0:7)",
          remote,
          input.subject.sourceSha,
          failed ? "failed" : "ok",
        ],
        cwd,
        timeout: "10 seconds",
        extendEnv: false,
        env: { PATH: "/usr/bin:/bin" },
      });
      const results = input.plan.requiredChecks.map((ref) => {
        const value = {
          ...ref,
          headSha: input.subject.sourceSha,
          checkId: `check:${input.subject.sourceSha}`,
          state: check.code === 0 ? ("passed" as const) : ("failed" as const),
          conclusion: check.code === 0 ? ("success" as const) : ("failure" as const),
        };
        return {
          ...value,
          receiptDigest: releaseDigest(value),
        } satisfies WorkbenchMergeRequiredResult;
      });
      return {
        schemaVersion: 1,
        repository: {
          host: input.plan.host,
          name: input.plan.repository,
          id: input.plan.repositoryId,
        },
        actor: { id: "fixture-actor=", login: "fixture" },
        ref: input.plan.ref,
        main: { sha: main, treeSha: tree },
        candidate:
          candidate.code === 0
            ? {
                sha: input.subject.sourceSha,
                treeSha: candidate.stdout.trim(),
                baseAncestor: ancestor.code === 0,
              }
            : null,
        requiredResults: results,
        protection: {
          state: "supported",
          policyDigest: releaseDigest({
            requiredChecks: input.plan.requiredChecks,
            force: false,
            bypass: [],
          }),
          reasons: [],
        },
        observedAt: DateTime.formatIso(yield* DateTime.now),
      } satisfies WorkbenchMergeObservation;
    },
    Effect.mapError(
      (cause) => new ReleaseRefsError({ reason: "unavailable", stage: "fixture_inspect", cause }),
    ),
  );
  const exactFF: ExactReleaseRefs["exactFF"] = Effect.fn("ReleaseMergeFixture.exactFF")(
    function* (
      input,
    ): Effect.fn.Return<ExactReleaseFFAttempt, ExactReleaseRefsError | Runner.ProcessRunError> {
      const before = yield* inspect(input);
      if (before.main?.sha !== input.subject.baseSha)
        return {
          kind: "rejected",
          sent: false,
          reason: "main_changed",
          ack: null,
          observation: before,
        };
      if (before.requiredResults.some((r) => r.state !== "passed"))
        return {
          kind: "rejected",
          sent: false,
          reason: "checks_failed",
          ack: null,
          observation: before,
        };
      if (input.subject.baseSha === input.subject.sourceSha)
        return {
          kind: "observed_matching",
          sent: false,
          reason: null,
          ack: null,
          observation: before,
        };
      yield* input.beforeMutation;
      yield* waitAt("after_intent");
      if (uncertainWithoutWrite)
        return {
          kind: "unknown",
          sent: false,
          reason: "mutation_unknown",
          ack: null,
          observation: null,
        };
      const remote = (yield* git(input.cwd, ["config", "remote.origin.url"])).stdout.trim();
      attempts++;
      const result = yield* git(input.cwd, [
        "--git-dir",
        remote,
        "update-ref",
        "-m",
        input.operationId,
        input.plan.ref,
        input.subject.sourceSha,
        input.subject.baseSha,
      ]);
      if (result.code !== 0)
        return {
          kind: "rejected",
          sent: true,
          reason: "protected_ref_rejected",
          ack: null,
          observation: yield* inspect(input),
        };
      yield* waitAt("after_apply");
      if (lost) {
        lost = false;
        return {
          kind: "unknown",
          sent: true,
          reason: "mutation_unknown",
          ack: null,
          observation: null,
        };
      }
      return {
        kind: "acknowledged",
        sent: true,
        reason: null,
        ack: { clientMutationId: input.operationId },
        observation: yield* inspect(input),
      };
    },
    Effect.mapError(
      (cause) => new ReleaseRefsError({ reason: "unavailable", stage: "fixture_exact", cause }),
    ),
  );
  return {
    service: {
      inspect,
      exactFF,
      matchesSource: (remote: string, plan: { repositoryId: string }) =>
        remote.startsWith("/") &&
        remote.endsWith("/remote.git") &&
        plan.repositoryId === "repository=",
    },
    failChecks: (value: boolean) => {
      failed = value;
    },
    loseResponse: () => {
      lost = true;
    },
    attempts: () => attempts,
    uncertainWithoutWrite: (value: boolean) => {
      uncertainWithoutWrite = value;
    },
    gateNext: Effect.fn("ReleaseMergeFixture.gateNext")(function* (point: Gate["point"]) {
      const reached = yield* Deferred.make<void>(),
        resume = yield* Deferred.make<void>();
      nextGate = { point, reached, resume };
      return { reached: Deferred.await(reached), release: Deferred.succeed(resume, undefined) };
    }),
  };
});
