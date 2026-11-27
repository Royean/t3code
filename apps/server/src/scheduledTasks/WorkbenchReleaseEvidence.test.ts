import * as HostProcess from "@t3tools/shared/HostProcess";
import {
  AuthSessionId,
  EnvironmentAuthenticatedAuth,
  EnvironmentAuthenticatedPrincipal,
  EnvironmentHttpApi,
  type AuthEnvironmentScope,
} from "@t3tools/contracts";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpPlatform from "effect/http/HttpPlatform";
import * as Etag from "effect/http/Etag";
import * as ReleaseHttp from "./workbenchReleaseHttp.ts";
import { failEnvironmentAuthInvalid } from "../auth/http.ts";
// @effect-diagnostics nodeBuiltinImport:off -- The fixture owns a real loopback HTTP server and closes it through its scope.
import * as NodeHttp from "node:http";
import * as FileSystem from "effect/FileSystem";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import { ProjectId, WorkbenchReleaseError } from "@t3tools/contracts";
import { releaseHash } from "./workbenchReleaseProfile.ts";
import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Config from "../config.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Git from "../vcs/GitVcsDriver.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as ProcessRunner from "../processRunner.ts";
import * as Environment from "../environment/ServerEnvironment.ts";
import { EnvironmentId } from "@t3tools/contracts";
import * as Release from "./WorkbenchReleaseEvidence.ts";

const platform = Layer.mergeAll(NodeServices.layer, NodeCrypto.layer);
const config = Config.layerTest(process.cwd(), { prefix: "issue15-native-config-" });
const dependencies = Layer.mergeAll(
  platform,
  config.pipe(Layer.provide(platform)),
  Sqlite.layerMemory,
  Git.layer.pipe(
    Layer.provide(VcsProcess.layer),
    Layer.provide(ProcessRunner.layer),
    Layer.provide(config),
    Layer.provide(platform),
  ),
  ProcessRunner.layer.pipe(Layer.provide(platform)),
  Layer.succeed(Environment.ServerEnvironmentIdentity, {
    getEnvironmentId: Effect.succeed(EnvironmentId.make("environment")),
  }),
);
const layer = Release.layer.pipe(Layer.provideMerge(dependencies));

it.effect("an unconfigured host exposes no release commands or candidate authority", () =>
  Effect.gen(function* () {
    const releases = yield* Release.WorkbenchReleaseEvidence;
    expect(yield* releases.catalog({})).toEqual({
      environmentId: "environment",
      configured: false,
      profiles: [],
    });
  }).pipe(Effect.provide(layer)),
);

const scope = {
  controllerId: "controller",
  projectId: ProjectId.make("project"),
  expectedTrackingId: "tracking",
};
const fixture = Effect.gen(function* () {
  const fs = yield* FileSystem.FileSystem,
    path = yield* Path.Path,
    runner = yield* ProcessRunner.ProcessRunner,
    sql = yield* SqlClient.SqlClient,
    cfg = yield* Config.ServerConfig;
  const dir = yield* fs.makeTempDirectoryScoped({ prefix: "issue15-native-git-" });
  const repo = path.join(dir, "repo"),
    remote = path.join(dir, "remote.git");
  yield* fs.makeDirectory(repo);
  const git = (args: string[]) =>
    runner.run({
      command: "git",
      args,
      cwd: repo,
      env: { GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null" },
      timeout: "10 seconds",
    });
  yield* git(["init", "-b", "main"]);
  yield* git(["config", "user.email", "fixture@example.invalid"]);
  yield* git(["config", "user.name", "Release fixture"]);
  const files: Record<string, string> = {
    ".gitignore": "out.bin\nbaseline-private/\nignored-credential\n",
    "input.txt": "prior",
    "build.cjs":
      "const f=require('node:fs');if(f.existsSync('ignored-credential')||f.existsSync('.git'))process.exit(10);let found=false;try{require('node:child_process').execFileSync('git',['rev-parse','--show-toplevel'],{stdio:'ignore'});found=true}catch{}if(found)process.exit(11);f.copyFileSync('input.txt','out.bin')",
    "check.cjs":
      "if(require('node:fs').readFileSync('input.txt','utf8')!=='candidate')process.exit(7)",
    "artifact.cjs":
      "if(require('node:fs').readFileSync(process.env.T3_RELEASE_ARTIFACT,'utf8')!=='candidate')process.exit(8)",
    "rollback.cjs":
      "if(require('node:fs').readFileSync(process.env.T3_RELEASE_ROLLBACK_ARTIFACT,'utf8')!=='prior-artifact')process.exit(9)",
    "execute.cjs": "require('node:fs').writeFileSync('MUST-NOT-EXECUTE','bad');process.exit(99)",
  };
  for (const [name, data] of Object.entries(files))
    yield* fs.writeFileString(path.join(repo, name), data);
  yield* fs.makeDirectory(path.join(repo, "baseline-private"));
  yield* fs.writeFileString(path.join(repo, "baseline-private/baseline.bin"), "prior-artifact");
  yield* fs.writeFileString(path.join(repo, "ignored-credential"), "fixture-private-never-copy");
  yield* runner.run({ command: "git", args: ["init", cfg.stateDir] });
  yield* git(["add", "."]);
  yield* git(["commit", "-qm", "baseline"]);
  const baseline = (yield* git(["rev-parse", "HEAD"])).stdout.trim();
  yield* git(["clone", "--bare", repo, remote]);
  yield* git(["remote", "add", "origin", remote]);
  yield* git(["checkout", "-b", "candidate"]);
  yield* fs.writeFileString(path.join(repo, "input.txt"), "candidate");
  yield* git(["add", "input.txt"]);
  yield* git(["commit", "-qm", "candidate"]);
  const head = (yield* git(["rev-parse", "HEAD"])).stdout.trim();
  let targetVersion = "v1";
  let targetStatus = 200;
  const server = NodeHttp.createServer((_request, response) => {
    response.statusCode = targetStatus;
    response.setHeader("content-type", "application/json");
    response.end(
      JSON.stringify({
        serviceId: "service",
        version: targetVersion,
        sourceSha: baseline,
        artifactSha256: releaseHash("prior-artifact"),
      }),
    );
  });
  yield* Effect.acquireRelease(
    Effect.tryPromise({
      try: () => new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve)),
      catch: () => new WorkbenchReleaseError({ reason: "unavailable" }),
    }),
    () => Effect.promise(() => new Promise<void>((resolve) => server.close(() => resolve()))),
  );
  const address = server.address();
  if (!address || typeof address === "string")
    return yield* new WorkbenchReleaseError({ reason: "unavailable" });
  yield* Effect.logInfo(
    `issue15-native fixture ${dir}; loopback port ${address.port}; cleanup scoped`,
  );
  const commands = {
    build: "build.cjs",
    check: "check.cjs",
    health: "artifact.cjs",
    core: "artifact.cjs",
    execute: "execute.cjs",
    verify: "rollback.cjs",
  };
  const scripts = Object.entries(commands).map(([id, file]) => ({
    id,
    name: id,
    command: `${process.execPath} ${file}`,
    icon: "test",
    runOnWorktreeCreate: false,
  }));
  const ref = (id: string) => ({
    scriptId: id,
    definitionHash: releaseHash(
      JSON.stringify({ id, command: scripts.find((s) => s.id === id)!.command }),
    ),
  });
  const profile = {
    id: "profile",
    version: 1,
    projectId: "project",
    name: "Fixture release",
    enabled: true,
    policy: {
      main: { remote: "origin", ref: "refs/heads/main" },
      target: {
        serviceId: "service",
        origin: `http://127.0.0.1:${address.port}`,
        versionPath: "/version",
      },
      build: ref("build"),
      artifact: { relativePath: "out.bin" },
      checks: [ref("check")],
      health: ref("health"),
      core: ref("core"),
      observation: { durationSeconds: 60, intervalSeconds: 10, consecutiveSuccesses: 2 },
      rollback: {
        artifactRelativePath: "baseline-private/baseline.bin",
        execute: ref("execute"),
        verify: ref("verify"),
      },
    },
  };
  yield* fs.writeFileString(
    path.join(cfg.stateDir, "workbench-release-profiles.json"),
    JSON.stringify({ schemaVersion: 1, profiles: [profile] }),
  );
  const time = "2026-11-27T00:00:00.000Z";
  yield* sql`UPDATE scheduled_task_execution_metadata SET tracking_id='tracking'`;
  yield* sql`INSERT INTO workbench_queues VALUES('queue','controller','project')`;
  yield* sql`INSERT INTO projection_projects(project_id,title,workspace_root,scripts_json,created_at,updated_at) VALUES('project','Fixture',${repo},${JSON.stringify(scripts)},${time},${time})`;
  yield* sql`INSERT INTO scheduled_tasks(task_id,title,prompt,enabled,schedule_json,project_id,thread_id,thread_mode,workspace_strategy_json,model_selection_json,runtime_mode,interaction_mode,created_by,creation_source,created_at,updated_at,last_run_status,run_count) VALUES('task','Fixture','Fixture',1,'{"type":"webhook"}','project','dispatcher','continue','{}','{}','full-access','default','user','web',${time},${time},'idle',0)`;
  yield* sql`INSERT INTO workbench_queue_bindings(task_id,task_created_at,queue_id,project_id,dispatcher_thread_id) VALUES('task',${time},'queue','project','dispatcher')`;
  yield* sql`INSERT INTO orchestration_v2_projection_threads(thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,payload_json) VALUES('child','project','Child','codex','full-access','default',${time},${time},${JSON.stringify({ projectId: "project", worktreePath: repo, branch: "candidate" })})`;
  yield* sql`INSERT INTO workbench_work_items(id,task_id,task_created_at,project_id,business_delivery_id,dispatcher_thread_id,event_json,work_thread_id,worktree_path,branch,business_state,version,latest_event_sequence,processed_through_sequence,created_at,updated_at) VALUES('item','task',${time},'project','event','dispatcher','{}','child',${repo},'candidate','awaiting_delivery',1,1,1,${time},${time})`;
  return {
    repo,
    dir,
    head,
    profile,
    scripts,
    fs,
    path,
    git,
    sql,
    setTargetStatus: (value: number) => {
      targetStatus = value;
    },
    setTargetVersion: (value: string) => {
      targetVersion = value;
    },
  };
});
it.live(
  "prepares actual committed bytes and role receipts, never runs rollback execution, and invalidates changed source",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture,
        releases = yield* Release.WorkbenchReleaseEvidence;
      const catalog = yield* releases.catalog({ projectId: "project" });
      expect(catalog.profiles).toHaveLength(1);
      const input = {
        ...scope,
        preparationId: "prepare-1",
        workItemId: "item",
        expectedWorkItemVersion: 1,
        profileId: "profile",
        profileDigest: catalog.profiles[0]!.digest,
      };
      expect((yield* releases.prepare("queue", input)).preparation.state).toBe("queued");
      const prepared = (yield* releases.awaitPrepared("queue", "prepare-1", scope)).preparation;
      expect(prepared.reasons).toEqual([]);
      expect(prepared.state).toBe("ready");
      const evidence = prepared.evidence!;
      expect(evidence.source.commitSha).toBe(f.head);
      expect(evidence.artifact.sha256).toBe(releaseHash("candidate"));
      expect(evidence.receipts.map((r) => r.role)).toEqual([
        "build",
        "check",
        "health",
        "core",
        "rollback_verify",
      ]);
      expect(evidence.receipts.filter((r) => r.subject === "candidate_artifact")).toHaveLength(2);
      expect(yield* f.fs.exists(f.path.join(f.repo, "MUST-NOT-EXECUTE"))).toBe(false);
      expect((yield* releases.diff("queue", "prepare-1", scope)).diff.patch).toContain(
        "+candidate",
      );
      const current = yield* releases.revalidate("queue", "prepare-1", {
        ...scope,
        expectedEvidenceDigest: evidence.digest,
      });
      expect(current.preparation.validation.state).toBe("current");
      expect((yield* releases.prepare("queue", input)).preparation.evidence?.digest).toBe(
        evidence.digest,
      );
      yield* f.fs.writeFileString(f.path.join(f.repo, "input.txt"), "changed");
      const invalid = yield* releases.revalidate("queue", "prepare-1", {
        ...scope,
        expectedEvidenceDigest: evidence.digest,
      });
      expect(invalid.preparation.state).toBe("invalidated");
      yield* f.git(["restore", "input.txt"]);
      expect(
        (yield* releases.revalidate("queue", "prepare-1", {
          ...scope,
          expectedEvidenceDigest: evidence.digest,
        })).preparation.state,
      ).toBe("invalidated");
    }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.live.each(["build", "check", "health", "core", "verify"] as const)(
  "a failed %s stage cannot produce approvable evidence",
  (stage) =>
    Effect.gen(function* () {
      const f = yield* fixture,
        releases = yield* Release.WorkbenchReleaseEvidence;
      const file =
        stage === "build"
          ? "build.cjs"
          : stage === "check"
            ? "check.cjs"
            : stage === "verify"
              ? "rollback.cjs"
              : "artifact.cjs";
      yield* f.fs.writeFileString(f.path.join(f.repo, file), "process.exit(42)");
      yield* f.git(["add", file]);
      yield* f.git(["commit", "-qm", "failed stage"]);
      const catalog = yield* releases.catalog({ projectId: "project" });
      yield* releases.prepare("queue", {
        ...scope,
        preparationId: "failed",
        workItemId: "item",
        expectedWorkItemVersion: 1,
        profileId: "profile",
        profileDigest: catalog.profiles[0]!.digest,
      });
      const p = (yield* releases.awaitPrepared("queue", "failed", scope)).preparation;
      expect(p.state).toBe("blocked");
      expect(p.evidence).toBeNull();
      expect(p.reasons).toContain(
        stage === "build"
          ? "build_failed"
          : stage === "check"
            ? "check_failed"
            : stage === "verify"
              ? "rollback_failed"
              : "preflight_failed",
      );
    }).pipe(Effect.scoped, Effect.provide(layer)),
);
it.live.each(["main", "watermark", "profile", "artifact", "rollback", "target"] as const)(
  "observed %s drift invalidates an immutable prepared packet",
  (kind) =>
    Effect.gen(function* () {
      const f = yield* fixture,
        releases = yield* Release.WorkbenchReleaseEvidence,
        cfg = yield* Config.ServerConfig;
      const catalog = yield* releases.catalog({});
      const input = {
        ...scope,
        preparationId: "drift",
        workItemId: "item",
        expectedWorkItemVersion: 1,
        profileId: "profile",
        profileDigest: catalog.profiles[0]!.digest,
      };
      yield* releases.prepare("queue", input);
      const before = (yield* releases.awaitPrepared("queue", "drift", scope)).preparation;
      expect(before.state).toBe("ready");
      if (kind === "main") yield* f.git(["push", "origin", "candidate:main"]);
      if (kind === "watermark")
        yield* f.sql`UPDATE workbench_work_items SET latest_event_sequence=2,processed_through_sequence=2 WHERE id='item'`;
      if (kind === "profile")
        yield* f.fs.writeFileString(
          f.path.join(cfg.stateDir, "workbench-release-profiles.json"),
          JSON.stringify({ schemaVersion: 1, profiles: [{ ...f.profile, version: 2 }] }),
        );
      if (kind === "artifact" || kind === "rollback") {
        const file = f.path.join(
          cfg.stateDir,
          "workbench-release-evidence",
          releaseHash("drift"),
          kind,
        );
        yield* f.fs.chmod(file, 0o600);
        yield* f.fs.writeFileString(file, "tampered");
      }
      if (kind === "target") f.setTargetVersion("v2");
      const after = (yield* releases.revalidate("queue", "drift", {
        ...scope,
        expectedEvidenceDigest: before.evidence!.digest,
      })).preparation;
      expect(after.state).toBe("invalidated");
      expect(after.evidence?.digest).toBe(before.evidence?.digest);
    }).pipe(Effect.scoped, Effect.provide(layer)),
);
it.live("a new service observes an interrupted durable prepare without rebuilding", () =>
  Effect.gen(function* () {
    yield* fixture;
    yield* Effect.scoped(
      Effect.gen(function* () {
        const releases = yield* Release.WorkbenchReleaseEvidence,
          catalog = yield* releases.catalog({});
        yield* releases.prepare("queue", {
          ...scope,
          preparationId: "interrupted",
          workItemId: "item",
          expectedWorkItemVersion: 1,
          profileId: "profile",
          profileDigest: catalog.profiles[0]!.digest,
        });
      }).pipe(Effect.provide(Release.layer)),
    );
    const observed = yield* Effect.scoped(
      Effect.gen(function* () {
        const releases = yield* Release.WorkbenchReleaseEvidence;
        return yield* releases.get("queue", "interrupted", scope);
      }).pipe(Effect.provide(Release.layer)),
    );
    expect(observed.preparation.state).toBe("unknown");
    expect(observed.preparation.reasons).toContain("preparation_interrupted");
  }).pipe(Effect.scoped, Effect.provide(dependencies)),
);

class ReleaseTestApi extends HttpApi.make("environment").add(
  EnvironmentHttpApi.groups.workbenchRelease,
) {}
it.live(
  "release HTTP requires both scopes, returns durable 202, and fences cross-controller reads",
  () =>
    Effect.gen(function* () {
      yield* fixture;
      const releases = yield* Release.WorkbenchReleaseEvidence;
      const web = HttpRouter.toWebHandler(
        HttpApiBuilder.layer(ReleaseTestApi).pipe(
          Layer.provide(ReleaseHttp.layer),
          Layer.provide(Layer.succeed(Release.WorkbenchReleaseEvidence, releases)),
          Layer.provide(
            Layer.succeed(EnvironmentAuthenticatedAuth, (effect) =>
              Effect.gen(function* () {
                const request = yield* HttpServerRequest.HttpServerRequest;
                if (!request.headers["test-scopes"])
                  return yield* failEnvironmentAuthInvalid("missing_credential");
                return yield* effect.pipe(
                  Effect.provideService(EnvironmentAuthenticatedPrincipal, {
                    sessionId: AuthSessionId.make("fixture"),
                    subject: "fixture",
                    method: "bearer-access-token",
                    scopes: new Set(
                      request.headers["test-scopes"].split(",") as AuthEnvironmentScope[],
                    ),
                  }),
                );
              }),
            ),
          ),
          Layer.provide(
            HttpPlatform.layer.pipe(
              Layer.provide(NodeServices.layer),
              Layer.provide(Etag.layerWeak),
            ),
          ),
          Layer.provide(Etag.layerWeak),
          Layer.provide(platform),
        ),
        { disableLogger: true },
      );
      yield* Effect.addFinalizer(() => Effect.promise(() => web.dispose()));
      const both = "orchestration:read,orchestration:operate";
      const call = (url: string, scopes?: string, body?: unknown) =>
        Effect.promise(() =>
          web.handler(
            new Request(`http://fixture.local${url}`, {
              method: body === undefined ? "GET" : "POST",
              headers: {
                ...(scopes ? { "test-scopes": scopes } : {}),
                ...(body === undefined ? {} : { "content-type": "application/json" }),
              },
              ...(body === undefined ? {} : { body: JSON.stringify(body) }),
            }),
          ),
        );
      expect((yield* call("/api/workbench/release-profiles")).status).toBe(401);
      expect((yield* call("/api/workbench/release-profiles", "orchestration:read")).status).toBe(
        403,
      );
      expect((yield* call("/api/workbench/release-profiles", "orchestration:operate")).status).toBe(
        403,
      );
      const catalogResponse = yield* call(
        "/api/workbench/release-profiles?projectId=project",
        both,
      );
      expect(catalogResponse.status).toBe(200);
      const catalog = yield* releases.catalog({});
      const input = {
        ...scope,
        preparationId: "http",
        workItemId: "item",
        expectedWorkItemVersion: 1,
        profileId: "profile",
        profileDigest: catalog.profiles[0]!.digest,
      };
      const accepted = yield* call("/api/workbench/queues/queue/release-preparations", both, input);
      expect(accepted.status).toBe(202);
      const prepared = yield* releases.awaitPrepared("queue", "http", scope);
      expect(prepared.preparation.state).toBe("ready");
      const base = "/api/workbench/queues/queue/release-preparations/http";
      expect(
        (yield* call(
          `${base}?controllerId=other&projectId=project&expectedTrackingId=tracking`,
          both,
        )).status,
      ).toBe(409);
      expect(
        (yield* call(
          "/api/workbench/queues/queue/release-preparations/absent?controllerId=controller&projectId=project&expectedTrackingId=tracking",
          both,
        )).status,
      ).toBe(404);
      expect(
        (yield* call("/api/workbench/queues/queue/release-preparations", both, {
          ...input,
          profileDigest: "a".repeat(64),
        })).status,
      ).toBe(409);
      expect(
        (yield* call(`${base}/revalidate`, both, {
          ...scope,
          expectedEvidenceDigest: prepared.preparation.evidence!.digest,
        })).status,
      ).toBe(200);
      const diff = yield* call(
        `${base}/diff?controllerId=controller&projectId=project&expectedTrackingId=tracking`,
        both,
      );
      expect(diff.status).toBe(200);
      expect(yield* Effect.promise(() => diff.text())).not.toContain("prior-artifact");
    }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.live.each(["rollback_missing", "rollback_mismatch", "artifact_invalid"] as const)(
  "preparation blocks on actual %s facts and removes its temporary source",
  (reason) =>
    Effect.gen(function* () {
      const f = yield* fixture,
        releases = yield* Release.WorkbenchReleaseEvidence,
        cfg = yield* Config.ServerConfig;
      const baseline = f.path.join(f.repo, "baseline-private/baseline.bin");
      if (reason === "rollback_missing") yield* f.fs.remove(baseline);
      if (reason === "rollback_mismatch") yield* f.fs.writeFileString(baseline, "wrong baseline");
      if (reason === "artifact_invalid") {
        yield* f.fs.writeFileString(
          f.path.join(f.repo, "build.cjs"),
          "require('node:fs').symlinkSync('input.txt','out.bin')",
        );
        yield* f.git(["add", "build.cjs"]);
        yield* f.git(["commit", "-qm", "invalid artifact"]);
      }
      const catalog = yield* releases.catalog({});
      yield* releases.prepare("queue", {
        ...scope,
        preparationId: "invalid",
        workItemId: "item",
        expectedWorkItemVersion: 1,
        profileId: "profile",
        profileDigest: catalog.profiles[0]!.digest,
      });
      const p = (yield* releases.awaitPrepared("queue", "invalid", scope)).preparation;
      expect(p.state).toBe("blocked");
      expect(p.evidence).toBeNull();
      expect(p.reasons).toContain(reason);
      expect(
        yield* f.fs.exists(
          f.path.join(cfg.stateDir, "workbench-release-evidence", releaseHash("invalid"), "source"),
        ),
      ).toBe(false);
    }).pipe(Effect.scoped, Effect.provide(layer)),
);
it.live("unprocessed events prevent preparation without touching the registered source", () =>
  Effect.gen(function* () {
    const f = yield* fixture,
      releases = yield* Release.WorkbenchReleaseEvidence;
    yield* f.sql`UPDATE workbench_work_items SET latest_event_sequence=2 WHERE id='item'`;
    const catalog = yield* releases.catalog({});
    const error = yield* releases
      .prepare("queue", {
        ...scope,
        preparationId: "pending",
        workItemId: "item",
        expectedWorkItemVersion: 1,
        profileId: "profile",
        profileDigest: catalog.profiles[0]!.digest,
      })
      .pipe(Effect.flip);
    expect(error.reason).toBe("stale_state");
    expect((yield* f.git(["rev-parse", "HEAD"])).stdout.trim()).toBe(f.head);
  }).pipe(Effect.scoped, Effect.provide(layer)),
);
it.live("an unavailable target is unknown until a fresh exact revalidation succeeds", () =>
  Effect.gen(function* () {
    const f = yield* fixture,
      releases = yield* Release.WorkbenchReleaseEvidence,
      catalog = yield* releases.catalog({});
    yield* releases.prepare("queue", {
      ...scope,
      preparationId: "outage",
      workItemId: "item",
      expectedWorkItemVersion: 1,
      profileId: "profile",
      profileDigest: catalog.profiles[0]!.digest,
    });
    const p = (yield* releases.awaitPrepared("queue", "outage", scope)).preparation;
    expect(p.state).toBe("ready");
    f.setTargetStatus(503);
    const input = { ...scope, expectedEvidenceDigest: p.evidence!.digest };
    expect(
      (yield* releases.revalidate("queue", "outage", input)).preparation.validation.state,
    ).toBe("unknown");
    f.setTargetStatus(200);
    expect(
      (yield* releases.revalidate("queue", "outage", input)).preparation.validation.state,
    ).toBe("current");
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.live("revalidation catches tracked source changing during its final remote read", () =>
  Effect.gen(function* () {
    const f = yield* fixture,
      releases = yield* Release.WorkbenchReleaseEvidence,
      catalog = yield* releases.catalog({});
    yield* releases.prepare("queue", {
      ...scope,
      preparationId: "remote-race",
      workItemId: "item",
      expectedWorkItemVersion: 1,
      profileId: "profile",
      profileDigest: catalog.profiles[0]!.digest,
    });
    const p = (yield* releases.awaitPrepared("queue", "remote-race", scope)).preparation;
    expect(p.state).toBe("ready");
    const wrapper = f.path.join(f.dir, "upload-pack"),
      counter = f.path.join(f.dir, "remote-count");
    const quote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`;
    yield* f.fs.writeFileString(
      wrapper,
      `#!/bin/sh\nprintf x >> ${quote(counter)}\nif [ "$(wc -c < ${quote(counter)})" -eq 2 ]; then printf changed > ${quote(f.path.join(f.repo, "input.txt"))}; fi\nexec /usr/bin/git upload-pack "$@"\n`,
    );
    yield* f.fs.chmod(wrapper, 0o700);
    yield* f.git(["config", "remote.origin.uploadpack", wrapper]);
    const after = (yield* releases.revalidate("queue", "remote-race", {
      ...scope,
      expectedEvidenceDigest: p.evidence!.digest,
    })).preparation;
    expect(after.state).toBe("invalidated");
    expect(after.reasons).toContain("source_dirty");
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.live.each(["single", "double"] as const)(
  "executes a registered %s quoted script path and argument as exact argv",
  (quoting) =>
    Effect.gen(function* () {
      const f = yield* fixture,
        releases = yield* Release.WorkbenchReleaseEvidence,
        cfg = yield* Config.ServerConfig;
      const contents = yield* f.fs.readFileString(f.path.join(f.repo, "build.cjs"));
      yield* f.fs.writeFileString(
        f.path.join(f.repo, "release build.cjs"),
        "if(process.argv.length!==3||process.argv[2]!=='argument with spaces')process.exit(12);" +
          contents,
      );
      yield* f.fs.remove(f.path.join(f.repo, "build.cjs"));
      yield* f.git(["add", "-A"]);
      yield* f.git(["commit", "-qm", "quoted build path"]);
      const quote = quoting === "single" ? "'" : '"';
      const command = `${process.execPath} ${quote}release build.cjs${quote} ${quote}argument with spaces${quote}`;
      const scripts = f.scripts.map((script) =>
        script.id === "build" ? { ...script, command } : script,
      );
      const definitionHash = releaseHash(JSON.stringify({ id: "build", command }));
      const profile = {
        ...f.profile,
        policy: { ...f.profile.policy, build: { scriptId: "build", definitionHash } },
      };
      yield* f.sql`UPDATE projection_projects SET scripts_json=${JSON.stringify(scripts)} WHERE project_id='project'`;
      yield* f.fs.writeFileString(
        f.path.join(cfg.stateDir, "workbench-release-profiles.json"),
        JSON.stringify({ schemaVersion: 1, profiles: [profile] }),
      );
      const catalog = yield* releases.catalog({});
      yield* releases.prepare("queue", {
        ...scope,
        preparationId: "quoted-command",
        workItemId: "item",
        expectedWorkItemVersion: 1,
        profileId: "profile",
        profileDigest: catalog.profiles[0]!.digest,
      });
      const p = (yield* releases.awaitPrepared("queue", "quoted-command", scope)).preparation;
      expect(p.state).toBe("ready");
      expect(p.reasons).toEqual([]);
      expect(p.evidence!.artifact.sha256).toBe(releaseHash("candidate"));
      expect(p.evidence!.receipts[0]).toMatchObject({ definitionHash, role: "build", exitCode: 0 });
    }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.live.each(["unclosed", "unsupported_escape", "operator", "glob"] as const)(
  "does not advertise a release profile with %s command syntax",
  (syntax) =>
    Effect.gen(function* () {
      const f = yield* fixture,
        releases = yield* Release.WorkbenchReleaseEvidence,
        cfg = yield* Config.ServerConfig;
      const suffix =
        syntax === "unclosed"
          ? '"build.cjs'
          : syntax === "unsupported_escape"
            ? String.raw`build\q.cjs`
            : syntax === "operator"
              ? "build.cjs && echo bad"
              : "*.cjs";
      const command = `${process.execPath} ${suffix}`;
      const scripts = f.scripts.map((script) =>
        script.id === "build" ? { ...script, command } : script,
      );
      const profile = {
        ...f.profile,
        policy: {
          ...f.profile.policy,
          build: {
            scriptId: "build",
            definitionHash: releaseHash(JSON.stringify({ id: "build", command })),
          },
        },
      };
      yield* f.sql`UPDATE projection_projects SET scripts_json=${JSON.stringify(scripts)} WHERE project_id='project'`;
      yield* f.fs.writeFileString(
        f.path.join(cfg.stateDir, "workbench-release-profiles.json"),
        JSON.stringify({ schemaVersion: 1, profiles: [profile] }),
      );
      expect((yield* releases.catalog({}).pipe(Effect.flip)).reason).toBe("profile_changed");
    }).pipe(Effect.scoped, Effect.provide(layer)),
);
it.live("does not advertise enabled release execution on an unsupported host platform", () =>
  Effect.gen(function* () {
    yield* fixture;
    const releases = yield* Release.WorkbenchReleaseEvidence;
    const error = yield* releases
      .catalog({})
      .pipe(Effect.provideService(HostProcess.Platform, "win32"), Effect.flip);
    expect(error.reason).toBe("profile_invalid");
  }).pipe(Effect.scoped, Effect.provide(layer)),
);
