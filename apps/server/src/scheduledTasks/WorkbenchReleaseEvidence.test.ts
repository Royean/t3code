import * as Clock from "effect/Clock";
import { canonicalReleaseJson, releaseDigest } from "./workbenchReleaseProfile.ts";
import * as TestClock from "effect/testing/TestClock";
import * as Scope from "effect/Scope";
import * as Exit from "effect/Exit";
import * as DeploymentHttp from "./workbenchDeploymentHttp.ts";
import * as Deployment from "./WorkbenchDeployment.ts";
import * as ReleaseAdapter from "./WorkbenchReleaseAdapter.ts";
import { deploymentFixture } from "./releaseDeploymentFixture.ts";
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
import * as Logger from "effect/Logger";
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
const systemDependencies = Layer.mergeAll(
  platform,
  config.pipe(Layer.provide(platform)),
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
const baseDependencies = Layer.mergeAll(systemDependencies, Sqlite.layerMemory);
const dependencies = ReleaseAdapter.layer.pipe(Layer.provideMerge(baseDependencies));
const releaseLayer = Release.layer.pipe(Layer.provideMerge(dependencies));
const layer = Deployment.layer.pipe(Layer.provideMerge(releaseLayer));

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
      "if(process.env.T3_RELEASE_PHASE==='online'){(async()=>{const r=await fetch(process.env.T3_RELEASE_TARGET_ORIGIN+'/checks/'+process.env.T3_RELEASE_CHECK_ROLE,{headers:{'x-release-execution':process.env.T3_RELEASE_EXECUTION_ID}});if(!r.ok)process.exit(31);const v=await r.json();if(v.artifactSha256!==process.env.T3_RELEASE_EXPECTED_ARTIFACT_SHA256||v.value!=='CANDIDATE')process.exit(32)})().catch(()=>process.exit(33))}else if(require('node:fs').readFileSync(process.env.T3_RELEASE_ARTIFACT,'utf8')!=='candidate')process.exit(8)",
    "rollback.cjs":
      "if(!['prior-artifact','candidate'].includes(require('node:fs').readFileSync(process.env.T3_RELEASE_ROLLBACK_ARTIFACT,'utf8')))process.exit(9)",
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
it.live("release HTTP fences scopes and keeps actual process failure diagnostics private", () =>
  Effect.gen(function* () {
    const f = yield* fixture,
      cfg = yield* Config.ServerConfig;
    const diagnostics: unknown[] = [];
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
          HttpPlatform.layer.pipe(Layer.provide(NodeServices.layer), Layer.provide(Etag.layerWeak)),
        ),
        Layer.provide(Etag.layerWeak),
        Layer.provide(platform),
        Layer.provide(
          Logger.layer([Logger.make(({ message }) => diagnostics.push(message))], {
            mergeWithExisting: false,
          }),
        ),
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
    expect((yield* call("/api/workbench/release-profiles", "orchestration:read")).status).toBe(403);
    expect((yield* call("/api/workbench/release-profiles", "orchestration:operate")).status).toBe(
      403,
    );
    const catalogResponse = yield* call("/api/workbench/release-profiles?projectId=project", both);
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
    const marker = "PRIVATE_RELEASE_SPAWN_DIAGNOSTIC";
    const command = f.path.join(f.dir, marker);
    const scripts = f.scripts.map((script) =>
      script.id === "build" ? { ...script, command } : script,
    );
    const failureProfile = {
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
      JSON.stringify({ schemaVersion: 1, profiles: [failureProfile] }),
    );
    const failureCatalog = yield* releases.catalog({});
    expect(
      (yield* call("/api/workbench/queues/queue/release-preparations", both, {
        ...input,
        preparationId: "diagnostic",
        profileDigest: failureCatalog.profiles[0]!.digest,
      })).status,
    ).toBe(202);
    expect((yield* releases.awaitPrepared("queue", "diagnostic", scope)).preparation.state).toBe(
      "blocked",
    );
    const result = yield* call(
      "/api/workbench/queues/queue/release-preparations/diagnostic?controllerId=controller&projectId=project&expectedTrackingId=tracking",
      both,
    );
    expect(result.status).toBe(200);
    const body = yield* Effect.promise(() => result.text());
    expect(JSON.parse(body)).toMatchObject({
      preparation: { state: "blocked", reasons: ["build_failed"], evidence: null },
    });
    for (const secret of [marker, f.dir, "cause", "ProcessSpawnError"])
      expect(body).not.toContain(secret);
    const diagnostic = diagnostics
      .flatMap((entry) => (Array.isArray(entry) ? entry : []))
      .find(
        (entry) =>
          typeof entry === "object" &&
          entry !== null &&
          entry._tag === "ReleaseEvidenceOperationError",
      );
    expect(diagnostic).toMatchObject({
      stage: "build",
      reason: "build_failed",
      cause: { _tag: "ProcessSpawnError", cause: expect.anything() },
    });
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

it.live.each(["tracked_source", "remote_url"] as const)(
  "revalidation catches %s changing during its final remote read",
  (material) =>
    Effect.gen(function* () {
      const f = yield* fixture,
        realRunner = yield* ProcessRunner.ProcessRunner;
      let observations = 0,
        armed = false;
      const alternate = f.path.join(f.dir, "race-alternate.git");
      yield* f.git(["clone", "--bare", f.path.join(f.dir, "remote.git"), alternate]);
      const gatedRunner: ProcessRunner.ProcessRunner["Service"] = {
        run: Effect.fn("releaseFixture.gatedProcess")(function* (input) {
          const result = yield* realRunner.run(input);
          if (
            armed &&
            input.command === "/usr/bin/git" &&
            input.args.includes("ls-remote") &&
            ++observations === 2
          ) {
            if (material === "tracked_source")
              yield* f.fs
                .writeFileString(f.path.join(f.repo, "input.txt"), "changed")
                .pipe(Effect.orDie);
            else
              yield* realRunner.run({
                command: "/usr/bin/git",
                args: ["-C", f.repo, "remote", "set-url", "origin", alternate],
              });
          }
          return result;
        }),
      };
      yield* Effect.gen(function* () {
        const releases = yield* Release.WorkbenchReleaseEvidence,
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
        armed = true;
        const after = (yield* releases.revalidate("queue", "remote-race", {
          ...scope,
          expectedEvidenceDigest: p.evidence!.digest,
        })).preparation;
        expect(after.state).toBe("invalidated");
        expect(after.reasons).toContain(
          material === "tracked_source" ? "source_dirty" : "source_changed",
        );
      }).pipe(
        Effect.provide(
          Release.layer.pipe(
            Layer.provide(Layer.succeed(ProcessRunner.ProcessRunner, gatedRunner)),
          ),
        ),
      );
    }).pipe(Effect.scoped, Effect.provide(dependencies)),
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

it.live("release Git reads suppress project fsmonitor and per-remote executable helpers", () =>
  Effect.gen(function* () {
    const f = yield* fixture,
      releases = yield* Release.WorkbenchReleaseEvidence,
      catalog = yield* releases.catalog({});
    const marker = f.path.join(f.dir, "UNREGISTERED_HELPER_RAN"),
      monitor = f.path.join(f.dir, "monitor"),
      upload = f.path.join(f.dir, "upload"),
      seenEnv = f.path.join(f.dir, "remote-env");
    const quote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`;
    yield* f.fs.writeFileString(
      monitor,
      `#!/bin/sh\nprintf helper > ${quote(marker)}\nprintf 'token\\0'\n`,
    );
    yield* f.fs.chmod(monitor, 0o700);
    yield* f.fs.writeFileString(
      upload,
      `#!/bin/sh\nprintf '%s' "\${T3_RELEASE_FIXTURE_SERVICE_SECRET-unset}" > ${quote(seenEnv)}\nexec /usr/bin/git upload-pack "$@"\n`,
    );
    yield* f.fs.chmod(upload, 0o700);
    yield* f.git(["config", "core.fsmonitor", monitor]);
    yield* f.git(["config", "core.fsmonitorHookVersion", "2"]);
    yield* f.git(["config", "remote.origin.uploadpack", upload]);
    const host = yield* HostProcess.Environment;
    const isolated = Effect.gen(function* () {
      yield* releases.prepare("queue", {
        ...scope,
        preparationId: "git-boundary",
        workItemId: "item",
        expectedWorkItemVersion: 1,
        profileId: "profile",
        profileDigest: catalog.profiles[0]!.digest,
      });
      const p = (yield* releases.awaitPrepared("queue", "git-boundary", scope)).preparation;
      expect(p.state).toBe("ready");
      expect(yield* f.fs.exists(marker)).toBe(false);
      expect(yield* f.fs.exists(seenEnv)).toBe(false);
      expect(
        (yield* releases.revalidate("queue", "git-boundary", {
          ...scope,
          expectedEvidenceDigest: p.evidence!.digest,
        })).preparation.validation.state,
      ).toBe("current");
      expect(yield* f.fs.exists(marker)).toBe(false);
      expect(yield* f.fs.exists(seenEnv)).toBe(false);
    });
    yield* isolated.pipe(
      Effect.provideService(HostProcess.Environment, {
        ...host,
        T3_RELEASE_FIXTURE_SERVICE_SECRET: "SYNTHETIC_NEVER_PASS_TO_GIT",
      }),
    );
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.live(
  "fresh main retains host credential authentication without passing service secrets to helpers",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture,
        releases = yield* Release.WorkbenchReleaseEvidence,
        cfg = yield* Config.ServerConfig,
        runner = yield* ProcessRunner.ProcessRunner;
      const home = f.path.join(f.dir, "auth-home"),
        helper = f.path.join(f.dir, "credential-helper"),
        observed = f.path.join(f.dir, "auth-env");
      yield* f.fs.makeDirectory(home);
      const quote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`;
      yield* f.fs.writeFileString(
        helper,
        `#!/bin/sh\nif [ "$1" = get ]; then printf '%s' "\${T3_RELEASE_FIXTURE_SERVICE_SECRET-unset}" > ${quote(observed)}; printf 'username=release-fixture\\npassword=only-fixture\\n'; fi\n`,
      );
      yield* f.fs.chmod(helper, 0o700);
      yield* f.fs.writeFileString(
        f.path.join(home, ".gitconfig"),
        `[credential]\n\thelper = ${helper}\n`,
      );
      const parentMarker = f.path.join(f.dir, "PARENT_GIT_CONFIG_RAN"),
        parentHelper = f.path.join(f.dir, "parent-helper");
      yield* f.fs.writeFileString(
        parentHelper,
        `#!/bin/sh\nprintf parent > ${quote(parentMarker)}\nprintf 'username=wrong\\npassword=wrong\\n'\n`,
      );
      yield* f.fs.chmod(parentHelper, 0o700);
      yield* runner.run({
        command: "/usr/bin/git",
        args: ["-C", cfg.stateDir, "config", "--add", "credential.helper", ""],
      });
      yield* runner.run({
        command: "/usr/bin/git",
        args: ["-C", cfg.stateDir, "config", "--add", "credential.helper", parentHelper],
      });

      const baseline = (yield* f.git(["rev-parse", "main"])).stdout.trim();
      const server = NodeHttp.createServer((request, response) => {
        if (request.headers.authorization !== "Basic cmVsZWFzZS1maXh0dXJlOm9ubHktZml4dHVyZQ==") {
          response.writeHead(401, { "www-authenticate": 'Basic realm="release-fixture"' });
          response.end();
          return;
        }
        response.setHeader("content-type", "text/plain");
        if (request.url?.startsWith("/repo/info/refs"))
          response.end(`${baseline}\trefs/heads/main\n`);
        else if (request.url === "/repo/HEAD") response.end("ref: refs/heads/main\n");
        else {
          response.statusCode = 404;
          response.end();
        }
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
      yield* f.git(["remote", "set-url", "origin", `http://127.0.0.1:${address.port}/repo`]);
      const host = yield* HostProcess.Environment;
      yield* Effect.gen(function* () {
        const catalog = yield* releases.catalog({});
        yield* releases.prepare("queue", {
          ...scope,
          preparationId: "host-auth",
          workItemId: "item",
          expectedWorkItemVersion: 1,
          profileId: "profile",
          profileDigest: catalog.profiles[0]!.digest,
        });
        const p = (yield* releases.awaitPrepared("queue", "host-auth", scope)).preparation;
        expect(p.state).toBe("ready");
        expect(yield* f.fs.readFileString(observed)).toBe("unset");
        expect(yield* f.fs.exists(parentMarker)).toBe(false);
        expect(
          (yield* releases.revalidate("queue", "host-auth", {
            ...scope,
            expectedEvidenceDigest: p.evidence!.digest,
          })).preparation.validation.state,
        ).toBe("current");
        expect(yield* f.fs.readFileString(observed)).toBe("unset");
        expect(yield* f.fs.exists(parentMarker)).toBe(false);
      }).pipe(
        Effect.provideService(HostProcess.Environment, {
          ...host,
          HOME: home,
          GIT_DIR: f.path.join(f.repo, ".git"),
          GIT_WORK_TREE: f.repo,
          T3_RELEASE_FIXTURE_SERVICE_SECRET: "SYNTHETIC_MUST_NOT_REACH_AUTH",
        }),
      );
    }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.live(
  "changing only the registered remote URL invalidates old evidence even at the same main SHA",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture,
        releases = yield* Release.WorkbenchReleaseEvidence,
        catalog = yield* releases.catalog({});
      yield* releases.prepare("queue", {
        ...scope,
        preparationId: "remote-url",
        workItemId: "item",
        expectedWorkItemVersion: 1,
        profileId: "profile",
        profileDigest: catalog.profiles[0]!.digest,
      });
      const p = (yield* releases.awaitPrepared("queue", "remote-url", scope)).preparation;
      expect(p.state).toBe("ready");
      const alternate = f.path.join(f.dir, "alternate.git");
      yield* f.git(["clone", "--bare", f.path.join(f.dir, "remote.git"), alternate]);
      yield* f.git(["remote", "set-url", "origin", alternate]);
      const after = (yield* releases.revalidate("queue", "remote-url", {
        ...scope,
        expectedEvidenceDigest: p.evidence!.digest,
      })).preparation;
      expect(after.state).toBe("invalidated");
      expect(after.reasons).toContain("source_changed");
    }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.live("an archive format helper in project config cannot execute during preparation", () =>
  Effect.gen(function* () {
    const f = yield* fixture,
      releases = yield* Release.WorkbenchReleaseEvidence,
      catalog = yield* releases.catalog({});
    const helper = f.path.join(f.dir, "archive-helper"),
      marker = f.path.join(f.dir, "ARCHIVE_HELPER_RAN");
    const quote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`;
    yield* f.fs.writeFileString(
      helper,
      `#!/bin/sh\nprintf archive > ${quote(marker)}\nexec /bin/cat\n`,
    );
    yield* f.fs.chmod(helper, 0o700);
    yield* f.git(["config", "tar.tar.command", helper]);
    yield* releases.prepare("queue", {
      ...scope,
      preparationId: "archive-config",
      workItemId: "item",
      expectedWorkItemVersion: 1,
      profileId: "profile",
      profileDigest: catalog.profiles[0]!.digest,
    });
    const p = (yield* releases.awaitPrepared("queue", "archive-config", scope)).preparation;
    expect(p.state).toBe("ready");
    expect(yield* f.fs.exists(marker)).toBe(false);
  }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.live.each(["multiple", "credentials", "external_protocol"] as const)(
  "does not execute a remote observation with %s remote configuration",
  (kind) =>
    Effect.gen(function* () {
      const f = yield* fixture,
        releases = yield* Release.WorkbenchReleaseEvidence,
        catalog = yield* releases.catalog({});
      const marker = f.path.join(f.dir, "FORBIDDEN_REMOTE_EXECUTED"),
        helper = f.path.join(f.dir, "forbidden-remote");
      const quote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`;
      yield* f.fs.writeFileString(helper, `#!/bin/sh\nprintf bad > ${quote(marker)}\n`);
      yield* f.fs.chmod(helper, 0o700);
      if (kind === "multiple")
        yield* f.git(["config", "--add", "remote.origin.url", f.path.join(f.dir, "remote.git")]);
      else
        yield* f.git([
          "remote",
          "set-url",
          "origin",
          kind === "credentials" ? "http://fixture:synthetic@127.0.0.1/repo" : `ext::${helper}`,
        ]);
      const error = yield* releases
        .prepare("queue", {
          ...scope,
          preparationId: "bad-remote",
          workItemId: "item",
          expectedWorkItemVersion: 1,
          profileId: "profile",
          profileDigest: catalog.profiles[0]!.digest,
        })
        .pipe(Effect.flip);
      expect(error.reason).toBe("stale_state");
      expect(yield* f.fs.exists(marker)).toBe(false);
    }).pipe(Effect.scoped, Effect.provide(layer)),
);

it.live(
  "pins the actual HTTP deployment plan in the host profile without enabling legacy profiles",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture,
        releases = yield* Release.WorkbenchReleaseEvidence;
      const legacy = (yield* releases.catalog({ projectId: "project" })).profiles[0]!;
      expect(legacy).not.toHaveProperty("deploymentPlan");
      const deployment = (yield* deploymentFixture(
        f.dir,
        (yield* f.git(["rev-parse", "main"])).stdout.trim(),
      )).policy;
      const cfg = yield* Config.ServerConfig;
      yield* f.fs.writeFileString(
        f.path.join(cfg.stateDir, "workbench-release-profiles.json"),
        JSON.stringify({
          schemaVersion: 1,
          profiles: [{ ...f.profile, version: 2, policy: { ...f.profile.policy, deployment } }],
        }),
      );
      const enabled = (yield* releases.catalog({ projectId: "project" })).profiles[0]!;
      expect(enabled.deploymentPlan).toEqual({
        adapterId: "adapter",
        adapterVersion: 1,
        generationId: "generation",
        serviceKey: "formal-service",
        coordinator: { id: deployment.coordinator.id, origin: deployment.coordinator.origin },
        deploy: deployment.deploy,
        rollback: { ...deployment.rollback, verification: "fixed_target_and_adapter" },
      });
      expect(enabled.digest).not.toBe(legacy.digest);
      yield* f.fs.writeFileString(
        f.path.join(cfg.stateDir, "workbench-release-profiles.json"),
        JSON.stringify({
          schemaVersion: 1,
          profiles: [
            {
              ...f.profile,
              policy: {
                ...f.profile.policy,
                deployment: {
                  ...deployment,
                  adapter: { ...deployment.adapter, origin: "http://unsafe.example.invalid" },
                },
              },
            },
          ],
        }),
      );
      expect((yield* releases.catalog({}).pipe(Effect.flip)).reason).toBe("profile_invalid");
    }).pipe(Effect.provide(layer)),
);

it.live(
  "seals actual adapter rollback readiness and staged byte identities into deployment evidence",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture,
        releases = yield* Release.WorkbenchReleaseEvidence,
        cfg = yield* Config.ServerConfig;
      const baselineSha = (yield* f.git(["rev-parse", "main"])).stdout.trim();
      const adapter = yield* deploymentFixture(f.dir, baselineSha);
      const profile = {
        ...f.profile,
        version: 2,
        policy: {
          ...f.profile.policy,
          target: { ...f.profile.policy.target, origin: adapter.policy.adapter.origin },
          deployment: adapter.policy,
        },
      };
      yield* f.fs.writeFileString(
        f.path.join(cfg.stateDir, "workbench-release-profiles.json"),
        JSON.stringify({ schemaVersion: 1, profiles: [profile] }),
      );
      const catalog = (yield* releases.catalog({ projectId: "project" })).profiles[0]!;
      yield* releases.prepare("queue", {
        ...scope,
        preparationId: "adapter-prepare",
        workItemId: "item",
        expectedWorkItemVersion: 1,
        profileId: "profile",
        profileDigest: catalog.digest,
      });
      const preparation = (yield* releases.awaitPrepared("queue", "adapter-prepare", scope))
        .preparation;
      expect(preparation.state).toBe("ready");
      expect(preparation.evidence?.deployment?.plan).toEqual(catalog.deploymentPlan);
      expect(preparation.evidence?.deployment?.preflight.state).toBe("succeeded");
      expect(preparation.evidence?.deployment?.preflight.result?.exitCode).toBe(0);
      expect(adapter.writes()).toBe(0);
      expect(adapter.bytes()).toBe("prior-artifact");
      const rechecked = yield* releases.revalidate("queue", "adapter-prepare", {
        ...scope,
        expectedEvidenceDigest: preparation.evidence!.digest,
      });
      expect(rechecked.preparation.validation.state).toBe("current");
      adapter.setAdapterAvailable(false);
      expect(
        (yield* releases.revalidate("queue", "adapter-prepare", {
          ...scope,
          expectedEvidenceDigest: preparation.evidence!.digest,
        })).preparation.validation.state,
      ).toBe("unknown");
      adapter.setAdapterAvailable(true);
      expect(
        (yield* releases.revalidate("queue", "adapter-prepare", {
          ...scope,
          expectedEvidenceDigest: preparation.evidence!.digest,
        })).preparation.validation.state,
      ).toBe("current");
    }).pipe(Effect.provide(layer)),
);

const approvedDeploymentFixtureFor = Effect.fn("releaseFixture.approved")(function* (
  observation?: {
    durationSeconds: number;
    intervalSeconds: number;
    consecutiveSuccesses: number;
  },
  unchanged = false,
) {
  const f = yield* fixture,
    releases = yield* Release.WorkbenchReleaseEvidence,
    cfg = yield* Config.ServerConfig;
  if (unchanged)
    yield* f.fs.writeFileString(f.path.join(f.repo, "baseline-private/baseline.bin"), "candidate");
  const a = yield* deploymentFixture(
    f.dir,
    unchanged ? f.head : (yield* f.git(["rev-parse", "main"])).stdout.trim(),
    unchanged ? { bytes: "candidate", version: `sha256-${releaseHash("candidate")}` } : undefined,
  );
  const profile = {
    ...f.profile,
    version: 2,
    policy: {
      ...f.profile.policy,
      target: { ...f.profile.policy.target, origin: a.policy.adapter.origin },
      deployment: a.policy,
      observation: observation ?? f.profile.policy.observation,
    },
  };
  yield* f.fs.writeFileString(
    f.path.join(cfg.stateDir, "workbench-release-profiles.json"),
    JSON.stringify({ schemaVersion: 1, profiles: [profile] }),
  );
  const catalog = (yield* releases.catalog({ projectId: "project" })).profiles[0]!;
  yield* releases.prepare("queue", {
    ...scope,
    preparationId: "approved-prepare",
    workItemId: "item",
    expectedWorkItemVersion: 1,
    profileId: "profile",
    profileDigest: catalog.digest,
  });
  const e = (yield* releases.awaitPrepared("queue", "approved-prepare", scope)).preparation
    .evidence!;
  expect(e?.deployment?.preflight.state).toBe("succeeded");
  const claimToken = "d".repeat(64);
  const payload = {
    schemaVersion: 1 as const,
    deploymentId: "wb-deployment",
    nativeDeploymentId: "native-deployment",
    ownerId: "owner",
    candidateId: "candidate",
    decisionId: "decision",
    decidedAt: 1,
    packetDigest: releaseHash("approved packet"),
    preparationId: e.preparationId,
    evidenceDigest: e.digest,
    profileDigest: e.profile.digest,
    binding: e.binding,
    sourceSha: e.source.commitSha,
    mainSha: e.source.main.commitSha,
    artifact: { sha256: e.artifact.sha256, bytes: e.artifact.bytes },
    rollbackArtifact: { sha256: e.rollback.artifact.sha256, bytes: e.rollback.artifact.bytes },
    plan: e.deployment!.plan,
    desired: e.deployment!.desired,
    baseline: {
      basis: { serviceId: e.target.serviceId, ...e.target.basis },
      revision: e.deployment!.baselineRevision,
    },
    deployOperationId: "deploy-operation",
    rollbackOperationId: "rollback-operation",
    lock: { epoch: 1, nonce: "e".repeat(64) },
    issuer: { id: a.policy.coordinator.id, origin: a.policy.coordinator.origin },
    claimTokenDigest: releaseHash(claimToken),
    createdAt: 2,
  };
  const input = {
    ...scope,
    deploymentId: payload.nativeDeploymentId,
    preparationId: e.preparationId,
    expectedEvidenceDigest: e.digest,
    reservation: a.reserve(payload),
    claimToken,
  };
  return { f, a, e, input, payload };
});
const approvedDeploymentFixture = approvedDeploymentFixtureFor();
it.live("deploys only a committed approval's sealed bytes and preserves exact retry identity", () =>
  Effect.gen(function* () {
    const { a, input } = yield* approvedDeploymentFixture,
      deployments = yield* Deployment.WorkbenchDeployment;
    yield* deployments.admit("queue", input);
    yield* deployments.sync;
    const result = yield* deployments.get("queue", input.deploymentId, scope);
    expect(result.deployment.state).toBe("deployed");
    expect(result.deployment.claimState).toBe("committed");
    expect(result.deployment.target?.basis?.artifactSha256).toBe(releaseHash("candidate"));
    expect(result.deployment.versionObservation?.basis.version).toBe(
      `sha256-${releaseHash("candidate")}`,
    );
    expect(a.bytes()).toBe("candidate");
    expect(a.writes()).toBe(1);
    yield* deployments.admit("queue", input);
    yield* deployments.sync;
    expect(a.writes()).toBe(1);
    const publicJson = JSON.stringify(result);
    expect(publicJson).not.toContain(input.claimToken);
    expect(publicJson).not.toContain("signature");
    expect(publicJson).not.toContain("fixture-adapter-private");
    const changed = yield* deployments
      .admit("queue", {
        ...input,
        reservation: { ...input.reservation, authorizationDigest: "f".repeat(64) },
      })
      .pipe(Effect.flip);
    expect(changed.reason).toBe("conflict");
  }).pipe(Effect.provide(layer)),
);

it.live.each([
  { mode: "before", state: "failed_unchanged", writes: 0 },
  { mode: "partial", state: "rolled_back", writes: 2 },
  { mode: "rollback_failure", state: "attention", writes: 1 },
] as const)(
  "verifies actual $mode effects and reports $state without treating a receipt as recovery",
  ({ mode, state, writes }) =>
    Effect.gen(function* () {
      const { f, a, input } = yield* approvedDeploymentFixture,
        deployments = yield* Deployment.WorkbenchDeployment;
      const main = (yield* f.git(["rev-parse", "main"])).stdout;
      a.setMode(mode);
      yield* deployments.admit("queue", input);
      yield* deployments.sync;
      const result = (yield* deployments.get("queue", input.deploymentId, scope)).deployment;
      expect(result.state).toBe(state);
      expect(a.writes()).toBe(writes);
      expect((yield* f.git(["rev-parse", "main"])).stdout).toBe(main);
      if (state !== "attention") {
        expect(a.bytes()).toBe("prior-artifact");
        expect(result.versionObservation?.basis).toEqual(a.baseline);
      }
      if (state === "rolled_back") {
        expect(result.rollbackOperation?.id).toBe("rollback-operation");
        expect(result.target?.lastOperationId).toBe("rollback-operation");
        expect(result.target?.revision).toBe(2);
      }
      if (state === "attention") expect(result.reasons).toContain("rollback_failed");
    }).pipe(Effect.provide(layer)),
);

it.live.each(["success", "partial"] as const)(
  "recovers %s after lost receipt and Native restart by querying the same operation despite changed source",
  (mode) =>
    Effect.gen(function* () {
      const { f, a, input } = yield* approvedDeploymentFixture;
      a.setMode(mode);
      a.loseNextResponse();
      yield* Effect.scoped(
        Effect.gen(function* () {
          const deployments = yield* Deployment.WorkbenchDeployment;
          yield* deployments.admit("queue", input);
          yield* deployments.sync;
          expect(
            (yield* deployments.get("queue", input.deploymentId, scope)).deployment.state,
          ).toBe("deploy_unknown");
          expect(a.writes()).toBe(1);
        }).pipe(Effect.provide(Deployment.layer)),
      );
      yield* f.fs.writeFileString(f.path.join(f.repo, "input.txt"), "changed after commitment");
      a.setOperationReads(true);
      yield* Effect.scoped(
        Effect.gen(function* () {
          const deployments = yield* Deployment.WorkbenchDeployment;
          yield* deployments.sync;
          const d = (yield* deployments.get("queue", input.deploymentId, scope)).deployment;
          expect(d.state).toBe(mode === "partial" ? "rolled_back" : "deployed");
          expect(d.deployOperation?.externalExecutionId).toBe("external:deploy-operation");
          expect(a.bytes()).toBe(mode === "partial" ? "prior-artifact" : "candidate");
          expect(a.writes()).toBe(mode === "partial" ? 2 : 1);
        }).pipe(Effect.provide(Deployment.layer)),
      );
    }).pipe(Effect.provide(releaseLayer)),
);

it.live(
  "rejects forged authority and stale admission without physical permission or response secrets",
  () =>
    Effect.gen(function* () {
      const { f, a, input } = yield* approvedDeploymentFixture,
        deployments = yield* Deployment.WorkbenchDeployment;
      const forged = { ...input, reservation: { ...input.reservation, signature: "a".repeat(86) } };
      expect((yield* deployments.admit("queue", forged).pipe(Effect.flip)).reason).toBe(
        "permit_invalid",
      );
      expect(
        (yield* deployments
          .admit("queue", { ...input, claimToken: "f".repeat(64) })
          .pipe(Effect.flip)).reason,
      ).toBe("permit_invalid");
      yield* f.fs.writeFileString(f.path.join(f.repo, "input.txt"), "unapproved");
      yield* deployments.admit("queue", input);
      yield* deployments.sync;
      const before = (yield* deployments.get("queue", input.deploymentId, scope)).deployment;
      expect(before.state).toBe("rejected");
      expect(before.claimState).toBe("reserved");
      expect(a.writes()).toBe(0);
      expect(a.bytes()).toBe("prior-artifact");
      yield* deployments.get("queue", input.deploymentId, scope);
      expect(a.writes()).toBe(0);
      expect(
        (yield* deployments
          .get("queue", input.deploymentId, { ...scope, projectId: ProjectId.make("other") })
          .pipe(Effect.flip)).reason,
      ).toBe("not_found");
    }).pipe(Effect.provide(layer)),
);

it.live("retains an uncertain release when the changed target belongs to another operation", () =>
  Effect.gen(function* () {
    const { a, input } = yield* approvedDeploymentFixture,
      deployments = yield* Deployment.WorkbenchDeployment;
    a.setMode("partial");
    a.loseNextResponse();
    yield* deployments.admit("queue", input);
    yield* deployments.sync;
    a.replaceTarget();
    a.setOperationReads(true);
    yield* deployments.sync;
    const d = (yield* deployments.get("queue", input.deploymentId, scope)).deployment;
    expect(d.state).toBe("attention");
    expect(d.reasons).toContain("target_changed_unrelated");
    expect(d.rollbackOperation).toBeNull();
    expect(a.writes()).toBe(1);
    expect(a.bytes()).toBe("third-party");
  }).pipe(Effect.provide(layer)),
);

class DeploymentTestApi extends HttpApi.make("environment").add(
  EnvironmentHttpApi.groups.workbenchDeployment,
) {}
it.live(
  "deployment HTTP requires both scopes and exposes only the original public deployment",
  () =>
    Effect.gen(function* () {
      const { a, input } = yield* approvedDeploymentFixture,
        deployments = yield* Deployment.WorkbenchDeployment;
      const web = HttpRouter.toWebHandler(
        HttpApiBuilder.layer(DeploymentTestApi).pipe(
          Layer.provide(DeploymentHttp.layer),
          Layer.provide(Layer.succeed(Deployment.WorkbenchDeployment, deployments)),
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
      const both = "orchestration:read,orchestration:operate",
        base = "/api/workbench/queues/queue/deployments";
      const call = (route: string, scopes?: string, body?: unknown) =>
        Effect.promise(() =>
          web.handler(
            new Request(`http://fixture.local${route}`, {
              method: body === undefined ? "GET" : "POST",
              headers: {
                ...(scopes ? { "test-scopes": scopes } : {}),
                ...(body === undefined ? {} : { "content-type": "application/json" }),
              },
              ...(body === undefined ? {} : { body: JSON.stringify(body) }),
            }),
          ),
        );
      expect((yield* call(base, undefined, input)).status).toBe(401);
      expect((yield* call(base, "orchestration:read", input)).status).toBe(403);
      expect((yield* call(base, "orchestration:operate", input)).status).toBe(403);
      const forged = yield* call(base, both, {
        ...input,
        reservation: { ...input.reservation, kind: "commit" },
      });
      expect(forged.status).toBe(403);
      expect(yield* Effect.promise(() => forged.json())).toEqual({
        _tag: "WorkbenchDeploymentError",
        reason: "permit_invalid",
      });
      expect(a.writes()).toBe(0);
      expect((yield* call(base, both, input)).status).toBe(202);
      yield* deployments.sync;
      const response = yield* call(
        `${base}/${input.deploymentId}?controllerId=controller&projectId=project&expectedTrackingId=tracking`,
        both,
      );
      expect(response.status).toBe(200);
      const text = yield* Effect.promise(() => response.text());
      expect(JSON.parse(text).deployment.state).toBe("deployed");
      for (const value of [
        input.claimToken,
        "signature",
        "publicKey",
        "credentialRef",
        "fixture-adapter-private",
        "cause",
        "input_json",
        "profile_json",
      ])
        expect(text).not.toContain(value);
      expect(
        (yield* call(
          `${base}/${input.deploymentId}?controllerId=wrong&projectId=project&expectedTrackingId=tracking`,
          both,
        )).status,
      ).toBe(404);
      expect(a.writes()).toBe(1);
    }).pipe(Effect.provide(layer)),
);

it.live(
  "queries a running durable operation across interruption before treating physical apply as deployed",
  () =>
    Effect.gen(function* () {
      const { a, input } = yield* approvedDeploymentFixture;
      const gate = a.gateNext("after_apply");
      yield* Effect.scoped(
        Effect.gen(function* () {
          const deployments = yield* Deployment.WorkbenchDeployment;
          yield* deployments.admit("queue", input);
          yield* gate.reached;
          expect(a.bytes()).toBe("candidate");
          expect(a.writes()).toBe(1);
          expect(
            (yield* deployments.get("queue", input.deploymentId, scope)).deployment.state,
          ).not.toBe("deployed");
        }).pipe(Effect.provide(Deployment.layer)),
      );
      yield* Effect.scoped(
        Effect.gen(function* () {
          const deployments = yield* Deployment.WorkbenchDeployment;
          yield* deployments.sync;
          const running = (yield* deployments.get("queue", input.deploymentId, scope)).deployment;
          expect(running.state).toBe("deploying");
          expect(running.deployOperation?.state).toBe("running");
          gate.release();
          yield* gate.completed;
          yield* deployments.sync;
          expect(
            (yield* deployments.get("queue", input.deploymentId, scope)).deployment.state,
          ).toBe("deployed");
          expect(a.writes()).toBe(1);
        }).pipe(Effect.provide(Deployment.layer)),
      );
    }).pipe(Effect.provide(releaseLayer)),
);

it.live(
  "keeps a verified deployed attempt read-only after an outage instead of newly executing recovery",
  () =>
    Effect.gen(function* () {
      const { a, input } = yield* approvedDeploymentFixture,
        deployments = yield* Deployment.WorkbenchDeployment;
      yield* deployments.admit("queue", input);
      yield* deployments.sync;
      expect((yield* deployments.get("queue", input.deploymentId, scope)).deployment.state).toBe(
        "deployed",
      );
      a.setAdapterAvailable(false);
      yield* deployments.sync;
      a.setAdapterAvailable(true);
      a.corruptTargetBytes();
      yield* deployments.sync;
      const d = (yield* deployments.get("queue", input.deploymentId, scope)).deployment;
      expect(d.state).toBe("attention");
      expect(d.rollbackOperation).toBeNull();
      expect(a.writes()).toBe(1);
    }).pipe(Effect.provide(layer)),
);

it.live(
  "verifies a lost rollback receipt by querying the original restore instead of writing it again",
  () =>
    Effect.gen(function* () {
      const { a, input } = yield* approvedDeploymentFixture,
        deployments = yield* Deployment.WorkbenchDeployment;
      a.setMode("partial");
      a.loseNextRollbackResponse();
      yield* deployments.admit("queue", input);
      yield* deployments.sync;
      expect((yield* deployments.get("queue", input.deploymentId, scope)).deployment.state).toBe(
        "rollback_unknown",
      );
      expect(a.bytes()).toBe("prior-artifact");
      expect(a.writes()).toBe(2);
      a.reopenLedger();
      a.setOperationReads(true);
      yield* deployments.sync;
      expect((yield* deployments.get("queue", input.deploymentId, scope)).deployment.state).toBe(
        "rolled_back",
      );
      expect(a.writes()).toBe(2);
    }).pipe(Effect.provide(layer)),
);
it.live(
  "recovers committed online authority after interruption before Native received the proof",
  () =>
    Effect.gen(function* () {
      const { f, a, input } = yield* approvedDeploymentFixture,
        gate = a.gateNext("after_claim");
      yield* Effect.scoped(
        Effect.gen(function* () {
          const deployments = yield* Deployment.WorkbenchDeployment;
          yield* deployments.admit("queue", input);
          yield* gate.reached;
          expect(a.writes()).toBe(0);
        }).pipe(Effect.provide(Deployment.layer)),
      );
      yield* f.fs.writeFileString(
        f.path.join(f.repo, "input.txt"),
        "changed after coordinator commit",
      );
      a.reopenLedger();
      yield* Effect.scoped(
        Effect.gen(function* () {
          const deployments = yield* Deployment.WorkbenchDeployment;
          yield* deployments.sync;
          const d = (yield* deployments.get("queue", input.deploymentId, scope)).deployment;
          expect(d.claimState).toBe("committed");
          expect(d.state).toBe("deployed");
          expect(a.bytes()).toBe("candidate");
          expect(a.writes()).toBe(1);
          gate.release();
          yield* gate.completed;
        }).pipe(Effect.provide(Deployment.layer)),
      );
    }).pipe(Effect.provide(releaseLayer)),
);

it.live(
  "rechecks the adapter journal generation after the online claim before sending physical authority",
  () =>
    Effect.gen(function* () {
      const { a, input } = yield* approvedDeploymentFixture,
        deployments = yield* Deployment.WorkbenchDeployment,
        gate = a.gateNext("after_claim");
      yield* deployments.admit("queue", input);
      yield* gate.reached;
      a.rotateGeneration();
      gate.release();
      yield* gate.completed;
      yield* deployments.sync;
      const d = (yield* deployments.get("queue", input.deploymentId, scope)).deployment;
      expect(d.claimState).toBe("committed");
      expect(d.state).toBe("deploy_unknown");
      expect(d.reasons).toContain("adapter_identity_mismatch");
      expect(a.operationPutRequests()).toBe(0);
      expect(a.writes()).toBe(0);
    }).pipe(Effect.provide(layer)),
);

it.live.each(["http500", "disconnect", "invalid_json"] as const)(
  "restores an attributable terminal partial deployment when its version endpoint fails with %s",
  (fault) =>
    Effect.gen(function* () {
      const { a, input } = yield* approvedDeploymentFixture,
        deployments = yield* Deployment.WorkbenchDeployment;
      a.setMode("partial");
      a.setVersionFault(fault);
      yield* deployments.admit("queue", input);
      yield* deployments.sync;
      const d = (yield* deployments.get("queue", input.deploymentId, scope)).deployment;
      expect(d.state).toBe("rolled_back");
      expect(d.rollbackOperation?.id).toBe("rollback-operation");
      expect(d.versionObservation?.basis).toEqual(a.baseline);
      expect(d.target?.basis).toEqual(a.baseline);
      expect(a.bytes()).toBe("prior-artifact");
      expect(a.writes()).toBe(2);
    }).pipe(Effect.provide(layer)),
);
it.live("keeps restoration unverified until the independent version endpoint recovers", () =>
  Effect.gen(function* () {
    const { a, input } = yield* approvedDeploymentFixture,
      deployments = yield* Deployment.WorkbenchDeployment,
      gate = a.gateNext("after_apply");
    a.setMode("partial");
    yield* deployments.admit("queue", input);
    yield* gate.reached;
    a.setVersionFault("http500", false);
    gate.release();
    yield* gate.completed;
    yield* deployments.sync;
    const unknown = (yield* deployments.get("queue", input.deploymentId, scope)).deployment;
    expect(unknown.state).toBe("rollback_unknown");
    expect(unknown.rollbackOperation?.state).toBe("succeeded");
    expect(a.bytes()).toBe("prior-artifact");
    expect(a.writes()).toBe(2);
    a.setVersionFault(null);
    yield* deployments.sync;
    expect((yield* deployments.get("queue", input.deploymentId, scope)).deployment.state).toBe(
      "rolled_back",
    );
    expect(a.writes()).toBe(2);
  }).pipe(Effect.provide(layer)),
);
it.live.each(["before", "partial"] as const)(
  "stops background material and adapter I/O after verified %s recovery",
  (mode) =>
    Effect.gen(function* () {
      const fs = yield* FileSystem.FileSystem;
      let byteReads = 0;
      const monitoredFs = {
        ...fs,
        readFile: Effect.fn("releaseFixture.materialRead")(function* (path: string) {
          byteReads++;
          return yield* fs.readFile(path);
        }),
      };
      yield* Effect.gen(function* () {
        const { a, input } = yield* approvedDeploymentFixture,
          deployments = yield* Deployment.WorkbenchDeployment;
        a.setMode(mode);
        yield* deployments.admit("queue", input);
        yield* deployments.sync;
        const completed = (yield* deployments.get("queue", input.deploymentId, scope)).deployment;
        expect(completed.state).toBe(mode === "before" ? "failed_unchanged" : "rolled_back");
        const reads = byteReads,
          requests = a.requests();
        yield* deployments.sync;
        const later = (yield* deployments.get("queue", input.deploymentId, scope)).deployment;
        expect(later).toEqual(completed);
        expect(byteReads).toBe(reads);
        expect(a.requests()).toBe(requests);
      }).pipe(
        Effect.provide(Deployment.layer.pipe(Layer.provideMerge(Release.layer))),
        Effect.provideService(FileSystem.FileSystem, monitoredFs),
      );
    }).pipe(Effect.provide(dependencies)),
);

it.live("seals verification source and exact definitions in new approved evidence", () =>
  Effect.gen(function* () {
    const { e } = yield* approvedDeploymentFixture;
    expect(e.verification).toMatchObject({
      schemaVersion: 1,
      semanticsVersion: 1,
      sourceTreeSha: e.source.treeSha,
    });
    expect(e.verification?.archive.bytes).toBeGreaterThan(0);
    expect(e.verification?.archive.sha256).toMatch(/^[a-f0-9]{64}$/);
    expect(e.verification?.manifestDigest).toMatch(/^[a-f0-9]{64}$/);
  }).pipe(Effect.provide(layer)),
);

it.live(
  "executes frozen health and core against the actual online target after the source project drifts",
  () =>
    Effect.gen(function* () {
      const { f, a, e, input } = yield* approvedDeploymentFixture,
        deployments = yield* Deployment.WorkbenchDeployment,
        releases = yield* Release.WorkbenchReleaseEvidence;
      yield* deployments.admit("queue", input);
      yield* deployments.sync;
      yield* deployments.awaitAcceptanceSample("queue", input.deploymentId, scope, 1);
      yield* f.fs.writeFileString(f.path.join(f.repo, "artifact.cjs"), "process.exit(99)");
      yield* f.sql`UPDATE projection_projects SET scripts_json='[]' WHERE project_id='project'`;
      const base = {
        ...scope,
        expectedEvidenceDigest: e.digest,
        sampleId: "sample",
        authorizationDigest: input.reservation.authorizationDigest,
        runnerEpoch: "epoch",
        candidate: e.deployment!.desired,
        activationRevision: 1,
      };
      const health = yield* releases.runOnlineCheck("queue", e.preparationId, {
        ...base,
        role: "health",
        executionId: "health-execution",
      });
      expect(health.state).toBe("succeeded");
      expect(health.exitCode).toBe(0);
      expect(health.materialDigest).toBe(e.verification!.manifestDigest);
      a.setCheckFault("core", true);
      const core = yield* releases.runOnlineCheck("queue", e.preparationId, {
        ...base,
        role: "core",
        executionId: "core-execution",
      });
      expect(core.state).toBe("failed");
      expect(core.exitCode).toBe(31);
      expect(core.outputDigest).toMatch(/^[a-f0-9]{64}$/);
      expect(a.bytes()).toBe("candidate");
      expect(a.writes()).toBe(1);
    }).pipe(Effect.provide(layer)),
);

it.live(
  "persists a real health failure and recovers the same approved baseline through the original rollback identity",
  () =>
    Effect.gen(function* () {
      const { a, input } = yield* approvedDeploymentFixture,
        deployments = yield* Deployment.WorkbenchDeployment;
      a.setCheckFault("health", true);
      yield* deployments.admit("queue", input);
      yield* deployments.sync;
      const failed = (yield* deployments.awaitAcceptanceSample(
        "queue",
        input.deploymentId,
        scope,
        1,
      )).deployment;
      expect(failed.acceptance?.state).toBe("failed");
      expect(failed.acceptance?.latestSample?.health?.exitCode).toBe(31);
      expect(failed.acceptance?.latestSample?.core).toBeNull();
      yield* deployments.sync;
      const restored = (yield* deployments.get("queue", input.deploymentId, scope)).deployment;
      expect(restored.state).toBe("rolled_back");
      expect(restored.acceptance?.failure).toEqual(failed.acceptance?.failure);
      expect(restored.rollbackOperation?.id).toBe("rollback-operation");
      expect(a.bytes()).toBe("prior-artifact");
      expect(a.writes()).toBe(2);
    }).pipe(Effect.provide(layer)),
);

it.live(
  "earns the full monotonic window from actual sequential health-core samples and seals passed proof",
  () =>
    Effect.gen(function* () {
      const { a, input } = yield* approvedDeploymentFixtureFor({
          durationSeconds: 2,
          intervalSeconds: 1,
          consecutiveSuccesses: 2,
        }),
        deployments = yield* Deployment.WorkbenchDeployment;
      yield* deployments.admit("queue", input);
      yield* deployments.sync;
      const first = (yield* deployments.awaitAcceptanceSample(
        "queue",
        input.deploymentId,
        scope,
        1,
      )).deployment.acceptance!;
      expect(first.state).toBe("observing");
      expect(first.window.elapsedMilliseconds).toBe(0);
      expect(first.window.successfulSamples).toBe(1);
      yield* TestClock.adjust("1 second");
      const second = (yield* deployments.awaitAcceptanceSample(
        "queue",
        input.deploymentId,
        scope,
        2,
      )).deployment.acceptance!;
      expect(second.state).toBe("observing");
      expect(second.window.elapsedMilliseconds).toBe(1000);
      yield* TestClock.adjust("1 second");
      const passed = (yield* deployments.awaitAcceptanceSample(
        "queue",
        input.deploymentId,
        scope,
        3,
      )).deployment.acceptance!;
      expect(passed.state).toBe("passed");
      expect(passed.window.elapsedMilliseconds).toBe(2000);
      expect(passed.window.successfulSamples).toBe(3);
      expect(passed.latestSample?.health?.state).toBe("succeeded");
      expect(passed.latestSample?.core?.state).toBe("succeeded");
      const calls = a.checkCalls();
      yield* TestClock.adjust("3 seconds");
      yield* deployments.sync;
      expect(
        (yield* deployments.get("queue", input.deploymentId, scope)).deployment.acceptance,
      ).toEqual(passed);
      expect(a.checkCalls()).toBe(calls);
      expect(a.writes()).toBe(1);
    }).pipe(Effect.provide(layer.pipe(Layer.provideMerge(TestClock.layer())))),
);
it.live(
  "reopens the actual Native SQLite database and restarts only the unfinished observation window",
  () =>
    Effect.gen(function* () {
      const cfg = yield* Config.ServerConfig,
        path = yield* Path.Path;
      const dbPath = path.join(cfg.stateDir, "issue17-reopen.sqlite");
      const nativeLayer = Deployment.layer.pipe(
        Layer.provideMerge(Release.layer),
        Layer.provideMerge(ReleaseAdapter.layer),
        Layer.provideMerge(Sqlite.layerFromPath(dbPath)),
      );
      const firstScope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void));
      const firstContext = yield* Layer.buildWithScope(nativeLayer, firstScope);
      const prepared = yield* approvedDeploymentFixtureFor({
        durationSeconds: 2,
        intervalSeconds: 1,
        consecutiveSuccesses: 2,
      }).pipe(Effect.provide(firstContext));
      const original = yield* Effect.gen(function* () {
        const deployments = yield* Deployment.WorkbenchDeployment;
        yield* deployments.admit("queue", prepared.input);
        yield* deployments.sync;
        return (yield* deployments.awaitAcceptanceSample(
          "queue",
          prepared.input.deploymentId,
          scope,
          1,
        )).deployment.acceptance!;
      }).pipe(Effect.provide(firstContext));
      yield* Scope.close(firstScope, Exit.void);
      const secondScope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(secondScope, Exit.void));
      const reopened = yield* Layer.buildWithScope(nativeLayer, secondScope);
      const passedProof = yield* Effect.gen(function* () {
        const deployments = yield* Deployment.WorkbenchDeployment;
        yield* deployments.sync;
        const reset = (yield* deployments.awaitAcceptanceSample(
          "queue",
          prepared.input.deploymentId,
          scope,
          2,
        )).deployment.acceptance!;
        expect(reset.runnerEpoch).not.toBe(original.runnerEpoch);
        expect(reset.window.resets).toBeGreaterThan(0);
        expect(reset.window.successfulSamples).toBe(1);
        expect(reset.window.elapsedMilliseconds).toBe(0);
        expect(reset.state).toBe("observing");
        yield* TestClock.adjust("1 second");
        yield* deployments.awaitAcceptanceSample("queue", prepared.input.deploymentId, scope, 3);
        yield* TestClock.adjust("1 second");
        const passed = (yield* deployments.awaitAcceptanceSample(
          "queue",
          prepared.input.deploymentId,
          scope,
          4,
        )).deployment.acceptance!;
        expect(passed.state).toBe("passed");
        expect(passed.ledger.count).toBe(4);
        expect(prepared.a.writes()).toBe(1);
        return passed;
      }).pipe(Effect.provide(reopened));
      yield* Scope.close(secondScope, Exit.void);
      prepared.a.setAdapterAvailable(false);
      const finalScope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(finalScope, Exit.void));
      const offline = yield* Layer.buildWithScope(nativeLayer, finalScope);
      yield* Effect.gen(function* () {
        const deployments = yield* Deployment.WorkbenchDeployment;
        const before = (yield* deployments.get("queue", prepared.input.deploymentId, scope))
          .deployment;
        expect(before.state).toBe("deploy_unknown");
        expect(before.acceptance).toEqual(passedProof);
        const calls = prepared.a.checkCalls();
        prepared.a.setAdapterAvailable(true);
        yield* deployments.sync;
        const fresh = (yield* deployments.get("queue", prepared.input.deploymentId, scope))
          .deployment;
        expect(fresh.state).toBe("deployed");
        expect(fresh.acceptance).toEqual(passedProof);
        expect(prepared.a.checkCalls()).toBe(calls);
        expect(prepared.a.writes()).toBe(1);
      }).pipe(Effect.provide(offline));
    }).pipe(Effect.provide(systemDependencies.pipe(Layer.provideMerge(TestClock.layer())))),
);

it.live(
  "keeps a legacy admitted packet missing verification material unknown without inventing checks",
  () =>
    Effect.gen(function* () {
      const { f, a, e, input, payload } = yield* approvedDeploymentFixture,
        deployments = yield* Deployment.WorkbenchDeployment;
      const { verification: _verification, digest: _digest, ...unsigned } = e,
        legacy = { ...unsigned, digest: releaseDigest(unsigned) };
      const [row] = yield* f.sql<{
        preparation_json: string;
      }>`SELECT preparation_json FROM workbench_release_preparations WHERE id=${e.preparationId}`;
      const prep = JSON.parse(row!.preparation_json);
      prep.evidence = legacy;
      yield* f.sql`UPDATE workbench_release_preparations SET preparation_json=${JSON.stringify(prep)} WHERE id=${e.preparationId}`;
      const grant = {
        ...payload,
        deploymentId: "legacy-wb",
        nativeDeploymentId: "legacy-native",
        evidenceDigest: legacy.digest,
      };
      const legacyInput = {
        ...input,
        deploymentId: grant.nativeDeploymentId,
        expectedEvidenceDigest: legacy.digest,
        reservation: a.reserve(grant),
      };
      yield* deployments.admit("queue", legacyInput);
      yield* deployments.sync;
      const d = (yield* deployments.awaitAcceptanceSample(
        "queue",
        legacyInput.deploymentId,
        scope,
        1,
      )).deployment;
      expect(d.state).toBe("deployed");
      expect(d.acceptance?.state).toBe("unknown");
      expect(d.acceptance?.reasons).toEqual(["material_missing"]);
      expect(d.acceptance?.ledger.count).toBe(0);
      expect(a.checkCalls()).toBe(0);
      expect(a.writes()).toBe(1);
    }).pipe(Effect.provide(layer)),
);
it.live(
  "damaged sealed verification source after commitment stays unknown and cannot authorize rollback",
  () =>
    Effect.gen(function* () {
      const { f, a, e, input } = yield* approvedDeploymentFixture,
        deployments = yield* Deployment.WorkbenchDeployment,
        cfg = yield* Config.ServerConfig,
        gate = a.gateNext("after_apply");
      yield* deployments.admit("queue", input);
      yield* gate.reached;
      const archive = f.path.join(
        cfg.stateDir,
        "workbench-release-evidence",
        releaseHash(e.preparationId),
        "verification.tar",
      );
      yield* f.fs.chmod(archive, 0o600);
      yield* f.fs.writeFileString(archive, "corrupt");
      gate.release();
      yield* gate.completed;
      yield* deployments.sync;
      const d = (yield* deployments.awaitAcceptanceSample("queue", input.deploymentId, scope, 1))
        .deployment;
      expect(d.acceptance?.state).toBe("unknown");
      expect(d.acceptance?.reasons).toEqual(["material_invalid"]);
      expect(d.acceptance?.failure).toBeNull();
      yield* deployments.sync;
      expect(a.checkCalls()).toBe(0);
      expect(a.writes()).toBe(1);
      expect(a.bytes()).toBe("candidate");
    }).pipe(Effect.provide(layer)),
);

it.live.each(["succeeded", "failed"] as const)(
  "keeps true %s receipts across a wall jump without inventing healthy coverage",
  (receiptState) =>
    Effect.gen(function* () {
      const clock = yield* Clock.Clock;
      let offset = 0;
      const shifted: Clock.Clock = {
        ...clock,
        currentTimeMillisUnsafe: () => clock.currentTimeMillisUnsafe() + offset,
        currentTimeMillis: Effect.sync(() => clock.currentTimeMillisUnsafe() + offset),
        currentTimeNanosUnsafe: () => clock.currentTimeNanosUnsafe() + BigInt(offset) * 1000000n,
        currentTimeNanos: Effect.sync(
          () => clock.currentTimeNanosUnsafe() + BigInt(offset) * 1000000n,
        ),
      };
      yield* Effect.gen(function* () {
        const { a, input } = yield* approvedDeploymentFixtureFor({
            durationSeconds: 2,
            intervalSeconds: 1,
            consecutiveSuccesses: 2,
          }),
          deployments = yield* Deployment.WorkbenchDeployment,
          gate = a.gateNext("core_check");
        a.setCheckFault("core", receiptState === "failed");
        yield* deployments.admit("queue", input);
        yield* gate.reached;
        offset = receiptState === "failed" ? -5000 : 5000;
        gate.release();
        yield* gate.completed;
        const result = (yield* deployments.awaitAcceptanceSample(
          "queue",
          input.deploymentId,
          scope,
          1,
        )).deployment.acceptance!;
        expect(result.latestSample?.core?.state).toBe(receiptState);
        if (receiptState === "succeeded") {
          expect(result.state).toBe("unknown");
          expect(result.reasons).toEqual(["clock_changed"]);
          expect(result.window.elapsedMilliseconds).toBe(0);
          expect(result.window.successfulSamples).toBe(0);
          expect(result.failure).toBeNull();
          expect(a.writes()).toBe(1);
        } else {
          expect(result.state).toBe("failed");
          expect(result.failure?.reason).toBe("core_failed");
          yield* deployments.sync;
          expect(
            (yield* deployments.get("queue", input.deploymentId, scope)).deployment.state,
          ).toBe("rolled_back");
          expect(a.writes()).toBe(2);
        }
      }).pipe(Effect.provide(layer), Effect.provideService(Clock.Clock, shifted));
    }).pipe(Effect.provide(TestClock.layer())),
);
it.live("a later genuine observation failure keeps its failed evidence after actual recovery", () =>
  Effect.gen(function* () {
    const { a, input } = yield* approvedDeploymentFixtureFor({
        durationSeconds: 3,
        intervalSeconds: 1,
        consecutiveSuccesses: 2,
      }),
      deployments = yield* Deployment.WorkbenchDeployment;
    yield* deployments.admit("queue", input);
    yield* deployments.sync;
    yield* deployments.awaitAcceptanceSample("queue", input.deploymentId, scope, 1);
    a.setCheckFault("health", true);
    yield* TestClock.adjust("1 second");
    const failed = (yield* deployments.awaitAcceptanceSample("queue", input.deploymentId, scope, 2))
      .deployment.acceptance!;
    expect(failed.state).toBe("failed");
    expect(failed.failure?.reason).toBe("observation_failed");
    expect(failed.latestSample?.core).toBeNull();
    yield* deployments.sync;
    const result = (yield* deployments.get("queue", input.deploymentId, scope)).deployment;
    expect(result.state).toBe("rolled_back");
    expect(result.acceptance).toEqual(failed);
    expect(a.writes()).toBe(2);
  }).pipe(Effect.provide(layer.pipe(Layer.provideMerge(TestClock.layer())))),
);
it.live("a wall rollback after both real check receipts cannot finalize healthy coverage", () =>
  Effect.gen(function* () {
    const clock = yield* Clock.Clock;
    let offset = 0;
    const shifted: Clock.Clock = {
      ...clock,
      currentTimeMillisUnsafe: () => clock.currentTimeMillisUnsafe() + offset,
      currentTimeMillis: Effect.sync(() => clock.currentTimeMillisUnsafe() + offset),
    };
    yield* Effect.gen(function* () {
      const { a, input } = yield* approvedDeploymentFixtureFor({
          durationSeconds: 2,
          intervalSeconds: 1,
          consecutiveSuccesses: 2,
        }),
        deployments = yield* Deployment.WorkbenchDeployment,
        core = a.gateNext("core_check");
      yield* deployments.admit("queue", input);
      yield* core.reached;
      const version = a.gateNext("version_read");
      core.release();
      yield* core.completed;
      yield* version.reached;
      offset = -5000;
      version.release();
      yield* version.completed;
      const result = (yield* deployments.awaitAcceptanceSample(
        "queue",
        input.deploymentId,
        scope,
        1,
      )).deployment.acceptance!;
      expect(result.latestSample?.health?.state).toBe("succeeded");
      expect(result.latestSample?.core?.state).toBe("succeeded");
      expect(result.state).toBe("unknown");
      expect(result.reasons).toEqual(["clock_changed"]);
      expect(result.window.successfulSamples).toBe(0);
      expect(result.failure).toBeNull();
      expect(a.writes()).toBe(1);
    }).pipe(Effect.provide(layer), Effect.provideService(Clock.Clock, shifted));
  }).pipe(Effect.provide(TestClock.layer())),
);

it.live(
  "a failed online check after a verified no-op deploy reports unchanged baseline without blind restoration",
  () =>
    Effect.gen(function* () {
      const { a, input } = yield* approvedDeploymentFixtureFor(undefined, true),
        deployments = yield* Deployment.WorkbenchDeployment;
      a.setCheckFault("health", true);
      yield* deployments.admit("queue", input);
      yield* deployments.sync;
      const failed = (yield* deployments.awaitAcceptanceSample(
        "queue",
        input.deploymentId,
        scope,
        1,
      )).deployment.acceptance!;
      expect(failed.state).toBe("failed");
      yield* deployments.sync;
      const d = (yield* deployments.get("queue", input.deploymentId, scope)).deployment;
      expect(d.state).toBe("failed_unchanged");
      expect(d.deployOperation?.result?.effect).toBe("unchanged");
      expect(d.rollbackOperation).toBeNull();
      expect(d.acceptance?.failure).toEqual(failed.failure);
      expect(d.versionObservation?.basis).toEqual(a.baseline);
      expect(a.writes()).toBe(0);
    }).pipe(Effect.provide(layer)),
);

it.live(
  "finalizes an interrupted core invocation unknown once across actual SQLite reopen without replaying execution IDs",
  () =>
    Effect.gen(function* () {
      const cfg = yield* Config.ServerConfig,
        path = yield* Path.Path,
        dbPath = path.join(cfg.stateDir, "issue17-orphan.sqlite");
      const nativeLayer = Deployment.layer.pipe(
        Layer.provideMerge(Release.layer),
        Layer.provideMerge(ReleaseAdapter.layer),
        Layer.provideMerge(Sqlite.layerFromPath(dbPath)),
      );
      const firstScope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void));
      const firstContext = yield* Layer.buildWithScope(nativeLayer, firstScope);
      const prepared = yield* approvedDeploymentFixtureFor({
        durationSeconds: 2,
        intervalSeconds: 1,
        consecutiveSuccesses: 2,
      }).pipe(Effect.provide(firstContext));
      const gate = prepared.a.gateNext("core_check");
      const active = yield* Effect.gen(function* () {
        const deployments = yield* Deployment.WorkbenchDeployment;
        yield* deployments.admit("queue", prepared.input);
        yield* gate.reached;
        return (yield* deployments.get("queue", prepared.input.deploymentId, scope)).deployment
          .acceptance!.activeSample!;
      }).pipe(Effect.provide(firstContext));
      expect(active.role).toBe("core");
      yield* Scope.close(firstScope, Exit.void);
      const reopenedScope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(reopenedScope, Exit.void));
      const reopened = yield* Layer.buildWithScope(nativeLayer, reopenedScope);
      yield* Effect.gen(function* () {
        const deployments = yield* Deployment.WorkbenchDeployment;
        yield* deployments.sync;
        const next = (yield* deployments.awaitAcceptanceSample(
          "queue",
          prepared.input.deploymentId,
          scope,
          2,
        )).deployment.acceptance!;
        expect(next.ledger.count).toBe(2);
        expect(next.firstSample?.ordinal).toBe(2);
        expect(next.window.successfulSamples).toBe(1);
        expect(next.window.elapsedMilliseconds).toBe(0);
        expect(next.runnerEpoch).not.toBe(active.runnerEpoch);
        expect(next.firstSample?.id).not.toBe(active.id);
        expect(next.state).toBe("observing");
        const ids = prepared.a.checkExecutions();
        expect(ids).toHaveLength(4);
        expect(new Set(ids).size).toBe(4);
        expect(prepared.a.writes()).toBe(1);
      }).pipe(Effect.provide(reopened));
      gate.release();
      yield* gate.completed;
    }).pipe(Effect.provide(systemDependencies.pipe(Layer.provideMerge(TestClock.layer())))),
);
it.live(
  "restores a durably recorded terminal failure after actual database reopen before sample finalization",
  () =>
    Effect.gen(function* () {
      const cfg = yield* Config.ServerConfig,
        path = yield* Path.Path,
        dbPath = path.join(cfg.stateDir, "issue17-terminal-intent.sqlite");
      const nativeLayer = Deployment.layer.pipe(
        Layer.provideMerge(Release.layer),
        Layer.provideMerge(ReleaseAdapter.layer),
        Layer.provideMerge(Sqlite.layerFromPath(dbPath)),
      );
      const firstScope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(firstScope, Exit.void));
      const first = yield* Layer.buildWithScope(nativeLayer, firstScope);
      const prepared = yield* approvedDeploymentFixture.pipe(Effect.provide(first));
      const core = prepared.a.gateNext("core_check");
      prepared.a.setCheckFault("core", true);
      yield* Effect.gen(function* () {
        const deployments = yield* Deployment.WorkbenchDeployment;
        yield* deployments.admit("queue", prepared.input);
        yield* core.reached;
      }).pipe(Effect.provide(first));
      const target = prepared.a.gateNext("target_read");
      core.release();
      yield* core.completed;
      yield* target.reached;
      const interrupted = yield* Effect.gen(function* () {
        const deployments = yield* Deployment.WorkbenchDeployment;
        return (yield* deployments.get("queue", prepared.input.deploymentId, scope)).deployment
          .acceptance!;
      }).pipe(Effect.provide(first));
      expect(interrupted.activeSample?.role).toBe("core");
      expect(interrupted.ledger.count).toBe(0);
      yield* Scope.close(firstScope, Exit.void);
      target.release();
      yield* target.completed;
      const reopenedScope = yield* Scope.make();
      yield* Effect.addFinalizer(() => Scope.close(reopenedScope, Exit.void));
      const reopened = yield* Layer.buildWithScope(nativeLayer, reopenedScope);
      yield* Effect.gen(function* () {
        const deployments = yield* Deployment.WorkbenchDeployment;
        yield* deployments.sync;
        const finalized = (yield* deployments.awaitAcceptanceSample(
          "queue",
          prepared.input.deploymentId,
          scope,
          1,
        )).deployment.acceptance!;
        expect(finalized.state).toBe("failed");
        expect(finalized.runnerEpoch).toBe(interrupted.runnerEpoch);
        expect(finalized.failure?.role).toBe("core");
        expect(finalized.latestSample?.id).toBe(interrupted.activeSample?.id);
        expect(finalized.latestSample?.core?.exitCode).toBe(31);
        expect(finalized.activeSample).toBeNull();
        expect(finalized.ledger.count).toBe(1);
        yield* deployments.sync;
        const result = (yield* deployments.get("queue", prepared.input.deploymentId, scope))
          .deployment;
        expect(result.state).toBe("rolled_back");
        expect(result.acceptance).toEqual(finalized);
        expect(result.rollbackOperation?.id).toBe("rollback-operation");
        expect(prepared.a.checkCalls()).toBe(2);
        expect(prepared.a.writes()).toBe(2);
        expect(prepared.a.bytes()).toBe("prior-artifact");
      }).pipe(Effect.provide(reopened));
    }).pipe(Effect.provide(systemDependencies.pipe(Layer.provideMerge(TestClock.layer())))),
);
it.live.each(["candidate", "always"] as const)(
  "a real core failure starts fixed recovery despite a %s-unavailable independent version endpoint",
  (fault) =>
    Effect.gen(function* () {
      const { a, input } = yield* approvedDeploymentFixture,
        deployments = yield* Deployment.WorkbenchDeployment,
        gate = a.gateNext("core_check");
      a.setCheckFault("core", true);
      yield* deployments.admit("queue", input);
      yield* gate.reached;
      a.setVersionFault("http500", fault === "candidate");
      gate.release();
      yield* gate.completed;
      const failed = (yield* deployments.awaitAcceptanceSample(
        "queue",
        input.deploymentId,
        scope,
        1,
      )).deployment.acceptance!;
      expect(failed.state).toBe("failed");
      expect(failed.failure?.role).toBe("core");
      yield* deployments.sync;
      const recovered = (yield* deployments.get("queue", input.deploymentId, scope)).deployment;
      expect(recovered.state).toBe(fault === "candidate" ? "rolled_back" : "rollback_unknown");
      expect(recovered.acceptance).toEqual(failed);
      expect(a.bytes()).toBe("prior-artifact");
      expect(a.writes()).toBe(2);
      a.setVersionFault(null);
      yield* deployments.sync;
      expect((yield* deployments.get("queue", input.deploymentId, scope)).deployment.state).toBe(
        "rolled_back",
      );
      expect(a.writes()).toBe(2);
    }).pipe(Effect.provide(layer)),
);
it.live(
  "failed actual acceptance rollback retains failure and explicit attention without claiming restoration",
  () =>
    Effect.gen(function* () {
      const { a, input } = yield* approvedDeploymentFixture,
        deployments = yield* Deployment.WorkbenchDeployment,
        gate = a.gateNext("core_check");
      a.setCheckFault("core", true);
      yield* deployments.admit("queue", input);
      yield* gate.reached;
      a.setMode("rollback_failure");
      gate.release();
      yield* gate.completed;
      const failed = (yield* deployments.awaitAcceptanceSample(
        "queue",
        input.deploymentId,
        scope,
        1,
      )).deployment.acceptance!;
      yield* deployments.sync;
      const d = (yield* deployments.get("queue", input.deploymentId, scope)).deployment;
      expect(d.state).toBe("attention");
      expect(d.reasons).toEqual(["rollback_failed"]);
      expect(d.acceptance).toEqual(failed);
      expect(a.writes()).toBe(1);
      expect(a.bytes()).toBe("candidate");
    }).pipe(Effect.provide(layer)),
);

it.live("a long real check gap starts a fresh window without crediting the missing period", () =>
  Effect.gen(function* () {
    const { a, input } = yield* approvedDeploymentFixtureFor({
        durationSeconds: 2,
        intervalSeconds: 1,
        consecutiveSuccesses: 2,
      }),
      deployments = yield* Deployment.WorkbenchDeployment;
    yield* deployments.admit("queue", input);
    yield* deployments.sync;
    yield* deployments.awaitAcceptanceSample("queue", input.deploymentId, scope, 1);
    const gate = a.gateNext("core_check");
    yield* TestClock.adjust("1 second");
    yield* gate.reached;
    yield* TestClock.adjust("3 seconds");
    gate.release();
    yield* gate.completed;
    const reset = (yield* deployments.awaitAcceptanceSample("queue", input.deploymentId, scope, 2))
      .deployment.acceptance!;
    expect(reset.state).toBe("observing");
    expect(reset.window.successfulSamples).toBe(1);
    expect(reset.window.elapsedMilliseconds).toBe(0);
    expect(reset.window.resets).toBeGreaterThan(0);
    expect(reset.firstSample?.ordinal).toBe(2);
    expect(reset.firstSample?.gapMilliseconds).toBeNull();
    expect(reset.firstSample?.wallGapMilliseconds).toBeNull();
    expect(reset.failure).toBeNull();
    expect(a.writes()).toBe(1);
  }).pipe(Effect.provide(layer.pipe(Layer.provideMerge(TestClock.layer())))),
);
it.live("exhausted historical evidence budget stops new probes and retains unknown", () =>
  Effect.gen(function* () {
    const { f, a, input } = yield* approvedDeploymentFixtureFor({
        durationSeconds: 2,
        intervalSeconds: 1,
        consecutiveSuccesses: 2,
      }),
      deployments = yield* Deployment.WorkbenchDeployment;
    yield* deployments.admit("queue", input);
    yield* deployments.sync;
    const d = (yield* deployments.awaitAcceptanceSample("queue", input.deploymentId, scope, 1))
        .deployment,
      previous = d.acceptance!;
    const ordinal = 131072,
      id = "historical-limit",
      sample = {
        ...previous.latestSample!,
        id,
        ordinal,
        state: "unknown" as const,
        completedAt: null,
        health: null,
        core: null,
        targetBefore: null,
        targetAfter: null,
        versionObservation: null,
        gapMilliseconds: null,
        wallGapMilliseconds: null,
        creditedElapsedMilliseconds: 0,
      };
    sample.inputDigest = releaseDigest({
      authorizationDigest: d.authorizationDigest,
      materialDigest: previous.materialDigest,
      sampleId: id,
      ordinal,
      runnerEpoch: previous.runnerEpoch,
      candidate: d.desired,
      activationRevision: previous.activationRevision,
      policyDigest: releaseDigest(previous.policy),
    });
    const { digest: _digest, ...unsigned } = sample;
    sample.digest = releaseDigest(unsigned);
    const acceptance = {
      ...previous,
      version: previous.version + 1,
      state: "unknown" as const,
      reasons: ["check_unknown"],
      firstSample: null,
      latestSample: sample,
      window: {
        ...previous.window,
        id: null,
        runnerEpoch: null,
        startedAt: null,
        endedAt: null,
        elapsedMilliseconds: 0,
        successfulSamples: 0,
        maxGapMilliseconds: 0,
        maxWallGapMilliseconds: 0,
        resets: previous.window.resets + 1,
        resetReason: "check_unknown",
      },
      ledger: {
        ...previous.ledger,
        count: ordinal,
        digest: releaseHash("historical unknown ledger prefix"),
      },
    };
    yield* f.sql`UPDATE workbench_deployments SET deployment_json=${canonicalReleaseJson({ ...d, version: d.version + 1, acceptance })} WHERE id=${d.id}`;
    yield* f.sql`UPDATE workbench_acceptance_runtime SET last_mono=NULL,last_wall=NULL WHERE deployment_id=${d.id}`;
    const calls = a.checkCalls();
    yield* TestClock.adjust("1 second");
    const limited = (yield* deployments.awaitAcceptanceSample("queue", d.id, scope, ordinal))
      .deployment.acceptance!;
    expect(limited.state).toBe("unknown");
    expect(limited.reasons).toEqual(["evidence_limit"]);
    expect(limited.activeSample).toBeNull();
    expect(limited.failure).toBeNull();
    expect(limited.ledger.count).toBe(ordinal);
    expect(a.checkCalls()).toBe(calls);
    expect(a.writes()).toBe(1);
  }).pipe(Effect.provide(layer.pipe(Layer.provideMerge(TestClock.layer())))),
);
it.live(
  "verification material refuses command arguments pointing into the mutable source project",
  () =>
    Effect.gen(function* () {
      const f = yield* fixture,
        releases = yield* Release.WorkbenchReleaseEvidence,
        cfg = yield* Config.ServerConfig;
      const command = `${process.execPath} ${f.path.join(f.repo, "artifact.cjs")}`,
        scripts = f.scripts.map((script) =>
          script.id === "health" ? { ...script, command } : script,
        );
      const profile = {
        ...f.profile,
        policy: {
          ...f.profile.policy,
          health: {
            scriptId: "health",
            definitionHash: releaseHash(JSON.stringify({ id: "health", command })),
          },
        },
      };
      yield* f.sql`UPDATE projection_projects SET scripts_json=${JSON.stringify(scripts)} WHERE project_id='project'`;
      yield* f.fs.writeFileString(
        f.path.join(cfg.stateDir, "workbench-release-profiles.json"),
        JSON.stringify({ schemaVersion: 1, profiles: [profile] }),
      );
      const catalog = (yield* releases.catalog({})).profiles[0]!;
      yield* releases.prepare("queue", {
        ...scope,
        preparationId: "absolute-probe",
        workItemId: "item",
        expectedWorkItemVersion: 1,
        profileId: profile.id,
        profileDigest: catalog.digest,
      });
      const result = (yield* releases.awaitPrepared("queue", "absolute-probe", scope)).preparation;
      expect(result.state).toBe("blocked");
      expect(result.reasons).toEqual(["artifact_invalid"]);
      expect(result.evidence).toBeNull();
    }).pipe(Effect.provide(layer)),
);
