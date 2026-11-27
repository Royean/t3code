// @effect-diagnostics globalFetch:off -- This named test adapter drives real HTTP across separately owned runtimes.
// @effect-diagnostics globalTimers:off -- The child-process adapter uses a readiness deadline, never a test completion sleep.
// @effect-diagnostics nodeBuiltinImport:off -- The isolated acceptance fixture owns its loopback server and Workbench subprocess.
import * as NodeHttp from "node:http";
import * as NodeChildProcess from "node:child_process";
import * as NodePath from "node:path";
import * as NodeFSP from "node:fs/promises";
import * as NodeNet from "node:net";
import * as NodeFS from "node:fs";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeHttpServer from "@effect/platform-node/NodeHttpServer";
import {
  EnvironmentHttpApi,
  CommandId,
  ThreadId,
  ProjectId,
  ScheduledTaskId,
  ProviderInstanceId,
  ProviderDriverKind,
  WsRpcGroup,
  WS_METHODS,
  ORCHESTRATION_V2_WS_METHODS,
} from "@t3tools/contracts";
import * as SqlClient from "effect/sql/SqlClient";
import * as DateTime from "effect/DateTime";
import * as Scope from "effect/Scope";
import * as Exit from "effect/Exit";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as FileSystem from "effect/FileSystem";
import * as HttpApi from "effect/http-api/HttpApi";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import * as HttpRouter from "effect/http/HttpRouter";
import * as HttpServer from "effect/http/HttpServer";
import * as HttpServerRequest from "effect/http/HttpServerRequest";
import * as HttpServerResponse from "effect/http/HttpServerResponse";
import * as HttpPlatform from "effect/http/HttpPlatform";
import * as Etag from "effect/http/Etag";
import * as RpcServer from "effect/rpc/RpcServer";
import * as RpcSerialization from "effect/rpc/RpcSerialization";
import * as Schema from "effect/Schema";
import * as SignalQueue from "effect/Queue";
import { withOutputWindow } from "../../terminal/OutputProtocol.ts";
import * as Stream from "effect/Stream";
import { makeScriptedProvider } from "./WorkbenchScriptedProvider.ts";
import { makeHarness } from "./ThreadLaunchHarness.ts";
import * as Config from "../../config.ts";
import * as Sqlite from "../../persistence/Sqlite.ts";
import * as ApplicationEvents from "../../persistence/OrchestrationEventStore.ts";
import * as Environment from "../../environment/ServerEnvironment.ts";
import * as EnvironmentAuth from "../../auth/EnvironmentAuth.ts";
import * as SecretStore from "../../auth/ServerSecretStore.ts";
import * as AuthHttp from "../../auth/http.ts";
import * as RpcAuthorization from "../../auth/RpcAuthorization.ts";
import * as Policy from "../../policy/ManagedPolicy.ts";
import * as Scheduler from "../../scheduling/Scheduler.ts";
import * as SecretRequests from "../../secrets/SecretRequests.ts";
import * as RemoteDelegation from "../../peer/RemoteDelegation.ts";
import * as Scheduled from "../../scheduledTasks/ScheduledTaskService.ts";
import * as WebhookHttp from "../../scheduledTasks/webhookRoute.ts";
import * as RelayProof from "../../scheduledTasks/RelayDeliveryProof.ts";
import * as ThreadLaunch from "../ThreadLaunchService.ts";
import * as Threads from "../ThreadManagementService.ts";
import * as ProjectStore from "../ProjectStore.ts";
import * as ProjectHttp from "../../project/http.ts";
import * as Projects from "../../project/ProjectService.ts";
import * as ProjectEnrichment from "../../project/ProjectEnrichmentService.ts";
import * as Startup from "../../serverRuntimeStartup.ts";
import * as OrchestrationHttp from "../http.ts";
import { subscribeOrchestrationV2Thread } from "../../ws.ts";
import * as Items from "../../scheduledTasks/WorkbenchWorkItems.ts";
import * as ItemsHttp from "../../scheduledTasks/workItemsHttp.ts";
import * as ExecutionsHttp from "../../scheduledTasks/executionsHttp.ts";
import * as Queue from "../../scheduledTasks/WorkbenchQueue.ts";
import * as QueueHttp from "../../scheduledTasks/workbenchQueueHttp.ts";
import * as Dispatch from "../../scheduledTasks/WorkbenchDispatch.ts";
import * as Mcp from "../../mcp/OrchestratorMcpService.ts";
import * as Git from "../../vcs/GitVcsDriver.ts";
import * as Checkpoints from "../../checkpointing/CheckpointStore.ts";
import * as VcsDrivers from "../../vcs/VcsDriverRegistry.ts";
import * as VcsProcess from "../../vcs/VcsProcess.ts";
import * as Reactor from "../../scheduledTasks/WorkbenchRepairReactor.ts";
import * as ProcessRunner from "../../processRunner.ts";
import * as Attention from "../../scheduledTasks/WorkbenchAttention.ts";
import * as AttentionHttp from "../../scheduledTasks/workbenchAttentionHttp.ts";
import * as Responses from "../../scheduledTasks/WorkbenchResponses.ts";
import * as ResponsesHttp from "../../scheduledTasks/workbenchResponsesHttp.ts";
import * as Repairs from "../../scheduledTasks/WorkbenchRepairs.ts";
import * as RepairsHttp from "../../scheduledTasks/workbenchRepairsHttp.ts";
import * as Progress from "../../scheduledTasks/WorkbenchTaskProgress.ts";
import * as ProgressHttp from "../../scheduledTasks/workbenchTaskProgressHttp.ts";
import * as Release from "../../scheduledTasks/WorkbenchReleaseEvidence.ts";
import * as ReleaseHttp from "../../scheduledTasks/workbenchReleaseHttp.ts";
import * as Adapter from "../../scheduledTasks/WorkbenchReleaseAdapter.ts";
import * as Deployment from "../../scheduledTasks/WorkbenchDeployment.ts";
import * as DeploymentHttp from "../../scheduledTasks/workbenchDeploymentHttp.ts";
import * as ReleaseRefs from "../../scheduledTasks/WorkbenchReleaseRefs.ts";
import { deploymentFixture } from "../../scheduledTasks/releaseDeploymentFixture.ts";
import { releaseHash } from "../../scheduledTasks/workbenchReleaseProfile.ts";
import { mergeFixture } from "../../scheduledTasks/releaseMergeFixture.ts";

export const projectId = ProjectId.make("project:launch-test");
export const secondaryProjectId = ProjectId.make("project:launch-other");
export const dispatcherId = ThreadId.make("thread:acceptance-dispatcher");
export const taskId = ScheduledTaskId.make("task:acceptance");
export const selection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.1-codex",
} as const;
const rpcTags = [
  WS_METHODS.scheduledTasksSubscribe,
  ORCHESTRATION_V2_WS_METHODS.subscribeThread,
] as const;
const rpcGroup = WsRpcGroup.omit(
  ...[...WsRpcGroup.requests.keys()].filter(
    (
      tag,
    ): tag is Exclude<
      keyof typeof RpcAuthorization.RPC_REQUIRED_SCOPES,
      (typeof rpcTags)[number]
    > => !(rpcTags as readonly string[]).includes(tag),
  ),
);
class AcceptanceApi extends HttpApi.make("environment")
  .add(EnvironmentHttpApi.groups.auth)
  .add(EnvironmentHttpApi.groups.projects)
  .add(EnvironmentHttpApi.groups.webhooks)
  .add(EnvironmentHttpApi.groups.orchestration)
  .add(EnvironmentHttpApi.groups.scheduledTaskExecutions)
  .add(EnvironmentHttpApi.groups.workbenchWorkItems)
  .add(EnvironmentHttpApi.groups.workbenchQueue)
  .add(EnvironmentHttpApi.groups.workbenchAttention)
  .add(EnvironmentHttpApi.groups.workbenchResponses)
  .add(EnvironmentHttpApi.groups.workbenchRepairs)
  .add(EnvironmentHttpApi.groups.workbenchTaskProgress)
  .add(EnvironmentHttpApi.groups.workbenchRelease)
  .add(EnvironmentHttpApi.groups.workbenchDeployment) {}

const released = (port: number) =>
  Effect.promise(
    () =>
      new Promise<void>((resolve, reject) => {
        const socket = NodeNet.createConnection({ host: "127.0.0.1", port });
        socket.once("connect", () => {
          socket.destroy();
          reject(new Error(`Owned port ${port} remains open`));
        });
        socket.once("error", (error) =>
          (error as NodeJS.ErrnoException).code === "ECONNREFUSED" ? resolve() : reject(error),
        );
      }),
  );

export const acquireAcceptanceFixture = Effect.fn("WorkbenchAcceptanceFixture.acquire")(function* (
  options: { readonly root?: string; readonly nativePort?: number } = {},
) {
  const fs = yield* FileSystem.FileSystem;
  const wbRepo = companionRepo;
  if (!wbRepo || !NodeFS.existsSync(NodePath.join(wbRepo, "server.mjs")))
    return yield* Effect.die("WORKBENCH_ACCEPTANCE_REPO must name the companion checkout");
  const peerHead = NodeChildProcess.execFileSync("git", ["rev-parse", "HEAD"], {
    cwd: wbRepo,
    encoding: "utf8",
  }).trim();
  const compatible = NodeChildProcess.spawnSync(
    "git",
    ["merge-base", "--is-ancestor", "08ab55528f53f8884d1c5698dcba9ee4c068faec", "HEAD"],
    { cwd: wbRepo },
  );
  if (compatible.status !== 0)
    return yield* Effect.die(
      "Companion checkout does not contain the accepted integration baseline",
    );
  yield* Effect.logInfo("Companion integration identity", {
    workbenchHead: peerHead,
    runtimeVersion: process.version,
  });
  let ownedRoot: string | undefined;
  yield* Effect.addFinalizer(() =>
    ownedRoot === undefined
      ? Effect.void
      : fs.exists(ownedRoot).pipe(
          Effect.flatMap((exists) =>
            exists
              ? Effect.die("Owned acceptance directory remains after scoped cleanup")
              : Effect.logInfo(`Reclaimed acceptance root ${ownedRoot}`),
          ),
          Effect.orDie,
        ),
  );
  const root =
    options.root === undefined
      ? yield* fs.realPath(yield* fs.makeTempDirectoryScoped({ prefix: "issue19-native-" }))
      : options.root;
  ownedRoot = options.root === undefined ? root : undefined;
  yield* Effect.logInfo(`Owned acceptance root ${root}; scoped removal`);
  const repo = NodePath.join(root, "project"),
    remote = NodePath.join(root, "remote.git");
  const existingRepo = NodeFS.existsSync(NodePath.join(repo, ".git"));
  if (!existingRepo) yield* fs.makeDirectory(repo);
  const git = (cwd: string, args: string[]) =>
    NodeChildProcess.execFileSync("git", args, {
      cwd,
      encoding: "utf8",
      env: { PATH: process.env.PATH, GIT_CONFIG_NOSYSTEM: "1", GIT_CONFIG_GLOBAL: "/dev/null" },
    }).trim();
  if (!existingRepo) {
    git(repo, ["init", "-b", "main"]);
    git(repo, ["config", "user.email", "acceptance@example.invalid"]);
    git(repo, ["config", "user.name", "Acceptance"]);
    yield* fs.writeFileString(NodePath.join(repo, "input.txt"), "prior");
  }
  const files = {
    ".gitignore": "out.bin\nbaseline-private/\n",
    "build.cjs": "require('node:fs').copyFileSync('input.txt','out.bin')",
    "check.cjs":
      "if(require('node:fs').readFileSync('input.txt','utf8')!=='candidate')process.exit(7)",
    "artifact.cjs":
      "if(process.env.T3_RELEASE_PHASE==='online'){(async()=>{const r=await fetch(process.env.T3_RELEASE_TARGET_ORIGIN+'/checks/'+process.env.T3_RELEASE_CHECK_ROLE,{headers:{'x-release-execution':process.env.T3_RELEASE_EXECUTION_ID}});if(!r.ok)process.exit(31);const v=await r.json();if(v.artifactSha256!==process.env.T3_RELEASE_EXPECTED_ARTIFACT_SHA256||v.value!=='CANDIDATE')process.exit(32)})().catch(()=>process.exit(33))}else if(require('node:fs').readFileSync(process.env.T3_RELEASE_ARTIFACT,'utf8')!=='candidate')process.exit(8)",
    "rollback.cjs":
      "if(require('node:fs').readFileSync(process.env.T3_RELEASE_ROLLBACK_ARTIFACT,'utf8')!=='prior-artifact')process.exit(9)",
    "execute.cjs": "process.exit(99)",
  };
  if (!existingRepo)
    for (const [name, content] of Object.entries(files))
      yield* fs.writeFileString(NodePath.join(repo, name), content);
  const scriptFiles = {
    build: "build.cjs",
    check: "check.cjs",
    health: "artifact.cjs",
    core: "artifact.cjs",
    execute: "execute.cjs",
    verify: "rollback.cjs",
  };
  const scripts = Object.entries(scriptFiles).map(([id, file]) => ({
    id,
    name: id,
    command: `${process.execPath} ${file}`,
    icon: "test" as const,
    runOnWorktreeCreate: false,
  }));
  yield* fs.makeDirectory(NodePath.join(repo, "baseline-private"), { recursive: true });
  yield* fs.writeFileString(NodePath.join(repo, "baseline-private/baseline.bin"), "prior-artifact");

  if (!existingRepo) {
    git(repo, ["add", "."]);
    git(repo, ["commit", "-qm", "baseline"]);
    git(repo, ["clone", "--bare", repo, remote]);
    git(repo, ["remote", "add", "origin", remote]);
  }
  const otherRepo = NodePath.join(root, "other-project");
  if (!NodeFS.existsSync(NodePath.join(otherRepo, ".git"))) {
    git(root, ["clone", repo, otherRepo]);
    git(otherRepo, ["config", "user.email", "acceptance@example.invalid"]);
    git(otherRepo, ["config", "user.name", "Acceptance"]);
  }
  const nativeScope = yield* Scope.make();
  yield* Effect.addFinalizer((exit) => Scope.close(nativeScope, exit));
  return yield* Effect.gen(function* () {
    const config = Config.layerTest(repo, NodePath.join(root, "native")).pipe(
      Layer.provide(NodeServices.layer),
    );
    const database = Sqlite.layerFromPath(
      NodePath.join(root, "native/userdata/statev2.sqlite"),
    ).pipe(Layer.provide(NodeServices.layer));
    const stamp = DateTime.formatIso(yield* DateTime.now);
    const provider = yield* makeScriptedProvider;
    let worktrees = NodeFS.readdirSync(root).filter((name) => /^child-[0-9]+$/.test(name)).length;
    const harness = makeHarness({
      workspaceRoot: repo,
      otherWorkspaceRoot: otherRepo,
      scripts,
      databaseLayer: database,
      configLayer: config,
      adapters: [provider.adapter],
      runEffectWorker: true,
      providers: [
        {
          instanceId: selection.instanceId,
          driver: ProviderDriverKind.make("codex"),
          enabled: true,
          installed: true,
          version: "fixture",
          status: "ready",
          auth: { status: "authenticated" },
          checkedAt: stamp,
          models: [
            { slug: selection.model, name: selection.model, isCustom: false, capabilities: null },
          ],
          slashCommands: [],
          skills: [],
        },
      ],
      hasCommit: () => Effect.succeed(true),
      git: {
        repositoryRoot: (input) => Effect.sync(() => git(input, ["rev-parse", "--show-toplevel"])),
        pruneWorktrees: (input) =>
          Effect.sync(() => {
            git(input.cwd, ["worktree", "prune"]);
          }),
        currentBranch: (cwd) =>
          Effect.sync(() => git(cwd, ["symbolic-ref", "--quiet", "--short", "HEAD"])),
        headPath: (cwd) =>
          Effect.sync(() =>
            git(cwd, ["rev-parse", "--path-format=absolute", "--git-path", "HEAD"]),
          ),
      },
      createWorktree: (input) =>
        Effect.sync(() => {
          const path = NodePath.join(root, `child-${++worktrees}`);
          git(input.cwd, [
            "worktree",
            "add",
            "-b",
            input.newRefName!,
            path,
            input.baseRefName ?? input.refName,
          ]);
          NodeFS.mkdirSync(NodePath.join(path, "baseline-private"));
          NodeFS.writeFileSync(
            NodePath.join(path, "baseline-private/baseline.bin"),
            "prior-artifact",
          );
          return {
            worktree: {
              path,
              refName: input.newRefName!,
              headSha: git(path, ["rev-parse", "HEAD"]),
            },
          };
        }),
      generateBranchName: (input) =>
        Effect.sync(() => ({
          branch: git(input.cwd, ["symbolic-ref", "--quiet", "--short", "HEAD"]),
        })),
      renameBranch: (input) =>
        Effect.sync(() => {
          git(input.cwd, ["branch", "-m", input.newBranch]);
          return { branch: input.newBranch };
        }),
    });
    const checkpoint = Checkpoints.layer.pipe(
      Layer.provide(VcsDrivers.layer.pipe(Layer.provide(VcsProcess.layer))),
      Layer.provide(config),
    );
    const base = Layer.mergeAll(
      harness.layer,
      checkpoint,
      config,
      database,
      Policy.layerTest(),
      Scheduler.layer,
      ProcessRunner.layer,
      Layer.mock(SecretRequests.SecretRequests)({}),
      Layer.mock(RemoteDelegation.RemoteDelegation)({}),
      Git.layer.pipe(Layer.provide(config)),
      Environment.layerIdentity.pipe(Layer.provide(config)),
      SecretStore.layer.pipe(Layer.provide(config)),
    );
    const baseContext = yield* Layer.build(base);
    const shared = Layer.succeedContext(baseContext);
    const auth = EnvironmentAuth.layer.pipe(Layer.provide(shared));
    const scheduled = Scheduled.layer.pipe(Layer.provide(shared));
    const domains = yield* Layer.build(
      Layer.mergeAll(auth, scheduled, ProjectStore.layer, ApplicationEvents.layer).pipe(
        Layer.provideMerge(shared),
      ),
    );
    const domainLayer = Layer.succeedContext(domains);
    const items = Items.layer.pipe(Layer.provide(domainLayer));
    const queue = Queue.layer.pipe(Layer.provide(items), Layer.provide(domainLayer));
    const attention = Attention.layer.pipe(
      Layer.provide(queue),
      Layer.provide(items),
      Layer.provide(domainLayer),
    );
    const refs = yield* mergeFixture().pipe(Effect.provide(domainLayer));
    const releaseDeps = Layer.mergeAll(
      domainLayer,
      Layer.succeed(ReleaseRefs.WorkbenchReleaseRefs, refs.service),
    );
    const release = Release.layer.pipe(
      Layer.provideMerge(Adapter.layer),
      Layer.provideMerge(releaseDeps),
    );
    const deployment = Deployment.layer.pipe(Layer.provide(release));
    const dispatch = Dispatch.layer.pipe(
      Layer.provide(attention),
      Layer.provide(queue),
      Layer.provide(items),
      Layer.provide(Mcp.layer.pipe(Layer.provide(domainLayer))),
      Layer.provide(domainLayer),
    );
    const repairs = Repairs.layer.pipe(Layer.provide(domainLayer));
    const reactor = Reactor.layer.pipe(
      Layer.provide(repairs),
      Layer.provide(attention),
      Layer.provide(domainLayer),
    );
    const workbenchDomains = yield* Layer.build(
      Layer.mergeAll(
        items,
        queue,
        dispatch,
        attention,
        release,
        deployment,
        Responses.layer.pipe(
          Layer.provide(queue),
          Layer.provide(items),
          Layer.provide(domainLayer),
        ),
        repairs,
        reactor,
        Progress.layer.pipe(Layer.provide(domainLayer)),
      ).pipe(Layer.provideMerge(domainLayer)),
    );
    const serviceLayer = Layer.succeedContext(workbenchDomains);
    const tasks = Context.get(workbenchDomains, Scheduled.ScheduledTaskService),
      threads = Context.get(workbenchDomains, Threads.ThreadManagementService);
    const launch = Context.get(workbenchDomains, ThreadLaunch.ThreadLaunchService),
      identity = Context.get(workbenchDomains, Environment.ServerEnvironmentIdentity);
    let saved = { task: (yield* tasks.list()).tasks.find((task) => task.id === taskId) };
    if (saved.task === undefined) {
      yield* launch.launch({
        commandId: CommandId.make("acceptance:dispatcher"),
        threadId: dispatcherId,
        projectId,
        title: "Acceptance dispatcher",
        workspaceStrategy: { type: "root" },
        modelSelection: selection,
        runtimeMode: "full-access",
        interactionMode: "default",
        createdBy: "user",
        creationSource: "web",
      });
      saved = yield* tasks.upsert({
        id: taskId,
        title: "Acceptance dispatcher",
        prompt: "Process {{body.deliveryId}}",
        enabled: true,
        schedule: { type: "webhook", dedupe: { replayKey: "{{body.deliveryId}}" } },
        projectId,
        threadId: dispatcherId,
        threadMode: "continue",
        workspaceStrategy: { type: "root" },
        modelSelection: selection,
        runtimeMode: "full-access",
        interactionMode: "default",
        createdBy: "user",
        creationSource: "web",
      });
    }
    const sql = Context.get(workbenchDomains, SqlClient.SqlClient);
    yield* sql`INSERT INTO projection_projects(project_id,title,workspace_root,scripts_json,created_at,updated_at) VALUES(${projectId},'Acceptance project',${repo},${encodeJson(scripts)},${stamp},${stamp}) ON CONFLICT(project_id) DO NOTHING`;
    yield* sql`INSERT INTO projection_projects(project_id,title,workspace_root,scripts_json,created_at,updated_at) VALUES(${secondaryProjectId},'Secondary acceptance project',${otherRepo},${encodeJson(scripts)},${stamp},${stamp}) ON CONFLICT(project_id) DO NOTHING`;
    const session = yield* Context.get(
      workbenchDomains,
      EnvironmentAuth.EnvironmentAuth,
    ).issueSession({
      label: "Isolated acceptance",
      subject: "acceptance",
      scopes: ["orchestration:read", "orchestration:operate"],
    });
    const environmentId = yield* identity.getEnvironmentId;
    const projected = Context.get(workbenchDomains, Projects.ProjectService);
    const project = yield* projected.getById(projectId),
      otherProject = yield* projected.getById(secondaryProjectId);
    const projectApi = Layer.succeed(Projects.ProjectService, {
      ...projected,
      snapshot: Effect.succeed({
        projects: [project, otherProject].flatMap((value) =>
          value._tag === "Some" ? [value.value] : [],
        ),
        updatedAt: stamp,
      }),
    });
    const deploymentAdmissions = yield* SignalQueue.unbounded<{
      queueId: string;
      input: Parameters<Deployment.WorkbenchDeployment["Service"]["admit"]>[1];
    }>();
    const realDeployment = Context.get(workbenchDomains, Deployment.WorkbenchDeployment);
    const deploymentTransport = Layer.succeed(Deployment.WorkbenchDeployment, {
      ...realDeployment,
      admit: (queueId, input) =>
        realDeployment
          .admit(queueId, input)
          .pipe(Effect.tap(() => SignalQueue.offer(deploymentAdmissions, { queueId, input }))),
    });
    const threadAcks = yield* SignalQueue.unbounded<{ threadId: string; sequence: number }>();
    const routes = HttpApiBuilder.layer(AcceptanceApi).pipe(
      Layer.provide(
        Layer.mergeAll(
          AuthHttp.layer,
          WebhookHttp.layer,
          OrchestrationHttp.layer,
          ProjectHttp.layer,
          ExecutionsHttp.layer,
          ItemsHttp.layer,
          QueueHttp.layer,
          AttentionHttp.layer,
          ResponsesHttp.layer,
          RepairsHttp.layer,
          ProgressHttp.layer,
          ReleaseHttp.layer,
          DeploymentHttp.layer.pipe(Layer.provide(deploymentTransport)),
        ),
      ),
      Layer.provide(
        RelayProof.layer.pipe(
          Layer.provide(serviceLayer),
          Layer.provide(
            Layer.mock(Environment.ServerEnvironment)({
              getEnvironmentId: identity.getEnvironmentId,
            }),
          ),
        ),
      ),
      Layer.provide(projectApi),
      Layer.provide(Layer.mock(Startup.ServerRuntimeStartup)({})),
      Layer.provide(
        Layer.mock(ProjectEnrichment.ProjectEnrichmentService)({
          getAvailable: () => Effect.succeed({ repositoryIdentity: null }) as never,
        }),
      ),
      Layer.provide(AuthHttp.layerAuthenticatedAuth),
      Layer.provide(serviceLayer),
      Layer.provide(HttpPlatform.layer),
      Layer.provide(Etag.layerWeak),
    );
    const additionalRoutes = Layer.effectDiscard(
      Effect.gen(function* () {
        const router = yield* HttpRouter.HttpRouter;
        yield* router.add(
          "GET",
          "/.well-known/t3/environment",
          HttpServerResponse.json({ environmentId }),
        );
        yield* router.add(
          "GET",
          "/ws",
          Effect.gen(function* () {
            const request = yield* HttpServerRequest.HttpServerRequest;
            const principal = yield* Context.get(
              workbenchDomains,
              EnvironmentAuth.EnvironmentAuth,
            ).authenticateWebSocketUpgrade(request, "rpc");
            const { protocol, httpEffect } = yield* RpcServer.makeProtocolWithHttpEffectWebsocket;
            const streams = new Map<string, string>(),
              pending = new Map<string, number[]>();
            const observedProtocol: RpcServer.Protocol["Service"] = {
              ...protocol,
              run: (receive) =>
                protocol.run((clientId, message) =>
                  Effect.gen(function* () {
                    const key = `${clientId}:${message._tag === "Request" ? message.id : "requestId" in message ? message.requestId : ""}`;
                    if (
                      message._tag === "Request" &&
                      message.tag === ORCHESTRATION_V2_WS_METHODS.subscribeThread
                    )
                      streams.set(key, (message.payload as { threadId: string }).threadId);
                    if (message._tag === "Ack" && streams.has(key)) {
                      const sequence = pending.get(key)?.shift();
                      if (sequence !== undefined)
                        yield* SignalQueue.offer(threadAcks, {
                          threadId: streams.get(key)!,
                          sequence,
                        });
                    }
                    return yield* receive(clientId, message);
                  }),
                ),
              send: (clientId, response, transferables) =>
                Effect.suspend(() => {
                  const key = `${clientId}:${"requestId" in response ? response.requestId : ""}`;
                  if (response._tag === "Chunk" && streams.has(key)) {
                    const sequence = Math.max(
                      0,
                      ...response.values.map((value) => {
                        const item = value as { sequence?: number; snapshotSequence?: number };
                        return item.sequence ?? item.snapshotSequence ?? 0;
                      }),
                    );
                    const rows = pending.get(key) ?? [];
                    rows.push(sequence);
                    pending.set(key, rows);
                  }
                  return protocol.send(clientId, response, transferables);
                }),
            };
            yield* RpcServer.make(rpcGroup, { disableTracing: true }).pipe(
              Effect.provideService(RpcServer.Protocol, withOutputWindow(observedProtocol)),
              Effect.provide(
                Layer.mergeAll(
                  rpcGroup.toLayerHandler(WS_METHODS.scheduledTasksSubscribe, () =>
                    tasks.subscribeList(),
                  ),
                  rpcGroup.toLayerHandler(ORCHESTRATION_V2_WS_METHODS.subscribeThread, (input) =>
                    Stream.unwrap(subscribeOrchestrationV2Thread(input)),
                  ),
                  RpcAuthorization.layer(principal.scopes),
                  RpcSerialization.layerJson,
                ).pipe(Layer.provideMerge(serviceLayer)),
              ),
              Effect.tapCause((cause) => Effect.logError("Acceptance RPC server failed", cause)),
              Effect.forkScoped,
            );
            return yield* httpEffect;
          }).pipe(
            Effect.provide(RpcSerialization.layerJson),
            Effect.tapCause((cause) => Effect.logError("Acceptance RPC failed", cause)),
          ),
        );
      }),
    );
    let nativePort = 0;
    yield* Effect.addFinalizer(() => (nativePort === 0 ? Effect.void : released(nativePort)));
    const responseFault = { dropNextAdmission: false, dropped: 0, requests: 0 };
    const network = yield* Layer.build(
      NodeHttpServer.layer(
        () =>
          NodeHttp.createServer((request, response) => {
            if (
              request.method === "POST" &&
              /\/api\/workbench\/queues\/[^/]+\/deployments$/.test(request.url ?? "")
            ) {
              responseFault.requests++;
              if (responseFault.dropNextAdmission) {
                responseFault.dropNextAdmission = false;
                response.writeHead = () => {
                  responseFault.dropped++;
                  response.destroy();
                  return response;
                };
              }
            }
          }),
        { port: options.nativePort ?? 0, host: "127.0.0.1" },
      ),
    );
    const address = Context.get(network, HttpServer.HttpServer).address;
    if (!("port" in address)) return yield* Effect.die("Expected test TCP address");
    nativePort = address.port;
    const origin = `http://127.0.0.1:${nativePort}`;
    yield* Layer.build(
      HttpRouter.serve(
        Layer.merge(routes, additionalRoutes.pipe(Layer.provide(RpcSerialization.layerJson))),
        { disableListenLog: true, disableLogger: true, routerConfig: { maxParamLength: 512 } },
      ).pipe(Layer.provide(Layer.succeedContext(network)), Layer.provide(serviceLayer)),
    );
    yield* Effect.logInfo(
      `Owned Native listener ${nativePort}; selected production HTTP/RPC groups`,
    );
    return {
      root,
      repo,
      otherRepo,
      remote,
      git,
      scripts,
      origin,
      nativePort,
      environmentId,
      credential: session.token,
      task: saved.task!,
      closeNative: Scope.close(nativeScope, Exit.succeed(undefined)),
      services: workbenchDomains,
      layer: serviceLayer,
      tasks,
      threads,
      provider,
      refs,
      threadAcks,
      deploymentAdmissions,
      responseFault,
      loseNextAdmissionResponse: () => {
        responseFault.dropNextAdmission = true;
      },
      config: Context.get(baseContext, Config.ServerConfig),
      worktrees: () => worktrees,
    };
  }).pipe(Effect.provideService(Scope.Scope, nativeScope));
});

const encodeJson = Schema.encodeSync(Schema.fromJsonString(Schema.Unknown));
const companionRepo = process.env.WORKBENCH_ACCEPTANCE_REPO;
const API = "/api/developer-hub/event-hub";
const isolatedProcessEnv = () => ({
  PATH: process.env.PATH ?? "/usr/bin:/bin",
  TMPDIR: process.env.TMPDIR ?? "/tmp",
  NODE_ENV: "test",
});
const availablePort = () =>
  new Promise<number>((resolve, reject) => {
    const server = NodeNet.createServer();
    server.once("error", reject);
    server.listen(0, "127.0.0.1", () => {
      const address = server.address();
      if (!address || typeof address === "string") return reject(new Error("No isolated port"));
      server.close((error) => (error ? reject(error) : resolve(address.port)));
    });
  });
async function startWorkbenchProcess(
  repo: string,
  directory: string,
  nativeOrigin: string,
  requestedPort?: number,
  github = false,
) {
  const port = requestedPort ?? (await availablePort()),
    origin = `http://127.0.0.1:${port}`;
  const email = "acceptance-owner@example.invalid",
    password = "Synthetic-acceptance-password-19";
  const env = {
    ...isolatedProcessEnv(),
    AUTH_REQUIRED: "1",
    APP_ORIGIN: origin,
    PORT: String(port),
    HEALTH_DATA_DIR: directory,
    WORKBENCH_EVENT_WEBHOOK_ORIGINS: nativeOrigin,
    WORKBENCH_ACCEPTANCE_FAULT_DIR: directory,
    WORKBENCH_GITHUB_TOKEN: github ? "synthetic-acceptance-github-token" : "",
    WORKBENCH_GITHUB_ACCOUNT_EMAIL: github ? email : "",
    WORKBENCH_ROLE_CATALOG_FILE: "",
    WORKBENCH_ROLE_CATALOG_ACCOUNT_EMAIL: "",
  };
  const setup = NodeFS.existsSync(NodePath.join(directory, "health.sqlite"))
    ? { status: 0, stderr: "" }
    : NodeChildProcess.spawnSync(
        process.execPath,
        ["scripts/account-admin.mjs", "create-admin", "--email", email],
        { cwd: repo, input: password + "\n", encoding: "utf8", env },
      );
  if (setup.status !== 0) throw new Error(`Synthetic owner setup failed: ${setup.stderr}`);
  const processHandle = NodeChildProcess.spawn(
    process.execPath,
    [
      "--import",
      NodePath.join(import.meta.dirname, "WorkbenchProcessBoundary.mjs"),
      ...(github
        ? ["--import", NodePath.join(import.meta.dirname, "WorkbenchGithubBoundary.mjs")]
        : []),
      "server.mjs",
    ],
    {
      cwd: repo,
      env,
      stdio: ["ignore", "pipe", "pipe", "ipc"],
    },
  );
  let stderr = "";
  type BoundaryMessage = {
    kind: string;
    queueId?: string;
    attentions?: Array<{
      id: string;
      workItemId: string;
      version: number;
      phase: string;
      canRequeue: boolean;
      repairGenerationId?: string;
    }>;
    method?: string;
    body?: string;
    issueNumber?: number;
  };
  const observations: BoundaryMessage[] = [],
    waiters: Array<{
      predicate: (message: BoundaryMessage) => boolean;
      resolve: (message: BoundaryMessage) => void;
      reject: (error: Error) => void;
    }> = [];
  processHandle.on("message", (value) => {
    const message = value as BoundaryMessage;
    const waiter = waiters.find((entry) => entry.predicate(message));
    if (waiter) {
      waiters.splice(waiters.indexOf(waiter), 1);
      waiter.resolve(message);
    } else {
      observations.push(message);
      if (observations.length > 1000) observations.shift();
    }
  });
  processHandle.once("exit", () => {
    for (const waiter of waiters.splice(0))
      waiter.reject(new Error("Owned Workbench stopped before its observation milestone"));
  });
  const waitBoundary = (predicate: (message: BoundaryMessage) => boolean) => {
    const index = observations.findIndex(predicate);
    if (index >= 0) return Promise.resolve(observations.splice(index, 1)[0]!);
    return new Promise<BoundaryMessage>((resolve, reject) =>
      waiters.push({ predicate, resolve, reject }),
    );
  };

  processHandle.stderr!.on("data", (value) => {
    stderr += value.toString();
  });
  const stop = async () => {
    if (processHandle.exitCode !== null || processHandle.signalCode !== null) return;
    const ended = new Promise<void>((resolve) => processHandle.once("exit", () => resolve()));
    processHandle.kill("SIGTERM");
    await ended;
  };
  try {
    await new Promise<void>((resolve, reject) => {
      const deadline = setTimeout(
        () => reject(new Error(`Isolated Workbench startup failed: ${stderr}`)),
        20_000,
      );
      processHandle.once("exit", (code) => {
        clearTimeout(deadline);
        reject(new Error(`Isolated Workbench exited before readiness (${code}): ${stderr}`));
      });
      processHandle.stdout!.on("data", (value) => {
        if (/localhost:\d+/.test(value.toString())) {
          clearTimeout(deadline);
          resolve();
        }
      });
    });
  } catch (error) {
    await stop();
    throw error;
  }
  let actor: { id: string; cookie: string } | undefined;
  async function request(
    path: string,
    options: {
      method?: string;
      body?: unknown;
      machine?: boolean;
      headers?: Record<string, string>;
    } = {},
  ) {
    const response = await fetch(origin + path, {
      method: options.method ?? "GET",
      headers: {
        "content-type": "application/json",
        ...(options.machine ? {} : { origin }),
        ...(actor && !options.machine
          ? { cookie: actor.cookie, "x-workbench-owner": actor.id }
          : {}),
        ...options.headers,
      },
      ...(options.body === undefined ? {} : { body: encodeJson(options.body) }),
    });
    const raw = await response.text(),
      body = raw ? JSON.parse(raw) : null;
    if (response.status >= 400)
      throw new Error(
        `Workbench ${path} failed (${response.status}): ${encodeJson(body)}; stderr=${stderr}`,
      );
    return {
      status: response.status,
      body,
      cookie: response.headers.get("set-cookie")?.split(";")[0],
    };
  }
  try {
    const login = await request("/api/auth/login", { method: "POST", body: { email, password } });
    actor = { id: login.body.user.id, cookie: login.cookie! };
    return {
      port,
      origin,
      process: processHandle,
      request,
      stop,
      actor,
      waitBoundary,
      stderr: () => stderr,
    };
  } catch (error) {
    await stop();
    throw error;
  }
}

export const startWorkbench = Effect.fn("WorkbenchAcceptanceFixture.startWorkbench")(function* (
  f: {
    root: string;
    origin: string;
  },
  options: { readonly github?: boolean } = {},
) {
  const fs = yield* FileSystem.FileSystem;
  const directory = NodePath.join(f.root, "workbench");
  yield* fs.makeDirectory(directory);
  const repo = companionRepo!;
  let preferredPort: number | undefined;
  const acquire = Effect.acquireRelease(
    Effect.promise(() =>
      startWorkbenchProcess(repo, directory, f.origin, preferredPort, options.github ?? false),
    ),
    (app) =>
      Effect.promise(app.stop).pipe(
        Effect.andThen(
          Effect.sync(() => {
            if (app.process.exitCode === null && app.process.signalCode === null)
              throw new Error("Owned Workbench process remains live");
          }),
        ),
        Effect.andThen(released(app.port)),
        Effect.andThen(
          Effect.logInfo("Reclaimed Workbench process and listener", {
            pid: app.process.pid,
            port: app.port,
          }),
        ),
      ),
  );
  let app = yield* acquire;
  preferredPort = app.port;
  yield* Effect.logInfo(
    `Owned Workbench PID ${app.process.pid}; loopback ${app.port}; state ${directory}; scoped shutdown and port check`,
  );
  const restart = Effect.gen(function* () {
    yield* Effect.promise(app.stop);
    yield* released(app.port);
    app = yield* acquire;
    yield* Effect.logInfo("Reopened Workbench SQLite and same origin", {
      pid: app.process.pid,
      port: app.port,
    });
  });
  const request = (path: string, options?: Parameters<typeof app.request>[1]) =>
    Effect.promise(() => app.request(path, options));
  return {
    directory,
    call: request,
    api: API,
    restart,
    observeBoundary: (predicate: Parameters<typeof app.waitBoundary>[0]) =>
      Effect.promise(() => app.waitBoundary(predicate)),
    awaitAttention: (workItemId: string, phase = "awaiting_user") =>
      Effect.promise(() =>
        app.waitBoundary(
          (message) =>
            message.kind === "attention" &&
            message.attentions?.some(
              (item) => item.workItemId === workItemId && item.phase === phase,
            ) === true,
        ),
      ),
    loseNextClaimResponse: Effect.sync(() =>
      NodeFS.writeFileSync(
        NodePath.join(directory, "drop-claim-response.flag"),
        "drop one committed claim response",
      ),
    ),
    boundaryEvents: Effect.promise(async () => {
      try {
        return (await NodeFSP.readFile(NodePath.join(directory, "boundary-events.jsonl"), "utf8"))
          .trim()
          .split("\n")
          .filter(Boolean)
          .map(
            (line) =>
              JSON.parse(line) as {
                role: string;
                method: string;
                status: number;
                dropped: boolean;
              },
          );
      } catch (error) {
        if ((error as NodeJS.ErrnoException).code === "ENOENT") return [];
        throw error;
      }
    }),
    get actor() {
      return app.actor;
    },
    get origin() {
      return app.origin;
    },
    get port() {
      return app.port;
    },
    get process() {
      return app.process;
    },
  };
});

export const configureRelease = Effect.fn("WorkbenchAcceptanceFixture.configureRelease")(function* (
  f: Effect.Success<ReturnType<typeof acquireAcceptanceFixture>>,
  wb: Effect.Success<ReturnType<typeof startWorkbench>>,
) {
  const fs = yield* FileSystem.FileSystem;
  const identity = (yield* wb.call("/.well-known/workbench-release-coordinator", { machine: true }))
    .body;
  if (!identity.configured)
    return yield* Effect.die("Real Workbench coordinator is not configured");
  const baseline = f.git(f.repo, ["rev-parse", "HEAD"]);
  let adapterPort = 0;
  yield* Effect.addFinalizer(() =>
    adapterPort === 0
      ? Effect.void
      : released(adapterPort).pipe(
          Effect.andThen(
            Effect.logInfo("Reclaimed independent target adapter listener", { port: adapterPort }),
          ),
        ),
  );
  const adapter = yield* deploymentFixture(f.root, baseline, undefined, identity).pipe(
    Effect.provide(f.layer),
  );
  adapterPort = Number(new URL(adapter.policy.adapter.origin).port);
  yield* Effect.logInfo("Owned independent target adapter listener", { port: adapterPort });
  const ref = (id: string) => ({
    scriptId: id,
    definitionHash: releaseHash(
      encodeJson({ id, command: f.scripts.find((script) => script.id === id)!.command }),
    ),
  });
  const profile = {
    id: "acceptance-profile",
    version: 1,
    projectId,
    name: "Isolated concrete target",
    enabled: true,
    policy: {
      main: { remote: "origin", ref: "refs/heads/main" },
      target: {
        serviceId: "service",
        origin: adapter.policy.adapter.origin,
        versionPath: "/version",
      },
      build: ref("build"),
      artifact: { relativePath: "out.bin" },
      checks: [ref("check")],
      health: ref("health"),
      core: ref("core"),
      rollback: {
        artifactRelativePath: "baseline-private/baseline.bin",
        execute: ref("execute"),
        verify: ref("verify"),
      },
      observation: { durationSeconds: 2, intervalSeconds: 1, consecutiveSuccesses: 2 },
      deployment: adapter.policy,
      merge: {
        schemaVersion: 1,
        kind: "github_exact_ff",
        host: "github.fixture.invalid:8443",
        repository: "fixture/project",
        repositoryId: "repository=",
        ref: "refs/heads/main",
        requiredChecks: [{ context: "release", appId: 1 }],
      },
    },
  };
  yield* fs.writeFileString(
    NodePath.join(f.config.stateDir, "workbench-release-profiles.json"),
    encodeJson({ schemaVersion: 1, profiles: [profile] }),
  );
  const publicProfile = (yield* Context.get(f.services, Release.WorkbenchReleaseEvidence).catalog({
    projectId,
  })).profiles[0]!;
  const plan = publicProfile.deploymentPlan!;
  yield* fs.writeFileString(
    NodePath.join(wb.directory, "workbench-release-targets.json"),
    encodeJson({
      schemaVersion: 1,
      targets: [
        {
          serviceKey: plan.serviceKey,
          serviceId: "service",
          adapterId: plan.adapterId,
          adapterVersion: plan.adapterVersion,
          generationId: plan.generationId,
          deployPlanDigest: plan.deploy.planDigest,
          rollbackPlanDigest: plan.rollback.planDigest,
          targetAliases: [
            { origin: publicProfile.target.origin, versionPath: publicProfile.target.versionPath },
          ],
          nativeBindings: [
            {
              origin: f.origin,
              environmentId: f.environmentId,
              projectId,
              ownerIds: [wb.actor.id],
            },
          ],
        },
      ],
    }),
  );
  return { profile: publicProfile, adapter, baseline };
});

/** Imperative network boundary for requests to the scoped loopback production router. */
async function requestNativeJson(
  f: { origin: string; credential: string },
  path: string,
  body?: unknown,
) {
  const response = await fetch(f.origin + path, {
    method: body === undefined ? "GET" : "POST",
    headers: { authorization: "Bearer " + f.credential, "content-type": "application/json" },
    ...(body === undefined ? {} : { body: encodeJson(body) }),
  });
  const value = await response.json();
  if (!response.ok)
    throw new Error(`Native acceptance request failed (${response.status}): ${encodeJson(value)}`);
  return value;
}
export const nativeJson = (
  f: { origin: string; credential: string },
  path: string,
  body?: unknown,
) => Effect.promise(() => requestNativeJson(f, path, body));
