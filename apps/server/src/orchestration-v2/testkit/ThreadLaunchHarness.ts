import * as NodeServices from "@effect/platform-node/NodeServices";
import { vi } from "@effect/vitest";
import {
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ServerProvider,
  type ProjectScript,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as McpProviderSessions from "@t3tools/provider-core/server/McpProviderSessions";
import type * as ProviderAdapter from "@t3tools/provider-core/server/ProviderAdapter";
import * as IdAllocator from "@t3tools/provider-core/server/IdAllocator";
import * as GitWorkflow from "../../git/GitWorkflowService.ts";
import * as GitVcsDriver from "../../vcs/GitVcsDriver.ts";
import * as SqlitePersistence from "../../persistence/Sqlite.ts";
import * as ProjectStore from "../ProjectStore.ts";
import * as ProjectService from "../../project/ProjectService.ts";
import * as ProjectSetupScriptRunner from "../../project/ProjectSetupScriptRunner.ts";
import * as ManagedProjectFolders from "../../project/ManagedProjectFolders.ts";
import * as ProviderRegistryMock from "../../provider/testUtils/providerRegistryMock.ts";
import * as ProjectCloneTracker from "../../project/ProjectCloneTracker.ts";
import * as WorktreeSetupTracker from "../../project/WorktreeSetupTracker.ts";
import * as TerminalManager from "../../terminal/Manager.ts";
import * as CheckpointStore from "../../checkpointing/CheckpointStore.ts";
import * as ServerSettings from "../../serverSettings.ts";
import * as TextGeneration from "../../textGeneration/TextGeneration.ts";
import { CodexProviderCapabilitiesV2 } from "../Adapters/CodexAdapterV2.ts";
import * as CommandReceiptStore from "../CommandReceiptStore.ts";
import * as EffectOutbox from "../EffectOutbox.ts";
import * as ProviderAdapterRegistry from "../ProviderAdapterRegistry.ts";
import * as ThreadLaunch from "../ThreadLaunchService.ts";
import * as ThreadManagement from "../ThreadManagementService.ts";
import * as ThreadTitleRegeneration from "../ThreadTitleRegenerationService.ts";
import * as ProviderReplayHarness from "./ProviderReplayHarness.ts";
import type * as SqlClient from "effect/sql/SqlClient";
import type * as PlatformError from "effect/PlatformError";
import type { MigrationError } from "effect/sql/Migrator";
import type { SqlError } from "effect/sql/SqlError";
import type * as Config from "../../config.ts";

const projectId = ProjectId.make("project:launch-test");
const otherProjectId = ProjectId.make("project:launch-other");
const modelSelection = {
  instanceId: ProviderInstanceId.make("codex"),
  model: "gpt-5.1-codex",
} as const;
const project = {
  id: projectId,
  title: "Project",
  workspaceRoot: "/repo",
  repositoryIdentity: null,
  faviconPath: null,
  defaultModelSelection: modelSelection,
  defaultThreadEnvMode: null,
  scripts: [],
  pinnedAt: null,
  archivedAt: null,
  createdAt: "2026-06-20T00:00:00.000Z",
  updatedAt: "2026-06-20T00:00:00.000Z",
  deletedAt: null,
} as const;

const otherProject = {
  ...project,
  id: otherProjectId,
  title: "Other",
  workspaceRoot: "/other-repo",
} as const;

const adapter = {
  instanceId: modelSelection.instanceId,
  driver: ProviderDriverKind.make("codex"),
  getCapabilities: () => Effect.succeed(CodexProviderCapabilitiesV2),
  planSelectionTransition: () => Effect.succeed({ type: "apply_on_next_turn" as const }),
  openSession: () => Effect.die("provider execution is disabled in launch tests"),
} as ProviderAdapter.ProviderAdapterV2["Service"];

export interface HarnessOptions {
  readonly workspaceRoot?: string;
  readonly otherWorkspaceRoot?: string;
  readonly scripts?: ReadonlyArray<ProjectScript>;
  readonly databaseLayer?: Layer.Layer<
    SqlClient.SqlClient,
    MigrationError | PlatformError.PlatformError | SqlError
  >;
  readonly configLayer?: Layer.Layer<Config.ServerConfig, PlatformError.PlatformError>;
  readonly runEffectWorker?: boolean;
  readonly closeTerminal?: TerminalManager.TerminalManager["Service"]["close"];
  readonly adapters?: ReadonlyArray<ProviderAdapter.ProviderAdapterV2["Service"]>;
  readonly registryLayer?: Layer.Layer<
    ProviderAdapterRegistry.ProviderAdapterRegistryV2,
    never,
    McpProviderSessions.McpProviderSessions
  >;
  readonly managedFolders?: Layer.Layer<ManagedProjectFolders.ManagedProjectFolders>;
  readonly createWorktree?: GitWorkflow.GitWorkflowService["Service"]["createWorktree"];
  readonly fetchWorktreeBaseCommit?: GitWorkflow.GitWorkflowService["Service"]["fetchWorktreeBaseCommit"];
  readonly remoteExists?: GitWorkflow.GitWorkflowService["Service"]["remoteExists"];
  readonly hasCommit?: GitWorkflow.GitWorkflowService["Service"]["hasCommit"];
  readonly renameBranch?: GitWorkflow.GitWorkflowService["Service"]["renameBranch"];
  readonly runSetup?: ProjectSetupScriptRunner.ProjectSetupScriptRunner["Service"]["runForThread"];
  readonly generateTitle?: TextGeneration.TextGeneration["Service"]["generateThreadTitle"];
  readonly generateBranchName?: TextGeneration.TextGeneration["Service"]["generateBranchName"];
  readonly serverSettings?: Parameters<typeof ServerSettings.layerTest>[0];
  readonly getLaunchSettings?: ServerSettings.ServerSettingsService["Service"]["getSettings"];
  readonly providers?: ReadonlyArray<ServerProvider>;
  readonly git?: Partial<GitWorkflow.GitWorkflowService["Service"]>;
  readonly isGitRepository?: CheckpointStore.CheckpointStore["Service"]["isGitRepository"];
}

export function makeHarness(options: HarnessOptions = {}) {
  const launchProject = {
    ...project,
    workspaceRoot: options.workspaceRoot ?? project.workspaceRoot,
    scripts: options.scripts ?? project.scripts,
  };
  const otherLaunchProject = {
    ...otherProject,
    workspaceRoot: options.otherWorkspaceRoot ?? otherProject.workspaceRoot,
    scripts: options.scripts ?? otherProject.scripts,
  };
  const layerDatabase = options.databaseLayer ?? SqlitePersistence.layerMemory;
  const layerRegistry = (
    options.registryLayer ??
    ProviderAdapterRegistry.layerFromAdapters(options.adapters ?? [adapter])
  ).pipe(Layer.provide(McpProviderSessions.layer));
  const layerOrchestrator = ProviderReplayHarness.layerWithRegistry(
    { name: "thread-launch" },
    layerRegistry,
    {
      databaseLayer: layerDatabase,
      runEffectWorker: options.runEffectWorker ?? false,
      ...(options.configLayer === undefined ? {} : { configLayer: options.configLayer }),
      ...(options.git === undefined
        ? {}
        : { gitWorkflowLayer: Layer.mock(GitWorkflow.GitWorkflowService)(options.git) }),
    },
  );
  const layerServerSettings = ServerSettings.layerTest(options.serverSettings);
  const layerReceipts = CommandReceiptStore.layer.pipe(Layer.provide(layerDatabase));
  const layerThreadManagement = ThreadManagement.layer.pipe(
    Layer.provide(layerDatabase),
    Layer.provide(layerOrchestrator),
    Layer.provide(layerServerSettings),
    Layer.provide(layerReceipts),
  );
  const layerOutbox = EffectOutbox.layer.pipe(Layer.provide(layerDatabase));
  const createWorktree = vi.fn(
    options.createWorktree ??
      ((input) =>
        Effect.succeed({
          worktree: { path: "/repo-worktrees/feature", refName: input.newRefName, headSha: "abc" },
        } as never)),
  );
  const renameBranch = vi.fn(
    options.renameBranch ?? ((input) => Effect.succeed({ branch: input.newBranch })),
  );
  const removeWorktree = vi.fn(
    (_input: Parameters<GitWorkflow.GitWorkflowService["Service"]["removeWorktree"]>[0]) =>
      Effect.void,
  );
  const deleteLocalBranch = vi.fn(
    (_input: Parameters<GitWorkflow.GitWorkflowService["Service"]["deleteLocalBranch"]>[0]) =>
      Effect.void,
  );
  const runSetup = vi.fn(
    options.runSetup ?? (() => Effect.succeed({ status: "no-script" as const })),
  );
  const closeTerminal = vi.fn(options.closeTerminal ?? (() => Effect.void));
  const generateBranchName = vi.fn(
    options.generateBranchName ?? (() => Effect.succeed({ branch: "generated-branch" })),
  );
  const generateThreadTitle = vi.fn(
    options.generateTitle ?? (() => Effect.succeed({ title: "Generated title" })),
  );
  const layerExternalServices = Layer.mergeAll(
    Layer.mock(GitVcsDriver.GitVcsDriver)({}),
    WorktreeSetupTracker.layer,
    Layer.mock(ProjectCloneTracker.ProjectCloneTracker)({ get: () => Effect.succeed(null) }),
    Layer.mock(TerminalManager.TerminalManager)({ close: closeTerminal }),
    Layer.mock(CheckpointStore.CheckpointStore)({
      isGitRepository: options.isGitRepository ?? (() => Effect.succeed(true)),
    }),
    Layer.succeed(ProjectService.ProjectService, {
      create: () => Effect.die("unused"),
      bootstrap: () => Effect.die("unused"),
      update: () => Effect.die("unused"),
      delete: () => Effect.die("unused"),
      listPage: () => Effect.die("unused"),
      getById: (id) =>
        Effect.succeed(
          id === projectId
            ? Option.some(launchProject)
            : id === otherProjectId
              ? Option.some(otherLaunchProject)
              : Option.none(),
        ),
      getByWorkspaceRoot: () => Effect.succeedSome(project),
      workspaceScanDirectory: () => Effect.die("unused"),
      snapshot: Effect.die("unused"),
      getShell: () => Effect.die("unused"),
      listShells: () => Effect.die("unused"),
    }),
    Layer.mock(GitWorkflow.GitWorkflowService)({
      repositoryRoot: () => Effect.succeed(launchProject.workspaceRoot),
      createWorktree,
      renameBranch,
      fetchWorktreeBaseCommit:
        options.fetchWorktreeBaseCommit ??
        (() => Effect.succeed({ commitSha: "remote-main-sha", remoteRefName: "origin/main" })),
      hasCommit: options.hasCommit ?? (() => Effect.succeed(false)),
      currentBranch: () => Effect.succeed(null),
      resolveWorktreeBaseRemote: () => Effect.succeed({ remoteName: "origin", branchName: "main" }),
      resolvePrimaryRemoteName: () => Effect.succeed("origin"),
      resolveDefaultBranchName: () => Effect.succeed("main"),
      remoteExists: options.remoteExists ?? (() => Effect.succeed(true)),
      remoteBranchExists: () => Effect.succeed(true),
      listLocalBranchNames: () => Effect.succeed([]),
      removeWorktree,
      deleteLocalBranch,
      ...options.git,
    }),
    Layer.succeed(ProjectSetupScriptRunner.ProjectSetupScriptRunner, {
      runForThread: runSetup,
    }),
    Layer.mock(TextGeneration.TextGeneration)({
      generateThreadTitle,
      generateBranchName,
    }),
    layerServerSettings,
    ProviderRegistryMock.layer(options.providers),
    options.managedFolders ??
      Layer.mock(ManagedProjectFolders.ManagedProjectFolders)({
        namedProjectsRoot: "/projects",
        isScratchProject: () => Effect.succeed(false),
        folderForThread: () => Effect.succeedNone,
      }),
  );
  const layerLaunch = ThreadLaunch.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerExternalServices,
        layerThreadManagement,
        layerReceipts,
        layerOrchestrator,
        IdAllocator.layer,
        options.getLaunchSettings === undefined
          ? Layer.empty
          : Layer.mock(ServerSettings.ServerSettingsService)({
              getSettings: options.getLaunchSettings,
            }),
      ),
    ),
  );
  const layerProjectedProjects = Layer.mock(ProjectStore.ProjectStoreV2)({
    get: (requestedProjectId) =>
      Effect.succeed(
        requestedProjectId === projectId
          ? Option.some({
              projectId,
              title: launchProject.title,
              workspaceRoot: launchProject.workspaceRoot,
              defaultModelSelection: launchProject.defaultModelSelection,
              defaultThreadEnvMode: null,
              hasCustomTitle: false,
              autoPull: false,
              faviconPath: null,
              projectIcon: null,
              scripts: launchProject.scripts,
              pinnedAt: null,
              archivedAt: null,
              createdAt: launchProject.createdAt,
              updatedAt: launchProject.updatedAt,
              deletedAt: launchProject.deletedAt,
            })
          : Option.none(),
      ),
  });
  const layerTitleRegeneration = ThreadTitleRegeneration.layer.pipe(
    Layer.provide(
      Layer.mergeAll(
        layerThreadManagement,
        layerProjectedProjects,
        layerExternalServices,
        NodeServices.layer,
      ),
    ),
  );
  return {
    layer: Layer.mergeAll(
      layerLaunch,
      layerOrchestrator,
      layerRegistry,
      layerThreadManagement,
      layerTitleRegeneration,
      layerOutbox,
      layerDatabase,
      layerExternalServices,
    ),
    createWorktree,
    removeWorktree,
    deleteLocalBranch,
    renameBranch,
    generateBranchName,
    generateThreadTitle,
    runSetup,
    closeTerminal,
  };
}
