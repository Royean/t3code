import { expect, it } from "@effect/vitest";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import {
  ALWAYS_ON_EVENT_THREAD_PREFIX,
  AlwaysOnPeerEvent,
  type AlwaysOnEventSubmission,
  EnvironmentId,
  IsoDateTime,
  OrchestratorMcpFailure,
  ProjectId,
  ProviderDriverKind,
  ProviderInstanceId,
  type ServerProvider,
  ThreadId,
} from "@t3tools/contracts";
import * as Digest from "effect/crypto/Digest";
import * as Effect from "effect/Effect";
import * as Hex from "effect/encoding/Hex";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Schema from "effect/Schema";

import { ServerEnvironmentIdentity } from "../environment/ServerEnvironment.ts";
import * as ThreadLaunch from "../orchestration-v2/ThreadLaunchService.ts";
import * as PeerMcp from "../peer/PeerMcpClient.ts";
import * as SqlitePersistence from "../persistence/Sqlite.ts";
import * as Project from "../project/ProjectService.ts";
import * as Providers from "../provider/ProviderRegistry.ts";
import * as ServerSettings from "../serverSettings.ts";
import * as Routing from "./AlwaysOnRouting.ts";

const local = EnvironmentId.make("environment:mac");
const peer = EnvironmentId.make("environment:remote");
const projectId = ProjectId.make("workspace");
const codex = ProviderInstanceId.make("work-codex");
const claude = ProviderInstanceId.make("work-claude");
const now = IsoDateTime.make("2026-11-20T00:00:00.000Z");
const event = {
  source: "builds",
  sourceEventId: "123",
  text: "Summarize the result of the fixture build.",
} satisfies AlwaysOnEventSubmission;
const decodePeerEvent = Schema.decodeUnknownEffect(AlwaysOnPeerEvent);
const encodeKey = Schema.encodeEffect(Schema.fromJsonString(Schema.Array(Schema.String)));

const catalog = (instanceId: ProviderInstanceId, driver: string, id: string): ServerProvider => ({
  instanceId,
  driver: ProviderDriverKind.make(driver),
  enabled: true,
  installed: true,
  version: null,
  status: "ready",
  auth: { status: "authenticated" },
  checkedAt: now,
  slashCommands: [],
  skills: [],
  models: [
    {
      slug: "test-model",
      name: "Test",
      isCustom: false,
      isDefault: true,
      capabilities: {
        optionDescriptors: [
          {
            id,
            type: "select",
            label: "Reasoning",
            options: [
              { id: "medium", label: "Medium" },
              { id: "high", label: "High" },
            ],
          },
        ],
      },
    },
  ],
});

const withRouter = <A, E, R>(
  use: (
    router: Routing.AlwaysOnRouting["Service"],
    settings: ServerSettings.ServerSettingsService["Service"],
    calls: Ref.Ref<readonly ThreadLaunch.ThreadLaunchInput[]>,
    forwards: Ref.Ref<readonly unknown[]>,
  ) => Effect.Effect<A, E, R>,
) =>
  Effect.gen(function* () {
    const crypto = yield* Digest.Digest;
    const calls = yield* Ref.make<readonly ThreadLaunch.ThreadLaunchInput[]>([]);
    const forwards = yield* Ref.make<readonly unknown[]>([]);
    const launch: ThreadLaunch.ThreadLaunchService["Service"]["launch"] = (input) =>
      Ref.update(calls, (existing) => [...existing, input]).pipe(
        Effect.as({
          threadId: input.threadId!,
          projection: {} as ThreadLaunch.ThreadLaunchResult["projection"],
          resumed: false,
        }),
      );
    const providers = Layer.mock(Providers.ProviderRegistry)({
      getProviders: Effect.succeed([
        catalog(codex, "codex", "reasoningEffort"),
        catalog(claude, "claudeAgent", "effort"),
      ]),
    });
    const projects = Layer.mock(Project.ProjectService)({
      getById: () =>
        Effect.succeedSome({
          id: projectId,
          title: "Workspace",
          workspaceRoot: "/workspace",
          defaultModelSelection: null,
          scripts: [],
          createdAt: now,
          updatedAt: now,
          deletedAt: null,
        }),
    });
    const launchLayer = Layer.mock(ThreadLaunch.ThreadLaunchService)({ launch });
    const environment = Layer.succeed(ServerEnvironmentIdentity, {
      getEnvironmentId: Effect.succeed(local),
    });
    const peers = Layer.mock(PeerMcp.PeerMcpClient)({
      call: (input) => {
        const decodeReceipt = Schema.decodeEffect(input.success);
        return Effect.gen(function* () {
          yield* Ref.update(forwards, (existing) => [...existing, input]);
          const event = yield* decodePeerEvent(input.arguments).pipe(Effect.orDie);
          if (event.effort === "invalid") {
            return yield* new OrchestratorMcpFailure({
              code: "invalid_request",
              message: "Unsupported effort.",
            });
          }
          if (event.effort === "offline") {
            return yield* new OrchestratorMcpFailure({
              code: "orchestration_error",
              message: "The peer did not answer.",
            });
          }
          const key = yield* encodeKey([
            "delivery",
            event.environmentId,
            event.originEnvironmentId,
            event.deliveryId,
          ]).pipe(Effect.orDie);
          const hash = yield* crypto
            .digest(Digest.sha256, new TextEncoder().encode(key))
            .pipe(Effect.orDie);
          return yield* decodeReceipt({
            environmentId: input.environmentId,
            threadId: ThreadId.make(ALWAYS_ON_EVENT_THREAD_PREFIX + Hex.encode(hash)),
            duplicate: false,
          }).pipe(Effect.orDie);
        });
      },
    });
    return yield* Effect.gen(function* () {
      const router = yield* Routing.AlwaysOnRouting;
      const settings = yield* ServerSettings.ServerSettingsService;
      return yield* use(router, settings, calls, forwards);
    }).pipe(
      Effect.provide(
        Routing.layer.pipe(
          Layer.provideMerge(
            ServerSettings.ServerSettingsService.layerTest({
              alwaysOnRouting: { projectId, harness: codex, effort: "medium" },
            }),
          ),
          Layer.provide(
            Layer.mergeAll(
              SqlitePersistence.layerMemory,
              environment,
              providers,
              projects,
              launchLayer,
              peers,
            ),
          ),
        ),
      ),
    );
  }).pipe(Effect.provide(NodeCrypto.layer));

it.effect(
  "runs without an agent ID using target defaults and explicit harness/effort overrides",
  () =>
    withRouter((router, _settings, calls) =>
      Effect.gen(function* () {
        const first = yield* router.submit(event);
        const second = yield* router.submit({
          ...event,
          sourceEventId: "124",
          harness: claude,
          effort: "high",
        });
        expect(first).toMatchObject({ environmentId: local, duplicate: false });
        expect(second.threadId).not.toBe(first.threadId);
        expect(yield* Ref.get(calls)).toMatchObject([
          {
            projectId,
            runtimeMode: "approval-required",
            modelSelection: {
              instanceId: codex,
              model: "test-model",
              options: [{ id: "reasoningEffort", value: "medium" }],
            },
          },
          {
            threadId: second.threadId,
            runtimeMode: "approval-required",
            modelSelection: {
              instanceId: claude,
              model: "test-model",
              options: [{ id: "effort", value: "high" }],
            },
          },
        ]);
        expect(
          yield* router
            .submit({ ...event, sourceEventId: "unsupported", effort: "max" })
            .pipe(Effect.flip),
        ).toMatchObject({ reason: "configuration" });
        expect(yield* Ref.get(calls)).toHaveLength(2);
      }),
    ),
);

it.effect("keeps the original receipt after configuration changes without launching twice", () =>
  withRouter((router, settings, calls) =>
    Effect.gen(function* () {
      const [first, retry] = yield* Effect.all([router.submit(event), router.submit(event)], {
        concurrency: "unbounded",
      });
      expect(first.threadId).toBe(retry.threadId);
      expect([first.duplicate, retry.duplicate].sort()).toEqual([false, true]);
      yield* settings.updateSettings({ alwaysOnRouting: { environmentId: peer } });
      expect(yield* router.submit(event)).toEqual({ ...first, duplicate: true });
      expect(
        yield* router.submit({ ...event, text: "A different event" }).pipe(Effect.flip),
      ).toMatchObject({ reason: "conflict" });
      expect(yield* Ref.get(calls)).toHaveLength(1);
    }),
  ),
);

it.effect(
  "uses the standard model selection saved by Settings and accepts a complete event override",
  () =>
    withRouter((router, settings, calls) =>
      Effect.gen(function* () {
        const savedModel = {
          instanceId: claude,
          model: "test-model",
          options: [
            { id: "effort", value: "high" },
            { id: "fastMode", value: true },
          ],
        };
        yield* settings.updateSettings({
          alwaysOnRouting: { projectId, modelSelection: savedModel },
        });
        yield* router.submit(event);
        const eventModel = {
          instanceId: codex,
          model: "test-model",
          options: [{ id: "reasoningEffort", value: "medium" }],
        };
        yield* router.submit({
          ...event,
          sourceEventId: "model-override",
          modelSelection: eventModel,
        });
        expect((yield* Ref.get(calls)).map((input) => input.modelSelection)).toEqual([
          savedModel,
          eventModel,
        ]);
      }),
    ),
);

it.effect(
  "accepts a peer event only at the addressed environment and stamps the link on the thread",
  () =>
    withRouter((router, _settings, calls) =>
      Effect.gen(function* () {
        const input = {
          ...event,
          environmentId: local,
          originEnvironmentId: peer,
          deliveryId: "fixture-delivery",
          harness: claude,
        };
        const link = {
          sessionId: "approved-peer",
          label: "Remote Mac",
          access: "approval-required" as const,
        };
        expect(
          yield* router.receive({ ...input, environmentId: peer }, link).pipe(Effect.flip),
        ).toMatchObject({ reason: "forbidden" });
        expect(
          yield* router.receive(input, { ...link, access: "read-only" }).pipe(Effect.flip),
        ).toMatchObject({ reason: "forbidden" });
        const result = yield* router.receive(input, link);
        expect(result.threadId).toMatch(/^always-on-event-/);
        expect(
          yield* router.receive(input, { ...link, sessionId: "different-peer" }).pipe(Effect.flip),
        ).toMatchObject({ reason: "forbidden" });
        expect(yield* Ref.get(calls)).toMatchObject([
          { threadId: result.threadId, linkOrigin: link },
        ]);
      }),
    ),
);

it.effect(
  "requires a corrected route after rejection and pauses unresolved deliveries when disabled",
  () =>
    withRouter((router, settings, _calls, forwards) =>
      Effect.gen(function* () {
        yield* settings.updateSettings({ alwaysOnRouting: { environmentId: peer } });
        const failure = yield* router.submit({ ...event, effort: "invalid" }).pipe(Effect.flip);
        expect(failure.reason).toBe("configuration");
        expect(yield* router.submit({ ...event, effort: "high" })).toMatchObject({
          environmentId: peer,
          duplicate: false,
        });
        const pending = { ...event, sourceEventId: "offline", effort: "offline" };
        expect(yield* router.submit(pending).pipe(Effect.flip)).toMatchObject({
          reason: "unavailable",
        });
        yield* settings.updateSettings({ alwaysOnRouting: null });
        expect(yield* router.submit(pending).pipe(Effect.flip)).toMatchObject({
          reason: "configuration",
        });
        expect(yield* Ref.get(forwards)).toHaveLength(3);
      }),
    ),
);

it.effect("forwards the event to the selected peer and keeps that destination on retries", () =>
  withRouter((router, settings, calls, forwards) =>
    Effect.gen(function* () {
      yield* settings.updateSettings({
        alwaysOnRouting: { environmentId: peer, projectId, harness: codex },
      });
      const explicitLocal = yield* router.submit({
        ...event,
        sourceEventId: "local",
        environmentId: local,
      });
      const modelSelection = {
        instanceId: claude,
        model: "test-model",
        options: [{ id: "effort", value: "high" }],
      };
      const forwarded = yield* router.submit({ ...event, modelSelection });
      expect(explicitLocal.environmentId).toBe(local);
      expect(forwarded).toMatchObject({ environmentId: peer, duplicate: false });
      yield* settings.updateSettings({ alwaysOnRouting: { projectId, harness: codex } });
      expect(yield* router.submit(event)).toEqual({ ...forwarded, duplicate: true });
      expect(yield* Ref.get(calls)).toHaveLength(1);
      expect(yield* Ref.get(forwards)).toMatchObject([
        {
          environmentId: peer,
          arguments: {
            environmentId: peer,
            originEnvironmentId: local,
            modelSelection,
          },
          limits: { runtimeMode: "approval-required", interactionMode: "default" },
        },
      ]);
    }),
  ),
);
