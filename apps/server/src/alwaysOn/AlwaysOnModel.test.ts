import { ProviderInstanceId, TextGenerationError, TextGeneratorError } from "@t3tools/contracts";
import { assert, it } from "@effect/vitest";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Ref from "effect/Ref";
import * as Stream from "effect/Stream";
import { NodeFileSystem } from "@effect/platform-node";

import type * as TextGenerationOperations from "@t3tools/provider-core/server/textGenerationOperations";

import * as ProviderInstanceRegistry from "../provider/ProviderInstanceRegistry.ts";
import * as TextGenerators from "../textGeneration/TextGenerators.ts";
import * as AlwaysOnModel from "./AlwaysOnModel.ts";

const registry = (instance: unknown) =>
  Layer.succeed(
    ProviderInstanceRegistry.ProviderInstanceRegistry,
    ProviderInstanceRegistry.ProviderInstanceRegistry.of({
      getInstance: () => Effect.sync(() => instance as never),
      listInstances: Effect.succeed([]),
      listUnavailable: Effect.succeed([]),
      streamChanges: Stream.empty,
      subscribeChanges: Effect.die("unused"),
    }),
  );

const noRunner: TextGenerationOperations.Runner | undefined = undefined;

const generators = (complete: TextGenerators.TextGenerators["Service"]["complete"]) =>
  Layer.succeed(
    TextGenerators.TextGenerators,
    TextGenerators.TextGenerators.of({
      runner: () => Effect.succeed(noRunner),
      complete,
      listModels: () => Effect.fail(new TextGeneratorError({ detail: "unused" })),
    }),
  );

const provide = (
  complete: TextGenerators.TextGenerators["Service"]["complete"],
  instance: unknown = undefined,
) =>
  AlwaysOnModel.layer.pipe(
    Layer.provide(generators(complete)),
    Layer.provide(registry(instance)),
    Layer.provide(NodeFileSystem.layer),
  );

it.effect(
  "asks a text generator with the prompt and does not treat the reply as already trusted",
  () =>
    Effect.gen(function* () {
      const prompts = yield* Ref.make<readonly string[]>([]);
      const raw = yield* Effect.gen(function* () {
        const model = yield* AlwaysOnModel.AlwaysOnModel;
        return yield* model.decide({
          providerKind: "textGenerator",
          instanceId: ProviderInstanceId.make("generator"),
          model: "fixture",
          prompt: "Instructions: Stay quiet.\nEvents:\n- external: hello",
        });
      }).pipe(
        Effect.provide(
          provide((input) =>
            Effect.gen(function* () {
              yield* Ref.update(prompts, (current) => [...current, input.prompt]);
              return '{"summary":"from generator","actions":[{"type":"shell","command":"rm"}]}';
            }),
          ),
        ),
      );
      assert.deepStrictEqual(raw, {
        summary: "from generator",
        actions: [{ type: "shell", command: "rm" }],
      });
      assert.deepStrictEqual(yield* Ref.get(prompts), [
        "Instructions: Stay quiet.\nEvents:\n- external: hello",
      ]);
    }),
);

it.effect("returns the text generator's configuration error and does not invent a decision", () =>
  Effect.gen(function* () {
    const error = yield* Effect.gen(function* () {
      const model = yield* AlwaysOnModel.AlwaysOnModel;
      return yield* model
        .decide({
          providerKind: "textGenerator",
          instanceId: ProviderInstanceId.make("missing"),
          model: "fixture",
          prompt: "Events:\n- external: hello",
        })
        .pipe(
          Effect.matchEffect({
            onFailure: (failure) => Effect.succeed(failure),
            onSuccess: () => Effect.die("expected the text generator to fail"),
          }),
        );
    }).pipe(
      Effect.provide(
        provide(() =>
          Effect.fail(
            new TextGenerationError({
              operation: "generateTranscriptionPostProcessing",
              detail: "That text generator does not exist.",
            }),
          ),
        ),
      ),
    );
    assert.strictEqual(error.detail, "That text generator does not exist.");
  }),
);

it.effect("refuses a Codex instance instead of starting its tools", () =>
  Effect.gen(function* () {
    const error = yield* Effect.gen(function* () {
      const model = yield* AlwaysOnModel.AlwaysOnModel;
      return yield* model
        .decide({
          providerKind: "claudeAgent",
          instanceId: ProviderInstanceId.make("codex"),
          model: "fixture",
          prompt: "Events:\n- external: hello",
        })
        .pipe(
          Effect.matchEffect({
            onFailure: (failure) => Effect.succeed(failure),
            onSuccess: () => Effect.die("expected Codex to be refused"),
          }),
        );
    }).pipe(
      Effect.provide(
        provide(() => Effect.die("text generators are not used"), { driverKind: "codex" }),
      ),
    );
    assert.strictEqual(
      error.detail,
      "That instance is not a Claude agent. Codex and other runtimes stay disabled because their tool lists are not empty.",
    );
  }),
);
