import { ProviderInstanceId, TextGenerationError } from "@t3tools/contracts";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Schema from "effect/Schema";

import * as ProviderInstanceRegistry from "../provider/ProviderInstanceRegistry.ts";
import * as TextGenerators from "../textGeneration/TextGenerators.ts";

export class AlwaysOnModelError extends Schema.TaggedError<AlwaysOnModelError>()(
  "AlwaysOnModelError",
  {
    detail: Schema.String,
  },
) {}

export interface AlwaysOnModelRequest {
  readonly providerKind: "textGenerator" | "claudeAgent";
  readonly instanceId: string;
  readonly model: string;
  readonly prompt: string;
}

/**
 * One structured completion. The result is untrusted until the always-on service decodes it.
 * Implementations must not grant the model tools.
 */
export class AlwaysOnModel extends Context.Service<
  AlwaysOnModel,
  {
    readonly decide: (input: AlwaysOnModelRequest) => Effect.Effect<unknown, AlwaysOnModelError>;
  }
>()("t3/alwaysOn/AlwaysOnModel") {}

const decodeUnknownJson = Schema.decodeUnknownSync(Schema.fromJsonString(Schema.Unknown));

const modelError = (detail: string) => new AlwaysOnModelError({ detail });

export const layer = Layer.effect(
  AlwaysOnModel,
  Effect.gen(function* () {
    const textGenerators = yield* TextGenerators.TextGenerators;
    const registry = yield* ProviderInstanceRegistry.ProviderInstanceRegistry;
    const fileSystem = yield* FileSystem.FileSystem;
    const decide = Effect.fn("AlwaysOnModel.decide")(function* (input: AlwaysOnModelRequest) {
      if (input.providerKind === "textGenerator") {
        const text = yield* textGenerators
          .complete({
            id: ProviderInstanceId.make(input.instanceId),
            model: input.model,
            prompt: input.prompt,
          })
          .pipe(
            Effect.mapError((error) =>
              modelError(
                error._tag === "TextGenerationError"
                  ? error.detail
                  : "The text generator could not answer.",
              ),
            ),
          );
        return yield* Effect.try({
          try: () => decodeUnknownJson(text),
          catch: () => modelError("The text generator did not return JSON."),
        });
      }
      const instance = yield* registry
        .getInstance(ProviderInstanceId.make(input.instanceId))
        .pipe(Effect.mapError(() => modelError("Could not read the Claude instance.")));
      if (instance === undefined || String(instance.driverKind) !== "claudeAgent") {
        return yield* modelError(
          "That instance is not a Claude agent. Codex and other runtimes stay disabled because their tool lists are not empty.",
        );
      }
      const cwd = yield* fileSystem
        .makeTempDirectory({ prefix: "t3-always-on-" })
        .pipe(
          Effect.mapError(() => modelError("Could not open a temporary directory for Claude.")),
        );
      const result = yield* instance.textGeneration
        .generateTranscriptionPostProcessing({
          cwd,
          // The CLI schema only allows a string. The service decodes the JSON inside it.
          prompt: `${input.prompt}\nPut the decision JSON object in the transcription string and nowhere else.`,
          modelSelection: {
            instanceId: ProviderInstanceId.make(input.instanceId),
            model: input.model,
          },
        })
        .pipe(
          Effect.mapError((error: TextGenerationError) => modelError(error.detail)),
          Effect.ensuring(fileSystem.remove(cwd, { recursive: true }).pipe(Effect.ignore)),
        );
      return yield* Effect.try({
        try: () => decodeUnknownJson(result.transcription),
        catch: () => modelError("Claude did not return JSON."),
      });
    });
    return AlwaysOnModel.of({ decide });
  }),
);
