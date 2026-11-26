import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Guard from "./WorkbenchAttentionGuard.ts";

/** Unit fixtures without workbench items; persistence authorization is tested with the real guard. */
export const layer = Layer.mock(Guard.WorkbenchAttentionGuard)({
  assertAdmission: () => Effect.void,
  assertCommand: () => Effect.void,
  originalResume: () => Effect.undefined,
  withAdmission: (_input, effect) => effect,
  awaitAdmissions: () => Effect.void,
});
