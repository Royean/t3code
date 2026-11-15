import * as Context from "effect/Context";

/** Opt-in delegation policy, checked against the current parent under the command lock. */
export const InheritParentModes = Context.Reference<boolean>(
  "t3/orchestration-v2/InheritParentModes",
  { defaultValue: () => false },
);
