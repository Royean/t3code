import type { ExactReleaseRefs } from "@t3tools/source-control-core/server/driver";
import { WorkbenchMergeRemoteReason, type WorkbenchMergePlan } from "@t3tools/contracts";
import { parseGitRemoteRepository } from "@t3tools/shared/git";
import * as Context from "effect/Context";
import * as Effect from "effect/Effect";
import * as Layer from "effect/Layer";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";
import * as Drivers from "../sourceControl/builtInDrivers.ts";

export class ReleaseRefsError extends Schema.TaggedError<ReleaseRefsError>()(
  "ExactReleaseRefsError",
  {
    reason: WorkbenchMergeRemoteReason,
    stage: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {
  override get message() {
    return `Release ref operation failed (${this.stage}).`;
  }
}
export class WorkbenchReleaseRefs extends Context.Service<
  WorkbenchReleaseRefs,
  ExactReleaseRefs & {
    readonly matchesSource: (remoteUrl: string, plan: WorkbenchMergePlan) => boolean;
  }
>()("t3/scheduledTasks/WorkbenchReleaseRefs") {}
const make = Effect.gen(function* () {
  const drivers = yield* Effect.serviceOption(Drivers.SourceControlDrivers);
  const selected = Option.isSome(drivers)
    ? drivers.value.find((entry) => entry.driver.kind === "github")?.instance.exactReleaseRefs
    : undefined;
  return WorkbenchReleaseRefs.of({
    matchesSource: (remote, plan) => {
      const parsed = parseGitRemoteRepository(remote);
      if (!parsed) return false;
      let host = parsed.host;
      if (/^https?:\/\//i.test(remote)) {
        try {
          host = new URL(remote).host.toLowerCase();
        } catch {
          return false;
        }
      }
      return (
        host === plan.host.toLowerCase() &&
        parsed.path.toLowerCase() === plan.repository.toLowerCase()
      );
    },
    inspect: Effect.fn("WorkbenchReleaseRefs.inspect")((input) =>
      selected
        ? selected.inspect(input)
        : Effect.fail(new ReleaseRefsError({ reason: "unsupported", stage: "capability" })),
    ),
    exactFF: Effect.fn("WorkbenchReleaseRefs.exactFF")((input) =>
      selected
        ? selected.exactFF(input)
        : Effect.fail(new ReleaseRefsError({ reason: "unsupported", stage: "capability" })),
    ),
  });
});
export const layer = Layer.effect(WorkbenchReleaseRefs, make);
