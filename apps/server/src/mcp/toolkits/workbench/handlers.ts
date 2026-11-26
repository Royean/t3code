import * as Effect from "effect/Effect";
import * as McpInvocationContext from "../../McpInvocationContext.ts";
import * as McpToolAccess from "../../McpToolAccess.ts";
import { WorkbenchToolkit } from "./tools.ts";
import * as WorkbenchDispatch from "../../../scheduledTasks/WorkbenchDispatch.ts";

export const layer = McpToolAccess.toLayer(WorkbenchToolkit, {
  t3_workbench_dispatch: McpToolAccess.actsAsCaller((input) =>
    Effect.gen(function* () {
      const scope = yield* McpInvocationContext.McpInvocationContext;
      const service = yield* WorkbenchDispatch.WorkbenchDispatch;
      return yield* service.dispatch(scope, input);
    }),
  ),
});
