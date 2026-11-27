import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  EnvironmentHttpApi,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import { requireEnvironmentScope } from "../auth/http.ts";
import * as Progress from "./WorkbenchTaskProgress.ts";

export const layer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "workbenchTaskProgress",
  Effect.fnUntraced(function* (handlers) {
    const progress = yield* Progress.WorkbenchTaskProgress;
    return handlers.handle(
      "get",
      Effect.fnUntraced(function* ({ params, query }) {
        yield* requireEnvironmentScope(AuthOrchestrationReadScope);
        yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
        return yield* progress.get(params.queueId, params.workItemId, query);
      }),
    );
  }),
);
