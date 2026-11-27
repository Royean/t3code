import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  EnvironmentHttpApi,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import { requireEnvironmentScope } from "../auth/http.ts";
import * as Deployment from "./WorkbenchDeployment.ts";

export const layer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "workbenchDeployment",
  Effect.fnUntraced(function* (handlers) {
    const service = yield* Deployment.WorkbenchDeployment;
    const authorize = requireEnvironmentScope(AuthOrchestrationReadScope).pipe(
      Effect.andThen(requireEnvironmentScope(AuthOrchestrationOperateScope)),
    );
    return handlers
      .handle(
        "admit",
        Effect.fnUntraced(function* ({ params, payload }) {
          yield* authorize;
          return yield* service.admit(params.queueId, payload);
        }),
      )
      .handle(
        "get",
        Effect.fnUntraced(function* ({ params, query }) {
          yield* authorize;
          return yield* service.get(params.queueId, params.deploymentId, query);
        }),
      );
  }),
);
