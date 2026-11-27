import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  EnvironmentHttpApi,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";
import { requireEnvironmentScope } from "../auth/http.ts";
import * as Release from "./WorkbenchReleaseEvidence.ts";
export const layer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "workbenchRelease",
  Effect.fnUntraced(function* (handlers) {
    const service = yield* Release.WorkbenchReleaseEvidence;
    const authorize = requireEnvironmentScope(AuthOrchestrationReadScope).pipe(
      Effect.andThen(requireEnvironmentScope(AuthOrchestrationOperateScope)),
    );
    return handlers
      .handle(
        "catalog",
        Effect.fnUntraced(function* ({ query }) {
          yield* authorize;
          return yield* service.catalog(query);
        }),
      )
      .handle(
        "prepare",
        Effect.fnUntraced(function* ({ params, payload }) {
          yield* authorize;
          return yield* service.prepare(params.queueId, payload);
        }),
      )
      .handle(
        "get",
        Effect.fnUntraced(function* ({ params, query }) {
          yield* authorize;
          return yield* service.get(params.queueId, params.preparationId, query);
        }),
      )
      .handle(
        "revalidate",
        Effect.fnUntraced(function* ({ params, payload }) {
          yield* authorize;
          return yield* service.revalidate(params.queueId, params.preparationId, payload);
        }),
      )
      .handle(
        "diff",
        Effect.fnUntraced(function* ({ params, query }) {
          yield* authorize;
          return yield* service.diff(params.queueId, params.preparationId, query);
        }),
      );
  }),
);
