import {
  type AlwaysOnRoutingError,
  AuthOrchestrationOperateScope,
  EnvironmentAlwaysOnUnavailableError,
  EnvironmentHttpApi,
  EnvironmentHttpBadRequestError,
  EnvironmentHttpConflictError,
  EnvironmentHttpForbiddenError,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as HttpApiBuilder from "effect/http-api/HttpApiBuilder";

import { requireEnvironmentScope } from "../auth/http.ts";
import { AlwaysOnRouting } from "./AlwaysOnRouting.ts";

const asHttpError = ({ reason, message }: AlwaysOnRoutingError) => {
  switch (reason) {
    case "configuration":
      return new EnvironmentHttpBadRequestError({ message });
    case "forbidden":
      return new EnvironmentHttpForbiddenError({ message });
    case "conflict":
      return new EnvironmentHttpConflictError({ message });
    case "unavailable":
      return new EnvironmentAlwaysOnUnavailableError({ message });
  }
};

export const layer = HttpApiBuilder.group(
  EnvironmentHttpApi,
  "alwaysOn",
  Effect.fnUntraced(function* (handlers) {
    const routing = yield* AlwaysOnRouting;
    return handlers.handle(
      "submitEvent",
      Effect.fn("environment.alwaysOn.submitEvent")(function* ({ payload }) {
        yield* requireEnvironmentScope(AuthOrchestrationOperateScope);
        return yield* routing.submit(payload).pipe(Effect.mapError(asHttpError));
      }),
    );
  }),
);
