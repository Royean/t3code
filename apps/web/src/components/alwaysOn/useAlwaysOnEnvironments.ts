import { useAtomValue } from "@effect/atom-react";
import { alwaysOnAttentionCount } from "@t3tools/client-runtime/always-on-conversation";
import {
  AuthOrchestrationOperateScope,
  AuthOrchestrationReadScope,
  type AlwaysOnAgentSummary,
} from "@t3tools/contracts";
import * as Cause from "effect/Cause";
import { Atom } from "effect/reactivity";

import { environmentAtomList, environmentIdsKey } from "../../state/environmentAtomList";
import { useEnvironments } from "../../state/environments";
import { serverEnvironment } from "../../state/server";
import { environmentSession, sessionHasScope } from "../../state/session";

const EMPTY_AGENTS: readonly AlwaysOnAgentSummary[] = [];

const environmentQueries = environmentAtomList("web-always-on-environments", (environmentId) =>
  Atom.make((get) => {
    const session = get(environmentSession.sessionStateAtom(environmentId));
    if (session._tag === "Initial") return { status: "loading" as const };
    if (session._tag === "Failure") return { status: "error" as const };
    if (!sessionHasScope(session, AuthOrchestrationReadScope))
      return { status: "forbidden" as const };

    const snapshot = get(serverEnvironment.alwaysOnLive({ environmentId, input: {} }));
    switch (snapshot._tag) {
      case "Initial":
        return { status: "loading" as const };
      case "Failure":
        return {
          status: Cause.pretty(snapshot.cause).includes("Unknown request tag: alwaysOn.subscribe")
            ? ("unsupported" as const)
            : ("error" as const),
        };
      case "Success":
        return {
          status: "ready" as const,
          agents: snapshot.value.agents,
          canOperate: sessionHasScope(session, AuthOrchestrationOperateScope),
        };
    }
  }),
);

export function useAlwaysOnEnvironments() {
  const { environments, isReady } = useEnvironments();
  const connected = environments.filter(({ connection }) => connection.phase === "connected");
  const queries = useAtomValue(
    environmentQueries(environmentIdsKey(connected.map(({ environmentId }) => environmentId))),
  );
  const byEnvironment = new Map(
    connected.map(({ environmentId }, index) => [environmentId, queries[index]] as const),
  );
  const entries = environments.map(({ environmentId, label, connection }) => {
    const query = byEnvironment.get(environmentId);
    return {
      environmentId,
      label,
      status:
        query?.status ??
        (connection.phase === "connecting" || connection.phase === "reconnecting"
          ? ("loading" as const)
          : connection.phase === "unsupported" || connection.phase === "error"
            ? connection.phase
            : ("disconnected" as const)),
      agents: query?.status === "ready" ? query.agents : EMPTY_AGENTS,
      canOperate: query?.status === "ready" && query.canOperate,
    };
  });

  const attention = entries.reduce(
    (sum, entry) =>
      entry.canOperate
        ? entry.agents.reduce((total, agent) => total + alwaysOnAttentionCount(agent), sum)
        : sum,
    0,
  );
  return { entries, attention, isReady };
}
