import { AlwaysOnAgentId, EnvironmentId } from "@t3tools/contracts";
import { createFileRoute } from "@tanstack/react-router";
import * as Option from "effect/Option";
import * as Schema from "effect/Schema";

import { AlwaysOnWorkspace } from "../components/alwaysOn/AlwaysOnWorkspace";

const readEnvironment = Schema.decodeUnknownOption(EnvironmentId);
const readAgent = Schema.decodeUnknownOption(AlwaysOnAgentId);

export const Route = createFileRoute("/_chat/always-on")({
  validateSearch: (search: Record<string, unknown>) => {
    const environment = readEnvironment(search.environment);
    const agent = readAgent(search.agent);
    return Option.isSome(environment) && Option.isSome(agent)
      ? { environment: environment.value, agent: agent.value }
      : { environment: undefined, agent: undefined };
  },
  component: AlwaysOnPage,
});

function AlwaysOnPage() {
  const { environment, agent } = Route.useSearch();
  return <AlwaysOnWorkspace environmentId={environment} agentId={agent} />;
}
