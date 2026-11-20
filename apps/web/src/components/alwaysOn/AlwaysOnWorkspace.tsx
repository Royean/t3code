import {
  AuthOrchestrationOperateScope,
  type AlwaysOnAgentId,
  type EnvironmentId,
} from "@t3tools/contracts";
import { Link } from "@tanstack/react-router";
import { useGT } from "gt-react";
import { InfinityIcon } from "lucide-react";

import { isElectron } from "../../env";
import { workspaceAreaProps } from "../../lib/workspaceFocus";
import { useEnvironmentQuery } from "../../state/query";
import { serverEnvironment } from "../../state/server";
import { useEnvironmentScope } from "../../state/session";
import { useAtomCommand } from "../../state/use-atom-command";
import { WorkspacePageHeader } from "../WorkspacePageHeader";
import { Button } from "../ui/button";
import { Empty, EmptyDescription, EmptyHeader, EmptyMedia, EmptyTitle } from "../ui/empty";
import { SidebarInset } from "../ui/sidebar";
import { AlwaysOnComposer } from "./AlwaysOnComposer";
import { AlwaysOnTimeline } from "./AlwaysOnTimeline";
import { useAlwaysOnAction } from "./useAlwaysOnAction";
import { useAlwaysOnEnvironments } from "./useAlwaysOnEnvironments";

export function AlwaysOnWorkspace({
  environmentId,
  agentId,
}: {
  readonly environmentId: EnvironmentId | undefined;
  readonly agentId: AlwaysOnAgentId | undefined;
}) {
  const gt = useGT();
  const { entries, isReady } = useAlwaysOnEnvironments();
  const environment = entries.find((entry) => entry.environmentId === environmentId);
  const contentArea = workspaceAreaProps("home", 1, gt("Always On"));
  const pending = !isReady || entries.some((entry) => entry.status === "loading");
  const hasAgents = entries.some((entry) => entry.agents.length > 0);
  const hasEvents = entries.some((entry) => entry.eventThreads.length > 0);
  const creator = entries.find((entry) => entry.canOperate);
  const ready = entries.some((entry) => entry.status === "ready");

  return (
    <SidebarInset {...contentArea} role="main" className="h-dvh min-h-0 overflow-hidden">
      {environmentId !== undefined && agentId !== undefined ? (
        <AlwaysOnConversation
          key={JSON.stringify([environmentId, agentId])}
          environmentId={environmentId}
          agentId={agentId}
          environmentLabel={environment?.label}
          status={isReady ? (environment?.status ?? "disconnected") : "loading"}
        />
      ) : (
        <>
          <WorkspacePageHeader electron={isElectron}>
            <h1 className="text-sm font-medium">{gt("Always On")}</h1>
          </WorkspacePageHeader>
          <Empty className="flex-1">
            <EmptyHeader>
              <EmptyMedia variant="icon">
                <InfinityIcon />
              </EmptyMedia>
              <EmptyTitle>
                {hasEvents || hasAgents
                  ? gt("Select a conversation")
                  : pending
                    ? gt("Loading agents…")
                    : ready
                      ? gt("No Always On activity")
                      : gt("No available environments")}
              </EmptyTitle>
              {ready && !pending && !hasEvents && !hasAgents ? (
                <EmptyDescription>
                  {creator ? gt("Set defaults for incoming events in Settings.") : null}
                </EmptyDescription>
              ) : null}
            </EmptyHeader>
            {!hasEvents && !hasAgents && creator && !pending ? (
              <Button
                render={
                  <Link to="/settings/always-on" search={{ machine: creator.environmentId }} />
                }
              >
                {gt("Open Always On settings")}
              </Button>
            ) : null}
            {!pending && entries.length === 0 ? (
              <Button render={<Link to="/settings/connections" />}>
                {gt("Connect an environment")}
              </Button>
            ) : null}
          </Empty>
        </>
      )}
    </SidebarInset>
  );
}

function AlwaysOnConversation({
  status,
  environmentLabel,
  environmentId,
  agentId,
}: {
  readonly status: ReturnType<typeof useAlwaysOnEnvironments>["entries"][number]["status"];
  readonly environmentLabel: string | undefined;
  readonly environmentId: EnvironmentId;
  readonly agentId: AlwaysOnAgentId;
}) {
  const gt = useGT();
  const ready = status === "ready";
  const canOperate = useEnvironmentScope(environmentId, AuthOrchestrationOperateScope);
  const query = useEnvironmentQuery(
    ready ? serverEnvironment.alwaysOnDetail({ environmentId, input: { agentId } }) : null,
  );
  const setEnabled = useAtomCommand(serverEnvironment.setAlwaysOnEnabled);
  const confirm = useAtomCommand(serverEnvironment.confirmAlwaysOnUnknown);
  const toggle = useAlwaysOnAction();
  const confirmation = useAlwaysOnAction();
  const detail = query.data;
  const address = { environmentId, input: { agentId } };
  const unavailable =
    status === "disconnected"
      ? gt("Environment disconnected")
      : status === "unsupported"
        ? gt("This server does not support Always On.")
        : status === "forbidden"
          ? gt("Read access required")
          : status === "error"
            ? gt("Could not load Always On agents.")
            : null;

  return (
    <>
      <WorkspacePageHeader electron={isElectron}>
        <div className="min-w-0 flex-1">
          <h1 className="truncate text-sm font-medium">{detail?.agent.title ?? gt("Always On")}</h1>
          {environmentLabel ? (
            <p className="truncate text-xs text-muted-foreground">{environmentLabel}</p>
          ) : null}
        </div>
        {detail && canOperate ? (
          <Button
            size="sm"
            variant="outline"
            disabled={!ready || toggle.pending}
            onClick={() =>
              void toggle.act(() =>
                setEnabled({ ...address, input: { agentId, enabled: !detail.agent.enabled } }),
              )
            }
          >
            {detail.agent.enabled ? gt("Pause") : gt("Resume")}
          </Button>
        ) : null}
      </WorkspacePageHeader>
      {toggle.error ? (
        <p role="alert" className="px-4 py-2 text-sm text-destructive">
          {toggle.error}
        </p>
      ) : null}
      {detail?.agent.phase === "needs_confirmation" ? (
        <div
          className="flex shrink-0 flex-wrap items-center justify-between gap-2 border-y border-warning/30 bg-warning-surface px-4 py-2 text-sm text-warning-foreground"
          role="status"
        >
          <span>{gt("A run needs confirmation.")}</span>
          {canOperate ? (
            <Button
              size="sm"
              variant="warning-outline"
              disabled={!ready || confirmation.pending}
              onClick={() => void confirmation.act(() => confirm(address))}
            >
              {gt("Confirm unknown effect")}
            </Button>
          ) : null}
          {confirmation.error ? (
            <p role="alert" className="w-full text-sm text-destructive">
              {confirmation.error}
            </p>
          ) : null}
        </div>
      ) : null}
      {unavailable ? (
        <Empty className="flex-1">
          <EmptyHeader>
            <EmptyTitle>{unavailable}</EmptyTitle>
          </EmptyHeader>
        </Empty>
      ) : query.error !== null ? (
        <div className="flex flex-1 flex-col items-center justify-center gap-3 px-4">
          <p role="alert" className="text-sm text-destructive">
            {query.error}
          </p>
          <Button size="sm" variant="outline" onClick={query.refresh}>
            {gt("Try again")}
          </Button>
        </div>
      ) : detail === null ? (
        <Empty className="flex-1">
          <EmptyHeader>
            <EmptyTitle>{gt("Loading agent…")}</EmptyTitle>
          </EmptyHeader>
        </Empty>
      ) : (
        <AlwaysOnTimeline address={address} detail={detail} canOperate={canOperate} />
      )}
      <AlwaysOnComposer
        address={address}
        disabled={!canOperate || !ready || query.error !== null || detail === null}
        readOnly={ready && detail !== null && !canOperate}
        agent={detail?.agent}
      />
    </>
  );
}
