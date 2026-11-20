import { alwaysOnAttentionCount } from "@t3tools/client-runtime/always-on-conversation";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/models";
import type { AlwaysOnAgentSummary } from "@t3tools/contracts";
import { Link, useParams, useSearch } from "@tanstack/react-router";
import { useGT } from "gt-react";

import { isElectron } from "../../env";
import { SidebarChromeFooter, SidebarChromeHeader } from "../sidebar/SidebarChrome";
import { type ChatWorkspace, useNavigateToChatWorkspace } from "../sidebar/mainAppLocation";
import { Badge } from "../ui/badge";
import { Button } from "../ui/button";
import {
  SidebarContent,
  SidebarGroup,
  SidebarMenu,
  SidebarMenuButton,
  SidebarMenuItem,
  useSidebar,
} from "../ui/sidebar";
import { useAlwaysOnEnvironments } from "./useAlwaysOnEnvironments";

export function ChatWorkspaceNavigation({ selected }: { readonly selected: ChatWorkspace }) {
  const { attention } = useAlwaysOnEnvironments();
  return <WorkspaceNavigation selected={selected} attention={attention} />;
}

function WorkspaceNavigation({
  selected,
  attention,
}: {
  readonly selected: ChatWorkspace;
  readonly attention: number;
}) {
  const gt = useGT();
  const navigate = useNavigateToChatWorkspace();
  return (
    <nav
      aria-label={gt("Chat views")}
      className="flex shrink-0 items-center gap-1 p-(--sidebar-content-inset)"
    >
      <Button
        aria-current={selected === "threads" ? "page" : undefined}
        size="sm"
        variant={selected === "threads" ? "secondary" : "ghost"}
        onClick={() => selected !== "threads" && void navigate("threads")}
      >
        {gt("Threads")}
      </Button>
      <Button
        aria-current={selected === "always-on" ? "page" : undefined}
        size="sm"
        variant={selected === "always-on" ? "secondary" : "ghost"}
        onClick={() => selected !== "always-on" && void navigate("always-on")}
      >
        {gt("Always On")}
        {attention > 0 ? (
          <Badge
            size="sm"
            variant="warning"
            aria-label={gt("{count} need attention", { count: attention })}
          >
            {attention}
          </Badge>
        ) : null}
      </Button>
    </nav>
  );
}

export function AlwaysOnSidebar() {
  const gt = useGT();
  const agent = useSearch({ strict: false, select: (search) => search.agent });
  const environment = useSearch({ strict: false, select: (search) => search.environment });
  const { attention, entries, isReady } = useAlwaysOnEnvironments();
  const params = useParams({ strict: false });
  const { isMobile, setOpenMobile } = useSidebar();

  const emptyState = isReady
    ? gt("Connect an environment to use Always On.")
    : gt("Loading environments…");

  return (
    <>
      <SidebarChromeHeader isElectron={isElectron} />
      <WorkspaceNavigation selected="always-on" attention={attention} />
      <SidebarContent>
        {entries.length === 0 ? (
          <SidebarGroup>
            <p className="px-2 text-sm text-muted-foreground">{emptyState}</p>
          </SidebarGroup>
        ) : null}
        {entries.map((entry) => (
          <SidebarGroup key={entry.environmentId}>
            <h2 className="truncate px-2 pb-1 text-xs font-medium text-muted-foreground">
              {entry.label}
            </h2>
            {entry.eventThreads.length > 0 ? (
              <SidebarMenu>
                {entry.eventThreads.map((thread) => {
                  const selected =
                    params.environmentId === thread.environmentId && params.threadId === thread.id;
                  return (
                    <SidebarMenuItem key={thread.id}>
                      <SidebarMenuButton
                        isActive={selected}
                        aria-current={selected ? "page" : undefined}
                        render={
                          <Link
                            to="/$environmentId/$threadId"
                            params={{ environmentId: thread.environmentId, threadId: thread.id }}
                            onClick={() => {
                              if (isMobile) setOpenMobile(false);
                            }}
                          />
                        }
                      >
                        <div className="min-w-0 flex-1">
                          <p className="truncate">{thread.title}</p>
                          <p className="text-muted-foreground truncate text-xs">
                            <EventRunStatus thread={thread} />
                          </p>
                        </div>
                      </SidebarMenuButton>
                    </SidebarMenuItem>
                  );
                })}
              </SidebarMenu>
            ) : null}
            {entry.status === "ready" && entry.agents.length > 0 ? (
              <SidebarMenu>
                {entry.agents.map((item) => {
                  const selected = agent === item.id && environment === entry.environmentId;
                  const pending = entry.canOperate ? alwaysOnAttentionCount(item) : 0;
                  return (
                    <SidebarMenuItem key={item.id}>
                      <SidebarMenuButton
                        isActive={selected}
                        aria-current={selected ? "page" : undefined}
                        render={
                          <Link
                            to="/always-on"
                            search={{ environment: entry.environmentId, agent: item.id }}
                            onClick={() => {
                              if (isMobile) setOpenMobile(false);
                            }}
                          />
                        }
                      >
                        <div className="min-w-0 flex-1">
                          <p className="truncate">{item.title}</p>
                          <p className="truncate text-xs text-muted-foreground">
                            <AgentPhase agent={item} />
                          </p>
                        </div>
                        {pending > 0 ? (
                          <Badge size="sm" variant="warning">
                            {pending}
                          </Badge>
                        ) : null}
                      </SidebarMenuButton>
                    </SidebarMenuItem>
                  );
                })}
              </SidebarMenu>
            ) : entry.eventThreads.length === 0 ? (
              <p className="px-2 text-sm text-muted-foreground">
                <EnvironmentMessage status={entry.status} />
              </p>
            ) : null}
          </SidebarGroup>
        ))}
      </SidebarContent>
      <SidebarChromeFooter />
    </>
  );
}

function EventRunStatus({ thread }: { readonly thread: EnvironmentThreadShell }) {
  const gt = useGT();
  if (thread.hasPendingApprovals) return gt("Awaiting approval");
  if (thread.hasPendingUserInput) return gt("Waiting for input");
  switch (thread.latestRun?.status) {
    case "preparing":
    case "starting":
    case "running":
      return gt("Running");
    case "waiting":
      return gt("Waiting");
    case "completed":
      return gt("Completed");
    case "failed":
      return gt("Failed");
    case "cancelled":
    case "interrupted":
    case "rolled_back":
      return gt("Stopped");
    case "queued":
    case undefined:
      return gt("Queued");
  }
}

function AgentPhase({ agent }: { readonly agent: AlwaysOnAgentSummary }) {
  const gt = useGT();
  if (agent.phase === "needs_confirmation") return gt("Needs confirmation");
  if (agent.pendingApprovalCount > 0) return gt("Awaiting approval");
  if (agent.phase === "running") return gt("Running");
  if (!agent.enabled) return gt("Paused");
  return gt("Sleeping");
}

function EnvironmentMessage({
  status,
}: {
  readonly status: ReturnType<typeof useAlwaysOnEnvironments>["entries"][number]["status"];
}) {
  const gt = useGT();
  switch (status) {
    case "ready":
      return gt("No Always On agents");
    case "loading":
      return gt("Loading agents…");
    case "disconnected":
      return gt("Environment disconnected");
    case "forbidden":
      return gt("Read access required");
    case "unsupported":
      return gt("This server does not support Always On.");
    case "error":
      return gt("Could not load Always On agents.");
  }
}
