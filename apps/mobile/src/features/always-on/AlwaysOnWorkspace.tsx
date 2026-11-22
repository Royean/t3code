import { useAtom, useAtomRefresh, useAtomValue } from "@effect/atom-react";
import { useNavigation, useNavigationState } from "@react-navigation/native";
import { alwaysOnAttentionCount } from "@t3tools/client-runtime/always-on-conversation";
import type { EnvironmentThreadShell } from "@t3tools/client-runtime/state/shell";
import { isAlwaysOnEventThread, type AlwaysOnPhase } from "@t3tools/contracts";
import { msg, useGT, useMessages } from "gt-react-native";
import type { ReactNode } from "react";
import { Keyboard, Platform, View } from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";

import { AppActivityIndicator } from "../../components/ui/AppActivityIndicator";
import { AppBar } from "../../components/ui/AppBar";
import { AppText as Text } from "../../components/ui/AppText";
import { Button } from "../../components/ui/Button";
import { ListRow } from "../../components/ui/ListRow";
import { ListSection } from "../../components/ui/ListSection";
import { ScreenScrollView } from "../../components/ui/ScreenScrollView";
import { SegmentedControl } from "../../components/ui/SegmentedControl";
import { useHardwareBack } from "../../lib/backHandler";
import { formatEnvironmentQueryError } from "../../state/query";
import { serverEnvironment } from "../../state/server";
import { useWorkspaceConnectionState } from "../../state/workspace";
import { useAdaptiveWorkspaceLayout } from "../layout/adaptive-workspace-context";
import { useSelectWorkspaceDetail } from "../layout/workspace/useSelectWorkspaceDetail";
import { WorkspaceTabPanes } from "../layout/workspace/WorkspaceTabPanes";
import { useWorkspaceTabs } from "../layout/workspace/WorkspaceTabsContext";
import { openSettings } from "../settings/settings-navigation";
import { resolveThreadListV2Status, type ThreadListV2Status } from "../threads/threadListV2";
import {
  alwaysOnEventThreadShellsAtom,
  alwaysOnWorkspaceEnvironmentsAtom,
  CHATS_WORKSPACE_TABS,
  chatsWorkspaceTabAtom,
  type AlwaysOnWorkspaceEnvironment,
  type ChatsWorkspaceTab,
} from "./always-on-workspace-state";

export const ALWAYS_ON_PHASE_LABELS: Record<AlwaysOnPhase, string> = {
  sleeping: msg("Sleeping"),
  running: msg("Running"),
  paused: msg("Paused"),
  needs_confirmation: msg("Needs confirmation"),
};

const EVENT_THREAD_STATUS: Record<ThreadListV2Status, string> = {
  approval: msg("Waiting for approval"),
  input: msg("Waiting for input"),
  request: msg("Needs attention"),
  working: msg("Running"),
  waiting: msg("Waiting"),
  failed: msg("Failed"),
  limited: msg("Limited", { $context: "thread status, usage limit reached" }),
  ready: msg("Ready"),
};

export function AlwaysOnWorkspace({
  renderThreads,
  sidebar = false,
}: {
  readonly renderThreads: (tabs: ReactNode) => ReactNode;
  readonly sidebar?: boolean;
}) {
  const gt = useGT();
  const { layout, openWorkspaceTab } = useAdaptiveWorkspaceLayout();
  const [tab, setTab] = useAtom(chatsWorkspaceTabAtom);
  const focusedRoute = useNavigationState((state) => state.routes[state.index]);
  const focusedPage = focusedRoute?.name;
  const focusedThread =
    focusedRoute?.name === "Thread"
      ? (focusedRoute.params as ReactNavigation.RootParamList["Thread"] | undefined)?.threadId
      : undefined;
  const selected =
    focusedPage === "AlwaysOnConversation" ||
    (typeof focusedThread === "string" && isAlwaysOnEventThread(focusedThread))
      ? "alwaysOn"
      : layout.usesSplitView && focusedPage === "Thread"
        ? "threads"
        : tab;
  const environments = useAtomValue(alwaysOnWorkspaceEnvironmentsAtom);
  const threads = useAtomValue(alwaysOnEventThreadShellsAtom);
  const actionableEnvironments = new Set(
    environments.filter(({ canOperate }) => canOperate).map(({ environmentId }) => environmentId),
  );
  const eventAttention = threads.filter((thread) => {
    if (!actionableEnvironments.has(thread.environmentId)) return false;
    const status = resolveThreadListV2Status(thread);
    return status === "approval" || status === "input" || status === "request";
  }).length;
  const attention = environments.reduce(
    (total, environment) =>
      total +
      (environment.canOperate && environment.result?._tag === "Success"
        ? environment.result.value.agents.reduce(
            (count, agent) => count + alwaysOnAttentionCount(agent),
            0,
          )
        : 0),
    eventAttention,
  );
  const selectTab = (next: ChatsWorkspaceTab) => {
    if (next === selected) return;
    Keyboard.dismiss();
    setTab(next);
    if (layout.usesSplitView && focusedPage !== "Home") {
      openWorkspaceTab("threads", { columns: false });
    }
  };
  useHardwareBack(
    !layout.usesSplitView && focusedPage === "Home" && selected === "alwaysOn",
    () => {
      selectTab("threads");
      return true;
    },
  );
  const alwaysOnLabel = gt("Always On");
  const tabsInHeader = Platform.OS === "android" && !sidebar && !layout.usesSplitView;
  const switcher = (
    <SegmentedControl
      role="tab"
      className="w-full max-w-64"
      selected={selected}
      onSelect={selectTab}
      options={[
        { value: "threads", label: gt("Threads") },
        {
          value: "alwaysOn",
          label:
            attention > 0
              ? gt("{tab} · {count}", { tab: alwaysOnLabel, count: attention })
              : alwaysOnLabel,
          accessibilityLabel:
            attention > 0
              ? gt("Always On, {count} need attention", { count: attention })
              : alwaysOnLabel,
        },
      ]}
    />
  );
  const tabs = tabsInHeader ? (
    switcher
  ) : (
    <View className="items-start bg-header px-4 py-2">{switcher}</View>
  );

  return (
    <WorkspaceTabPanes
      tabs={CHATS_WORKSPACE_TABS}
      selected={selected}
      renderTab={(pane) =>
        pane === "threads" ? (
          renderThreads(tabs)
        ) : (
          <>
            {!sidebar && Platform.OS === "android" ? (
              <AppBar
                embedded
                title={alwaysOnLabel}
                titleContent={tabsInHeader ? tabs : undefined}
              />
            ) : null}
            {tabsInHeader ? null : tabs}
            <AlwaysOnActivityList environments={environments} threads={threads} />
          </>
        )
      }
    />
  );
}

function AlwaysOnActivityList({
  environments,
  threads,
}: {
  readonly environments: readonly AlwaysOnWorkspaceEnvironment[];
  readonly threads: readonly EnvironmentThreadShell[];
}) {
  const gt = useGT();
  const navigation = useNavigation();
  const focusedRoute = useNavigationState((state) => state.routes[state.index]);
  const insets = useSafeAreaInsets();
  const workspace = useWorkspaceTabs();
  const { isLoadingConnections } = useWorkspaceConnectionState();
  const selectedThread =
    focusedRoute?.name === "Thread"
      ? (focusedRoute.params as ReactNavigation.RootParamList["Thread"] | undefined)
      : undefined;
  const selected =
    focusedRoute?.name === "AlwaysOnConversation"
      ? (focusedRoute.params as ReactNavigation.RootParamList["AlwaysOnConversation"] | undefined)
      : undefined;

  return (
    <ScreenScrollView
      className="flex-1 bg-screen"
      contentInsetAdjustmentBehavior="automatic"
      contentContainerClassName="gap-4 px-4 pt-3"
      contentContainerStyle={{
        paddingBottom: Math.max(insets.bottom, workspace?.bottomClearance ?? 0) + 16,
      }}
    >
      {isLoadingConnections && environments.length === 0 ? (
        <AppActivityIndicator className="self-start p-4" />
      ) : environments.length === 0 ? (
        <Text className="text-foreground-muted">
          {gt("Connect an environment to use Always On.")}
        </Text>
      ) : null}
      {environments.map((environment) => (
        <EnvironmentActivity
          key={environment.environmentId}
          environment={environment}
          threads={threads.filter(
            (thread) =>
              !environment.readDenied && thread.environmentId === environment.environmentId,
          )}
          selectedThreadId={
            selectedThread?.environmentId === environment.environmentId &&
            typeof selectedThread?.threadId === "string"
              ? selectedThread.threadId
              : undefined
          }
          selectedAgentId={
            selected?.environmentId === environment.environmentId ? selected?.agentId : undefined
          }
        />
      ))}
      {threads.length === 0 &&
      environments.some(({ canConfigureRouting }) => canConfigureRouting) ? (
        <Button
          label={gt("Configure event routing")}
          onPress={() => openSettings(navigation, "SettingsAlwaysOn")}
        />
      ) : null}
    </ScreenScrollView>
  );
}

function EnvironmentActivity({
  environment,
  selectedAgentId,
  selectedThreadId,
  threads,
}: {
  readonly environment: AlwaysOnWorkspaceEnvironment;
  readonly selectedAgentId:
    | ReactNavigation.RootParamList["AlwaysOnConversation"]["agentId"]
    | undefined;
  readonly selectedThreadId: string | undefined;
  readonly threads: readonly EnvironmentThreadShell[];
}) {
  const gt = useGT();
  const messages = useMessages();
  const selectDetail = useSelectWorkspaceDetail();
  const { environmentId } = environment;
  const refresh = useAtomRefresh(serverEnvironment.alwaysOnLive({ environmentId, input: {} }));
  return (
    <ListSection title={environment.label}>
      {[...threads]
        .sort((left, right) => right.updatedAt.localeCompare(left.updatedAt))
        .map((thread) => {
          const status = resolveThreadListV2Status(thread);
          const latestRunStatus = thread.latestRun?.status;
          let label = messages(EVENT_THREAD_STATUS[status]);
          if (status === "working" && thread.runtime?.status === "queued") {
            label = gt("Queued");
          } else if (status === "ready" || status === "waiting") {
            if (latestRunStatus === "completed") label = gt("Completed");
            if (latestRunStatus === "interrupted" || latestRunStatus === "cancelled") {
              label = gt("Stopped");
            }
          }
          return (
            <ListRow
              key={thread.id}
              label={thread.title}
              description={label}
              detail={thread.modelSelection?.model}
              selected={selectedThreadId === thread.id ? true : undefined}
              onPress={() =>
                selectDetail({ name: "Thread", params: { environmentId, threadId: thread.id } })
              }
            />
          );
        })}
      {!environment.connected ? (
        <Text className="px-4 py-3 text-foreground-muted">
          {gt("This environment is disconnected.")}
        </Text>
      ) : environment.readDenied ? (
        <Text className="px-4 py-3 text-foreground-muted">
          {gt("This connection cannot view always-on agents.")}
        </Text>
      ) : environment.result?._tag === "Failure" ? (
        <View className="gap-2 px-4 py-3">
          <Text className="text-foreground-muted" accessibilityRole="alert">
            {formatEnvironmentQueryError(environment.result.cause, messages)}
          </Text>
          <Button label={gt("Retry")} size="compact" onPress={refresh} />
        </View>
      ) : environment.snapshot === null ? (
        <AppActivityIndicator className="self-start px-4 py-3" />
      ) : environment.snapshot.agents.length === 0 ? (
        threads.length === 0 ? (
          <Text className="px-4 py-3 text-foreground-muted">{gt("No runs yet.")}</Text>
        ) : null
      ) : (
        <>
          {threads.length > 0 ? (
            <Text textRole="footnote" className="px-4 pt-3 text-foreground-muted">
              {gt("Always-on agents")}
            </Text>
          ) : null}
          {[...environment.snapshot.agents]
            .sort((left, right) => right.updatedAt.localeCompare(left.updatedAt))
            .map((agent) => {
              const count = alwaysOnAttentionCount(agent);
              return (
                <ListRow
                  key={agent.id}
                  label={agent.title}
                  description={messages(ALWAYS_ON_PHASE_LABELS[agent.phase])}
                  detail={count > 0 ? gt("{count} need attention", { count }) : undefined}
                  selected={selectedAgentId === agent.id ? true : undefined}
                  onPress={() =>
                    selectDetail({
                      name: "AlwaysOnConversation",
                      params: { environmentId, agentId: agent.id },
                    })
                  }
                />
              );
            })}
        </>
      )}
    </ListSection>
  );
}
