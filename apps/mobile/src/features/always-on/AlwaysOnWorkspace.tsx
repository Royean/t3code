import { useAtom, useAtomRefresh, useAtomValue } from "@effect/atom-react";
import { useNavigation, useNavigationState } from "@react-navigation/native";
import { alwaysOnAttentionCount } from "@t3tools/client-runtime/always-on-conversation";
import { msg, useGT, useMessages } from "gt-react-native";
import type { ReactNode } from "react";
import { Keyboard, Platform, View } from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";
import type { AlwaysOnPhase } from "@t3tools/contracts";

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
import {
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
  const focusedPage = useNavigationState((state) => state.routes[state.index]?.name);
  const selected =
    layout.usesSplitView && focusedPage === "Thread"
      ? "threads"
      : focusedPage === "AlwaysOnConversation"
        ? "alwaysOn"
        : tab;
  const environments = useAtomValue(alwaysOnWorkspaceEnvironmentsAtom);
  const attention = environments.reduce(
    (total, environment) =>
      total +
      (environment.canOperate && environment.result?._tag === "Success"
        ? environment.result.value.agents.reduce(
            (count, agent) => count + alwaysOnAttentionCount(agent),
            0,
          )
        : 0),
    0,
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
  const tabs = (
    <View className="items-start bg-header px-4 py-2">
      <SegmentedControl
        variant="card"
        size="compact"
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
    </View>
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
              <AppBar embedded title={alwaysOnLabel} />
            ) : null}
            {tabs}
            <AlwaysOnAgentList environments={environments} />
          </>
        )
      }
    />
  );
}

function AlwaysOnAgentList({
  environments,
}: {
  readonly environments: readonly AlwaysOnWorkspaceEnvironment[];
}) {
  const gt = useGT();
  const navigation = useNavigation();
  const focusedRoute = useNavigationState((state) => state.routes[state.index]);
  const insets = useSafeAreaInsets();
  const workspace = useWorkspaceTabs();
  const { isLoadingConnections } = useWorkspaceConnectionState();
  const showCreate = environments.some(
    ({ result, canOperate }) => canOperate && result?._tag === "Success",
  );
  const hasAgents = environments.some(({ snapshot }) => (snapshot?.agents.length ?? 0) > 0);
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
          {gt("Connect an environment to see always-on agents.")}
        </Text>
      ) : null}
      {environments.map((environment) => (
        <EnvironmentAgents
          key={environment.environmentId}
          environment={environment}
          selectedAgentId={
            selected?.environmentId === environment.environmentId ? selected?.agentId : undefined
          }
        />
      ))}
      {showCreate && !hasAgents ? (
        <Button
          label={gt("Create an agent in Settings")}
          onPress={() => openSettings(navigation, "SettingsAlwaysOn")}
        />
      ) : null}
    </ScreenScrollView>
  );
}

function EnvironmentAgents({
  environment,
  selectedAgentId,
}: {
  readonly environment: AlwaysOnWorkspaceEnvironment;
  readonly selectedAgentId:
    | ReactNavigation.RootParamList["AlwaysOnConversation"]["agentId"]
    | undefined;
}) {
  const gt = useGT();
  const messages = useMessages();
  const selectDetail = useSelectWorkspaceDetail();
  const { environmentId } = environment;
  const refresh = useAtomRefresh(serverEnvironment.alwaysOnLive({ environmentId, input: {} }));
  return (
    <ListSection title={environment.label}>
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
        <Text className="px-4 py-3 text-foreground-muted">{gt("No always-on agents yet.")}</Text>
      ) : (
        [...environment.snapshot.agents]
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
          })
      )}
    </ListSection>
  );
}
