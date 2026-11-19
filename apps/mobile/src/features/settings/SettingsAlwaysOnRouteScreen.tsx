import { useAtomValue } from "@effect/atom-react";
import { useNavigation, type StaticScreenProps } from "@react-navigation/native";
import {
  AuthOrchestrationReadScope,
  type AlwaysOnAgentId,
  type AlwaysOnApprovalEntry,
  type AlwaysOnPhase,
  type AlwaysOnResolveApprovalInput,
  type AlwaysOnRunStatus,
  type EnvironmentId,
  type ProjectId,
} from "@t3tools/contracts";
import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import { msg, useGT, useLocale, useMessages } from "gt-react-native";
import { useState, type ReactNode } from "react";
import { View } from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";

import { AppActivityIndicator } from "../../components/ui/AppActivityIndicator";
import { AppText as Text } from "../../components/ui/AppText";
import { Button } from "../../components/ui/Button";
import { ListRow } from "../../components/ui/ListRow";
import { ScreenScrollView } from "../../components/ui/ScreenScrollView";
import { useEnvironmentQuery } from "../../state/query";
import { serverEnvironment } from "../../state/server";
import { useEnvironmentScope, useEnvironmentScopeDenied } from "../../state/session";
import { useAtomCommand } from "../../state/use-atom-command";
import { useSettingsEnvironmentMenu } from "./components/SettingsEnvironmentFilterHeader";
import { SettingsScreen } from "./components/SettingsScreen";
import { SettingsSection } from "./components/SettingsSection";
import { useSettingsEnvironmentFilter, type SettingsTarget } from "./settings-environment-filter";
import { settingsTargetsForProject } from "./settings-environment-filter.logic";

const PHASE_LABELS: Record<AlwaysOnPhase, string> = {
  sleeping: msg("Sleeping"),
  running: msg("Running"),
  paused: msg("Paused"),
  needs_confirmation: msg("Needs confirmation"),
};

const RUN_LABELS: Record<AlwaysOnRunStatus, string> = {
  running: msg("Running"),
  completed: msg("Completed"),
  awaiting_approval: msg("Approval requested"),
  interrupted: msg("Interrupted"),
  unknown: msg("Outcome unknown"),
  cancelled: msg("Cancelled"),
  failed: msg("Failed"),
};

type AgentPageProps = StaticScreenProps<{
  readonly environmentId: EnvironmentId;
  readonly agentId: AlwaysOnAgentId;
}>;

function AlwaysOnScroll({ children }: { readonly children: ReactNode }) {
  const insets = useSafeAreaInsets();
  return (
    <ScreenScrollView
      className="flex-1"
      contentInsetAdjustmentBehavior="automatic"
      contentContainerClassName="gap-5 px-5 pt-4"
      contentContainerStyle={{ paddingBottom: Math.max(insets.bottom, 18) + 18 }}
    >
      {children}
    </ScreenScrollView>
  );
}

function LoadFailure({ message, retry }: { readonly message: string; readonly retry: () => void }) {
  const gt = useGT();
  return (
    <View className="gap-2 px-4 py-3">
      <Text accessibilityRole="alert" className="text-foreground-muted">
        {message}
      </Text>
      <Button label={gt("Retry")} onPress={retry} size="compact" />
    </View>
  );
}

export function SettingsAlwaysOnRouteScreen() {
  const gt = useGT();
  const { availableTargets, selectedTargets, selectedProjectKey, projectGroups } =
    useSettingsEnvironmentFilter();
  const environmentMenu = useSettingsEnvironmentMenu();
  const selectedGroup =
    selectedProjectKey === null
      ? null
      : projectGroups.find((group) => group.key === selectedProjectKey);
  const targets = settingsTargetsForProject(selectedTargets, selectedGroup);

  return (
    <SettingsScreen title={gt("Always-on agents")} menus={[environmentMenu]}>
      <AlwaysOnScroll>
        {targets.length === 0 ? (
          <Text className="px-2 text-foreground-muted">
            {availableTargets.length === 0
              ? gt("Connect an environment to see always-on agents.")
              : gt("No environments match these filters. Change the filter above.")}
          </Text>
        ) : (
          targets.map((environment) => (
            <EnvironmentAgents
              key={environment.environmentId}
              environment={environment}
              projectIds={
                selectedProjectKey === null
                  ? null
                  : (selectedGroup?.members
                      .filter(({ project }) => project.environmentId === environment.environmentId)
                      .map(({ project }) => project.id) ?? [])
              }
            />
          ))
        )}
      </AlwaysOnScroll>
    </SettingsScreen>
  );
}

function EnvironmentAgents({
  environment,
  projectIds,
}: {
  readonly environment: SettingsTarget;
  readonly projectIds: readonly ProjectId[] | null;
}) {
  const gt = useGT();
  const messages = useMessages();
  const navigation = useNavigation();
  const { environmentId } = environment;
  const canRead = useEnvironmentScope(environmentId, AuthOrchestrationReadScope);
  const readDenied = useEnvironmentScopeDenied(environmentId, AuthOrchestrationReadScope);
  const query = useEnvironmentQuery(
    canRead ? serverEnvironment.alwaysOnLive({ environmentId, input: {} }) : null,
  );
  const agents = query.data?.agents.filter(
    (agent) => projectIds === null || projectIds.includes(agent.projectId),
  );

  return (
    <SettingsSection title={environment.label}>
      {readDenied ? (
        <Text className="px-4 py-3 text-foreground-muted">
          {gt("This connection cannot view always-on agents.")}
        </Text>
      ) : query.error !== null ? (
        <LoadFailure message={query.error} retry={query.refresh} />
      ) : agents === undefined ? (
        <AppActivityIndicator className="self-start px-4 py-3" />
      ) : agents.length === 0 ? (
        <Text className="px-4 py-3 text-foreground-muted">{gt("No always-on agents yet.")}</Text>
      ) : (
        agents.map((agent) => (
          <ListRow
            key={agent.id}
            label={agent.title}
            description={
              agent.pendingApprovalCount > 0
                ? gt(
                    "{count, plural, one {{count} pending approval} other {{count} pending approvals}}",
                    { count: agent.pendingApprovalCount },
                  )
                : messages(PHASE_LABELS[agent.phase])
            }
            onPress={() =>
              navigation.navigate("SettingsAlwaysOnAgent", { environmentId, agentId: agent.id })
            }
          />
        ))
      )}
    </SettingsSection>
  );
}

export function SettingsAlwaysOnAgentRouteScreen({ route }: AgentPageProps) {
  const gt = useGT();
  const locale = useLocale();
  const messages = useMessages();
  const { environmentId, agentId } = route.params;
  const { availableTargets } = useSettingsEnvironmentFilter();
  const environment = availableTargets.find((target) => target.environmentId === environmentId);
  const canRead = useEnvironmentScope(environmentId, AuthOrchestrationReadScope);
  const readDenied = useEnvironmentScopeDenied(environmentId, AuthOrchestrationReadScope);
  const canOperate = useAtomValue(
    serverEnvironment.resolveAlwaysOnApproval.permissionAtom(environmentId),
  );
  const query = useEnvironmentQuery(
    environment && canRead
      ? serverEnvironment.alwaysOnDetail({ environmentId, input: { agentId } })
      : null,
  );
  const detail = query.data;
  const approvals = detail?.approvals.filter((approval) => approval.status === "pending") ?? [];
  const date = new Intl.DateTimeFormat(locale, { dateStyle: "medium", timeStyle: "short" });

  return (
    <SettingsScreen title={detail?.agent.title ?? gt("Always-on agents")}>
      <AlwaysOnScroll>
        {!environment ? (
          <Text className="px-2 text-foreground-muted">
            {gt("This environment is disconnected.")}
          </Text>
        ) : readDenied ? (
          <Text className="px-2 text-foreground-muted">
            {gt("This connection cannot view always-on agents.")}
          </Text>
        ) : query.error !== null ? (
          <LoadFailure message={query.error} retry={query.refresh} />
        ) : detail === null ? (
          <AppActivityIndicator className="self-start px-2 py-3" />
        ) : (
          <>
            <Text className="px-2 text-foreground-muted">
              {gt("{environment} · {status}", {
                environment: environment.label,
                status: messages(PHASE_LABELS[detail.agent.phase]),
              })}
            </Text>
            {approvals.length > 0 ? (
              <SettingsSection
                title={gt("Pending approvals")}
                footer={canOperate ? undefined : gt("This connection has read-only access.")}
              >
                {approvals.map((approval) => (
                  <ApprovalRow
                    key={approval.id}
                    environmentId={environmentId}
                    agentId={agentId}
                    approval={approval}
                    canOperate={canOperate && !query.isPending}
                    refresh={query.refresh}
                  />
                ))}
              </SettingsSection>
            ) : null}
            {detail.deliveries.length > 0 ? (
              <SettingsSection title={gt("Deliveries")}>
                {detail.deliveries.map((delivery) => (
                  <View key={delivery.id} className="gap-1 border-b border-border-subtle px-4 py-3">
                    <Text className="text-foreground" selectable>
                      {delivery.body}
                    </Text>
                    {delivery.status === "delivered" ? null : (
                      <Text textRole="footnote" className="text-foreground-muted">
                        {delivery.status === "pending"
                          ? gt("Pending delivery")
                          : gt("Delivery failed")}
                      </Text>
                    )}
                  </View>
                ))}
              </SettingsSection>
            ) : null}
            <SettingsSection title={gt("Recent runs")}>
              {detail.runs.length === 0 ? (
                <Text className="px-4 py-3 text-foreground-muted">{gt("No runs yet.")}</Text>
              ) : (
                [...detail.runs].reverse().map((run) => (
                  <View key={run.id} className="gap-1 border-b border-border-subtle px-4 py-3">
                    <Text className="text-foreground">{messages(RUN_LABELS[run.status])}</Text>
                    <Text textRole="footnote" className="text-foreground-muted">
                      {date.format(new Date(run.startedAt))}
                    </Text>
                    {(run.modelError ?? run.modelSummary) ? (
                      <Text className="text-foreground" selectable>
                        {run.modelError ?? run.modelSummary}
                      </Text>
                    ) : null}
                  </View>
                ))
              )}
            </SettingsSection>
          </>
        )}
      </AlwaysOnScroll>
    </SettingsScreen>
  );
}

function ApprovalRow({
  environmentId,
  agentId,
  approval,
  canOperate,
  refresh,
}: {
  readonly environmentId: EnvironmentId;
  readonly agentId: AlwaysOnAgentId;
  readonly approval: AlwaysOnApprovalEntry;
  readonly canOperate: boolean;
  readonly refresh: () => void;
}) {
  const gt = useGT();
  const resolve = useAtomCommand(serverEnvironment.resolveAlwaysOnApproval);
  type Decision = AlwaysOnResolveApprovalInput["decision"];
  const [pending, setPending] = useState<Decision | null>(null);
  const [error, setError] = useState<string | null>(null);
  const decide = async (decision: Decision) => {
    if (!canOperate || pending !== null) return;
    setError(null);
    setPending(decision);
    const result = await resolve({
      environmentId,
      input: { agentId, approvalId: approval.id, decision },
    });
    if (result._tag === "Failure") {
      const cause = squashAtomCommandFailure(result);
      setError(
        cause instanceof Error ? cause.message : gt("Could not save the approval decision."),
      );
      setPending(null);
      refresh();
    }
  };

  return (
    <View className="gap-3 border-b border-border-subtle px-4 py-3">
      <Text className="text-foreground" selectable>
        {approval.summary}
      </Text>
      {error === null ? null : (
        <Text accessibilityRole="alert" className="text-danger">
          {error}
        </Text>
      )}
      <View className="flex-row flex-wrap gap-2">
        <Button
          label={gt("Approve")}
          tone="primary"
          disabled={!canOperate || pending !== null}
          loading={pending === "approved"}
          onPress={() => void decide("approved")}
        />
        <Button
          label={gt("Reject")}
          disabled={!canOperate || pending !== null}
          loading={pending === "rejected"}
          onPress={() => void decide("rejected")}
        />
      </View>
    </View>
  );
}
