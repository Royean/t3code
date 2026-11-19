import { useAtomValue } from "@effect/atom-react";
import type { EnvironmentProject } from "@t3tools/client-runtime/state/models";
import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import { AlwaysOnCreateInput, AuthOrchestrationReadScope } from "@t3tools/contracts";
import type { AlwaysOnProviderKind } from "@t3tools/contracts";
import * as Schema from "effect/Schema";
import { useGT } from "gt-react-native";
import { useRef, useState } from "react";
import { Pressable, View } from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";

import { AppActivityIndicator } from "../../components/ui/AppActivityIndicator";
import { AppText as Text } from "../../components/ui/AppText";
import { Button } from "../../components/ui/Button";
import { FormField, FormSection } from "../../components/ui/FormSection";
import { Menu } from "../../components/ui/Menu";
import { ScreenScrollView } from "../../components/ui/ScreenScrollView";
import { SymbolView } from "../../components/ui/AppSymbol";
import { useProjects } from "../../state/entities";
import { useEnvironmentQuery } from "../../state/query";
import { serverEnvironment } from "../../state/server";
import { useEnvironmentScope, useEnvironmentScopeDenied } from "../../state/session";
import { useAtomCommand } from "../../state/use-atom-command";
import { useSettingsEnvironmentMenu } from "./components/SettingsEnvironmentFilterHeader";
import { SettingsScreen } from "./components/SettingsScreen";
import { SettingsSection } from "./components/SettingsSection";
import { useSettingsEnvironmentFilter, type SettingsTarget } from "./settings-environment-filter";
import { settingsTargetsForProject } from "./settings-environment-filter.logic";

const isCreateInput = Schema.is(AlwaysOnCreateInput);

export function SettingsAlwaysOnRouteScreen() {
  const gt = useGT();
  const insets = useSafeAreaInsets();
  const projects = useProjects();
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
      <ScreenScrollView
        className="flex-1"
        contentInsetAdjustmentBehavior="automatic"
        contentContainerClassName="gap-5 px-5 pt-4"
        contentContainerStyle={{ paddingBottom: Math.max(insets.bottom, 18) + 18 }}
        keyboardShouldPersistTaps="handled"
      >
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
              scopedToProject={selectedGroup !== null}
              projects={projects.filter(
                (project) =>
                  project.environmentId === environment.environmentId &&
                  (selectedGroup === null ||
                    selectedGroup?.members.some(
                      (member) =>
                        member.project.environmentId === project.environmentId &&
                        member.project.id === project.id,
                    )),
              )}
            />
          ))
        )}
      </ScreenScrollView>
    </SettingsScreen>
  );
}

function EnvironmentAgents({
  environment,
  projects,
  scopedToProject,
}: {
  readonly environment: SettingsTarget;
  readonly projects: readonly EnvironmentProject[];
  readonly scopedToProject: boolean;
}) {
  const gt = useGT();
  const { environmentId } = environment;
  const canRead = useEnvironmentScope(environmentId, AuthOrchestrationReadScope);
  const readDenied = useEnvironmentScopeDenied(environmentId, AuthOrchestrationReadScope);
  const canCreate = useAtomValue(
    serverEnvironment.createAlwaysOnAgent.permissionAtom(environmentId),
  );
  const query = useEnvironmentQuery(
    canRead ? serverEnvironment.alwaysOnLive({ environmentId, input: {} }) : null,
  );
  const projectIds = new Set(projects.map((project) => project.id));
  const agents = query.data?.agents.filter(
    (agent) => !scopedToProject || projectIds.has(agent.projectId),
  );
  const openProjects = projects.filter((project) => !project.archivedAt);
  const [creating, setCreating] = useState(false);
  const [created, setCreated] = useState<string | null>(null);

  return (
    <>
      <SettingsSection title={environment.label}>
        {readDenied ? (
          <Text className="px-4 py-3 text-foreground-muted">
            {gt("This connection cannot view always-on agents.")}
          </Text>
        ) : query.error !== null ? (
          <View className="gap-2 px-4 py-3">
            <Text accessibilityRole="alert" className="text-foreground-muted">
              {query.error}
            </Text>
            <Button label={gt("Retry")} onPress={query.refresh} size="compact" />
          </View>
        ) : agents === undefined ? (
          <AppActivityIndicator className="self-start px-4 py-3" />
        ) : (
          <>
            <View className="gap-2 px-4 py-3">
              {agents.length === 0 ? (
                <Text className="text-foreground-muted">{gt("No always-on agents yet.")}</Text>
              ) : (
                agents.map((agent) => <Text key={agent.id}>{agent.title}</Text>)
              )}
            </View>
            <View className="gap-2 border-t border-border-subtle px-4 py-3">
              {created === null ? null : (
                <Text accessibilityLiveRegion="polite">
                  {gt("Created {title}. Find it in Chats under Always On.", { title: created })}
                </Text>
              )}
              {!canCreate ? (
                <Text className="text-foreground-muted">
                  {gt("This connection has read-only access.")}
                </Text>
              ) : openProjects.length === 0 ? (
                <Text className="text-foreground-muted">
                  {gt("Select an active project to create an always-on agent.")}
                </Text>
              ) : !creating ? (
                <Button
                  label={gt("Create agent")}
                  size="compact"
                  onPress={() => {
                    setCreated(null);
                    setCreating(true);
                  }}
                />
              ) : null}
            </View>
          </>
        )}
      </SettingsSection>
      {creating && query.error === null && canCreate && canRead && openProjects.length > 0 ? (
        <CreateAgentForm
          environment={environment}
          projects={openProjects}
          onCancel={() => setCreating(false)}
          onCreated={(title) => {
            setCreating(false);
            setCreated(title);
          }}
        />
      ) : null}
    </>
  );
}

type Choice<Value extends string = string> = { readonly id: Value; readonly title: string };

function ChooseField<Value extends string>({
  label,
  choices,
  value,
  onChange,
  disabled,
}: {
  readonly label: string;
  readonly choices: readonly Choice<Value>[];
  readonly value: Choice<Value> | undefined;
  readonly onChange: (value: Value) => void;
  readonly disabled: boolean;
}) {
  const gt = useGT();
  if (choices.length === 0 || (choices.length === 1 && value !== undefined) || disabled) {
    return (
      <View className="min-h-10 flex-row items-center justify-between gap-3">
        <Text>{label}</Text>
        <Text className="shrink text-foreground-muted">{value?.title}</Text>
      </View>
    );
  }
  return (
    <Menu
      title={label}
      actions={choices.map((choice) => ({
        id: choice.id,
        title: choice.title,
        state: choice.id === value?.id ? "on" : undefined,
      }))}
      onPressAction={({ nativeEvent }) => {
        const selected = choices.find((choice) => choice.id === nativeEvent.event);
        if (selected) onChange(selected.id);
      }}
    >
      <Pressable
        accessibilityRole="button"
        accessibilityLabel={value ? gt("{label}, {value}", { label, value: value.title }) : label}
        className="min-h-10 flex-row items-center gap-3 active:opacity-70"
      >
        <Text>{label}</Text>
        <Text numberOfLines={1} className="flex-1 text-right text-foreground-muted">
          {value?.title ?? gt("Choose")}
        </Text>
        <SymbolView
          name="chevron.down"
          size={14}
          tintColorClassName="accent-chevron"
          type="monochrome"
        />
      </Pressable>
    </Menu>
  );
}

function CreateAgentForm({
  environment,
  projects,
  onCreated,
  onCancel,
}: {
  readonly environment: SettingsTarget;
  readonly projects: readonly EnvironmentProject[];
  readonly onCreated: (title: string) => void;
  readonly onCancel: () => void;
}) {
  const gt = useGT();
  const { environmentId, serverConfig } = environment;
  const providers = useAtomValue(serverEnvironment.providersValueAtom(environmentId));
  const canCreate = useAtomValue(
    serverEnvironment.createAlwaysOnAgent.permissionAtom(environmentId),
  );
  const create = useAtomCommand(serverEnvironment.createAlwaysOnAgent);
  const [projectId, setProjectId] = useState<EnvironmentProject["id"] | null>(null);
  const [providerKind, setProviderKind] = useState<AlwaysOnProviderKind | null>(null);
  const [instanceId, setInstanceId] = useState<string | null>(null);
  const [modelId, setModelId] = useState<string | null>(null);
  const [title, setTitle] = useState("");
  const [instructions, setInstructions] = useState("");
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const inFlight = useRef(false);

  const projectChoices = projects.map((project) => ({ id: project.id, title: project.title }));
  const project =
    projectId === null
      ? projectChoices[0]
      : projectChoices.find((choice) => choice.id === projectId);
  const textGenerators = Object.entries(serverConfig.settings.textGenerators)
    .filter(([, generator]) => generator.enabled)
    .map(([id, generator]) => ({ id, title: generator.label ?? id }));
  const claudeInstances = (providers ?? []).filter(
    (provider) => provider.driver === "claudeAgent" && provider.enabled && provider.installed,
  );
  const runtimes: Choice<AlwaysOnProviderKind>[] = [];
  if (textGenerators.length > 0) {
    runtimes.push({ id: "textGenerator", title: gt("Text generator") });
  }
  if (claudeInstances.length > 0) {
    runtimes.push({ id: "claudeAgent", title: gt("Claude") });
  }
  const runtime =
    providerKind === null ? runtimes[0] : runtimes.find((choice) => choice.id === providerKind);
  const textGenerator = runtime?.id === "textGenerator";
  const instances: Choice[] = textGenerator
    ? textGenerators
    : claudeInstances.map((provider) => ({
        id: provider.instanceId,
        title: provider.displayName ?? provider.instanceId,
      }));
  const instance =
    instanceId === null ? instances[0] : instances.find((choice) => choice.id === instanceId);
  const modelSelection =
    (project
      ? serverConfig.settings.projectSettingsOverrides[project.id]?.textGenerationModelSelection
      : undefined) ?? serverConfig.settings.textGenerationModelSelection;
  const defaultModel = modelSelection.instanceId === instance?.id ? modelSelection.model : "";
  const models =
    claudeInstances
      .find((provider) => provider.instanceId === instance?.id)
      ?.models.filter((model) => model.supportsTextGeneration !== false)
      .map((model) => ({ id: model.slug, title: model.name })) ?? [];
  const model =
    models.find((choice) => choice.id === (modelId ?? defaultModel)) ??
    (modelId === null ? models[0] : undefined);
  const modelText = modelId ?? defaultModel;
  const input = {
    projectId: project?.id,
    title: title.trim(),
    instructions: instructions.trim(),
    providerKind: runtime?.id,
    instanceId: instance?.id,
    model: textGenerator || models.length === 0 ? modelText.trim() : model?.id,
  };
  const valid = isCreateInput(input);

  const submit = async () => {
    if (!canCreate || !valid || inFlight.current) return;
    inFlight.current = true;
    setSaving(true);
    setError(null);
    const result = await create({ environmentId, input });
    inFlight.current = false;
    setSaving(false);
    if (result._tag === "Failure") {
      const cause = squashAtomCommandFailure(result);
      setError(cause instanceof Error ? cause.message : gt("Could not create the agent."));
    } else {
      onCreated(result.value.agent.title);
    }
  };

  return (
    <FormSection title={gt("New agent")}>
      {runtimes.length === 0 ? (
        <Text className="text-foreground-muted">
          {gt("Configure a text generator or Claude on this environment to create an agent.")}
        </Text>
      ) : (
        <>
          <ChooseField
            label={gt("Project")}
            choices={projectChoices}
            value={project}
            disabled={saving}
            onChange={(id) => {
              setProjectId(id);
              setModelId(null);
            }}
          />
          <FormField
            label={gt("Agent title")}
            placeholder={gt("Agent title")}
            accessibilityLabel={gt("Agent title")}
            value={title}
            onChangeText={setTitle}
            editable={!saving}
            maxLength={160}
          />
          <FormField
            label={gt("Instructions")}
            placeholder={gt("Instructions loaded at the start of every run")}
            accessibilityLabel={gt("Instructions")}
            value={instructions}
            onChangeText={setInstructions}
            editable={!saving}
            multiline
            numberOfLines={4}
            maxLength={8_000}
          />
          <ChooseField
            label={gt("Model runtime")}
            choices={runtimes}
            value={runtime}
            disabled={saving}
            onChange={(kind) => {
              setProviderKind(kind);
              setInstanceId(null);
              setModelId(null);
            }}
          />
          <ChooseField
            label={gt("Provider instance")}
            choices={instances}
            value={instance}
            disabled={saving}
            onChange={(id) => {
              setInstanceId(id);
              setModelId(null);
            }}
          />
          {!textGenerator && models.length > 0 ? (
            <ChooseField
              label={gt("Model")}
              choices={models}
              value={model}
              disabled={saving}
              onChange={setModelId}
            />
          ) : (
            <FormField
              label={gt("Model ID")}
              placeholder={gt("Model ID")}
              accessibilityLabel={gt("Model ID")}
              value={modelText}
              onChangeText={setModelId}
              autoCapitalize="none"
              autoCorrect={false}
              editable={!saving}
              maxLength={160}
            />
          )}
          {error === null ? null : (
            <Text accessibilityRole="alert" className="text-danger">
              {error}
            </Text>
          )}
        </>
      )}
      <View className="flex-row flex-wrap gap-2">
        {runtimes.length === 0 ? null : (
          <Button
            label={gt("Create agent")}
            tone="primary"
            disabled={!canCreate || !valid}
            loading={saving}
            onPress={() => void submit()}
          />
        )}
        <Button label={gt("Cancel")} tone="text" disabled={saving} onPress={onCancel} />
      </View>
    </FormSection>
  );
}
