import { useAtom, useAtomValue } from "@effect/atom-react";
import { useNavigation, type StaticScreenProps } from "@react-navigation/native";
import { offersLinkedEnvironments } from "@t3tools/client-runtime/link-request";
import type { EnvironmentProject } from "@t3tools/client-runtime/state/models";
import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import {
  AuthAccessReadScope,
  AuthSettingsWriteScope,
  EnvironmentId,
  ProjectId,
  type AlwaysOnRoutingPreferences,
  type ModelSelection,
  type ServerProvider,
} from "@t3tools/contracts";
import { modelSelectionsEqual, resolveAlwaysOnSettingsModelSelection } from "@t3tools/shared/model";
import { Atom } from "effect/reactivity";
import { useGT } from "gt-react-native";
import { useId, useMemo, useState } from "react";
import { Pressable, View } from "react-native";
import { useSafeAreaInsets } from "react-native-safe-area-context";

import { AppText as Text } from "../../components/ui/AppText";
import { Button } from "../../components/ui/Button";
import { FormSection } from "../../components/ui/FormSection";
import { Menu } from "../../components/ui/Menu";
import { ScreenScrollView } from "../../components/ui/ScreenScrollView";
import { SymbolView } from "../../components/ui/AppSymbol";
import { buildModelOptions, groupByProvider } from "../../lib/modelOptions";
import { resolveProviderOptionDescriptors } from "../../lib/providerOptions";
import { useEnvironmentServerConfig, useProjects } from "../../state/entities";
import { peerLinkEnvironment } from "../../state/peerLinks";
import { useEnvironmentQuery } from "../../state/query";
import { serverEnvironment } from "../../state/server";
import { useEnvironmentsWithScope, useEnvironmentScope } from "../../state/session";
import { useAtomCommand } from "../../state/use-atom-command";
import { ThreadSettingsPickerScreen } from "../threads/ThreadSettingsSheet";
import { useSettingsEnvironmentMenu } from "./components/SettingsEnvironmentFilterHeader";
import { SettingsScreen } from "./components/SettingsScreen";
import { useSettingsEnvironmentFilter, type SettingsTarget } from "./settings-environment-filter";
import { settingsTargetsForProject } from "./settings-environment-filter.logic";

type RoutingDraft = {
  readonly environmentId?: EnvironmentId | null;
  readonly projectId?: ProjectId | null;
  readonly modelSelection?: ModelSelection | null;
};

// Each settings page and its native model picker share drafts by environment.
const routingDraftsAtom = Atom.family((_editorId: string) =>
  Atom.make<ReadonlyMap<EnvironmentId, RoutingDraft>>(new Map()),
);

function changeRoutingDraft(
  drafts: ReadonlyMap<EnvironmentId, RoutingDraft>,
  environmentId: EnvironmentId,
  patch: RoutingDraft,
) {
  return new Map(drafts).set(environmentId, { ...drafts.get(environmentId), ...patch });
}

function routingDefaults(
  environment: SettingsTarget,
  projects: readonly EnvironmentProject[],
  providers: readonly ServerProvider[],
): AlwaysOnRoutingPreferences | null {
  const saved = environment.serverConfig.settings.alwaysOnRouting;
  if (!saved) return null;
  const { environmentId, projectId, harness, model, effort } = saved;
  const selection = resolveAlwaysOnSettingsModelSelection(
    saved,
    providers,
    projects.find((project) => project.id === projectId)?.defaultModelSelection,
  );
  return {
    ...(environmentId && environmentId !== environment.environmentId ? { environmentId } : {}),
    ...(projectId ? { projectId } : {}),
    ...(selection
      ? { modelSelection: selection }
      : {
          ...(harness === undefined ? {} : { harness }),
          ...(model === undefined ? {} : { model }),
          ...(effort === undefined ? {} : { effort }),
        }),
  };
}

function applyRoutingDraft(
  initial: AlwaysOnRoutingPreferences | null,
  draft: RoutingDraft | undefined,
): AlwaysOnRoutingPreferences | null {
  if (!draft) return initial;
  const environmentId =
    draft.environmentId === undefined ? initial?.environmentId : draft.environmentId;
  const projectId = draft.projectId === undefined ? initial?.projectId : draft.projectId;
  const selection =
    draft.modelSelection === undefined ? initial?.modelSelection : draft.modelSelection;
  const current = {
    ...(environmentId ? { environmentId } : {}),
    ...(projectId ? { projectId } : {}),
    ...(selection ? { modelSelection: selection } : {}),
    // Keep older fields until the user changes the model or the catalog can resolve it.
    ...(draft.modelSelection === undefined && !initial?.modelSelection
      ? {
          ...(initial?.harness === undefined ? {} : { harness: initial.harness }),
          ...(initial?.model === undefined ? {} : { model: initial.model }),
          ...(initial?.effort === undefined ? {} : { effort: initial.effort }),
        }
      : {}),
  };
  return Object.keys(current).length === 0 ? null : current;
}

function sameRoutingPreferences(
  left: AlwaysOnRoutingPreferences | null,
  right: AlwaysOnRoutingPreferences | null,
): boolean {
  if (left === null || right === null) return left === right;
  const lhsModel = left.modelSelection;
  const rhsModel = right.modelSelection;
  return (
    left.environmentId === right.environmentId &&
    left.projectId === right.projectId &&
    left.harness === right.harness &&
    left.model === right.model &&
    left.effort === right.effort &&
    (lhsModel && rhsModel ? modelSelectionsEqual(lhsModel, rhsModel) : lhsModel === rhsModel)
  );
}

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
  const providersByEnvironment = useAtomValue(
    useMemo(
      () =>
        Atom.make(
          (get) =>
            new Map(
              targets.map(({ environmentId }) => [
                environmentId,
                get(serverEnvironment.providersValueAtom(environmentId)),
              ]),
            ),
        ),
      [targets],
    ),
  );
  const editorId = useId();
  const [drafts, setDrafts] = useAtom(routingDraftsAtom(editorId));
  const canWrite = useEnvironmentsWithScope(targets, AuthSettingsWriteScope);
  const update = useAtomCommand(serverEnvironment.updateSettings);
  const [saving, setSaving] = useState(false);
  const [errors, setErrors] = useState<ReadonlyMap<EnvironmentId, string>>(new Map());
  const rows = targets.map((environment) => {
    const environmentProjects = projects.filter(
      (project) => project.environmentId === environment.environmentId,
    );
    const providers = providersByEnvironment.get(environment.environmentId) ?? [];
    const initial = routingDefaults(environment, environmentProjects, providers);
    return {
      environment,
      projects: environmentProjects.filter((project) => !project.archivedAt),
      providers,
      initial,
      current: applyRoutingDraft(initial, drafts.get(environment.environmentId)),
    };
  });
  const changes = rows.flatMap(({ environment, initial, current }) =>
    canWrite.has(environment.environmentId) && !sameRoutingPreferences(initial, current)
      ? [{ environmentId: environment.environmentId, preferences: current }]
      : [],
  );
  const persist = async (
    writes: readonly {
      environmentId: EnvironmentId;
      preferences: AlwaysOnRoutingPreferences | null;
    }[],
  ) => {
    if (saving || writes.length === 0) return;
    setSaving(true);
    const results = await Promise.all(
      writes.map(async ({ environmentId, preferences }) => {
        const result = await update({
          environmentId,
          input: { patch: { alwaysOnRouting: preferences } },
        });
        if (result._tag === "Failure") {
          const cause = squashAtomCommandFailure(result);
          return {
            environmentId,
            error: cause instanceof Error ? cause.message : gt("Could not save settings"),
          };
        }
        const saved = result.value.alwaysOnRouting ?? null;
        return {
          environmentId,
          error: sameRoutingPreferences(preferences, saved) ? null : gt("Could not save settings"),
        };
      }),
    ).finally(() => setSaving(false));
    setErrors((previous) => {
      const next = new Map(previous);
      for (const { environmentId, error } of results) {
        if (error === null) next.delete(environmentId);
        else next.set(environmentId, error);
      }
      return next;
    });
    setDrafts((previous) => {
      const next = new Map(previous);
      for (const { environmentId, error } of results) {
        if (error === null && previous.get(environmentId) === drafts.get(environmentId)) {
          next.delete(environmentId);
        }
      }
      return next;
    });
  };

  return (
    <SettingsScreen
      title={gt("Always On")}
      menus={[environmentMenu]}
      confirm={
        canWrite.size > 0 || saving
          ? {
              label: gt("Save"),
              disabled: saving || changes.length === 0,
              loading: saving,
              onPress: () => void persist(changes),
            }
          : undefined
      }
    >
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
              ? gt("Connect an environment to use Always On.")
              : gt("No environments match these filters. Change the filter above.")}
          </Text>
        ) : (
          rows.map(({ environment, projects, providers, initial, current }) => (
            <RoutingDefaults
              key={environment.environmentId}
              editorId={editorId}
              environment={environment}
              projects={projects}
              providers={providers}
              preferences={current}
              canWrite={canWrite.has(environment.environmentId)}
              saving={saving}
              error={errors.get(environment.environmentId)}
              onChange={(patch) =>
                setDrafts((previous) =>
                  changeRoutingDraft(previous, environment.environmentId, patch),
                )
              }
              onClear={
                initial === null
                  ? undefined
                  : () =>
                      void persist([
                        { environmentId: environment.environmentId, preferences: null },
                      ])
              }
            />
          ))
        )}
      </ScreenScrollView>
    </SettingsScreen>
  );
}

const ROUTING_DEFAULT_CHOICE = "__always-on-default__";

function routingChoice(choices: readonly Choice[], id: string | null) {
  return (
    choices.find((choice) => choice.id === (id ?? ROUTING_DEFAULT_CHOICE)) ??
    (id === null ? undefined : { id, title: id })
  );
}

function RoutingDefaults({
  editorId,
  environment,
  projects,
  providers,
  preferences,
  canWrite,
  saving,
  error,
  onChange,
  onClear,
}: {
  readonly editorId: string;
  readonly environment: SettingsTarget;
  readonly projects: readonly EnvironmentProject[];
  readonly providers: readonly ServerProvider[];
  readonly preferences: AlwaysOnRoutingPreferences | null;
  readonly canWrite: boolean;
  readonly saving: boolean;
  readonly error: string | undefined;
  readonly onChange: (patch: RoutingDraft) => void;
  readonly onClear: (() => void) | undefined;
}) {
  const gt = useGT();
  const navigation = useNavigation();
  const { environmentId, serverConfig } = environment;
  const canReadLinks = useEnvironmentScope(environmentId, AuthAccessReadScope);
  const links = useEnvironmentQuery(
    canReadLinks && offersLinkedEnvironments(serverConfig)
      ? peerLinkEnvironment.list({ environmentId, input: {} })
      : null,
  );
  const chosenDestination = preferences?.environmentId ?? null;
  const project = preferences?.projectId ?? null;
  const modelSelection = preferences?.modelSelection ?? null;
  const pendingLegacyModel =
    modelSelection === null &&
    (preferences?.harness !== undefined ||
      preferences?.model !== undefined ||
      preferences?.effort !== undefined);
  const label = modelSelection
    ? (providers
        .find((provider) => provider.instanceId === modelSelection.instanceId)
        ?.models.find((model) => model.slug === modelSelection.model)?.name ?? modelSelection.model)
    : ((pendingLegacyModel ? (preferences?.model ?? preferences?.harness) : undefined) ??
      gt("Automatic"));

  const peerChoices = (links.data?.links ?? [])
    .filter((peer) => peer.status !== "expired" && peer.access !== "read-only")
    .map((peer) => ({ id: peer.environmentId, title: peer.label }));
  const environments = [
    { id: ROUTING_DEFAULT_CHOICE, title: gt("This environment") },
    ...peerChoices,
  ];
  const projectChoices = [
    { id: ROUTING_DEFAULT_CHOICE, title: gt("None") },
    ...projects.map((project) => ({ id: project.id, title: project.title })),
  ];
  const unavailableDestination =
    chosenDestination !== null &&
    !links.isPending &&
    (!peerChoices.some((peer) => peer.id === chosenDestination) ||
      links.data?.links.some(
        (peer) => peer.environmentId === chosenDestination && peer.status === "unreachable",
      ));
  const disabled = !canWrite || saving;

  return (
    <FormSection title={gt("{environment} · Event routing", { environment: environment.label })}>
      <ChooseField
        label={gt("Default environment")}
        choices={environments}
        value={routingChoice(environments, chosenDestination)}
        disabled={disabled}
        onChange={(id) =>
          onChange({ environmentId: id === ROUTING_DEFAULT_CHOICE ? null : EnvironmentId.make(id) })
        }
      />
      {unavailableDestination ? (
        <Text className="text-foreground-muted">
          {gt("The selected linked environment is unavailable.")}
        </Text>
      ) : null}
      <Text className="text-foreground-muted">
        {gt("The following defaults apply when an event runs on this environment.")}
      </Text>
      <ChooseField
        label={gt("Project")}
        choices={projectChoices}
        value={routingChoice(projectChoices, project)}
        disabled={disabled}
        onChange={(id) =>
          onChange({ projectId: id === ROUTING_DEFAULT_CHOICE ? null : ProjectId.make(id) })
        }
      />
      <Pressable
        accessibilityRole="button"
        accessibilityLabel={gt("{label}, {value}", { label: gt("Model"), value: label })}
        disabled={disabled}
        className="min-h-10 flex-row items-center justify-between gap-3 active:opacity-70"
        onPress={() =>
          navigation.navigate("SettingsAlwaysOnModel", {
            editorId,
            environmentId,
            projectId: project,
            selection: modelSelection,
          })
        }
      >
        <Text>{gt("Model")}</Text>
        <Text numberOfLines={1} className="flex-1 text-right text-foreground-muted">
          {label}
        </Text>
        {disabled ? null : (
          <SymbolView
            name="chevron.right"
            size={14}
            tintColorClassName="accent-chevron"
            type="monochrome"
          />
        )}
      </Pressable>
      {(modelSelection !== null || pendingLegacyModel) && !disabled ? (
        <Button
          label={gt("Use project default")}
          tone="text"
          onPress={() => onChange({ modelSelection: null })}
        />
      ) : null}
      {error === undefined ? null : (
        <Text accessibilityRole="alert" className="text-danger">
          {error}
        </Text>
      )}
      {canWrite ? (
        onClear ? (
          <Button
            label={gt("Clear defaults")}
            tone="secondary"
            disabled={saving}
            onPress={onClear}
          />
        ) : null
      ) : (
        <Text className="text-foreground-muted">{gt("This connection has read-only access.")}</Text>
      )}
    </FormSection>
  );
}

type AlwaysOnModelPickerParams = {
  readonly editorId: string;
  readonly environmentId: EnvironmentId;
  readonly projectId: ProjectId | null;
  readonly selection: ModelSelection | null;
};

export function AlwaysOnModelPickerRouteScreen({
  route,
}: StaticScreenProps<AlwaysOnModelPickerParams>) {
  const gt = useGT();
  const navigation = useNavigation();
  const { editorId, environmentId, projectId, selection } = route.params;
  const canWrite = useEnvironmentScope(environmentId, AuthSettingsWriteScope);
  const config = useEnvironmentServerConfig(environmentId);
  const [drafts, setDrafts] = useAtom(routingDraftsAtom(editorId));
  const modelDraft = drafts.get(environmentId)?.modelSelection;
  const selectedModel = modelDraft === undefined ? selection : modelDraft;
  const providers = (config?.providers ?? []).filter(
    (provider) =>
      provider.availability !== "unavailable" &&
      provider.supportedRuntimeModes?.includes("approval-required") !== false,
  );
  const models = buildModelOptions(
    config ? { ...config, providers } : null,
    selectedModel,
    undefined,
    projectId,
  );
  const selectedOption = models.find(
    (option) =>
      option.providerKey === selectedModel?.instanceId &&
      option.selection.model === selectedModel.model,
  );
  const optionDescriptors = resolveProviderOptionDescriptors({
    capabilities: selectedOption?.capabilities,
    selections: selectedModel?.options,
  });
  const providerGroups = groupByProvider(
    models.filter((option) =>
      providers.some((provider) => provider.instanceId === option.providerKey),
    ),
  );

  if (!canWrite) {
    return (
      <SettingsScreen title={gt("Model")}>
        <Text className="p-5 text-foreground-muted">
          {gt("This connection has read-only access.")}
        </Text>
      </SettingsScreen>
    );
  }

  return (
    <ThreadSettingsPickerScreen
      environmentId={environmentId}
      projectId={projectId}
      cwd={null}
      providerGroups={providerGroups}
      selectedModel={selectedModel}
      modelOnly
      prompt=""
      onPromptChange={() => undefined}
      onSelectModel={(option) =>
        setDrafts((previous) =>
          changeRoutingDraft(previous, environmentId, { modelSelection: option.selection }),
        )
      }
      optionDescriptors={optionDescriptors}
      onUpdateOptionSelections={(options) =>
        setDrafts((previous) => {
          const current = previous.get(environmentId)?.modelSelection;
          const model = current === undefined ? selection : current;
          return model
            ? changeRoutingDraft(previous, environmentId, { modelSelection: { ...model, options } })
            : previous;
        })
      }
      runtimeMode="approval-required"
      onUpdateRuntimeMode={() => undefined}
      onClose={() => navigation.goBack()}
    />
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
  if (choices.length === 0 || (choices.length === 1 && choices[0]?.id === value?.id) || disabled) {
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
