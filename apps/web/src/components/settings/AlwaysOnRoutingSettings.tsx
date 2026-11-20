import { squashAtomCommandFailure } from "@t3tools/client-runtime/state/runtime";
import {
  AuthAccessReadScope,
  AuthSettingsWriteScope,
  EnvironmentId,
  ProjectId,
  type AlwaysOnRoutingPreferences,
  type ModelSelection,
} from "@t3tools/contracts";
import {
  createModelSelection,
  modelSelectionsEqual,
  pickModelSelection,
  resolveAlwaysOnSettingsModelSelection,
} from "@t3tools/shared/model";
import { useGT, useMessages } from "gt-react";
import { useState } from "react";

import { useClientSettings, useEnvironmentSettings } from "../../hooks/useSettings";
import { getCustomModelOptionsByInstance } from "../../modelSelection";
import { providerInstanceEntriesFor } from "../../providerInstances";
import { useProjects } from "../../state/entities";
import { useEnvironmentProviders, type EnvironmentPresentation } from "../../state/environments";
import { peerLinkEnvironment } from "../../state/peerLinks";
import { useEnvironmentQuery } from "../../state/query";
import { serverEnvironment } from "../../state/server";
import { useEnvironmentScope } from "../../state/session";
import { useAtomCommand } from "../../state/use-atom-command";
import {
  ComposerEffortColumn,
  describeComposerEffort,
  ModelPickerSpeedControls,
} from "../chat/ComposerModelEffort";
import { ProviderModelPicker } from "../chat/ProviderModelPicker";
import type { TraitsSelectionInput } from "../chat/TraitsPicker";
import { Button } from "../ui/button";
import { SETTINGS_PICKER_TRIGGER_CLASSNAME } from "./settingsLayout";

const SELECT_CLASS_NAME = "border-input bg-background h-8 w-full rounded-md border px-2 text-sm";

function SelectField({
  title,
  value,
  none,
  options,
  onChange,
  disabled,
  required,
}: {
  readonly title: string;
  readonly value: string;
  readonly none: string;
  readonly options: ReadonlyArray<{ readonly value: string; readonly label: string }>;
  readonly onChange: (value: string) => void;
  readonly disabled?: boolean;
  readonly required?: boolean;
}) {
  const gt = useGT();
  return (
    <label className="grid gap-1 text-sm">
      <span>{title}</span>
      <select
        className={SELECT_CLASS_NAME}
        value={value}
        disabled={disabled}
        required={required}
        onChange={(event) => onChange(event.target.value)}
      >
        <option value="">{none}</option>
        {value !== "" && !options.some((option) => option.value === value) ? (
          <option value={value} disabled>
            {gt("Unavailable")} · {value}
          </option>
        ) : null}
        {options.map((option) => (
          <option key={option.value} value={option.value}>
            {option.label}
          </option>
        ))}
      </select>
    </label>
  );
}

export function AlwaysOnRoutingSettings({
  environment,
}: {
  readonly environment: EnvironmentPresentation;
}) {
  const saved = environment.serverConfig?.settings.alwaysOnRouting ?? null;
  return (
    <RoutingForm
      key={JSON.stringify([environment.environmentId, saved])}
      environmentId={environment.environmentId}
      environmentLabel={environment.label}
      saved={saved}
    />
  );
}

function RoutingForm({
  environmentId,
  environmentLabel,
  saved,
}: {
  readonly environmentId: EnvironmentId;
  readonly environmentLabel: string;
  readonly saved: AlwaysOnRoutingPreferences | null;
}) {
  const gt = useGT();
  const messages = useMessages();
  const canReadLinks = useEnvironmentScope(environmentId, AuthAccessReadScope);
  const canWrite = useEnvironmentScope(environmentId, AuthSettingsWriteScope);
  const effortDisplay = useClientSettings((settings) => settings.effortDisplay);
  const settings = useEnvironmentSettings(environmentId);
  const peers = useEnvironmentQuery(
    canReadLinks ? peerLinkEnvironment.list({ environmentId, input: {} }) : null,
  );
  const projects = useProjects().filter(
    (project) => project.environmentId === environmentId && project.archivedAt === null,
  );
  const providers = (useEnvironmentProviders().get(environmentId) ?? []).filter(
    (provider) =>
      provider.enabled &&
      provider.installed &&
      provider.status !== "disabled" &&
      provider.auth.status !== "unauthenticated" &&
      provider.availability !== "unavailable" &&
      (provider.supportedRuntimeModes?.includes("approval-required") ?? true),
  );
  const updateSettings = useAtomCommand(serverEnvironment.updateSettings);
  const [destination, setDestination] = useState(
    saved?.environmentId === environmentId ? "" : (saved?.environmentId ?? ""),
  );
  const [project, setProject] = useState<string>(saved?.projectId ?? "");
  const [modelDraft, setModelDraft] = useState<ModelSelection | null>();
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const savedProject = projects.find((candidate) => candidate.id === saved?.projectId);
  const selectedProject = projects.find((candidate) => candidate.id === project);
  const savedModelSelection = resolveAlwaysOnSettingsModelSelection(
    saved,
    providers,
    savedProject?.defaultModelSelection,
  );
  const modelSelection = modelDraft === undefined ? savedModelSelection : modelDraft;
  // A model catalog can arrive after the saved route. Keep the earlier fields until
  // the selected model and any saved effort can be displayed in the picker.
  const keepLegacyModel =
    modelDraft === undefined &&
    saved !== null &&
    saved.modelSelection === undefined &&
    ((savedModelSelection === null &&
      (saved.harness !== undefined || saved.model !== undefined || saved.effort !== undefined)) ||
      (saved.effort !== undefined &&
        !savedModelSelection?.options?.some((option) => option.value === saved.effort)));
  const routingModel = keepLegacyModel
    ? {
        ...(saved.harness ? { harness: saved.harness } : {}),
        ...(saved.model ? { model: saved.model } : {}),
        ...(saved.effort ? { effort: saved.effort } : {}),
      }
    : modelSelection
      ? { modelSelection }
      : {};
  const shownSelection = modelSelection ?? selectedProject?.defaultModelSelection ?? null;
  const entries = providerInstanceEntriesFor(providers, settings, selectedProject?.id ?? null);
  const activeEntry = entries.find((entry) => entry.instanceId === shownSelection?.instanceId);
  const activeInstanceId = shownSelection?.instanceId ?? entries[0]?.instanceId;
  const modelOptions = getCustomModelOptionsByInstance(
    settings,
    providers,
    activeInstanceId,
    shownSelection?.model,
  );
  const disabled = !canWrite || busy;
  const traits: TraitsSelectionInput | null =
    activeEntry && shownSelection
      ? {
          provider: activeEntry.driverKind,
          instanceId: shownSelection.instanceId,
          models: activeEntry.models,
          model: shownSelection.model,
          modelOptions: shownSelection.options,
          allowPromptInjectedEffort: false,
          onModelOptionsChange: (options) => {
            if (disabled) return;
            setModelDraft(
              createModelSelection(shownSelection.instanceId, shownSelection.model, options),
            );
          },
        }
      : null;
  const effort = describeComposerEffort(
    modelSelection ? traits : null,
    effortDisplay,
    "sm",
    messages,
  );

  async function save(alwaysOnRouting: AlwaysOnRoutingPreferences | null) {
    if (!canWrite || busy) return;
    setBusy(true);
    setError(null);
    const result = await updateSettings({
      environmentId,
      input: { patch: { alwaysOnRouting } },
    }).finally(() => setBusy(false));
    if (result._tag === "Failure") {
      const cause = squashAtomCommandFailure(result);
      setError(cause instanceof Error ? cause.message : String(cause));
      return;
    }
    const persisted = result.value.alwaysOnRouting ?? null;
    const expectedModel = alwaysOnRouting?.modelSelection;
    const savedModel = persisted?.modelSelection;
    if (
      (alwaysOnRouting === null) !== (persisted === null) ||
      alwaysOnRouting?.environmentId !== persisted?.environmentId ||
      alwaysOnRouting?.projectId !== persisted?.projectId ||
      alwaysOnRouting?.harness !== persisted?.harness ||
      alwaysOnRouting?.model !== persisted?.model ||
      alwaysOnRouting?.effort !== persisted?.effort ||
      (expectedModel && savedModel
        ? !modelSelectionsEqual(expectedModel, savedModel)
        : expectedModel !== savedModel)
    ) {
      setError(gt("Could not save event routing."));
    }
  }

  return (
    <form
      className="grid gap-3"
      onSubmit={(event) => {
        event.preventDefault();
        void save({
          ...(destination ? { environmentId: EnvironmentId.make(destination) } : {}),
          ...(project ? { projectId: ProjectId.make(project) } : {}),
          ...routingModel,
        });
      }}
    >
      <p className="text-muted-foreground text-sm">
        {gt("Default route for events received by {environment}.", {
          environment: environmentLabel,
        })}
      </p>
      <fieldset disabled={disabled} className="grid gap-3">
        <SelectField
          title={gt("Default destination")}
          value={destination}
          none={gt("This environment")}
          options={(peers.data?.links ?? [])
            .filter((link) => link.access !== "read-only" && link.status !== "expired")
            .map((link) => ({ value: link.environmentId, label: link.label }))}
          onChange={setDestination}
        />
        <p className="text-muted-foreground text-sm">
          {gt(
            "The project and model apply to runs on this environment. Set those defaults on the destination for remote runs.",
          )}
        </p>
        <div className="grid gap-3 sm:grid-cols-2">
          <SelectField
            title={gt("Project")}
            value={project}
            none={gt("None")}
            options={projects.map((candidate) => ({ value: candidate.id, label: candidate.title }))}
            onChange={setProject}
            required={destination === ""}
          />
          <div className="grid min-w-0 gap-1 text-sm">
            <span>{gt("Model")}</span>
            {activeInstanceId && entries.length > 0 ? (
              <ProviderModelPicker
                environmentId={environmentId}
                workspaceCwd={selectedProject?.workspaceRoot ?? null}
                activeInstanceId={activeInstanceId}
                model={shownSelection?.model ?? ""}
                lockedProvider={null}
                instanceEntries={entries}
                modelOptionsByInstance={modelOptions}
                disabled={disabled}
                isComposerOwned={false}
                triggerClassName={SETTINGS_PICKER_TRIGGER_CLASSNAME}
                {...(modelSelection === null
                  ? { triggerLabel: keepLegacyModel ? gt("Unavailable") : gt("Automatic") }
                  : {})}
                triggerSuffix={effort.suffix}
                triggerDetail={effort.detail}
                effortColumn={<ComposerEffortColumn traits={traits} />}
                footer={
                  <div className="flex flex-wrap items-center gap-2 border-t border-border/70 bg-muted/40 px-3 py-2">
                    <ModelPickerSpeedControls traits={traits} />
                    <Button
                      type="button"
                      variant="ghost"
                      size="sm"
                      disabled={disabled || (modelSelection === null && !keepLegacyModel)}
                      onClick={() => setModelDraft(null)}
                    >
                      {gt("Automatic")}
                    </Button>
                  </div>
                }
                onInstanceModelChange={(instanceId, model) =>
                  setModelDraft(pickModelSelection(shownSelection, instanceId, model))
                }
              />
            ) : (
              <span className="text-muted-foreground">{gt("No providers available")}</span>
            )}
          </div>
        </div>
        <div className="flex flex-wrap gap-2">
          <Button type="submit">{busy ? gt("Saving…") : gt("Save")}</Button>
          <Button
            type="button"
            variant="outline"
            disabled={saved === null}
            onClick={() => void save(null)}
          >
            {gt("Disable event routing")}
          </Button>
        </div>
      </fieldset>
      {error ? (
        <p role="alert" className="text-destructive text-sm">
          {error}
        </p>
      ) : null}
    </form>
  );
}
