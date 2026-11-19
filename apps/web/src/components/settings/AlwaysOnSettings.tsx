import {
  AlwaysOnAgentId,
  AlwaysOnApprovalId,
  AlwaysOnMemoryId,
  AlwaysOnRunId,
  ProjectId,
  type AlwaysOnAgentSummary,
  type AlwaysOnDetail,
  type AlwaysOnProviderPolicy,
  type EnvironmentId,
} from "@t3tools/contracts";
import * as SecureRandom from "effect/crypto/SecureRandom";
import { useAtomValue } from "@effect/atom-react";
import { AsyncResult } from "effect/reactivity";
import { useGT } from "gt-react";
import { InfinityIcon } from "lucide-react";
import { useState } from "react";

import { environmentAtomList, environmentIdsKey } from "../../state/environmentAtomList";
import { serverEnvironment } from "../../state/server";
import { useAtomCommand } from "../../state/use-atom-command";
import { Button } from "../ui/button";
import { Input } from "../ui/input";
import { useSettingsScope } from "./SettingsScopeContext";
import { SettingsPageContainer, SettingsSection } from "./settingsLayout";

const alwaysOnListAtom = environmentAtomList("web-always-on-list", (environmentId) =>
  serverEnvironment.alwaysOnLive({ environmentId, input: {} }),
);

export function AlwaysOnSettings() {
  const gt = useGT();
  const { scope, environments } = useSettingsScope();
  const connected = environments.filter(
    (environment) =>
      environment.connection.phase === "connected" && environment.serverConfig !== null,
  );
  const environment = connected[0];
  const queries = useAtomValue(
    alwaysOnListAtom(environmentIdsKey(connected.map((item) => item.environmentId))),
  );
  const snapshot =
    environment === undefined
      ? null
      : AsyncResult.getOrElse(
          queries[connected.indexOf(environment)] ?? AsyncResult.initial(),
          () => null,
        );

  return (
    <SettingsPageContainer>
      <SettingsSection title={gt("Always-on agents")} icon={<InfinityIcon />}>
        <p className="text-muted-foreground text-sm">
          {gt(
            "A natural-language event wakes one short model call. The server accepts only schema-checked actions. Text generators send no tools. Claude text generation is called with an empty tool list. Other runtimes stay disabled.",
          )}
        </p>
        {environment === undefined || snapshot === null ? (
          <p className="text-sm">{gt("Connect an environment to manage always-on agents.")}</p>
        ) : (
          <AlwaysOnEnvironment
            environmentId={environment.environmentId}
            projectId={
              scope.members.find((member) => member.environmentId === environment.environmentId)?.id
            }
            agents={snapshot.agents}
            providers={snapshot.providers}
          />
        )}
      </SettingsSection>
    </SettingsPageContainer>
  );
}

function AlwaysOnEnvironment({
  environmentId,
  projectId,
  agents,
  providers,
}: {
  readonly environmentId: EnvironmentId;
  readonly projectId: ProjectId | undefined;
  readonly agents: readonly AlwaysOnAgentSummary[];
  readonly providers: readonly AlwaysOnProviderPolicy[];
}) {
  const gt = useGT();
  const canOperate = useAtomValue(
    serverEnvironment.createAlwaysOnAgent.permissionAtom(environmentId),
  );
  const create = useAtomCommand(serverEnvironment.createAlwaysOnAgent, "always-on create");
  const [title, setTitle] = useState("");
  const [instructions, setInstructions] = useState("");
  const [project, setProject] = useState(projectId ?? "");
  const [providerKind, setProviderKind] = useState<"textGenerator" | "claudeAgent">(
    "textGenerator",
  );
  const [instanceId, setInstanceId] = useState("");
  const [model, setModel] = useState("");
  const [selected, setSelected] = useState<AlwaysOnAgentId | null>(agents[0]?.id ?? null);
  const [error, setError] = useState<string | null>(null);

  return (
    <div className="space-y-4">
      <form
        className="space-y-2"
        onSubmit={(event) => {
          event.preventDefault();
          if (!canOperate) return;
          void create({
            environmentId,
            input: {
              projectId: ProjectId.make(project.trim()),
              title: title.trim(),
              instructions: instructions.trim(),
              providerKind,
              instanceId: instanceId.trim(),
              model: model.trim(),
            },
          }).then((result) => {
            if (result._tag === "Failure") {
              setError(gt("Could not create the agent."));
              return;
            }
            setSelected(result.value.agent.id);
            setTitle("");
            setError(null);
          });
        }}
      >
        <Input
          aria-label={gt("Agent title")}
          value={title}
          placeholder={gt("Agent title")}
          onChange={(event) => setTitle(event.target.value)}
        />
        <Input
          aria-label={gt("Project id")}
          value={project}
          placeholder={gt("Project id")}
          onChange={(event) => setProject(event.target.value)}
        />
        <textarea
          aria-label={gt("Instructions")}
          className="border-input bg-background min-h-24 w-full rounded-md border px-3 py-2 text-sm"
          value={instructions}
          placeholder={gt("Instructions loaded at the start of every run")}
          onChange={(event) => setInstructions(event.target.value)}
        />
        <label className="text-sm">
          {gt("Model runtime")}
          <select
            className="border-input ms-2 rounded-md border bg-transparent px-2 py-1"
            value={providerKind}
            onChange={(event) =>
              setProviderKind(
                event.target.value === "claudeAgent" ? "claudeAgent" : "textGenerator",
              )
            }
          >
            <option value="textGenerator">{gt("Text generator, no tools")}</option>
            <option value="claudeAgent">{gt("Claude, empty tool list")}</option>
          </select>
        </label>
        <Input
          aria-label={gt("Provider instance id")}
          value={instanceId}
          placeholder={gt("Provider instance id")}
          onChange={(event) => setInstanceId(event.target.value)}
        />
        <Input
          aria-label={gt("Model")}
          value={model}
          placeholder={gt("Model")}
          onChange={(event) => setModel(event.target.value)}
        />
        <ul className="text-muted-foreground text-sm">
          {providers
            .filter((provider) => !provider.supported)
            .map((provider) => (
              <li key={provider.id}>
                {provider.id}: {provider.detail}
              </li>
            ))}
        </ul>
        <Button
          type="submit"
          disabled={
            !canOperate ||
            title.trim() === "" ||
            instructions.trim() === "" ||
            project.trim() === "" ||
            instanceId.trim() === "" ||
            model.trim() === ""
          }
        >
          {gt("Create agent")}
        </Button>
      </form>
      {error !== null ? <p className="text-destructive text-sm">{error}</p> : null}
      {agents.length === 0 ? (
        <p className="text-sm">{gt("No always-on agents yet.")}</p>
      ) : (
        <div className="flex flex-wrap gap-2">
          {agents.map((agent) => (
            <Button
              key={agent.id}
              type="button"
              variant={agent.id === selected ? "default" : "outline"}
              onClick={() => setSelected(agent.id)}
            >
              {agent.title} · {agent.phase}
            </Button>
          ))}
        </div>
      )}
      {selected !== null ? (
        <AlwaysOnAgentDetail
          environmentId={environmentId}
          agentId={selected}
          canOperate={canOperate}
        />
      ) : null}
    </div>
  );
}

function AlwaysOnAgentDetail({
  environmentId,
  agentId,
  canOperate,
}: {
  readonly environmentId: EnvironmentId;
  readonly agentId: AlwaysOnAgentId;
  readonly canOperate: boolean;
}) {
  const gt = useGT();
  const detailQuery = useAtomValue(
    serverEnvironment.alwaysOnDetail({ environmentId, input: { agentId } }),
  );
  const detail = AsyncResult.getOrElse(detailQuery, () => null);
  const setEnabled = useAtomCommand(serverEnvironment.setAlwaysOnEnabled, "always-on enabled");
  const submit = useAtomCommand(serverEnvironment.submitAlwaysOnEvent, "always-on submit");
  const resolve = useAtomCommand(serverEnvironment.resolveAlwaysOnApproval, "always-on approval");
  const confirm = useAtomCommand(serverEnvironment.confirmAlwaysOnUnknown, "always-on confirm");
  const cancel = useAtomCommand(serverEnvironment.cancelAlwaysOnRun, "always-on cancel");
  const readMemory = useAtomCommand(serverEnvironment.readAlwaysOnMemory, "always-on memory");
  const [memory, setMemory] = useState<string | null>(null);
  const [text, setText] = useState("");
  const [kind, setKind] = useState<"user_reply" | "external">("external");

  if (detail === null) return <p className="text-sm">{gt("Loading agent")}</p>;

  const send = () => {
    if (!canOperate || text.trim() === "") return;
    void submit({
      environmentId,
      input: {
        agentId,
        source: "settings",
        sourceEventId: `ui:${SecureRandom.uuidV4Unsafe()}`,
        kind,
        text: text.trim(),
      },
    });
    setText("");
  };

  return (
    <div className="space-y-3">
      <div className="flex flex-wrap gap-2">
        <Button
          type="button"
          variant="outline"
          disabled={!canOperate}
          onClick={() =>
            void setEnabled({ environmentId, input: { agentId, enabled: !detail.agent.enabled } })
          }
        >
          {detail.agent.enabled ? gt("Pause") : gt("Resume")}
        </Button>
        {detail.agent.phase === "needs_confirmation" ? (
          <Button
            type="button"
            disabled={!canOperate}
            onClick={() => void confirm({ environmentId, input: { agentId } })}
          >
            {gt("Confirm unknown effect")}
          </Button>
        ) : null}
      </div>
      <p className="text-sm">
        {gt("Phase")}: {detail.agent.phase}. {gt("Pending events")}:{" "}
        {detail.agent.pendingInboxCount}.
      </p>
      <div className="flex flex-wrap gap-2">
        <label className="text-sm">
          {gt("Wake as")}
          <select
            className="border-input ms-2 rounded-md border bg-transparent px-2 py-1"
            value={kind}
            onChange={(event) =>
              setKind(event.target.value === "user_reply" ? "user_reply" : "external")
            }
          >
            <option value="external">{gt("New session")}</option>
            <option value="user_reply">{gt("Continue user session")}</option>
          </select>
        </label>
      </div>
      <textarea
        aria-label={gt("Event")}
        className="border-input bg-background min-h-20 w-full rounded-md border px-3 py-2 text-sm"
        value={text}
        placeholder={gt("Tell the agent what happened")}
        onChange={(event) => setText(event.target.value)}
      />
      <Button type="button" disabled={!canOperate || text.trim() === ""} onClick={send}>
        {gt("Queue event")}
      </Button>
      <RecordList
        title={gt("Inbox")}
        rows={detail.inbox.map((event) => `${event.kind} ${event.status} ${event.sourceEventId}`)}
      />
      <RecordList
        title={gt("Runs")}
        rows={detail.runs.map(
          (run) =>
            `${run.status} ${run.modelSummary ?? run.modelError ?? ""} ${run.executedActions.map((action) => action.type).join(", ")}`,
        )}
      />
      {detail.runs
        .filter((run) => run.status === "running" || run.status === "awaiting_approval")
        .map((run) => (
          <Button
            key={run.id}
            type="button"
            variant="outline"
            disabled={!canOperate}
            onClick={() =>
              void cancel({ environmentId, input: { agentId, runId: AlwaysOnRunId.make(run.id) } })
            }
          >
            {gt("Cancel run")}
          </Button>
        ))}
      <RecordList
        title={gt("Memory")}
        rows={detail.memoryIndex.map((entry) => `${entry.kind} ${entry.title}`)}
      />
      <div className="flex flex-wrap gap-2">
        {detail.memoryIndex.map((entry) => (
          <Button
            key={entry.id}
            type="button"
            variant="outline"
            onClick={() =>
              void readMemory({
                environmentId,
                input: { agentId, memoryId: AlwaysOnMemoryId.make(entry.id) },
              }).then((result) => {
                if (result._tag === "Success") {
                  setMemory(result.value.body ?? result.value.pointer ?? result.value.title);
                }
              })
            }
          >
            {gt("Read")} {entry.title}
          </Button>
        ))}
      </div>
      {memory !== null ? <p className="text-sm">{memory}</p> : null}
      {detail.approvals
        .filter((approval) => approval.status === "pending")
        .map((approval) => (
          <div key={approval.id} className="flex flex-wrap items-center gap-2">
            <span className="text-sm">{approval.summary}</span>
            <Button
              type="button"
              disabled={!canOperate}
              onClick={() =>
                void resolve({
                  environmentId,
                  input: {
                    agentId,
                    approvalId: AlwaysOnApprovalId.make(approval.id),
                    decision: "approved",
                  },
                })
              }
            >
              {gt("Approve")}
            </Button>
            <Button
              type="button"
              variant="outline"
              disabled={!canOperate}
              onClick={() =>
                void resolve({
                  environmentId,
                  input: {
                    agentId,
                    approvalId: AlwaysOnApprovalId.make(approval.id),
                    decision: "rejected",
                  },
                })
              }
            >
              {gt("Reject")}
            </Button>
          </div>
        ))}
      <RecordList
        title={gt("Deliveries")}
        rows={detail.deliveries.map((delivery) => `${delivery.status} ${delivery.body}`)}
      />
      <TimerList detail={detail} />
    </div>
  );
}

function RecordList({ title, rows }: { readonly title: string; readonly rows: readonly string[] }) {
  return (
    <div>
      <h3 className="text-sm font-medium">{title}</h3>
      {rows.length === 0 ? null : (
        <ul className="text-sm">
          {rows.map((row) => (
            <li key={row}>{row}</li>
          ))}
        </ul>
      )}
    </div>
  );
}

function TimerList({ detail }: { readonly detail: AlwaysOnDetail }) {
  const gt = useGT();
  return (
    <RecordList
      title={gt("Timers")}
      rows={detail.timers.map((timer) => `${timer.kind} ${timer.status} ${timer.dueAt}`)}
    />
  );
}
