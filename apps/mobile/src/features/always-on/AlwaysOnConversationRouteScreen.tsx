import { useAtomValue } from "@effect/atom-react";
import { useNavigation, type StaticScreenProps } from "@react-navigation/native";
import {
  buildAlwaysOnTimeline,
  type AlwaysOnTimelineItem,
} from "@t3tools/client-runtime/always-on-conversation";
import {
  squashAtomCommandFailure,
  type AtomCommandResult,
} from "@t3tools/client-runtime/state/runtime";
import {
  AuthOrchestrationReadScope,
  type AlwaysOnAgentId,
  type AlwaysOnApprovalEntry,
  type AlwaysOnDeliveryEntry,
  type AlwaysOnEventKind,
  type AlwaysOnRunStatus,
  type EnvironmentId,
} from "@t3tools/contracts";
import { msg, useGT, useLocale, useMessages } from "gt-react-native";
import { useRef, useState } from "react";
import { Pressable, TextInput, View, type ScrollViewInstance } from "react-native";
import { KeyboardAvoidingView } from "react-native-keyboard-controller";
import { useSafeAreaInsets } from "react-native-safe-area-context";

import { AppActivityIndicator } from "../../components/ui/AppActivityIndicator";
import { AppBar } from "../../components/ui/AppBar";
import { AppText as Text } from "../../components/ui/AppText";
import { Button } from "../../components/ui/Button";
import { ScreenContent } from "../../components/ui/ScreenContent";
import { ScreenScrollView } from "../../components/ui/ScreenScrollView";
import { uuidv4 } from "../../lib/uuid";
import { useEnvironments } from "../../state/environments";
import { useEnvironmentQuery } from "../../state/query";
import { serverEnvironment } from "../../state/server";
import { useEnvironmentScope, useEnvironmentScopeDenied } from "../../state/session";
import { useAtomCommand } from "../../state/use-atom-command";
import { ALWAYS_ON_PHASE_LABELS } from "./AlwaysOnWorkspace";

const RUN_LABELS: Record<AlwaysOnRunStatus, string> = {
  running: msg("Running"),
  completed: msg("Completed"),
  awaiting_approval: msg("Approval requested"),
  interrupted: msg("Interrupted"),
  unknown: msg("Outcome unknown"),
  cancelled: msg("Cancelled"),
  failed: msg("Failed"),
};

const EVENT_LABELS: Record<AlwaysOnEventKind, string> = {
  approval: msg("Approval decision"),
  external: msg("External event"),
  system: msg("System event"),
  timer: msg("Timer"),
  user_reply: msg("Your message"),
};

const EVENT_STATES = {
  pending: msg("Queued"),
  running: msg("Processing"),
  processed: msg("Processed"),
  cancelled: msg("Cancelled"),
};

const APPROVAL_STATES = {
  pending: msg("Pending approval"),
  approved: msg("Approved"),
  rejected: msg("Rejected"),
  cancelled: msg("Cancelled"),
};

type ConversationProps = {
  readonly environmentId: EnvironmentId;
  readonly agentId: AlwaysOnAgentId;
};

export function AlwaysOnConversationRouteScreen({ route }: StaticScreenProps<ConversationProps>) {
  const { environmentId, agentId } = route.params;
  return <Conversation key={JSON.stringify([environmentId, agentId])} {...route.params} />;
}

function Conversation({ environmentId, agentId }: ConversationProps) {
  const gt = useGT();
  const messages = useMessages();
  const locale = useLocale();
  const navigation = useNavigation();
  const insets = useSafeAreaInsets();
  const { environments } = useEnvironments();
  const environment = environments.find((entry) => entry.environmentId === environmentId);
  const connected = environment?.connection.phase === "connected";
  const canRead = useEnvironmentScope(environmentId, AuthOrchestrationReadScope);
  const readDenied = useEnvironmentScopeDenied(environmentId, AuthOrchestrationReadScope);
  const canOperate =
    useAtomValue(serverEnvironment.resolveAlwaysOnApproval.permissionAtom(environmentId)) &&
    connected;
  const query = useEnvironmentQuery(
    connected && canRead
      ? serverEnvironment.alwaysOnDetail({ environmentId, input: { agentId } })
      : null,
  );
  const detail = query.data;
  const timeline = detail === null ? [] : buildAlwaysOnTimeline(detail);
  const date = new Intl.DateTimeFormat(locale, { dateStyle: "medium", timeStyle: "short" });
  const submit = useAtomCommand(serverEnvironment.submitAlwaysOnEvent);
  const setEnabled = useAtomCommand(serverEnvironment.setAlwaysOnEnabled);
  const resolve = useAtomCommand(serverEnvironment.resolveAlwaysOnApproval);
  const confirm = useAtomCommand(serverEnvironment.confirmAlwaysOnUnknown);
  const cancel = useAtomCommand(serverEnvironment.cancelAlwaysOnRun);
  const [draft, setDraft] = useState("");
  const [sending, setSending] = useState(false);
  const [sendError, setSendError] = useState<string | null>(null);
  const [busy, setBusy] = useState<string | null>(null);
  const [actionError, setActionError] = useState<string | null>(null);
  const sendLock = useRef(false);
  const actionLock = useRef(false);
  const submission = useRef<{ readonly id: string; readonly text: string } | null>(null);
  const scroll = useRef<ScrollViewInstance>(null);
  const followTail = useRef(true);

  const send = async () => {
    const text = draft.trim();
    if (!canOperate || sendLock.current || text === "") return;
    sendLock.current = true;
    setSending(true);
    setSendError(null);
    const previous = submission.current;
    const id = previous?.text === text ? previous.id : uuidv4();
    submission.current = { id, text };
    const result = await submit({
      environmentId,
      input: { agentId, source: "chats", sourceEventId: id, kind: "user_reply", text },
    });
    sendLock.current = false;
    setSending(false);
    if (result._tag === "Success") {
      submission.current = null;
      followTail.current = true;
      setDraft("");
      scroll.current?.scrollToEnd({ animated: true });
    } else {
      const error = squashAtomCommandFailure(result);
      setSendError(
        error instanceof Error && error.message
          ? error.message
          : gt("Could not send your message."),
      );
    }
  };

  const runAction = async (
    key: string,
    action: () => Promise<AtomCommandResult<unknown, unknown>>,
  ) => {
    if (!canOperate || actionLock.current) return;
    actionLock.current = true;
    setBusy(key);
    setActionError(null);
    const result = await action();
    actionLock.current = false;
    setBusy(null);
    if (result._tag === "Failure") {
      const error = squashAtomCommandFailure(result);
      setActionError(
        error instanceof Error && error.message ? error.message : gt("Could not update the agent."),
      );
    }
  };
  const canAct = canOperate && busy === null && query.error === null && !query.isPending;
  const approvalCard = (approval: AlwaysOnApprovalEntry) => (
    <View key={approval.id} className="gap-2 rounded-xl bg-subtle px-3 py-2">
      <Text textRole="footnote" className="font-t3-medium text-foreground-muted">
        {messages(APPROVAL_STATES[approval.status])}
      </Text>
      <Text selectable>{approval.summary}</Text>
      {approval.status === "pending" ? (
        <View className="flex-row flex-wrap gap-2">
          <Button
            label={gt("Approve")}
            tone="primary"
            size="compact"
            disabled={!canAct}
            loading={busy === `${approval.id}:approved`}
            onPress={() =>
              void runAction(`${approval.id}:approved`, () =>
                resolve({
                  environmentId,
                  input: { agentId, approvalId: approval.id, decision: "approved" },
                }),
              )
            }
          />
          <Button
            label={gt("Reject")}
            size="compact"
            disabled={!canAct}
            loading={busy === `${approval.id}:rejected`}
            onPress={() =>
              void runAction(`${approval.id}:rejected`, () =>
                resolve({
                  environmentId,
                  input: { agentId, approvalId: approval.id, decision: "rejected" },
                }),
              )
            }
          />
        </View>
      ) : null}
    </View>
  );

  const agent = detail?.agent;
  const pauseLabel = agent?.enabled ? gt("Pause") : gt("Resume");
  return (
    <KeyboardAvoidingView automaticOffset behavior="height" className="flex-1 bg-header">
      <AppBar
        title={agent?.title ?? gt("Always On")}
        subtitle={environment?.label}
        onBackToList={() => navigation.goBack()}
        actions={
          agent
            ? [
                {
                  accessibilityLabel: pauseLabel,
                  icon: agent.enabled ? "pause" : "play",
                  disabled: !canAct,
                  loading: busy === "enabled",
                  onPress: () =>
                    void runAction("enabled", () =>
                      setEnabled({
                        environmentId,
                        input: { agentId, enabled: !agent.enabled },
                      }),
                    ),
                },
              ]
            : undefined
        }
      />
      <ScreenContent surface="canvas">
        {!connected || readDenied || query.error !== null ? (
          <View className="gap-3 p-5">
            <Text accessibilityRole="alert" className="text-foreground-muted">
              {!connected
                ? gt("This environment is disconnected.")
                : readDenied
                  ? gt("This connection cannot view always-on agents.")
                  : query.error}
            </Text>
            {query.error !== null ? <Button label={gt("Retry")} onPress={query.refresh} /> : null}
          </View>
        ) : !detail || !agent ? (
          <AppActivityIndicator className="self-start p-5" />
        ) : (
          <>
            <View className="gap-2 border-b border-border-subtle px-4 py-2">
              <Text textRole="footnote" className="text-foreground-muted">
                {messages(ALWAYS_ON_PHASE_LABELS[agent.phase])}
              </Text>
              {agent.phase === "needs_confirmation" ? (
                <Button
                  label={gt("Confirm unknown result")}
                  disabled={!canAct}
                  loading={busy === "confirm"}
                  size="compact"
                  onPress={() =>
                    void runAction("confirm", () => confirm({ environmentId, input: { agentId } }))
                  }
                />
              ) : null}
              {actionError === null ? null : (
                <Text accessibilityRole="alert" className="text-danger">
                  {actionError}
                </Text>
              )}
            </View>
            <ScreenScrollView
              ref={scroll}
              className="flex-1"
              contentInsetAdjustmentBehavior="automatic"
              contentContainerClassName="gap-3 px-4 py-4"
              keyboardShouldPersistTaps="handled"
              onScroll={({ nativeEvent: { contentOffset, contentSize, layoutMeasurement } }) => {
                followTail.current =
                  contentSize.height - contentOffset.y - layoutMeasurement.height < 72;
              }}
              onContentSizeChange={() => {
                if (followTail.current) scroll.current?.scrollToEnd({ animated: false });
              }}
            >
              {timeline.length === 0 ? (
                <Text className="text-foreground-muted">{gt("No runs yet.")}</Text>
              ) : null}
              {timeline.map((item) => {
                const time = item.at === null ? null : date.format(new Date(item.at));
                switch (item.kind) {
                  case "run":
                    return (
                      <RunCard
                        key={item.key}
                        item={item}
                        time={time}
                        approvals={item.approvals.map(approvalCard)}
                        canStop={canAct}
                        stopping={busy === item.run.id}
                        onStop={() =>
                          void runAction(item.run.id, () =>
                            cancel({ environmentId, input: { agentId, runId: item.run.id } }),
                          )
                        }
                      />
                    );
                  case "approval":
                    return approvalCard(item.approval);
                  case "delivery":
                    return <Delivery key={item.key} delivery={item.delivery} />;
                  case "event":
                    return (
                      <View
                        key={item.key}
                        className={
                          item.event.kind === "user_reply"
                            ? "max-w-[90%] gap-1 self-end rounded-2xl bg-subtle-strong px-4 py-3"
                            : "gap-1 rounded-2xl bg-grouped-card px-4 py-3"
                        }
                      >
                        <Text textRole="footnote" className="font-t3-medium text-foreground-muted">
                          {messages(EVENT_LABELS[item.event.kind])} ·{" "}
                          {messages(EVENT_STATES[item.state])}
                        </Text>
                        <Text selectable>{item.event.text ?? item.event.source}</Text>
                        {time === null ? null : (
                          <Text textRole="caption" className="text-foreground-muted">
                            {time}
                          </Text>
                        )}
                      </View>
                    );
                }
              })}
            </ScreenScrollView>
            <View
              className="gap-2 border-t border-border-subtle px-4 pt-3"
              style={{ paddingBottom: Math.max(insets.bottom, 12) }}
            >
              {canOperate ? (
                <>
                  {sendError === null ? null : (
                    <Text accessibilityRole="alert" className="text-danger">
                      {sendError}
                    </Text>
                  )}
                  <View className="flex-row items-end gap-2">
                    <TextInput
                      accessibilityLabel={gt("Message the agent")}
                      placeholder={gt("Message the agent")}
                      placeholderTextColorClassName="accent-foreground-muted"
                      className="min-h-11 max-h-32 flex-1 rounded-2xl bg-subtle px-4 py-2 font-sans text-base text-foreground"
                      value={draft}
                      editable={!sending}
                      onChangeText={setDraft}
                      maxLength={2_000}
                      multiline
                    />
                    <Button
                      label={gt("Send")}
                      tone="primary"
                      disabled={draft.trim() === ""}
                      loading={sending}
                      onPress={() => void send()}
                    />
                  </View>
                </>
              ) : (
                <Text textRole="footnote" className="text-foreground-muted">
                  {gt("This connection has read-only access.")}
                </Text>
              )}
            </View>
          </>
        )}
      </ScreenContent>
    </KeyboardAvoidingView>
  );
}

function RunCard({
  item,
  time,
  approvals,
  canStop,
  stopping,
  onStop,
}: {
  readonly item: Extract<AlwaysOnTimelineItem, { kind: "run" }>;
  readonly time: string | null;
  readonly approvals: readonly React.ReactNode[];
  readonly canStop: boolean;
  readonly stopping: boolean;
  readonly onStop: () => void;
}) {
  const gt = useGT();
  const messages = useMessages();
  const [expanded, setExpanded] = useState(false);
  const source = item.sources[0];
  const status = messages(RUN_LABELS[item.run.status]);
  const title =
    source === undefined
      ? gt("Agent run")
      : source.kind === "external"
        ? source.source
        : messages(EVENT_LABELS[source.kind]);
  const heading = (
    <View className="gap-1">
      <Text className="font-t3-bold text-foreground">
        {title} · {status}
      </Text>
      {time === null ? null : (
        <Text textRole="caption" className="text-foreground-muted">
          {time}
        </Text>
      )}
    </View>
  );
  const result = item.run.modelError ?? item.run.modelSummary;
  return (
    <View className="gap-3 rounded-2xl bg-grouped-card px-4 py-3">
      {heading}
      {result ? (
        <Text selectable className="text-foreground">
          {result}
        </Text>
      ) : null}
      {item.sources.length > 0 ? (
        <Pressable
          className="min-h-8 self-start justify-center"
          accessibilityRole="button"
          accessibilityState={{ expanded }}
          onPress={() => setExpanded(!expanded)}
        >
          <Text textRole="footnote" className="font-t3-medium text-primary-text">
            {expanded ? gt("Hide trigger details") : gt("Show trigger details")}
          </Text>
        </Pressable>
      ) : null}
      {expanded
        ? item.sources.map((event) => (
            <View key={event.id} className="gap-1 rounded-xl bg-subtle px-3 py-2">
              <Text textRole="footnote" className="text-foreground-muted">
                {messages(EVENT_LABELS[event.kind])} · {event.source}
              </Text>
              <Text selectable>{event.text ?? event.sourceEventId}</Text>
            </View>
          ))
        : null}
      {approvals}
      {item.deliveries.map((delivery) => (
        <Delivery key={delivery.id} delivery={delivery} />
      ))}
      {item.run.status === "running" ? (
        <Button
          label={gt("Stop run")}
          size="compact"
          disabled={!canStop}
          loading={stopping}
          onPress={onStop}
        />
      ) : null}
    </View>
  );
}

function Delivery({ delivery }: { readonly delivery: AlwaysOnDeliveryEntry }) {
  const gt = useGT();
  const status =
    delivery.status === "delivered"
      ? gt("Delivered")
      : delivery.status === "pending"
        ? gt("Pending delivery")
        : gt("Delivery failed");
  return (
    <View className="gap-1 rounded-xl bg-subtle px-3 py-2">
      <Text textRole="footnote" className="font-t3-medium text-foreground-muted">
        {status}
      </Text>
      <Text selectable>{delivery.body}</Text>
    </View>
  );
}
