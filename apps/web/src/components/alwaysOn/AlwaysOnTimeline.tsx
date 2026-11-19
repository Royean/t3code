import {
  buildAlwaysOnTimeline,
  type AlwaysOnTimelineItem,
} from "@t3tools/client-runtime/always-on-conversation";
import type {
  AlwaysOnAgentId,
  AlwaysOnApprovalEntry,
  AlwaysOnDeliveryEntry,
  AlwaysOnDetail,
  AlwaysOnInboxEntry,
  EnvironmentId,
} from "@t3tools/contracts";
import { msg, useGT, useLocale, useMessages } from "gt-react";
import { useRef } from "react";

import { observeResize } from "../../lib/observeResize";
import { serverEnvironment } from "../../state/server";
import { useAtomCommand } from "../../state/use-atom-command";
import { Badge } from "../ui/badge";
import { Button } from "../ui/button";
import { Empty, EmptyDescription, EmptyHeader, EmptyTitle } from "../ui/empty";
import { useAlwaysOnAction } from "./useAlwaysOnAction";

type Address = {
  readonly environmentId: EnvironmentId;
  readonly input: { readonly agentId: AlwaysOnAgentId };
};

const eventLabels = {
  user_reply: msg("You"),
  timer: msg("Timer"),
  approval: msg("Approval"),
  external: msg("External event"),
  system: msg("System"),
} satisfies Record<AlwaysOnInboxEntry["kind"], ReturnType<typeof msg>>;

const runLabels = {
  running: msg("Running"),
  completed: msg("Completed"),
  awaiting_approval: msg("Approval requested"),
  interrupted: msg("Interrupted"),
  unknown: msg("Needs confirmation"),
  cancelled: msg("Stopped"),
  failed: msg("Failed"),
} satisfies Record<AlwaysOnDetail["runs"][number]["status"], ReturnType<typeof msg>>;

const eventStates = {
  pending: msg("Queued"),
  running: msg("Running"),
  processed: msg("Processed"),
  cancelled: msg("Cancelled"),
} satisfies Record<
  Extract<AlwaysOnTimelineItem, { kind: "event" }>["state"],
  ReturnType<typeof msg>
>;

const approvalStates = {
  pending: msg("Awaiting approval"),
  approved: msg("Approved"),
  rejected: msg("Rejected"),
  cancelled: msg("Cancelled"),
} satisfies Record<AlwaysOnApprovalEntry["status"], ReturnType<typeof msg>>;

const deliveryStates = {
  pending: msg("Pending delivery"),
  delivered: msg("Delivered"),
  failed: msg("Delivery failed"),
} satisfies Record<AlwaysOnDeliveryEntry["status"], ReturnType<typeof msg>>;

export function AlwaysOnTimeline({
  address,
  detail,
  canOperate,
}: {
  readonly address: Address;
  readonly detail: AlwaysOnDetail;
  readonly canOperate: boolean;
}) {
  const gt = useGT();
  const locale = useLocale();
  const timeFormatter = new Intl.DateTimeFormat(locale, { dateStyle: "short", timeStyle: "short" });
  const items = buildAlwaysOnTimeline(detail);
  const stickToLatest = useRef(true);
  const followLatest = (list: HTMLOListElement | null) => {
    const viewport = list?.parentElement;
    if (!list || !viewport) return undefined;
    return observeResize(list, () => {
      if (stickToLatest.current) viewport.scrollTop = viewport.scrollHeight;
    });
  };

  if (items.length === 0) {
    return (
      <Empty className="flex-1">
        <EmptyHeader>
          <EmptyTitle>{gt("No activity yet")}</EmptyTitle>
          <EmptyDescription>{gt("Events and messages will appear here.")}</EmptyDescription>
        </EmptyHeader>
      </Empty>
    );
  }

  return (
    <div
      aria-label={gt("Agent conversation")}
      className="min-h-0 flex-1 overflow-y-auto overscroll-contain scrollbar-gutter-both"
      role="log"
      onScroll={(event) => {
        const node = event.currentTarget;
        stickToLatest.current = node.scrollHeight - node.scrollTop - node.clientHeight < 32;
      }}
    >
      <ol className="mx-auto max-w-3xl space-y-4 px-4 py-5" ref={followLatest}>
        {items.map((item) => (
          <li
            key={item.key}
            className={
              item.kind === "event" && item.event.kind === "user_reply"
                ? "ml-auto w-fit max-w-[85%]"
                : undefined
            }
          >
            <TimelineEntry
              address={address}
              item={item}
              canOperate={canOperate}
              time={item.at ? timeFormatter.format(new Date(item.at)) : null}
            />
          </li>
        ))}
      </ol>
    </div>
  );
}

function TimelineEntry({
  address,
  item,
  canOperate,
  time,
}: {
  readonly address: Address;
  readonly item: AlwaysOnTimelineItem;
  readonly canOperate: boolean;
  readonly time: string | null;
}) {
  const resolve = useMessages();
  switch (item.kind) {
    case "event":
      return (
        <article className="min-w-0 space-y-2 rounded-lg bg-muted/60 px-4 py-3">
          <div className="flex flex-wrap items-center gap-2 text-xs text-muted-foreground">
            <span>{resolve(eventLabels[item.event.kind])}</span>
            <Badge variant="secondary" size="sm">
              {resolve(eventStates[item.state])}
            </Badge>
            {time && item.at ? <time dateTime={item.at}>{time}</time> : null}
          </div>
          <p className="text-sm break-words whitespace-pre-wrap">
            {item.event.text ?? item.event.sourceEventId}
          </p>
        </article>
      );
    case "run":
      return <RunCard address={address} item={item} canOperate={canOperate} time={time} />;
    case "approval":
      return <ApprovalCard address={address} approval={item.approval} canOperate={canOperate} />;
    case "delivery":
      return <DeliveryCard delivery={item.delivery} />;
  }
}

function RunCard({
  address,
  item,
  canOperate,
  time,
}: {
  readonly address: Address;
  readonly item: Extract<AlwaysOnTimelineItem, { kind: "run" }>;
  readonly canOperate: boolean;
  readonly time: string | null;
}) {
  const gt = useGT();
  const resolve = useMessages();
  const cancel = useAtomCommand(serverEnvironment.cancelAlwaysOnRun);
  const { act, error, pending } = useAlwaysOnAction();
  const run = item.run;
  const approvalsPending = item.approvals.some(({ status }) => status === "pending");

  return (
    <article className="space-y-3 rounded-xl border bg-card px-4 py-3">
      <header className="flex flex-wrap items-center justify-between gap-2">
        <div className="flex flex-wrap items-center gap-2 text-sm">
          <h2 className="font-medium">{gt("Run")}</h2>
          <Badge
            variant={
              run.status === "failed"
                ? "error"
                : approvalsPending || run.status === "unknown"
                  ? "warning"
                  : "secondary"
            }
            size="sm"
          >
            {approvalsPending ? gt("Awaiting approval") : resolve(runLabels[run.status])}
          </Badge>
          {time && item.at ? (
            <time className="text-xs text-muted-foreground" dateTime={item.at}>
              {time}
            </time>
          ) : null}
        </div>
        {run.status === "running" && canOperate ? (
          <Button
            variant="outline"
            size="sm"
            disabled={pending}
            onClick={() =>
              void act(() => cancel({ ...address, input: { ...address.input, runId: run.id } }))
            }
          >
            {gt("Stop run")}
          </Button>
        ) : null}
      </header>
      {run.modelSummary ? (
        <p className="text-sm break-words whitespace-pre-wrap">{run.modelSummary}</p>
      ) : null}
      {run.modelError ? (
        <p className="text-sm text-destructive break-words whitespace-pre-wrap">{run.modelError}</p>
      ) : null}
      {error ? (
        <p role="alert" className="text-sm text-destructive">
          {error}
        </p>
      ) : null}
      {item.sources.length > 0 ? (
        <details className="text-sm text-muted-foreground">
          <summary className="cursor-pointer">
            {gt("Event sources")}
            {item.sources.length === 1 && item.sources[0]?.source ? (
              <span className="ml-2 break-all">{item.sources[0].source}</span>
            ) : null}
          </summary>
          <ul className="mt-2 space-y-2 pl-4">
            {item.sources.map((event) => (
              <li key={event.id}>
                <p className="text-xs font-medium break-all">
                  {resolve(eventLabels[event.kind])} · {event.source}
                </p>
                <p className="break-words whitespace-pre-wrap">
                  {event.text ?? event.sourceEventId}
                </p>
              </li>
            ))}
          </ul>
        </details>
      ) : null}
      {item.approvals.map((approval) => (
        <ApprovalCard
          key={approval.id}
          address={address}
          approval={approval}
          canOperate={canOperate}
        />
      ))}
      {item.deliveries.map((delivery) => (
        <DeliveryCard key={delivery.id} delivery={delivery} />
      ))}
    </article>
  );
}

function ApprovalCard({
  address,
  approval,
  canOperate,
}: {
  readonly address: Address;
  readonly approval: AlwaysOnApprovalEntry;
  readonly canOperate: boolean;
}) {
  const gt = useGT();
  const resolve = useMessages();
  const decide = useAtomCommand(serverEnvironment.resolveAlwaysOnApproval);
  const { act, error, pending } = useAlwaysOnAction();
  const onDecision = (decision: "approved" | "rejected") => {
    void act(() =>
      decide({ ...address, input: { ...address.input, approvalId: approval.id, decision } }),
    );
  };

  return (
    <article className="space-y-2 rounded-lg border bg-muted/30 px-3 py-2">
      <Badge variant={approval.status === "pending" ? "warning" : "secondary"} size="sm">
        {resolve(approvalStates[approval.status])}
      </Badge>
      <p className="text-sm break-words whitespace-pre-wrap">{approval.summary}</p>
      {error ? (
        <p role="alert" className="text-sm text-destructive">
          {error}
        </p>
      ) : null}
      {approval.status === "pending" && canOperate ? (
        <div className="flex flex-wrap gap-2">
          <Button size="sm" disabled={pending} onClick={() => onDecision("approved")}>
            {gt("Approve")}
          </Button>
          <Button
            variant="outline"
            size="sm"
            disabled={pending}
            onClick={() => onDecision("rejected")}
          >
            {gt("Reject")}
          </Button>
        </div>
      ) : null}
    </article>
  );
}

function DeliveryCard({ delivery }: { readonly delivery: AlwaysOnDeliveryEntry }) {
  const resolve = useMessages();
  return (
    <article className="space-y-2 rounded-lg bg-muted/60 px-3 py-2">
      <Badge variant={delivery.status === "failed" ? "error" : "secondary"} size="sm">
        {resolve(deliveryStates[delivery.status])}
      </Badge>
      <p className="text-sm break-words whitespace-pre-wrap">{delivery.body}</p>
    </article>
  );
}
