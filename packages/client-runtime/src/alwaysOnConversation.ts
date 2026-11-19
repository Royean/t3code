import type {
  AlwaysOnAgentSummary,
  AlwaysOnApprovalEntry,
  AlwaysOnDeliveryEntry,
  AlwaysOnDetail,
  AlwaysOnEventId,
  AlwaysOnInboxEntry,
  AlwaysOnRunEntry,
  AlwaysOnRunId,
} from "@t3tools/contracts";

type TimelinePosition = { readonly key: string; readonly at: string | null };

export type AlwaysOnTimelineItem = TimelinePosition &
  (
    | {
        readonly kind: "event";
        readonly event: AlwaysOnInboxEntry;
        readonly state: "pending" | "running" | "processed" | "cancelled";
      }
    | {
        readonly kind: "run";
        readonly run: AlwaysOnRunEntry;
        readonly sources: readonly AlwaysOnInboxEntry[];
        readonly approvals: readonly AlwaysOnApprovalEntry[];
        readonly deliveries: readonly AlwaysOnDeliveryEntry[];
      }
    | { readonly kind: "approval"; readonly approval: AlwaysOnApprovalEntry }
    | { readonly kind: "delivery"; readonly delivery: AlwaysOnDeliveryEntry }
  );

export function alwaysOnAttentionCount(agent: AlwaysOnAgentSummary): number {
  return agent.pendingApprovalCount + Number(agent.phase === "needs_confirmation");
}

export function buildAlwaysOnTimeline(detail: AlwaysOnDetail): readonly AlwaysOnTimelineItem[] {
  const runByEvent = new Map<AlwaysOnEventId, AlwaysOnRunEntry>();
  const eventsById = new Map(detail.inbox.map((event) => [event.id, event]));
  const approvalsByRun = new Map<AlwaysOnRunId, AlwaysOnApprovalEntry[]>();
  const deliveriesByRun = new Map<AlwaysOnRunId, AlwaysOnDeliveryEntry[]>();
  const runIds = new Set(detail.runs.map((run) => run.id));
  const items: AlwaysOnTimelineItem[] = [];

  for (const run of detail.runs) {
    for (const eventId of run.eventIds) runByEvent.set(eventId, run);
  }
  for (const event of detail.inbox) {
    const run = runByEvent.get(event.id);
    if (run && event.status === "consumed" && event.kind !== "user_reply") continue;
    items.push({
      kind: "event",
      key: event.id,
      at: event.createdAt,
      event,
      state:
        event.status === "consumed"
          ? run?.status === "running"
            ? "running"
            : run?.status === "cancelled"
              ? "cancelled"
              : "processed"
          : event.status,
    });
  }
  for (const approval of detail.approvals) {
    if (!runIds.has(approval.runId)) {
      items.push({ kind: "approval", key: approval.id, at: approval.createdAt, approval });
      continue;
    }
    const group = approvalsByRun.get(approval.runId);
    if (group) group.push(approval);
    else approvalsByRun.set(approval.runId, [approval]);
  }
  for (const delivery of detail.deliveries) {
    if (!delivery.runId || !runIds.has(delivery.runId)) {
      items.push({ kind: "delivery", key: delivery.id, at: delivery.createdAt ?? null, delivery });
      continue;
    }
    const group = deliveriesByRun.get(delivery.runId);
    if (group) group.push(delivery);
    else deliveriesByRun.set(delivery.runId, [delivery]);
  }
  for (const run of detail.runs) {
    items.push({
      kind: "run",
      key: run.id,
      at: run.startedAt,
      run,
      sources: run.eventIds.flatMap((id) => eventsById.get(id) ?? []),
      approvals: approvalsByRun.get(run.id) ?? [],
      deliveries: deliveriesByRun.get(run.id) ?? [],
    });
  }

  return items.sort((left, right) => (left.at ?? "").localeCompare(right.at ?? ""));
}
