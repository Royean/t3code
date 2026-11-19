import {
  AlwaysOnAgentId,
  AlwaysOnApprovalId,
  AlwaysOnEventId,
  AlwaysOnRunId,
  AlwaysOnSessionId,
  ProjectId,
  type AlwaysOnDetail,
  type AlwaysOnInboxEntry,
  type AlwaysOnRunEntry,
} from "@t3tools/contracts";
import { describe, expect, it } from "vite-plus/test";

import { alwaysOnAttentionCount, buildAlwaysOnTimeline } from "./alwaysOnConversation.ts";

const time = (second: number) => `2026-11-19T00:00:${String(second).padStart(2, "0")}.000Z`;
const sessionId = AlwaysOnSessionId.make("session");
const runId = AlwaysOnRunId.make("run");
const external: AlwaysOnInboxEntry = {
  id: AlwaysOnEventId.make("external"),
  kind: "external",
  source: "ci",
  sourceEventId: "build-failed",
  status: "consumed",
  sessionPolicy: "fresh",
  createdAt: time(1),
};
const reply: AlwaysOnInboxEntry = {
  ...external,
  id: AlwaysOnEventId.make("reply"),
  kind: "user_reply",
  text: "Only check Android.",
  createdAt: time(2),
};
const run: AlwaysOnRunEntry = {
  id: runId,
  sessionId,
  status: "running",
  outcome: null,
  uncertain: false,
  startedAt: time(3),
  finishedAt: null,
  lastProgressAt: null,
  eventIds: [external.id, reply.id],
  loadedContext: { instructions: "", noteIndex: [], progressIndex: [], recentResults: [] },
  modelSummary: null,
  modelError: null,
  executedActions: [],
};
const detail: AlwaysOnDetail = {
  agent: {
    id: AlwaysOnAgentId.make("agent"),
    projectId: ProjectId.make("project"),
    title: "Build watcher",
    enabled: true,
    phase: "running",
    userSessionId: sessionId,
    pendingInboxCount: 1,
    pendingApprovalCount: 1,
    pendingOutboxCount: 0,
    updatedAt: time(3),
  },
  instructions: "",
  inbox: [external, reply],
  runs: [run],
  memoryIndex: [],
  approvals: [
    {
      id: AlwaysOnApprovalId.make("approval"),
      runId,
      sessionId,
      status: "pending",
      summary: "Send the report?",
      createdAt: time(4),
    },
  ],
  deliveries: [
    {
      id: "delivery",
      runId,
      sessionId,
      body: "Report delivered",
      status: "delivered",
      attempts: 1,
      dedupeKey: "report",
      createdAt: time(5),
    },
  ],
  timers: [],
};

describe("Always On conversation", () => {
  it("keeps a user message visible as it moves from the queue into its run", () => {
    const queued = buildAlwaysOnTimeline({
      ...detail,
      inbox: [{ ...reply, status: "pending" }],
      runs: [],
      approvals: [],
      deliveries: [],
    });
    const running = buildAlwaysOnTimeline(detail);
    expect(queued[0]).toMatchObject({ key: reply.id, kind: "event", state: "pending" });
    expect(running).toMatchObject([
      { key: reply.id, kind: "event", state: "running", event: { text: reply.text } },
      {
        key: run.id,
        kind: "run",
        sources: [external, reply],
        approvals: detail.approvals,
        deliveries: detail.deliveries,
      },
    ]);
    expect(
      buildAlwaysOnTimeline({ ...detail, runs: [{ ...run, status: "completed" }] })[0],
    ).toMatchObject({ key: reply.id, state: "processed" });
    expect(
      buildAlwaysOnTimeline({ ...detail, runs: [{ ...run, status: "cancelled" }] })[0],
    ).toMatchObject({ key: reply.id, state: "cancelled" });
  });

  it("preserves approvals and old deliveries when their run is absent", () => {
    const [delivery] = detail.deliveries;
    const [approval] = detail.approvals;
    const orphaned = buildAlwaysOnTimeline({
      ...detail,
      inbox: [],
      runs: [],
      deliveries: [
        delivery!,
        { ...delivery!, id: "legacy", runId: undefined, createdAt: undefined },
      ],
    });
    expect(orphaned.map(({ kind, key }) => [kind, key])).toEqual([
      ["delivery", "legacy"],
      ["approval", approval!.id],
      ["delivery", delivery!.id],
    ]);
  });

  it("shows an external event that was requeued after its earlier run was interrupted", () => {
    const timeline = buildAlwaysOnTimeline({
      ...detail,
      inbox: [{ ...external, status: "pending" }],
      runs: [{ ...run, status: "interrupted" }],
    });
    expect(timeline[0]).toMatchObject({ key: external.id, kind: "event", state: "pending" });
    expect(timeline[1]).toMatchObject({ key: run.id, kind: "run" });
  });

  it("counts pending approvals and the agent-level confirmation only", () => {
    expect(alwaysOnAttentionCount(detail.agent)).toBe(1);
    expect(alwaysOnAttentionCount({ ...detail.agent, phase: "needs_confirmation" })).toBe(2);
  });
});
