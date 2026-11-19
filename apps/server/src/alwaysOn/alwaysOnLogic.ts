import type { AlwaysOnEventKind } from "@t3tools/contracts";

const CREDENTIAL =
  /(?:(?:api[_ -]?key|password|secret|token)\s*[:=]\s*\S+|bearer\s+\S+|\bsk-[a-z0-9]|xox[baprs]-)/i;

export function containsCredential(value: unknown): boolean {
  if (typeof value === "string") return CREDENTIAL.test(value);
  if (Array.isArray(value)) return value.some(containsCredential);
  if (value !== null && typeof value === "object") {
    return Object.values(value).some(containsCredential);
  }
  return false;
}

export interface BatchEvent {
  readonly kind: AlwaysOnEventKind;
  readonly relatedSessionId: string | null;
  readonly text: string;
}

const MAX_PROMPT_CHARS = 8_000;
const MAX_BATCH_EVENT_CHARS = 4_500;

const eventLine = (event: { readonly kind: string; readonly text: string }) =>
  `- ${event.kind}: ${event.text}`;

/** One run consumes one session affinity. Later events wait for the next run. */
export function selectInboxBatch<E extends BatchEvent>(events: readonly E[]): readonly E[] {
  const first = events[0];
  if (first === undefined) return [];
  const batch: E[] = [];
  let chars = 0;
  for (const event of events) {
    const sameSession =
      first.kind === "user_reply"
        ? event.kind === "user_reply"
        : first.kind === "approval"
          ? event.kind === "approval" && event.relatedSessionId === first.relatedSessionId
          : event.kind !== "user_reply" && event.kind !== "approval";
    if (!sameSession) break;
    chars += eventLine(event).length + 1;
    if (batch.length > 0 && chars > MAX_BATCH_EVENT_CHARS) break;
    batch.push(event);
  }
  return batch;
}

/**
 * Lean context for one model call. The allowed-type sentence only steers the reply.
 * The service rejects every other shape; this text is not the enforcement.
 */
export function alwaysOnPrompt(input: {
  readonly instructions: string;
  readonly notes: readonly { readonly title: string; readonly pointer: string | null }[];
  readonly progress: readonly { readonly title: string; readonly taskKey: string | null }[];
  readonly recentResults: readonly { readonly title: string; readonly body: string }[];
  readonly events: readonly { readonly kind: string; readonly text: string }[];
}): string {
  const context = [
    "Decide the next always-on actions. Reply with JSON {summary, actions}.",
    "Allowed action types: remember_note, record_progress, remember_result, set_timer, deliver, silent, request_approval, record_uncertain.",
    "Do not invent tools. Shell and network calls are not actions.",
    `Instructions: ${input.instructions}`,
    "Notes:",
    ...input.notes.map((note) => `- ${note.title}${note.pointer ? ` (${note.pointer})` : ""}`),
    "Progress:",
    ...input.progress.map((item) => `- ${item.taskKey ?? item.title}: ${item.title}`),
    "Recent results:",
    ...input.recentResults.map((result) => `- ${result.title}: ${result.body}`),
  ].join("\n");
  const events = ["Events:", ...input.events.map(eventLine)].join("\n");
  return `${context.slice(0, Math.max(0, MAX_PROMPT_CHARS - events.length - 1))}\n${events}`;
}

/** Collapse missed recurrences into the next future due time and one inbox event. */
export function nextRecurringDue(dueAtMs: number, everyMs: number, nowMs: number): number {
  const step = Math.max(1, everyMs);
  let next = dueAtMs + step;
  if (next <= nowMs) {
    const steps = Math.floor((nowMs - dueAtMs) / step) + 1;
    next = dueAtMs + steps * step;
  }
  return next;
}
