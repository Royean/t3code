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
}

/** One run consumes one session affinity. Later events wait for the next run. */
export function selectInboxBatch<E extends BatchEvent>(events: readonly E[]): readonly E[] {
  const first = events[0];
  if (first === undefined) return [];
  const batch: E[] = [];
  if (first.kind === "user_reply") {
    for (const event of events) {
      if (event.kind !== "user_reply") break;
      batch.push(event);
    }
    return batch;
  }
  if (first.kind === "approval") {
    for (const event of events) {
      if (event.kind !== "approval" || event.relatedSessionId !== first.relatedSessionId) break;
      batch.push(event);
    }
    return batch;
  }
  for (const event of events) {
    if (event.kind === "user_reply" || event.kind === "approval") break;
    batch.push(event);
  }
  return batch;
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
