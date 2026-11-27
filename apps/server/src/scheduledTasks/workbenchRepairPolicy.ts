import {
  ProjectScript,
  WorkbenchQueueError,
  WorkbenchRepairPolicy,
  type ProjectId,
} from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import { digest } from "./workbenchLifecycle.ts";

const FrozenCheck = Schema.Struct({
  scriptId: Schema.String,
  name: Schema.String,
  command: Schema.String,
  definitionHash: Schema.String,
  acceptedInputs: Schema.Array(Schema.String),
  cwdPolicy: Schema.Literal("original_worktree"),
});
export const FrozenRepairPolicy = Schema.Array(FrozenCheck);
export const decodeFrozenRepairPolicy = Schema.decodeEffect(
  Schema.fromJsonString(FrozenRepairPolicy),
);
const decodeScripts = Schema.decodeEffect(Schema.fromJsonString(Schema.Array(ProjectScript)));
export const decodeRepairPolicy = Schema.decodeEffect(Schema.fromJsonString(WorkbenchRepairPolicy));
const quote = (value: string) => `'${value.replaceAll("'", "'\\''")}'`;
const definitionHash = (script: { id: string; command: string }) =>
  digest(JSON.stringify({ id: script.id, command: script.command }));

export const freezeRepairPolicy = Effect.fn("WorkbenchRepairs.freezePolicy")(function* (
  projectId: ProjectId,
  policy: WorkbenchRepairPolicy,
) {
  if (policy.checks.length === 0) return null;
  if (
    policy.checks.length > 10 ||
    new Set(policy.checks.map((c) => c.scriptId)).size !== policy.checks.length
  )
    return yield* new WorkbenchQueueError({ reason: "stale_state" });
  const sql = yield* SqlClient.SqlClient;
  const [row] = yield* sql<{
    scripts_json: string;
  }>`SELECT scripts_json FROM projection_projects WHERE project_id=${projectId} AND deleted_at IS NULL`;
  if (!row) return yield* new WorkbenchQueueError({ reason: "stale_state" });
  const scripts = yield* decodeScripts(row.scripts_json);
  if (scripts.length > 100) return yield* new WorkbenchQueueError({ reason: "stale_state" });
  return yield* Effect.forEach(
    policy.checks,
    Effect.fn("WorkbenchRepairs.freezeCheck")(function* (check) {
      const script = scripts.find((s) => s.id === check.scriptId);
      if (
        !script ||
        script.async === true ||
        script.id.length > 200 ||
        script.name.length > 1024 ||
        script.command.length > 4096 ||
        /[\x00-\x08\x0A-\x1F\x7F;&|`<>$()]/.test(script.command) ||
        /[\x00-\x1F\x7F]/.test(script.id) ||
        definitionHash(script) !== check.definitionHash
      )
        return yield* new WorkbenchQueueError({ reason: "stale_state" });
      return {
        ...check,
        name: script.name,
        command: script.command,
        cwdPolicy: "original_worktree" as const,
        acceptedInputs: [
          script.command,
          ...["/bin/sh", "sh", "/bin/bash", "bash", "/bin/zsh", "zsh"].map(
            (shell) => `${shell} -lc ${quote(script.command)}`,
          ),
        ],
      };
    }),
  );
});

export function publicRepairPolicy(
  checks: typeof FrozenRepairPolicy.Type | null,
): WorkbenchRepairPolicy | null {
  return checks === null
    ? null
    : { checks: checks.map(({ scriptId, definitionHash }) => ({ scriptId, definitionHash })) };
}
