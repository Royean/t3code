import { expect, it } from "@effect/vitest";
import * as NodeServices from "@effect/platform-node/NodeServices";
import * as NodeCrypto from "@effect/platform-node/NodeCrypto";
import { EnvironmentId, ProjectId } from "@t3tools/contracts";
import * as Effect from "effect/Effect";
import * as Context from "effect/Context";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Logger from "effect/Logger";
import * as Schema from "effect/Schema";
import * as SqlClient from "effect/sql/SqlClient";
import * as Progress from "./WorkbenchTaskProgress.ts";
import * as Sqlite from "../persistence/Sqlite.ts";
import * as Git from "../vcs/GitVcsDriver.ts";
import * as VcsProcess from "../vcs/VcsProcess.ts";
import * as Config from "../config.ts";
import * as Environment from "../environment/ServerEnvironment.ts";

const stamp = "2026-11-27T00:00:00.000Z";
const scope = {
  controllerId: "controller",
  projectId: ProjectId.make("project"),
  expectedTrackingId: "tracking",
};
const encode = Schema.encodeEffect(Schema.fromJsonString(Schema.Unknown));
const gitLayer = Git.layer.pipe(
  Layer.provide(
    Config.layerTest(process.cwd(), { prefix: "issue14-native-config-" }).pipe(
      Layer.tap((context) =>
        Effect.logInfo(
          `issue14-native owned config fixture: ${Context.get(context, Config.ServerConfig).baseDir}`,
        ),
      ),
    ),
  ),
  Layer.provideMerge(VcsProcess.layer),
  Layer.provideMerge(Layer.mergeAll(NodeServices.layer, NodeCrypto.layer)),
);
const dependencies = Layer.mergeAll(
  Sqlite.layerMemory,
  gitLayer,
  Layer.succeed(Environment.ServerEnvironmentIdentity, {
    getEnvironmentId: Effect.succeed(EnvironmentId.make("environment")),
  }),
);
const layer = Progress.layer.pipe(Layer.provideMerge(dependencies));
const seed = Effect.gen(function* () {
  const sql = yield* SqlClient.SqlClient;
  yield* sql`UPDATE scheduled_task_execution_metadata SET tracking_id='tracking'`;
  yield* sql`INSERT INTO workbench_queues VALUES('queue','controller','project')`;
  yield* sql`INSERT INTO workbench_queue_bindings(task_id,task_created_at,queue_id,project_id,dispatcher_thread_id) VALUES('task',${stamp},'queue','project','dispatcher')`;
  yield* sql`INSERT INTO scheduled_tasks(task_id,title,prompt,enabled,schedule_json,project_id,thread_id,thread_mode,workspace_strategy_json,model_selection_json,runtime_mode,interaction_mode,created_by,creation_source,created_at,updated_at,last_run_status,run_count)
    VALUES('task','PRIVATE title','PRIVATE prompt',1,'{"type":"webhook"}','project','dispatcher','continue','{}','{}','full-access','default','user','web',${stamp},${stamp},'idle',0)`;
  yield* sql`INSERT INTO orchestration_v2_projection_threads(thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,payload_json) VALUES('dispatcher','project','Dispatcher','codex','full-access','default',${stamp},${stamp},'{}')`;
  const matterKey = JSON.stringify([
    "github",
    "https://github.com",
    "issue",
    "repository:918:issue:42",
  ]);
  yield* sql`INSERT INTO workbench_matters(id,queue_id,subscription_scope_id,matter_key,matter_json,task_id,task_created_at,dispatcher_thread_id)
    VALUES('matter','queue','source-subscription',${matterKey},'{}','task',${stamp},'dispatcher')`;
  yield* sql`INSERT INTO workbench_work_items(id,task_id,task_created_at,project_id,business_delivery_id,dispatcher_thread_id,event_json,matter_id,version,created_at,updated_at)
    VALUES('item','task',${stamp},'project','event','dispatcher','{"PRIVATE":"event"}','matter',1,${stamp},${stamp})`;
  yield* sql`INSERT INTO workbench_queue_entries(work_item_id,queue_id,state,created_at,updated_at) VALUES('item','queue','queued',${stamp},${stamp})`;
});

it.effect(
  "checks queue, tracking, task incarnation and matter scope without changing work state",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const progress = yield* Progress.WorkbenchTaskProgress;
      const sql = yield* SqlClient.SqlClient;
      const value = yield* progress.get("queue", "item", scope);
      expect(value.task).toMatchObject({
        id: "item",
        cycleNumber: 1,
        version: 1,
        nextStep: "await_dispatch",
        head: { state: "not_created", sha: null, checksBound: false },
        matter: {
          provider: "github",
          kind: "issue",
          namespace: "https://github.com",
          key: "repository:918:issue:42",
        },
      });
      expect(JSON.parse(value.task.matter!.scopeKey)).toEqual([
        "source-subscription",
        JSON.stringify(["github", "https://github.com", "issue", "repository:918:issue:42"]),
      ]);
      expect(
        (yield* progress
          .get("queue", "item", { ...scope, expectedTrackingId: "other" })
          .pipe(Effect.flip)).reason,
      ).toBe("invalid_binding");
      expect(
        (yield* progress
          .get("queue", "item", { ...scope, controllerId: "other" })
          .pipe(Effect.flip)).reason,
      ).toBe("invalid_binding");
      yield* sql`UPDATE scheduled_tasks SET created_at='2026-11-28T00:00:00.000Z' WHERE task_id='task'`;
      expect((yield* progress.get("queue", "item", scope).pipe(Effect.flip)).reason).toBe(
        "invalid_binding",
      );
      expect(yield* sql`SELECT version,business_state FROM workbench_work_items`).toEqual([
        { version: 1, business_state: "active" },
      ]);
      expect(yield* sql`SELECT id FROM workbench_attention_items`).toEqual([]);
    }).pipe(Effect.provide(layer), Effect.withLogger(Logger.consoleLogFmt)),
);

it.live(
  "reads real Git HEAD, counts only the latest check per script and hides private context",
  () =>
    Effect.gen(function* () {
      yield* seed;
      const fs = yield* FileSystem.FileSystem;
      let directory = "";
      yield* Effect.gen(function* () {
        directory = yield* fs.makeTempDirectoryScoped({ prefix: "issue14-native-git-" });
        yield* Effect.logInfo(`issue14-native owned Git fixture: ${directory}`);
        const git = yield* Git.GitVcsDriver;
        const sql = yield* SqlClient.SqlClient;
        yield* git.execute({
          operation: "test.init",
          cwd: directory,
          args: ["init", "-b", "progress-fixture"],
        });
        yield* git.execute({
          operation: "test.commit",
          cwd: directory,
          args: [
            "-c",
            "user.name=Fixture",
            "-c",
            "user.email=fixture@example.test",
            "commit",
            "--allow-empty",
            "-m",
            "Fixture",
          ],
        });
        const real = yield* fs.realPath(directory);
        const head = yield* git.resolveCommit({ cwd: directory, revision: "HEAD" });
        yield* sql`UPDATE workbench_work_items SET work_thread_id='child',worktree_path=${directory},branch='progress-fixture',run_id='run' WHERE id='item'`;
        yield* sql`UPDATE workbench_queue_entries SET state='occupied',work_state='working' WHERE work_item_id='item'`;
        yield* sql`INSERT INTO orchestration_v2_projection_runs(run_id,thread_id,ordinal,provider,status,requested_at,payload_json) VALUES('run','child',1,'codex','completed',${stamp},'{}')`;
        yield* sql`INSERT INTO orchestration_v2_projection_threads(thread_id,project_id,title,default_provider,runtime_mode,interaction_mode,created_at,updated_at,payload_json)
        VALUES('child','project','PRIVATE title','codex','full-access','default',${stamp},${stamp},${yield* encode({ projectId: "project", worktreePath: directory, branch: "progress-fixture" })})`;
        const context = {
          worktreePath: directory,
          worktreeRealPath: real,
          branch: "progress-fixture",
          gitCommonDir: `${real}/.git`,
          providerSessionId: "PRIVATE session",
        };
        yield* sql`INSERT INTO workbench_repair_generations(id,work_item_id,queue_id,task_id,task_created_at,project_id,dispatcher_thread_id,thread_id,generation,phase,used,policy_json,context_json,source_run_id,created_at,updated_at)
        VALUES('generation','item','queue','task',${stamp},'project','dispatcher','child',1,'succeeded',2,'[{"scriptId":"a"},{"scriptId":"b"}]',${yield* encode(context)},'run',${stamp},${stamp})`;
        const evidence = (
          id: string,
          checkId: string,
          sequence: number,
          exit: number,
          run = "run",
        ) => sql`INSERT INTO workbench_repair_evidence(id,generation_id,check_id,check_name,definition_hash,thread_id,item_id,run_id,run_attempt_id,node_id,provider_thread_id,provider_turn_id,native_item_json,sequence,exit_code,command_text,cwd,output_sha256,payload_sha256,completed_at,created_at)
        VALUES(${id},'generation',${checkId},'PRIVATE name','hash','child',${id},${run},'attempt','node','provider','turn','{}',${sequence},${exit},'PRIVATE command',${directory},'hash','hash',${stamp},${stamp})`;
        yield* evidence("old-failed", "a", 1, 1);
        yield* evidence("new-passed", "a", 2, 0);
        yield* evidence("b-failed", "b", 3, 1);
        yield* evidence("other-source", "a", 4, 1, "old-run");
        const progress = yield* Progress.WorkbenchTaskProgress;
        const value = yield* progress.get("queue", "item", scope);
        expect(value.task.head).toMatchObject({
          state: "known",
          sha: head.commitSha,
          checksBound: false,
        });
        expect(value.task.checks).toEqual({
          state: "failed",
          passed: 1,
          failed: 1,
          used: 2,
          sourceRunId: "run",
        });
        expect(value.task.threadUrl).toBe("https://app.t3.codes/threads/environment/child");
        expect(JSON.stringify(value)).not.toContain("PRIVATE");
        expect(JSON.stringify(value)).not.toContain(directory);
        const readDuringChange = (change: Effect.Effect<unknown, never>) =>
          Effect.gen(function* () {
            const reader = yield* Progress.WorkbenchTaskProgress;
            return yield* reader.get("queue", "item", scope);
          }).pipe(
            Effect.provide(
              Layer.fresh(Progress.layer).pipe(
                Layer.provide(
                  Layer.mock(Git.GitVcsDriver)({
                    execute: (input) =>
                      git
                        .execute(input)
                        .pipe(
                          Effect.tap(() =>
                            input.args[0] === "rev-parse" && input.args[1] === "--verify"
                              ? change
                              : Effect.void,
                          ),
                        ),
                  }),
                ),
              ),
            ),
          );
        const observedStamp = "2026-11-27T00:00:01.000Z";
        const refreshed = yield* readDuringChange(
          sql`UPDATE workbench_work_items SET updated_at=${observedStamp} WHERE id='item'`.pipe(
            Effect.orDie,
          ),
        );
        expect(refreshed.task.head.sha).toBe(head.commitSha);
        expect(refreshed.task.updatedAt).toBe(observedStamp);
        const changed = yield* readDuringChange(
          sql`UPDATE workbench_work_items SET version=version+1 WHERE id='item'`.pipe(Effect.orDie),
        ).pipe(Effect.flip);
        expect(changed.reason).toBe("stale_state");
        yield* sql`UPDATE workbench_repair_generations SET context_json=${yield* encode({ ...context, gitCommonDir: `${real}/different` })}`;
        expect((yield* progress.get("queue", "item", scope)).task.head.state).toBe("unknown");
        yield* sql`UPDATE workbench_repair_generations SET context_json=${yield* encode(context)}`;
        yield* git.execute({
          operation: "test.branch",
          cwd: directory,
          args: ["switch", "-c", "changed-branch"],
        });
        expect((yield* progress.get("queue", "item", scope)).task.head.state).toBe("unknown");
        yield* sql`UPDATE workbench_work_items SET business_state='completed'`;
        expect((yield* progress.get("queue", "item", scope)).task.nextStep).toBe("completed");
        yield* git.execute({
          operation: "test.restoreBranch",
          cwd: directory,
          args: ["switch", "progress-fixture"],
        });
        const revoked = yield* readDuringChange(
          sql`UPDATE scheduled_task_execution_metadata SET tracking_id='revoked'`.pipe(
            Effect.orDie,
          ),
        ).pipe(Effect.flip);
        expect(revoked.reason).toBe("invalid_binding");
      }).pipe(Effect.scoped);
      expect(yield* fs.exists(directory)).toBe(false);
      yield* Effect.logInfo(`issue14-native reclaimed Git fixture: ${directory}`);
    }).pipe(Effect.provide(layer), Effect.withLogger(Logger.consoleLogFmt)),
);
