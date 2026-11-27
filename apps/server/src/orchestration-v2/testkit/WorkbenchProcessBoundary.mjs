// The acceptance subprocess uses the real Workbench HTTP handlers and database.
// This transport boundary can lose one response after its handler commits.
import * as NodeModule from "node:module";
import * as NodeFS from "node:fs";
import * as NodePath from "node:path";

const NodeHttp = NodeModule.createRequire(import.meta.url)("node:http");
const directory = process.env.WORKBENCH_ACCEPTANCE_FAULT_DIR;
if (!directory) throw new Error("The acceptance boundary requires its owned fault directory.");
const originalCreateServer = NodeHttp.createServer;
NodeHttp.createServer = function (...args) {
  const server = originalCreateServer.apply(this, args);
  server.prependListener("request", (request, response) => {
    const match = /^\/api\/release-coordinator\/v1\/claims\/[^/?]+(\/merge)?$/.exec(
      request.url ?? "",
    );
    if (!match) return;
    const role = match[1] ? "merge" : "claim";
    const originalWriteHead = response.writeHead;
    let observed = false;
    response.writeHead = function (statusCode, ...rest) {
      if (observed) return originalWriteHead.call(this, statusCode, ...rest);
      observed = true;
      const flag = NodePath.join(directory, "drop-claim-response.flag");
      const dropped =
        role === "claim" &&
        request.method === "POST" &&
        statusCode === 200 &&
        NodeFS.existsSync(flag);
      if (dropped) NodeFS.unlinkSync(flag);
      NodeFS.appendFileSync(
        NodePath.join(directory, "boundary-events.jsonl"),
        JSON.stringify({ role, method: request.method, status: statusCode, dropped }) + "\n",
      );
      if (dropped) {
        response.destroy();
        return response;
      }
      return originalWriteHead.call(this, statusCode, ...rest);
    };
  });
  return server;
};

// Observe the real Workbench projection write as a test milestone. The statement
// and transaction still execute in SQLite; subsequent HTTP actions reauthorize it.
const NodeSqlite = NodeModule.createRequire(import.meta.url)("node:sqlite");
const prepare = NodeSqlite.DatabaseSync.prototype.prepare;
NodeSqlite.DatabaseSync.prototype.prepare = function (sql) {
  const statement = prepare.call(this, sql);
  const attention = /UPDATE event_hub_queue_attention SET snapshot_json/i.test(sql);
  const queue = /UPDATE event_hub_task_queues SET snapshot_json/i.test(sql);
  if (!attention && !queue) return statement;
  const run = statement.run;
  statement.run = function (...parameters) {
    const result = run.apply(this, parameters);
    const value = JSON.parse(parameters[0]);
    const message = attention
      ? {
          kind: "attention",
          queueId: value.queueId,
          attentions: value.attentions.map((item) => ({
            id: item.id,
            workItemId: item.workItemId,
            version: item.version,
            phase: item.phase,
            canRequeue: item.canRequeue,
            repairGenerationId: item.repairGenerationId,
          })),
        }
      : {
          kind: "queue",
          body: JSON.stringify({
            queueId: value.queueId,
            projectId: value.projectId,
            items: value.items.map((item) => ({
              id: item.id,
              queueState: item.queueState,
              priority: item.priority,
            })),
          }),
        };
    queueMicrotask(() => {
      if (process.connected) process.send(message, () => {});
    });
    return result;
  };
  return statement;
};
