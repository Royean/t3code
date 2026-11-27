import * as NodeFS from "node:fs";
import * as NodeModule from "node:module";
import * as NodePath from "node:path";

const directory = process.env.WORKBENCH_ACCEPTANCE_FAULT_DIR;
if (
  process.env.NODE_ENV !== "test" ||
  !directory ||
  process.env.WORKBENCH_GITHUB_TOKEN !== "synthetic-acceptance-github-token"
)
  throw new Error("The GitHub boundary requires an explicitly isolated acceptance process.");

const filename = NodePath.join(directory, "github-boundary-state.json");
const actor = { id: 77, login: "acceptance-writer" };
const repository = {
  id: 42,
  owner: { login: "fixture" },
  name: "project",
  full_name: "fixture/project",
  html_url: "https://github.com/fixture/project",
};
const root = "/repos/fixture/project";
const issue = {
  id: 301,
  number: 1,
  title: "A real isolated work item",
  body: "Original user requirement; the progress writer must preserve it.",
  user: { id: 123, login: "acceptance-user" },
  state: "open",
  html_url: repository.html_url + "/issues/1",
  url: "https://api.github.com" + root + "/issues/1",
  repository_url: "https://api.github.com" + root,
};
const state = NodeFS.existsSync(filename)
  ? JSON.parse(NodeFS.readFileSync(filename, "utf8"))
  : { repository, actor, issue, comment: null, requests: [], lostResponses: 0 };
if (
  JSON.stringify(state.repository) !== JSON.stringify(repository) ||
  JSON.stringify(state.actor) !== JSON.stringify(actor) ||
  JSON.stringify(state.issue) !== JSON.stringify(issue) ||
  !Array.isArray(state.requests) ||
  (state.comment !== null && (state.comment.id !== 900 || state.comment.user.id !== actor.id))
)
  throw new Error("The persisted synthetic GitHub identity changed.");

const persist = () => {
  NodeFS.writeFileSync(filename + ".tmp", JSON.stringify(state), { mode: 0o600 });
  NodeFS.renameSync(filename + ".tmp", filename);
};
const send = (message) => {
  if (process.connected) process.send(message, () => {});
};
const originalFetch = globalThis.fetch;
const nativeOrigin = process.env.WORKBENCH_EVENT_WEBHOOK_ORIGINS;
persist();

globalThis.fetch = async (input, options = {}) => {
  const url = new URL(typeof input === "string" || input instanceof URL ? input : input.url);
  if (url.origin !== "https://api.github.com") {
    if (url.origin !== nativeOrigin || url.protocol !== "http:" || url.hostname !== "127.0.0.1")
      throw new Error("An external request escaped the isolated GitHub boundary.");
    return originalFetch(input, options);
  }
  const method = options.method ?? "GET";
  const headers = new Headers(options.headers);
  if (
    url.username ||
    url.password ||
    url.hash ||
    options.redirect !== "error" ||
    headers.get("authorization") !== "Bearer synthetic-acceptance-github-token"
  )
    throw new Error("The synthetic GitHub request identity is invalid.");
  const path = url.pathname + url.search;
  state.requests.push({ method, path });
  if (state.requests.length > 1000)
    throw new Error("The synthetic GitHub request bound was exceeded.");
  persist();
  if (
    method === "GET" &&
    path.includes("/comments") &&
    state.lostResponses > 0 &&
    !NodeFS.existsSync(NodePath.join(directory, "github-recovery-enabled.flag"))
  )
    throw new Error("Synthetic GitHub readback remains disconnected until explicitly restored.");
  let result;
  if (method === "GET" && path === "/user") result = actor;
  else if (method === "GET" && [root, "/repositories/42"].includes(path)) result = repository;
  else if (method === "GET" && path === root + "/issues/1") result = issue;
  else if (method === "GET" && path === root + "/issues/1/comments?per_page=100&page=1")
    result = state.comment === null ? [] : [state.comment];
  else if (method === "GET" && path === root + "/issues/comments/900" && state.comment !== null)
    result = state.comment;
  else if (
    (method === "POST" && path === root + "/issues/1/comments" && state.comment === null) ||
    (method === "PATCH" && path === root + "/issues/comments/900" && state.comment !== null)
  ) {
    const payload = JSON.parse(options.body);
    if (
      Object.keys(payload).length !== 1 ||
      typeof payload.body !== "string" ||
      payload.body.length > 65536 ||
      !/^<!-- workbench-progress:v1:[a-f0-9]{64} -->\n### 开发任务进度\n/.test(payload.body)
    )
      throw new Error("The synthetic summary is outside the selected progress target.");
    state.comment = {
      id: 900,
      body: payload.body,
      user: actor,
      url: "https://api.github.com" + root + "/issues/comments/900",
      issue_url: issue.url,
      html_url: issue.html_url + "#issuecomment-900",
    };
    result = state.comment;
  } else throw new Error("An unhandled synthetic GitHub request was rejected.");
  const mutation = method === "POST" || method === "PATCH";
  const drop = NodePath.join(directory, "drop-github-response.flag");
  const lost = mutation && NodeFS.existsSync(drop);
  if (lost) {
    NodeFS.unlinkSync(drop);
    state.lostResponses++;
  }
  persist();
  if (mutation) send({ kind: "github-summary", method, body: result.body, issueNumber: 1 });
  else if (path.includes("/comments") && state.comment !== null)
    send({ kind: "github-readback", method, body: state.comment.body, issueNumber: 1 });
  if (lost) throw new Error("The synthetic GitHub response was lost after persistence.");
  return Response.json(result, { status: method === "POST" ? 201 : 200 });
};

// Observe committed domain results without replacing the SQLite statement or its transaction.
const NodeSqlite = NodeModule.createRequire(import.meta.url)("node:sqlite");
const prepare = NodeSqlite.DatabaseSync.prototype.prepare;
NodeSqlite.DatabaseSync.prototype.prepare = function (sql) {
  const statement = prepare.call(this, sql);
  if (!/^UPDATE event_hub_github_anchors SET /i.test(sql)) return statement;
  const database = this,
    run = statement.run;
  statement.run = function (...parameters) {
    const result = run.apply(this, parameters);
    queueMicrotask(() => {
      if (!process.connected) return;
      const rows = prepare
        .call(
          database,
          `SELECT id,work_item_id,state,version,error_code,summary_url,progress_json
        FROM event_hub_github_anchors`,
        )
        .all();
      for (const row of rows) {
        const progress = JSON.parse(row.progress_json);
        send({
          kind: "github-sync",
          body: JSON.stringify({
            id: row.id,
            workItemId: row.work_item_id,
            state: row.state,
            version: row.version,
            errorCode: row.error_code,
            summaryUrl: row.summary_url,
            checksState: progress.checks.state,
            businessState: progress.businessState,
            workState: progress.workState,
            workThreadId: progress.workThreadId,
          }),
        });
      }
    });
    return result;
  };
  return statement;
};
