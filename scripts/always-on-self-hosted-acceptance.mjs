#!/usr/bin/env node
// Local acceptance for the self-hosted always-on integration.
// Starts only loopback processes and a fixture model. It does not use Clerk,
// Cloudflare, a public tunnel, or a paid model. Tokens are redacted in evidence.

import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { createServer as createHttpServer, request as httpRequest } from "node:http";
import { createServer as createHttpsServer } from "node:https";
import { mkdirSync, writeFileSync, readFileSync, chmodSync, existsSync } from "node:fs";
import { execFileSync } from "node:child_process";
import path from "node:path";
import { setTimeout as delay } from "node:timers/promises";

const ROOT = "/workspace";
const NODE = process.env.HOME + "/.nvm/versions/node/v26.13.1/bin";
const PATH = `${NODE}:${ROOT}/node_modules/.bin:${process.env.PATH}`;
const HOME = "/tmp/t3-acceptance-env-a";
const HOME_B = "/tmp/t3-acceptance-env-b";
const DIRECTORY = "/tmp/t3-acceptance-directory";
const EVIDENCE_DIR = "/tmp/t3-acceptance-evidence";
const MODEL_PORT = 18931;
const PROXY_PORT = 18443;
const DIRECTORY_PORT = 3055;
const SERVER_PORT = 13773;
const WEB_PORT = 5733;
const PASSWORD = "fixture-password-1";
const FIXTURE_KEY = "fixture-local-key";
const results = [];
const children = [];
let modelRequests = [];
let releaseHang = () => {};
let hangPromise = Promise.resolve();

mkdirSync(EVIDENCE_DIR, { recursive: true });

const redact = (value) =>
  String(value).replace(/[A-Za-z0-9_-]{12,}/g, (token) => {
    if (["orchestration", "textGenerator", "remember_note", "fixture-model"].some((keep) => token.includes(keep))) {
      return token;
    }
    return `<redacted:${createHash("sha256").update(token).digest("hex").slice(0, 8)}>`;
  });

const record = (id, ok, detail) => {
  const entry = { id, ok, detail: redact(detail) };
  results.push(entry);
  console.log(`${ok ? "PASS" : "FAIL"} ${id}: ${entry.detail}`);
};

const step = async (id, fn) => {
  try {
    const detail = await fn();
    record(id, true, detail ?? "ok");
  } catch (error) {
    record(id, false, error?.stack || error?.message || String(error));
  }
};

const spawnLogged = (name, command, args, extraEnv = {}) => {
  const log = path.join(EVIDENCE_DIR, `${name}.log`);
  const out = writeFileSync.bind(null); // keep lint quiet if unused
  void out;
  const file = spawn("bash", ["-lc", `export PATH=${JSON.stringify(PATH)}; ${[command, ...args].join(" ")}`], {
    cwd: ROOT,
    env: { ...process.env, PATH, ...extraEnv },
    detached: true,
    stdio: ["ignore", "pipe", "pipe"],
  });
  const stream = (chunk) => {
    writeFileSync(log, chunk, { flag: "a" });
  };
  writeFileSync(log, "");
  file.stdout.on("data", stream);
  file.stderr.on("data", stream);
  children.push({ name, pid: file.pid, log });
  return { file, log };
};

const waitFor = async (label, read, predicate, timeoutMs = 90_000) => {
  const start = Date.now();
  let last = "";
  while (Date.now() - start < timeoutMs) {
    last = read();
    if (predicate(last)) return last;
    await delay(500);
  }
  throw new Error(`${label} timed out. Last output:\n${last.slice(-1500)}`);
};

const json = async (url, options = {}) => {
  const response = await fetch(url, options);
  const text = await response.text();
  let body;
  try {
    body = text ? JSON.parse(text) : null;
  } catch {
    body = text.slice(0, 500);
  }
  return { status: response.status, body, headers: response.headers };
};

const cookieHeader = (response) => {
  const raw = response.headers.getSetCookie?.() ?? [];
  return raw.map((cookie) => cookie.split(";", 1)[0]).filter((cookie) => !/=;?$/.test(cookie) && !cookie.endsWith("=")).join("; ");
};

const killChildren = () => {
  for (const child of children.reverse()) {
    try {
      process.kill(-child.pid, "SIGTERM");
    } catch {
      try {
        process.kill(child.pid, "SIGTERM");
      } catch {
        // already gone
      }
    }
  }
};

process.on("exit", killChildren);
process.on("SIGINT", () => {
  killChildren();
  process.exit(1);
});

const startModel = () =>
  new Promise((resolve) => {
    const server = createHttpServer((request, response) => {
      const chunks = [];
      request.on("data", (chunk) => chunks.push(chunk));
      request.on("end", () => {
        const raw = Buffer.concat(chunks).toString("utf8");
        let body = {};
        try {
          body = raw ? JSON.parse(raw) : {};
        } catch {
          body = { unreadable: true };
        }
        const prompt = body.messages?.map((message) => message.content).join("\n") ?? "";
        modelRequests.push({
          url: request.url,
          hasTools: Object.hasOwn(body, "tools"),
          keys: Object.keys(body),
          model: body.model,
          promptIncludes: {
            instructions: prompt.includes("Stay quiet unless asked."),
            memory: prompt.includes("Build is green") || prompt.includes("Notes:"),
            event: prompt.slice(0, 240),
            timerDue: prompt.includes("Timer due."),
            hang: prompt.includes("HANG"),
            approved: prompt.includes("The user approved"),
            rejected: prompt.includes("The user rejected"),
          },
          authorization: Boolean(request.headers.authorization),
        });
        const reply = (decision) => {
          response.writeHead(200, { "content-type": "application/json" });
          response.end(JSON.stringify({ choices: [{ finish_reason: "stop", message: { content: JSON.stringify(decision) } }] }));
        };
        if (request.url?.endsWith("/models")) {
          response.writeHead(200, { "content-type": "application/json" });
          response.end(JSON.stringify({ data: [{ id: "fixture-model", name: "Fixture" }] }));
          return;
        }
        if (prompt.includes("HANG")) {
          hangPromise = new Promise((done) => {
            releaseHang = () => {
              reply({ summary: "Released.", actions: [{ type: "silent", key: "hung" }] });
              done();
            };
          });
          return;
        }
        if (prompt.includes("SHELL")) {
          reply({ summary: "I will use a shell.", actions: [{ type: "shell", command: "rm -rf /" }] });
          return;
        }
        if (prompt.includes("REJECT THIS")) {
          reply({
            summary: "Need a rejection.",
            actions: [{ type: "request_approval", key: "reject-me", summary: "Reject this?", onApprove: [] }],
          });
          return;
        }
        if (prompt.includes("NEED APPROVAL")) {
          reply({
            summary: "Need approval.",
            actions: [{ type: "request_approval", key: "ask", summary: "Send the update?", onApprove: [] }],
          });
          return;
        }
        if (prompt.includes("OTHER EVENT")) {
          reply({
            summary: "Did other work.",
            actions: [{ type: "remember_note", key: "other", title: "Other work", body: "done" }],
          });
          return;
        }
        if (prompt.includes("The user approved")) {
          reply({
            summary: "Approved send.",
            actions: [{ type: "deliver", key: "send", dedupeKey: "ship", body: "Ship it." }],
          });
          return;
        }
        if (prompt.includes("The user rejected")) {
          reply({ summary: "Staying quiet.", actions: [{ type: "silent", key: "quiet" }] });
          return;
        }
        if (prompt.includes("ARM A TIMER")) {
          const dueAt = new Date(Date.now() + 2000).toISOString();
          reply({
            summary: "Armed.",
            actions: [{ type: "set_timer", key: "soon", timerKind: "deadline", dueAt }],
          });
          return;
        }
        if (prompt.includes("Timer due.")) {
          reply({ summary: "Timer seen.", actions: [{ type: "silent", key: "timer-quiet" }] });
          return;
        }
        if (prompt.includes("MISSING")) {
          response.writeHead(500, { "content-type": "application/json" });
          response.end(JSON.stringify({ error: { message: "fixture forced failure" } }));
          return;
        }
        reply({
          summary: "Noted the build.",
          actions: [
            { type: "remember_note", key: "build", title: "Build is green", body: "The build is green." },
            { type: "deliver", key: "tell", dedupeKey: "build-green", body: "The build is green." },
          ],
        });
      });
    });
    server.listen(MODEL_PORT, "127.0.0.1", () => resolve(server));
  });

const sql = (home, statement) =>
  execFileSync("python3", ["-c", `
import sqlite3,sys
con=sqlite3.connect(${JSON.stringify(`file:${home}/userdata/statev2.sqlite?mode=ro`)}, uri=True)
cur=con.cursor()
print(repr(cur.execute(sys.argv[1]).fetchall()))
`, statement], { encoding: "utf8" }).trim();

const sqlWrite = (home, statement) =>
  execFileSync("python3", ["-c", `
import sqlite3,sys
con=sqlite3.connect(${JSON.stringify(`${home}/userdata/statev2.sqlite`)})
con.execute(sys.argv[1])
con.commit()
print("ok")
`, statement], { encoding: "utf8" }).trim();

const main = async () => {
  execFileSync("rm", ["-rf", HOME, HOME_B, DIRECTORY]);
  const model = await startModel();
  record("model-listen", true, `fixture model on 127.0.0.1:${MODEL_PORT}`);

  mkdirSync(`${HOME}/userdata`, { recursive: true });
  writeFileSync(
    `${HOME}/userdata/settings.json`,
    JSON.stringify({
      textGenerators: {
        "fixture-generator": {
          kind: "openaiCompatible",
          label: "Fixture",
          baseUrl: `http://127.0.0.1:${MODEL_PORT}/v1`,
          apiKey: FIXTURE_KEY,
          enabled: true,
        },
      },
    }),
  );

  const server = spawnLogged(
    "env-a",
    "vp run dev --home-dir " + HOME + " --no-seed --no-auto-bootstrap-project-from-cwd",
    [],
    { T3CODE_DEV_SEED: "0", T3CODE_AUTO_BOOTSTRAP_PROJECT_FROM_CWD: "0" },
  );
  await waitFor("env-a", () => readFileSync(server.log, "utf8"), (text) => text.includes("pairingUrl:"));
  const baseDir = readFileSync(server.log, "utf8").match(/baseDir=(\S+)/)?.[1];
  record("env-a-home", baseDir === HOME, `baseDir=${baseDir}`);

  const descriptor = await json(`http://127.0.0.1:${SERVER_PORT}/.well-known/t3/environment`);
  const environmentId = descriptor.body?.environmentId;
  record(
    "well-known",
    descriptor.status === 200 && typeof environmentId === "string",
    `status=${descriptor.status} migration=${descriptor.body?.databaseMigration} idLength=${environmentId?.length ?? 0}`,
  );
  const fork = sql(HOME, "SELECT migration_id, name FROM t3_fork_migrations ORDER BY migration_id");
  const upstreamAlwaysOn = sql(HOME, "SELECT COUNT(*) FROM effect_sql_migrations WHERE name = 'AlwaysOn'");
  record(
    "fresh-database-fork-migrations",
    fork.includes("EventOrchestration") && fork.includes("AlwaysOn") && upstreamAlwaysOn.includes("0"),
    `fork=${fork} upstreamAlwaysOn=${upstreamAlwaysOn}`,
  );

  const issued = execFileSync(
    "node",
    ["apps/server/src/bin.ts", "auth", "session", "issue", "--base-dir", HOME, "--json"],
    { cwd: ROOT, env: { ...process.env, PATH }, encoding: "utf8" },
  );
  const session = JSON.parse(issued.slice(issued.indexOf("{")));
  const environmentCredential = session.token ?? session.access_token ?? session.credential;
  record(
    "environment-credential",
    typeof environmentCredential === "string" && environmentCredential.length > 10,
    `issued session keys=${Object.keys(session).join(",")}`,
  );

  execFileSync("openssl", [
    "req", "-x509", "-newkey", "rsa:2048", "-keyout", `${EVIDENCE_DIR}/key.pem`, "-out", `${EVIDENCE_DIR}/cert.pem`,
    "-days", "1", "-nodes", "-subj", "/CN=127.0.0.1", "-addext", "subjectAltName=IP:127.0.0.1",
  ], { stdio: "ignore" });
  const proxy = createHttpsServer(
    { key: readFileSync(`${EVIDENCE_DIR}/key.pem`), cert: readFileSync(`${EVIDENCE_DIR}/cert.pem`) },
    (request, response) => {
      const headers = { ...request.headers, host: `localhost:${WEB_PORT}`, "x-forwarded-proto": "https" };
      const proxied = httpRequest(
        { hostname: "localhost", port: WEB_PORT, path: request.url, method: request.method, headers },
        (incoming) => {
          response.writeHead(incoming.statusCode ?? 502, incoming.headers);
          incoming.pipe(response);
        },
      );
      proxied.on("error", () => response.writeHead(502).end());
      request.pipe(proxied);
    },
  );
  proxy.on("upgrade", (request, socket, head) => {
    const headers = { ...request.headers, host: `localhost:${WEB_PORT}`, "x-forwarded-proto": "https" };
    const proxied = httpRequest({
      hostname: "localhost",
      port: WEB_PORT,
      path: request.url,
      method: request.method,
      headers,
    });
    proxied.on("upgrade", (incoming, upstream, upstreamHead) => {
      const headerLines = Object.entries(incoming.headers).map(([key, value]) => `${key}: ${value}`);
      socket.write(`HTTP/1.1 ${incoming.statusCode} ${incoming.statusMessage}\r\n${headerLines.join("\r\n")}\r\n\r\n`);
      if (upstreamHead.length) socket.write(upstreamHead);
      upstream.pipe(socket);
      socket.pipe(upstream);
    });
    proxied.on("error", () => socket.destroy());
    socket.on("error", () => {});
    proxied.end(head);
  });
  await new Promise((resolve) => proxy.listen(PROXY_PORT, "127.0.0.1", resolve));
  record("local-https", true, `https://127.0.0.1:${PROXY_PORT} proxies the dev web origin`);

  const hash = execFileSync("node", ["infra/self-hosted/src/bin.ts", "hash-password"], {
    cwd: ROOT,
    env: { ...process.env, PATH },
    input: PASSWORD,
    encoding: "utf8",
  }).trim();
  mkdirSync(DIRECTORY, { recursive: true, mode: 0o700 });
  chmodSync(DIRECTORY, 0o700);
  const configPath = `${DIRECTORY}/config.json`;
  writeFileSync(
    configPath,
    JSON.stringify({
      host: "127.0.0.1",
      port: DIRECTORY_PORT,
      databasePath: `${DIRECTORY}/sessions.sqlite`,
      account: { accountId: "fixture", username: "fixture", passwordHash: hash },
      environments: [{
        environmentId,
        label: "Fixture computer",
        origin: `https://127.0.0.1:${PROXY_PORT}`,
        pairingOrigin: `http://127.0.0.1:${SERVER_PORT}`,
        credential: environmentCredential,
      }],
      allowedOrigins: [`https://127.0.0.1:${PROXY_PORT}`, "http://127.0.0.1:5733", "http://localhost:5733"],
    }),
  );
  chmodSync(configPath, 0o600);
  const directory = spawnLogged("directory", "node infra/self-hosted/src/bin.ts --config " + configPath, []);
  await waitFor("directory", () => readFileSync(directory.log, "utf8"), (text) => text.includes("Self-hosted Connect is ready."));
  const health = await json(`http://127.0.0.1:${DIRECTORY_PORT}/health`);
  record("directory-health", health.status === 200 && health.body?.ok === true, `status=${health.status}`);

  const anonymous = await json(`http://127.0.0.1:${DIRECTORY_PORT}/v1/environments`);
  record("directory-unauthenticated", anonymous.status === 401, `status=${anonymous.status}`);
  const login = await json(`http://127.0.0.1:${DIRECTORY_PORT}/v1/auth/login`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ username: "fixture", password: PASSWORD }),
  });
  record("directory-login", login.status === 200 && login.body?.accountId === "fixture", `status=${login.status}`);
  const access = login.body?.accessToken;
  const listed = await json(`http://127.0.0.1:${DIRECTORY_PORT}/v1/environments`, {
    headers: { authorization: `Bearer ${access}` },
  });
  const listedText = JSON.stringify(listed.body);
  record(
    "directory-lists-environment",
    listed.status === 200 && listed.body?.environments?.[0]?.environmentId === environmentId && !listedText.includes(environmentCredential) && !listedText.includes("always_on"),
    `status=${listed.status} available=${listed.body?.environments?.[0]?.available} leakedCredential=${listedText.includes(environmentCredential)}`,
  );
  const connected = await json(`http://127.0.0.1:${DIRECTORY_PORT}/v1/environments/${environmentId}/connect`, {
    method: "POST",
    headers: { authorization: `Bearer ${access}`, "content-type": "application/json" },
    body: "{}",
  });
  const pairingUrl = connected.body?.pairingUrl;
  record(
    "directory-pairing-link",
    connected.status === 200 && typeof pairingUrl === "string" && pairingUrl.startsWith(`https://127.0.0.1:${PROXY_PORT}/pair#token=`),
    `status=${connected.status}`,
  );
  const reuse = await json(`http://127.0.0.1:${DIRECTORY_PORT}/v1/environments/${environmentId}/connect`, {
    method: "POST",
    headers: { authorization: `Bearer ${access}`, "content-type": "application/json" },
    body: "{}",
  });
  const reuseToken = decodeURIComponent(new URL(reuse.body.pairingUrl).hash.split("token=")[1]);
  const firstUse = await json(`http://127.0.0.1:${SERVER_PORT}/api/auth/browser-session`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ credential: reuseToken }),
  });
  const secondUse = await json(`http://127.0.0.1:${SERVER_PORT}/api/auth/browser-session`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ credential: reuseToken }),
  });
  record("pairing-link-single-use", firstUse.status === 200 && secondUse.status >= 400, `first=${firstUse.status} second=${secondUse.status}`);

  const shortPair = execFileSync("node", ["apps/server/src/bin.ts", "pair", "--base-dir", HOME, "--ttl", "2 seconds"], {
    cwd: ROOT, env: { ...process.env, PATH }, encoding: "utf8",
  });
  const shortUrl = shortPair.match(/Pairing URL: (\S+)/)?.[1];
  const shortToken = shortUrl ? decodeURIComponent(new URL(shortUrl).hash.split("token=")[1] ?? "") : "";
  await delay(3000);
  const expired = await json(`http://127.0.0.1:${SERVER_PORT}/api/auth/browser-session`, {
    method: "POST",
    headers: { "content-type": "application/json" },
    body: JSON.stringify({ credential: shortToken }),
  });
  record("pairing-link-expires", Boolean(shortToken) && expired.status >= 400, `status=${expired.status}`);

  const noSession = await json(`http://127.0.0.1:${SERVER_PORT}/api/auth/session`);
  const noJobs = await json(`http://127.0.0.1:${SERVER_PORT}/api/event-orchestration/jobs`);
  record("unauthenticated-environment", noSession.body?.authenticated === false && noJobs.status === 401, `session=${noSession.body?.authenticated} jobs=${noJobs.status}`);

  const operate = { authorization: `Bearer ${environmentCredential}`, "content-type": "application/json" };
  const jobs = await json(`http://127.0.0.1:${SERVER_PORT}/api/event-orchestration/jobs`, { headers: operate });
  const configured = await json(`http://127.0.0.1:${SERVER_PORT}/api/event-orchestration/config`, {
    method: "POST",
    headers: operate,
    body: JSON.stringify({
      enabled: false,
      projectId: "project-acceptance",
      coordinatorExecutorId: "primary",
      executors: [{ id: "primary", modelSelection: { instanceId: "fixture-generator", model: "fixture-model" } }],
      roles: [],
      allowedRepositories: ["acme/app"],
      allowedEventTypes: ["issues.ready"],
      runtimeMode: "auto-accept-edits",
      interactionMode: "default",
    }),
  });
  const submitted = await json(`http://127.0.0.1:${SERVER_PORT}/api/event-orchestration/events`, {
    method: "POST",
    headers: operate,
    body: JSON.stringify({
      source: "github",
      deliveryId: "delivery-acceptance",
      repository: "acme/app",
      eventType: "issues.ready",
      text: "Do not launch a provider.",
    }),
  });
  const jobRows = sql(HOME, "SELECT COUNT(*) FROM event_orchestration_jobs");
  record(
    "event-orchestration-stays-disabled",
    jobs.status === 200 && configured.status === 200 && submitted.status >= 400 && jobRows.includes("0"),
    `jobs=${jobs.status} config=${configured.status} submit=${submitted.status} body=${JSON.stringify(submitted.body).slice(0, 180)} rows=${jobRows}`,
  );

  const playwright = await import("/workspace/node_modules/.pnpm/playwright@1.64.1/node_modules/playwright/index.js");
  const chromium = playwright.chromium ?? playwright.default.chromium;
  const browser = await chromium.launch({ headless: true });
  const page = await browser.newPage({ viewport: { width: 1440, height: 1200 }, ignoreHTTPSErrors: true });
  page.on("console", (msg) => {
    if (msg.type() === "error") writeFileSync(path.join(EVIDENCE_DIR, "browser.log"), msg.text().slice(0, 300) + "\n", { flag: "a" });
  });
  page.on("response", (response) => {
    const url = response.url();
    if (url.includes("/api/") || url.includes("/pair")) {
      writeFileSync(path.join(EVIDENCE_DIR, "browser.log"), `${response.status()} ${url.split("?")[0].split("#")[0]}\n`, { flag: "a" });
    }
  });
  const directPair = `http://localhost:${WEB_PORT}/pair#${new URL(pairingUrl).hash.slice(1)}`;
  await page.goto(directPair, { waitUntil: "domcontentloaded", timeout: 60000 });
  await page.waitForURL((current) => !current.pathname.startsWith("/pair"), { timeout: 30000, waitUntil: "domcontentloaded" });
  await page.goto(`http://localhost:${WEB_PORT}/settings/always-on`, { waitUntil: "domcontentloaded" });
  await page.getByLabel("Agent title").waitFor({ timeout: 30000 });
  record("paired-with-directory-token", true, "directory token opened the dev origin");
  await page.screenshot({ path: "/opt/cursor/artifacts/acceptance-always-on-empty.png", fullPage: true });
  await page.getByLabel("Agent title").fill("Desk");
  await page.getByLabel("Project id").fill("project-acceptance");
  await page.getByLabel("Instructions").fill("Stay quiet unless asked.");
  await page.getByLabel("Provider instance id").fill("fixture-generator");
  await page.getByRole("textbox", { name: "Model" }).fill("fixture-model");
  await page.getByRole("button", { name: "Create agent" }).click();
  await page.getByRole("button", { name: /Desk/ }).waitFor({ timeout: 15000 });
  const before = modelRequests.length;
  await page.getByLabel("Event").fill("SUCCESS remember the build is green");
  await page.getByRole("button", { name: "Queue event" }).click();
  const started = Date.now();
  while (Date.now() - started < 20000 && !modelRequests.slice(before).some((item) => item.url?.includes("chat/completions"))) {
    await delay(250);
  }
  const call = modelRequests.slice(before).find((item) => item.url?.includes("chat/completions"));
  record(
    "model-request-has-no-tools",
    Boolean(call) && call.hasTools === false && call.keys.includes("model") && call.keys.includes("messages") && call.promptIncludes.instructions,
    `keys=${call?.keys?.join(",") ?? "none"} hasTools=${call?.hasTools} instructions=${call?.promptIncludes?.instructions}`,
  );
  await page.getByText("Noted the build.").waitFor({ timeout: 20000 });
  await page.getByText("Noted the build.").scrollIntoViewIfNeeded();
  await page.screenshot({ path: "/opt/cursor/artifacts/acceptance-model-success.png" });
  const runs = sql(HOME, "SELECT status, model_summary, model_error, executed_actions_json FROM always_on_run");
  const notes = sql(HOME, "SELECT title, body FROM always_on_memory WHERE kind = 'note'");
  const deliveries = sql(HOME, "SELECT body FROM always_on_delivery");
  record("model-success-persisted", runs.includes("Noted the build.") && notes.includes("Build is green") && deliveries.includes("The build is green."), `${runs} ${notes} ${deliveries}`);

  const requestsBeforeShell = modelRequests.length;
  await page.getByLabel("Event").fill("SHELL do not run this");
  await page.getByRole("button", { name: "Queue event" }).click();
  await page.getByText("will not run").waitFor({ timeout: 20000 });
  const shellCalls = modelRequests.length - requestsBeforeShell;
  const effectsAfterShell = sql(HOME, "SELECT COUNT(*) FROM always_on_effect");
  record("shell-rejected-without-effects", shellCalls >= 1 && effectsAfterShell.includes("(2,)"), `newCalls=${shellCalls} effects=${effectsAfterShell}`);
  await page.screenshot({ path: "/opt/cursor/artifacts/acceptance-shell-rejected.png" });

  await page.getByLabel("Event").fill("NEED APPROVAL before sending");
  await page.getByRole("button", { name: "Queue event" }).click();
  await page.getByRole("button", { name: "Approve" }).waitFor({ timeout: 20000 });
  const beforeOther = modelRequests.length;
  await page.getByLabel("Event").fill("OTHER EVENT arrived meanwhile");
  await page.getByRole("button", { name: "Queue event" }).click();
  await page.getByText("completed Did other work.").waitFor({ timeout: 20000 });
  record("other-event-while-approval", modelRequests.length > beforeOther, "the other event completed before approval");
  await page.getByRole("button", { name: "Approve" }).click();
  await page.getByText("Ship it.").waitFor({ timeout: 20000 });
  await page.reload({ waitUntil: "domcontentloaded" });
  await page.getByRole("button", { name: /Desk/ }).click();
  await page.getByText("Ship it.").waitFor({ timeout: 20000 });
  await page.screenshot({ path: "/opt/cursor/artifacts/acceptance-approval.png" });
  record("approval-persists-after-reload", true, "approved delivery still visible");

  await page.getByLabel("Event").fill("REJECT THIS request");
  await page.getByRole("button", { name: "Queue event" }).click();
  await page.getByRole("button", { name: "Reject" }).waitFor({ timeout: 20000 });
  const deliveriesBeforeReject = sql(HOME, "SELECT COUNT(*) FROM always_on_delivery");
  await page.getByRole("button", { name: "Reject" }).click();
  await page.getByText("Staying quiet.").waitFor({ timeout: 20000 });
  const deliveriesAfterReject = sql(HOME, "SELECT COUNT(*) FROM always_on_delivery");
  record("reject-does-not-deliver", deliveriesBeforeReject === deliveriesAfterReject, `before=${deliveriesBeforeReject} after=${deliveriesAfterReject}`);

  const beforeTimer = modelRequests.length;
  await page.getByLabel("Event").fill("ARM A TIMER now");
  await page.getByRole("button", { name: "Queue event" }).click();
  const timerStart = Date.now();
  while (Date.now() - timerStart < 15000 && !modelRequests.slice(beforeTimer).some((item) => item.promptIncludes.event.includes("Timer due."))) {
    await delay(500);
  }
  const timerCalled = modelRequests.slice(beforeTimer).some((item) => item.promptIncludes.timerDue);
  record("timer-wakes-through-scheduler", timerCalled, `elapsedMs=${Date.now() - timerStart}`);

  const beforePause = modelRequests.length;
  await page.getByRole("button", { name: "Pause" }).click();
  await page.getByRole("button", { name: "Resume" }).waitFor({ timeout: 10000 });
  await page.getByLabel("Event").fill("SHOULD STAY PENDING");
  await page.getByRole("button", { name: "Queue event" }).click();
  await delay(3000);
  record("pause-does-not-call-model", modelRequests.length === beforePause, `before=${beforePause} after=${modelRequests.length}`);
  await page.getByRole("button", { name: "Resume" }).click();

  await browser.close();

  const callsBeforeRestart = modelRequests.length;
  const deliveriesBeforeRestart = sql(HOME, "SELECT COUNT(*) FROM always_on_delivery");
  const serverChild = children.find((child) => child.name === "env-a");
  process.kill(-serverChild.pid, "SIGTERM");
  await delay(1500);
  sqlWrite(HOME, `INSERT INTO always_on_run (run_id, agent_id, session_id, status, uncertain, cancel_requested, context_json, event_ids_json, executed_actions_json, started_at)
    SELECT 'run-unknown', agent_id, session_id, 'running', 1, 0, context_json, '[]', '[]', started_at FROM always_on_run LIMIT 1`);
  sqlWrite(HOME, `INSERT INTO always_on_effect (agent_id, action_key, run_id, status)
    SELECT agent_id, 'unknown-key', 'run-unknown', 'uncertain' FROM always_on_run WHERE run_id = 'run-unknown'`);
  const restarted = spawnLogged("env-a-restart", "vp run dev --home-dir " + HOME + " --no-seed --no-auto-bootstrap-project-from-cwd", [], { T3CODE_DEV_SEED: "0" });
  await waitFor("env-a-restart", () => readFileSync(restarted.log, "utf8"), (text) => text.includes("Listening on"));
  await delay(1000);
  const unknownStatus = sql(HOME, "SELECT status FROM always_on_run WHERE run_id = 'run-unknown'");
  const deliveriesAfterRestart = sql(HOME, "SELECT COUNT(*) FROM always_on_delivery");
  record(
    "restart-keeps-deliveries-and-does-not-call-model",
    modelRequests.length === callsBeforeRestart && deliveriesAfterRestart === deliveriesBeforeRestart,
    `calls=${modelRequests.length} deliveries=${deliveriesAfterRestart}`,
  );
  record("unknown-effect-is-not-replayed", unknownStatus.includes("unknown"), unknownStatus);

  const readPair = await json(`http://127.0.0.1:${SERVER_PORT}/api/auth/pairing-token`, {
    method: "POST",
    headers: { authorization: `Bearer ${environmentCredential}`, "content-type": "application/json" },
    body: JSON.stringify({ label: "reader", scopes: ["orchestration:read"] }),
  });
  record("read-scope-pairing-minted", readPair.status === 200, `status=${readPair.status}`);
  if (readPair.status === 200 && readPair.body?.credential) {
    const readerBrowser = await chromium.launch({ headless: true });
    const readerPage = await readerBrowser.newPage({ viewport: { width: 1440, height: 900 } });
    const readerToken = encodeURIComponent(readPair.body.credential);
    await readerPage.goto(`http://localhost:${WEB_PORT}/pair#token=${readerToken}`, { waitUntil: "domcontentloaded" });
    await readerPage.waitForURL((current) => !current.pathname.startsWith("/pair"), { timeout: 30000, waitUntil: "domcontentloaded" });
    await readerPage.goto(`http://localhost:${WEB_PORT}/settings/always-on`, { waitUntil: "domcontentloaded" });
    await readerPage.getByRole("button", { name: "Create agent" }).waitFor({ timeout: 20000 });
    const disabled = await readerPage.getByRole("button", { name: "Create agent" }).isDisabled();
    await readerPage.screenshot({ path: "/opt/cursor/artifacts/acceptance-read-only.png" });
    record("read-scope-create-disabled", disabled, `disabled=${disabled}`);
    await readerBrowser.close();
  }

  try {
    mkdirSync(`${HOME_B}/userdata`, { recursive: true });
    const envB = spawnLogged("env-b", "vp run dev --home-dir " + HOME_B + " --no-seed --port 14773 --dev-url http://127.0.0.1:6733", [], { T3CODE_DEV_SEED: "0" });
    await waitFor("env-b", () => readFileSync(envB.log, "utf8"), (text) => text.includes("Listening on"), 60000);
    const other = await json("http://127.0.0.1:14773/.well-known/t3/environment");
    const otherAgents = sql(HOME_B, "SELECT COUNT(*) FROM always_on_agent");
    record(
      "second-environment-does-not-see-agents",
      other.status === 200 && other.body?.environmentId !== environmentId && otherAgents.includes("(0,)"),
      `status=${other.status} agents=${otherAgents}`,
    );
  } catch (error) {
    record("second-environment-does-not-see-agents", false, error.message);
  }

  writeFileSync(path.join(EVIDENCE_DIR, "model-requests.json"), JSON.stringify(modelRequests, null, 2));
  writeFileSync(path.join(EVIDENCE_DIR, "results.json"), JSON.stringify(results, null, 2));
  model.close();
  proxy.close();
};

main().catch((error) => {
  record("harness", false, error?.stack || String(error));
  writeFileSync(path.join(EVIDENCE_DIR, "results.json"), JSON.stringify(results, null, 2));
  process.exitCode = 1;
}).finally(() => {
  killChildren();
  setTimeout(() => process.exit(process.exitCode ?? 0), 250);
});
