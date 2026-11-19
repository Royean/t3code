# Always-on agents

An always-on agent is an experience: it looks continuous, but it spends most of its time asleep. A short run wakes for an inbox event, saves what must carry forward, and goes back to sleep.

The shape comes from William Hu's article [Building always-on agents](https://claude.com/resources/articles/building-always-on-agents) (13 November 2026). This page separates that article, the T3 behavior that already existed, and the restricted build in this fork.

## What the article describes

The article's loop is the same agent loop as any other agent. Five primitives sit around it:

- **Inbox.** Sources append events. One worker at a time batches whatever is pending. A user reply continues the related session. Other events start a new session. The handler acknowledges quickly. The inbox is the source of truth if the worker crashes.
- **Timers.** A deadline or a recurring schedule lives outside the inbox. A scheduler writes a due timer into the inbox. The timer does not start a run itself.
- **Memory.** Instructions load on every run. Notes and task progress load as an index; the body is fetched when needed. Past messages to the user are kept so they are not repeated. Store only what cannot be looked up again, or store a pointer.
- **Permissions.** A deferred tool call ends the run, saves the session, and frees the worker. The answer returns as an inbox event and resumes that session. The article also describes a credential proxy outside the agent and auto-mode classifiers. This build does not include those.
- **Outbox.** A run ends by delivering a message, staying silent, or waiting for approval.

The article's dreaming pass and risk classifiers are not part of this build.

## What T3 already did

T3 already persists threads, runs a five-second `Scheduler`, and stores projections in SQLite. Scheduled tasks can wake a thread on an interval, a clock time, or a webhook. Provider sessions go idle and thread settlement can detach them. Permission modes are enforced by each provider's own runtime; T3 does not sit in front of every tool call.

The fork branch `feat/event-orchestration` is a separate GitHub role pipeline. This always-on work does not depend on it and does not change it.

## What this build adapts

Always-on state lives in its own SQLite tables on the existing T3 server. There is no Temporal service and no Anthropic-only SDK.

The worker is fail-closed. No installed provider runtime was verified to intercept each tool before it runs, so an always-on run never starts Claude, Codex, or another provider process. The run applies only an allowlist: save a note or progress pointer, set a timer, deliver a message into this agent's session feed, stay quiet, or ask for approval. Shell commands are not an action. Credential-shaped text is rejected and is not written to the inbox, memory, logs, or prompts.

That is narrower than the article. The secret proxy is not implemented. Unattended tool use is not available. A future executor can sit behind the same inbox only after a runtime can prove a per-tool boundary.

Other adaptations, marked as such:

- Events that arrive while a run is in progress wait for the next run. The article also allows folding them in or interrupting; this version does not.
- Missed ticks of a recurring timer collapse into one inbox event.
- Deliveries go to this agent's session feed in T3. They are not email or Slack. Repeated delivery keys are deduplicated, and a failed delivery retries. The default action is silence.
- A heartbeat only refreshes the worker lease. It is not progress. If the process dies while an external effect is unresolved, the run becomes `unknown` and is not replayed. Confirming it records that the effect stayed withheld.
- On process start, a leftover lease is treated as a crashed worker, because one T3 server owns the database.

## Use it

Open **Settings → Always-on agents** in the web app. The desktop app uses the same page. The mobile app does not have this screen yet; the websocket methods and schemas are in the shared contract.

Create an agent with a project id and the instructions that should load on every run. Queue an event as a new session or as a continuation of the user session. Pause stops new runs and leaves the inbox in place. Resume continues. Approve or reject a waiting card; reject does not perform the approved action. Confirm an unknown run only after you have checked the external system yourself.

The page needs `orchestration:read` to view state and `orchestration:operate` to create agents, queue events, approve, confirm, pause, or cancel. A reader cannot call the operate methods.

## Try the tests

From the repository root, with the dev server stopped and without pointing at `~/.t3`:

```sh
vp test run apps/server/src/alwaysOn/AlwaysOnService.test.ts apps/server/src/auth/RpcAuthorization.test.ts apps/web/src/components/settings/settingsSearch.test.ts
```

The service tests use an in-memory database and fixtures. They do not call a model or a provider.
