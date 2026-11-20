# Always-on agents

## Route a new event

Open **Settings → Always On** on the environment that receives events. Choose a default destination there. On each environment that will run events, set the default project and use the standard **Model** picker to choose a model and its options. The destination can be the receiving environment or another environment linked with permission to start work.

Submit events to `POST /api/always-on/events` on the receiving T3 environment using a session with `orchestration:operate`.

```json
{
  "source": "build-monitor",
  "sourceEventId": "build-1281",
  "title": "Investigate failed build",
  "text": "The integration test failed. Find the cause and report back.",
  "modelSelection": {
    "instanceId": "codex",
    "model": "gpt-5.4",
    "options": [{ "id": "reasoningEffort", "value": "high" }]
  }
}
```

The optional `environmentId` selects a destination. An explicit `modelSelection` takes precedence over the destination's default. Use an instance, model, and options available on the destination. A retry with the same source and event ID keeps the first destination and thread.

The response contains `environmentId` and `threadId`. Open the run under **Chats → Always On** to see the native Codex or Claude conversation, respond, stop it, or handle approvals. New events start with approval-required permissions.

## Existing always-on agents

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

This copy is built on `feat/self-hosted-connect` (`e264e79bf0` at the time of the integration). That branch already contains the event-orchestration role pipeline and the optional self-hosted account directory. Always-on does not replace either of them. Official T3 Connect is unchanged.

## What this build adapts

Always-on state lives in its own SQLite tables on the environment's T3 server, applied as fork migration 2 after the event-orchestration fork migration. It does not take an upstream migration number. There is no Temporal service and no Anthropic-only SDK. The self-hosted account directory only lists computers and issues a one-time environment pairing link. It does not store always-on memory, provider credentials, or runs, and it has no method that calls them.

A queued event is natural language. The worker sends the instructions, a memory index, and the event to one model completion. The reply is untrusted. The server decodes at most four actions: a note, progress, a sent result, a timer, a delivery into this agent's session, silence, an approval request, or a record that an external effect was not performed. A shell action or any other shape fails the run and executes nothing. Each call is one completion and 20 seconds.

Only two runtimes are enabled, and both are empty of tools in code rather than by a prompt:

- Text generators send a chat message with no `tools` field (`textGeneratorDrivers.ts`).
- Claude text generation, when it is not investigating a checkout, passes `--tools` with an empty list, disables hooks, and sets `--permission-mode dontAsk`. Always-on uses that completion.

Codex stays disabled because `codex exec -s read-only` still has tools. Antigravity stays disabled because rejecting tool callbacks is not a native sandbox. Other agent runtimes stay disabled until they have an empty tool list in code.

A pattern match rejects some credential-shaped strings before they are stored. That is not a guarantee that a secret never reaches a prompt, a log, or a model. Do not put secrets in instructions or events. The article's secret proxy is not implemented. Tests use a fake completion and do not call a paid model. You configure the text generator or Claude account locally before a real run.

Other adaptations, marked as such:

- Events that arrive while a run is in progress wait for the next run. The article also allows folding them in or interrupting; this version does not.
- Missed ticks of a recurring timer collapse into one inbox event.
- Deliveries go to this agent's session feed in T3. They are not email or Slack. Repeated delivery keys are deduplicated, and a failed delivery retries. The default action is silence.
- A heartbeat only refreshes the worker lease. It is not progress. If the process dies while an external effect is unresolved, the run becomes `unknown` and is not replayed. Confirming it records that the effect stayed withheld.
- On process start, a leftover lease is treated as a crashed worker, because one T3 server owns the database.

## Use it

Existing agent conversations can still be opened from **Always On** after pairing with their environment.

Open **Always On** beside **Threads** and select an agent. Its conversation shows what triggered each run, the live status, and the result. Send a message to wake the agent. Messages wait in its queue while it is already running or paused.

Use **Stop run** to stop the current run. Pause the agent to hold future runs while preserving the queue, and resume it to continue. Approve or reject a request in its card. Confirm an unknown outcome after checking what happened in the external system.

The page needs `orchestration:read` to view state and `orchestration:operate` to create agents, queue events, approve, confirm, pause, or cancel. A reader cannot call the operate methods.

## Try the tests

From the repository root, with the dev server stopped and without pointing at `~/.t3`:

```sh
vp test run apps/server/src/alwaysOn/AlwaysOnService.test.ts apps/server/src/alwaysOn/AlwaysOnModel.test.ts apps/server/src/textGeneration/ClaudeTextGeneration.test.ts apps/server/src/auth/RpcAuthorization.test.ts apps/server/src/persistence/Migrations.test.ts apps/server/src/eventOrchestration/http.test.ts infra/self-hosted/src/SelfHostedConnect.test.ts apps/web/src/components/settings/settingsSearch.test.ts
```

The service tests use an in-memory database and a fake completion. They check that the completion is called with memory, and they do not send a paid request. `AlwaysOnModel.test.ts` calls `TextGenerators.complete` through a fixture and does not open a network connection. The Claude text-generation test runs a fake `claude` binary and checks that the transcription completion always-on uses receives `--tools` with an empty list. A recorded local run of the self-hosted directory, a fixture model, and the web page is in [Always-on acceptance on self-hosted connect](../operations/always-on-self-hosted-acceptance.md). A real run still needs a text generator or Claude account configured on your machine.
