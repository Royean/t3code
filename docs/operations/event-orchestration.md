# Event-driven role orchestration

This fork can accept gated GitHub events from an event hub. The event hub receives, normalizes, deduplicates and delivers events. T3 independently owns the task records and execution: it starts a coordinator thread, assigns professional roles to real T3 child agents, and persists attempts, progress, checkpoints and outcomes. The event hub's acknowledgement tracks delivery only; it is not a task status record.

The feature is off until configured. It currently targets one project in one T3 environment. Web, desktop and mobile can follow the resulting normal threads. The control API is intended for an authenticated integration, rather than a new client settings screen.

## Configure roles and executors

Use an isolated T3 home when testing this fork. Do not start it against another running installation's data directory. Follow [development setup](./development.md) and run focused server tests before connecting a real event source.

Create a private configuration file on the T3 host. Role files remain in their existing private checkout; they are not copied into this public repository. Registration snapshots the role text for each job, so changing the registry does not silently change an in-flight assignment.

```json
{
  "enabled": true,
  "projectId": "YOUR_REGISTERED_PROJECT_ID",
  "coordinatorExecutorId": "primary",
  "executors": [
    {
      "id": "primary",
      "modelSelection": { "instanceId": "codex", "model": "YOUR_CONFIGURED_MODEL" }
    },
    {
      "id": "backup",
      "modelSelection": { "instanceId": "claudeAgent", "model": "YOUR_BACKUP_MODEL" }
    }
  ],
  "roles": [
    {
      "id": "developer",
      "title": "Developer",
      "instructionsPath": "/absolute/private/roles/developer/ROLE.md",
      "executorIds": ["primary", "backup"]
    },
    {
      "id": "acceptance",
      "title": "Test acceptance engineer",
      "instructionsPath": "/absolute/private/roles/test-acceptance-engineer/ROLE.md",
      "executorIds": ["backup"]
    }
  ],
  "allowedRepositories": ["acme/app"],
  "allowedEventTypes": ["issues.ready", "pull_request.ready"],
  "runtimeMode": "auto-accept-edits",
  "interactionMode": "default"
}
```

Use provider instance and model IDs that are actually installed and enabled in the destination project. Registration does not grant tools, credentials or a higher organization runtime mode. A role's referenced files and skills must also be available in that environment.

Send the configuration to `POST /api/event-orchestration/config`. The bearer session needs both `settings:write` and `orchestration:operate`; an event sender only needs `orchestration:operate`. Use the environment's existing authenticated access-token flow. Keep tokens out of event bodies, role files and source control.

## Deliver an event

Keep the hub's existing gate. Only approved repository and event types should reach T3. For example, submit the following to `POST /api/event-orchestration/events` after an issue has passed its label gate:

```json
{
  "source": "github",
  "deliveryId": "github-delivery-unique-id",
  "repository": "acme/app",
  "eventType": "issues.ready",
  "subject": "acme/app#123",
  "text": "Investigate and fix the approved issue; preserve its acceptance requirements.",
  "url": "https://github.com/acme/app/issues/123"
}
```

Admission returns only `{ "accepted": true, "jobId": "..." }`, a delivery acknowledgement and task reference. Retry the same delivery with the same ID if that acknowledgement is lost. It does not report execution status or business completion. The event cannot choose a role file, model or permission mode. GitHub signatures and label verification remain the delivering hub's responsibility; this is not a raw unauthenticated GitHub webhook endpoint.

The coordinator uses `event_task_assign`, `event_task_read` and `event_task_report`. It coordinates and verifies work; implementation belongs to the role tasks. Tasks can depend on previously declared tasks; dependent work starts after those tasks are accepted. This version serializes child tasks sharing the registered project workspace. The managed MCP path rejects ordinary handoff tools that would bypass role and attempt accounting. Runtime permissions still govern shell tools and external systems; role registration is not an operating-system sandbox.

## Read task state from T3

T3's task service is the authoritative source for lifecycle state. A task dashboard or other authorized client reads `GET /api/event-orchestration/jobs` and `GET /api/event-orchestration/jobs/:jobId` with `orchestration:read`. Tasks continue to update without a connection back to the event hub. Cancellation, retry limits, failure recovery and acceptance remain in T3.

For incremental views, read `GET /api/event-orchestration/changes?after=0&limit=100`. Apply the returned changes to the client's view and continue from `nextCursor`; an empty page keeps the cursor. The feed carries identifiers, status and timestamps, without the raw event or role instructions. A client projection is a read model; it does not replace T3's records. No hub source credential, callback or status mirror is required.

No live event hub, dashboard or desktop installation is changed by building this fork.

## Failure, verification and cancellation

Runtime heartbeats record whether T3's provider-session registry still recognizes the execution; they are not a separate operating-system health probe. Progress and checkpoints are recorded separately. A recognized session is not proof that useful work is happening. A lost session whose termination cannot be established becomes `unknown`, without starting a replacement. Missing authorization, authentication or task requirements blocks the task. Retryable failures can use allowlisted executors, with at most three attempts including the first.

Completed child work waits for coordinator acceptance. Keep independent review and acceptance requirements in the role instructions; a provider exit code or a completion claim does not establish business success. Checkpoint summaries should reference accessible commits, artifacts, checks and the next action. Local uncommitted changes on an unavailable machine are not automatically transported.

Waiting tasks retain their executor. A recognized session with no progress checkpoint for ten minutes becomes `stalled`; it does not trigger a second executor. Coordinator launch failures and uncertain recovery become `blocked` with a persisted reason instead of starting an endless series of model turns. After resolving the blocker, an operator can send `{ "jobId": "..." }` to `POST /api/event-orchestration/resume` with `orchestration:operate`. Resume rechecks the old execution and its durable receipt, and keeps the three-launch limit. A still-active or uncertain coordinator cannot be replaced.

To stop a job, send `{ "jobId": "..." }` to `POST /api/event-orchestration/cancel` with `orchestration:operate`. Cancellation remains in progress until T3 can confirm termination. Inspect the job and its normal T3 thread history if recovery needs operator input.
