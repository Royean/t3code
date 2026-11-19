# Always-on acceptance on self-hosted connect

This records one local run of `scripts/always-on-self-hosted-acceptance.mjs` against `cursor/always-on-on-self-hosted-e104` at `3811fb318f`, based on `feat/self-hosted-connect` at `e264e79bf0`. The script starts a loopback fixture model, one T3 dev server in a fresh home, a local self-signed HTTPS proxy, and the self-hosted account directory. It uses the fixture account `fixture` / `fixture-password-1` and the fixture key `fixture-local-key`. It does not use Clerk, Cloudflare, a public tunnel, a paid model, or any credential from the machine's real T3 home.

Run it from the repository root with Node 26 on `PATH`:

```sh
node scripts/always-on-self-hosted-acceptance.mjs
```

The script deletes and recreates `/tmp/t3-acceptance-env-a`, `/tmp/t3-acceptance-env-b`, and `/tmp/t3-acceptance-directory`. It writes a redacted `results.json` under `/tmp/t3-acceptance-evidence`. Those directories are not part of the checkout.

## Result of the 2026-11-19 run

Passed on the live processes:

- Fresh home `/tmp/t3-acceptance-env-a`. Upstream migration ledger stopped at 98. Fork migrations were EventOrchestration then AlwaysOn. `/.well-known/t3/environment` returned 200.
- Account directory on `127.0.0.1:3055`: unauthenticated `GET /v1/environments` returned 401. The fixture account logged in. The environment list did not contain the environment credential or always-on data. `POST /v1/environments/<id>/connect` with `{}` returned a one-time `https://127.0.0.1:18443/pair#token=...` link.
- That link's credential succeeded once at `POST /api/auth/browser-session` and returned 401 the second time. A separate `pair --ttl 2 seconds` credential returned 401 after three seconds.
- `GET /api/auth/session` without a cookie was unauthenticated. `GET /api/event-orchestration/jobs` without a cookie returned 401.
- With an operate bearer: event-orchestration config saved while `enabled` was false, submit returned `reason: disabled`, and `event_orchestration_jobs` stayed at 0 rows. No provider was launched.
- The directory token opened `http://localhost:5733/pair` and the always-on settings page. Creating an agent for `fixture-generator` / `fixture-model` and queueing a natural-language event produced one HTTP call to the fixture. The JSON body keys were `model` and `messages`. There was no `tools` field. The prompt included the agent instructions. The run completed with summary `Noted the build.`, actions `remember_note` and `deliver`, memory `Build is green`, and delivery `The build is green.`
- A structured `shell` action failed the run and left the effect count at the two actions above.
- While an approval was pending, another event completed (`Did other work.`). Approving resumed the model and delivered `Ship it.` The delivery was still on the page after reload. Rejecting a later approval left the delivery count unchanged and the run summary was `Staying quiet.`
- A deadline timer was consumed by the real five-second scheduler. The next model prompt contained `Timer due.` about 15 seconds after the arming event.
- Pause queued an event and made no further model call during the next three seconds.
- After SIGTERM and a new process on the same home, the fixture received no additional request and the delivery count stayed 2. A run marked `running` with an `uncertain` effect became `unknown`.
- A pairing link limited to `orchestration:read` left **Create agent** disabled. The page showed `needs_confirmation` for the unknown run.

Screenshots from this run:

- `/opt/cursor/artifacts/acceptance-model-success.png` shows `completed Noted the build. remember_note, deliver` and `delivered The build is green.`
- `/opt/cursor/artifacts/acceptance-shell-rejected.png`
- `/opt/cursor/artifacts/acceptance-approval.png` is the reloaded page. The script also waited until the text `Ship it.` was present after reload.
- `/opt/cursor/artifacts/acceptance-read-only.png` shows Create agent unavailable and `Phase: needs_confirmation`.
- `/opt/cursor/artifacts/acceptance-https-pair-failed.png` shows `Still connecting` through the local TLS proxy.
- `/tmp/t3-acceptance-evidence/model-requests.json` from this run has 10 requests. Every recorded body has `hasTools: false` and keys `model`, `messages`.

A second home was not created by the script's own second dev server: `vp run dev --port 14773` still selected `webPort=5733`, which the restarted first server already held, and Vite exited. After that process was gone, a separate `vp run dev --home-dir /tmp/t3-acceptance-env-b --no-seed` migrated a new database to upstream 98 and fork migrations 1 and 2, with `always_on_agent` count 0. The first home still had 1 agent and 2 deliveries.

`vp test run apps/server/src/persistence/Migrations.test.ts` on this same checkout reported 14 passed. That includes the upgrade that adds always-on beside an existing event-orchestration fork migration. It is a unit test, not a substitute for the live server run above.

GitHub reported PR 19 `MERGEABLE` and `statusCheckRollup` length 0 at the time of this run. No check run was present, so CI is not claimed.

## Not covered by this run

- The directory's HTTPS URL was not the page that completed pairing. A local self-signed proxy forwarded HTTP and `POST /api/auth/browser-session` returned 200, but the app stayed on `Still connecting` because that proxy did not finish the WebSocket. The same unused directory token was then opened on the dev origin `http://localhost:5733`.
- Two dev servers were not up at the same time, because both wanted web port 5733.
- A duplicate `sourceEventId`, a model call cancelled while the fixture was still hanging, a missing text-generator id, and the 20-second model timeout were not driven against this live server.
- No public tunnel, Clerk or Cloudflare login, production account, or paid model was used. Those still need a real HTTPS origin for the account directory, an environment credential issued on that environment, and a text generator or Claude account configured locally.
