# Self-hosted connection

The optional [account service](../../infra/self-hosted/README.md) lists a fixed set of environments for one account. On mobile, open **Settings → T3 Account → Self-hosted connection**. On web and desktop, open **T3 Account** from the account button at the bottom of the settings sidebar. T3 Pro and mobile use the configured server. Other builds accept a server URL. On web and desktop, use **Import environments** to choose a computer.

## T3 Pro preview

Build with `T3CODE_SELF_HOSTED_ONLY=1` to use only the self-hosted **T3 Account → T3 Connect**. The default account origin is `https://t3.vivid-dream.online`. Set `T3CODE_SELF_HOSTED_CONNECT_URL` for a separate preview service. Upgrade the account service and the host environment before testing this client.

The preview reuses the environment ID and uses account-owned connection routes. Removing a computer from **Environments** remembers that preference on the current client. You can add it back from the account. The account's device list can sign out other devices and revoke their account-issued environment sessions. If an environment is offline when a device is revoked, the service retries revocation when it can reach the environment again.

For activity updates and held webhooks, provision a separate random 32-byte base64url key per configured environment. Set `activityKey` on that environment in the account service's private configuration. Provide the same key to the environment server as `T3CODE_SELF_HOSTED_ACTIVITY_KEY` and set `T3CODE_SELF_HOSTED_ACTIVITY_URL` to the account origin. Official webhook URLs need to be replaced with the new URL displayed by each scheduled task.

On Android, grant notification permission with the app open. A quiet **Background notifications** entry keeps a native connection to this account server while the app is in the background. Battery restrictions may delay delivery, and stopping the app from Android settings stops the connection until you reopen it. If you customize the APK package instead of using `com.royean.t3code`, set the server's `androidPackageName` accordingly. The account origin's proxy should allow HTTPS requests to wait at least 25 seconds.

After upgrading and migrating every account client, set `allowLegacyPairing: false` to turn off account connections that cannot be revoked from the device list.

## Route an environment through your server

Give the account service and each computer separate HTTPS origins, for example `connect.example.com` and `computer.example.com`. The computer's origin must proxy every HTTP route and WebSocket upgrade to that computer's T3 server. A path prefix is not supported. Preserve `Host` and `X-Forwarded-Proto`, turn off response buffering, and allow long-lived WebSockets.

The transport can be managed outside T3. For an initial SSH tunnel, run this from the computer:

```sh
ssh -N -i /private/path/tunnel-key \
  -o IdentitiesOnly=yes -o ExitOnForwardFailure=yes \
  -o ServerAliveInterval=20 -o ServerAliveCountMax=3 \
  -R 127.0.0.1:13773:127.0.0.1:3773 tunnel-user@relay-host
```

Use a dedicated key and account. Restrict its authorized key to the chosen loopback listener, deny command execution and interactive access, and deny unrelated forwarding. Keep the remote listener on loopback; the HTTPS proxy is the public entry point. The account service's `pairingOrigin` can then be `http://127.0.0.1:13773`, while `origin` is `https://computer.example.com`.

Generate the account service's environment credential with the environment's existing `auth session issue` command. Store it only in the private service configuration, with the configured expiry recorded for renewal. It must grant `AuthStandardClientScopes` to delegate ordinary clients. Account logout removes account-owned routes from the current client. Other saved routes on the same environment remain usable.

The account service probes each configured environment and uses the environment's authentication API to issue and revoke client access. Always On agents, memory, timers, and provider credentials stay in the environment database. Clients use the environment's session and scopes for orchestration.

## Verify before exposing a real workspace

Run a pilot T3 server with an explicit, separate home directory. A development server must not use the live `~/.t3/userdata` database. The [mobile testing workflow](../../.agents/skills/test-t3-mobile/SKILL.md) describes native-client installation and isolated fixtures.

Verify that the unauthenticated directory returns 401, the owner can sign in, the computer is listed with the correct environment identity, and connecting creates a fresh pairing credential. Then verify the public HTTPS origin and WebSocket through the actual client. Check logout, app restart, an offline computer, and reconnection after restarting the tunnel. A loopback or SSH-forwarded test alone does not establish the public HTTPS path.

Mobile stores the account profile in device-only SecureStore. Web and desktop retain public connection metadata but keep account tokens in memory, so reloading requires another login. Passwords are never persisted by either client.

To roll back the pilot, remove its client connection, stop only its own T3 server and tunnel, disable the dedicated account service, and remove its dedicated proxy configuration after validating the remaining proxy configuration. Keep the account database and configuration private if retaining them for a later restart.
