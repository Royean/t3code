# Self-hosted connection pilot

The optional [account service](../../infra/self-hosted/README.md) lists a fixed set of environments for one account. Clients sign in under **Self-hosted connection** in connection settings, choose a computer, and redeem its ordinary T3 pairing credential. Official T3 Connect remains available independently.

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

Generate the account service's environment credential with the environment's existing `auth session issue` command. Store it only in the private service configuration, with the configured expiry recorded for renewal. It must grant `AuthStandardClientScopes` to delegate ordinary clients. Account logout revokes the account session and removes that account's saved environments from the current client; environment session revocation remains an environment operation.

## Verify before exposing a real workspace

Run a pilot T3 server with an explicit, separate home directory. A development server must not use the live `~/.t3/userdata` database. The [mobile testing workflow](../../.agents/skills/test-t3-mobile/SKILL.md) describes native-client installation and isolated fixtures.

Verify that the unauthenticated directory returns 401, the owner can sign in, the computer is listed with the correct environment identity, and connecting creates a fresh pairing credential. Then verify the public HTTPS origin and WebSocket through the actual client. Check logout, app restart, an offline computer, and reconnection after restarting the tunnel. A loopback or SSH-forwarded test alone does not establish the public HTTPS path.

Mobile stores the account profile in device-only SecureStore. Web and desktop retain public connection metadata but keep account tokens in memory, so reloading requires another login. Passwords are never persisted by either client.

To roll back the pilot, remove its client connection, stop only its own T3 server and tunnel, disable the dedicated account service, and remove its dedicated proxy configuration after validating the remaining proxy configuration. Keep the account database and configuration private if retaining them for a later restart.
