# Self-hosted Connect

This optional service provides one configured account and a fixed list of T3 environments. It mints existing T3 one-time pairing links; the environment still owns its sessions and permissions.

Build from this directory with `vp run build`. Copy `dist/bin.mjs` to the service host and run it with Node 24.22 or newer; the bundle includes Effect and contracts and requires only Node built-ins. Source execution uses Node 26.

Create a private configuration outside the checkout, owned by the service user with mode `0600`. Its database directory must be owned by that user and have mode `0700`. For example:

```json
{
  "host": "127.0.0.1",
  "port": 3050,
  "databasePath": "/var/lib/t3-connect/sessions.sqlite",
  "account": {
    "accountId": "owner",
    "username": "owner",
    "passwordHash": "REPLACE_WITH_SCRYPT_HASH"
  },
  "environments": [
    {
      "environmentId": "REPLACE_WITH_REAL_ENVIRONMENT_ID",
      "label": "My computer",
      "origin": "https://computer.example.com",
      "pairingOrigin": "http://127.0.0.1:3773",
      "credential": "REPLACE_WITH_PRIVATE_ENVIRONMENT_BEARER"
    }
  ],
  "allowedOrigins": ["https://app.example.com"]
}
```

Use the environment's actual `environmentId` from `/.well-known/t3/environment`. `origin` is its publicly reachable HTTPS origin, including the existing `/pair` route and WebSocket endpoint. `pairingOrigin` is the fixed address reachable by this service; HTTP is accepted only for localhost, `127.0.0.1`, or `[::1]`. Origins cannot contain credentials, paths, queries, or fragments. The private environment credential needs authority to create pairing grants with `AuthStandardClientScopes`; it never reaches the client.

Generate a password hash without placing the password in process arguments or shell history:

```sh
python3 -c 'import getpass,sys; sys.stdout.write(getpass.getpass("Connect password: "))' |
  node dist/bin.mjs hash-password
```

The password must contain at least 12 characters and at most 1024 bytes. Copy the resulting hash into the private config. Start the service with:

```sh
node dist/bin.mjs --config /etc/t3-connect/config.json
```

Put an HTTPS reverse proxy in front of the loopback listener. Add every browser app origin to `allowedOrigins`; native requests without an Origin header are accepted. The service does not trust forwarded headers for login limits, so a reverse proxy shares the conservative five-attempts-per-minute budget. Avoid logging request bodies or Authorization headers in the proxy.

Optional settings are `requestTimeoutMs` (default 5000, range 100–30000), `accessTokenTtlSeconds` (default 900, range 60–3600), and `refreshTokenTtlSeconds` (default 2592000, range 60–7776000). Refresh tokens rotate without extending the original expiry. Reusing a consumed refresh token revokes its entire session family; logout also revokes the family. SQLite stores only SHA-256 token hashes. Restarting preserves sessions.

`GET /health` reports readiness. Login, refresh, logout, environment listing, and connect routes live under `/v1`. All responses use `Cache-Control: no-store`; connect requests accept only an empty JSON object, and upstream redirects are rejected. An unavailable environment stays listed with `available: false`; connect returns 503 for upstream failure and 504 for timeout.

Run focused verification from the repository root:

```sh
vp test run infra/self-hosted/src/SelfHostedConnect.test.ts infra/self-hosted/src/http.test.ts
vp run --filter @t3tools/self-hosted typecheck
```
