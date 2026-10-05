# Authentication

SensApp is **not open unless you ask for it**. The protected endpoints need a JWT (JSON Web Token) signed with a secret that only SensApp and its operator know. A token has a scope (read, write, delete, admin) and can optionally restrict which sensors a client may interact with.

Tokens are stateless: SensApp signs them and checks the signature, and keeps nothing. That keeps several instances behind a load balancer in agreement with no shared state, other than the secret.

## Starting SensApp

What happens at startup depends on the secret and on the address SensApp listens on (`SENSAPP_ENDPOINT`, `127.0.0.1` by default; the container image uses `0.0.0.0`):

| `SENSAPP_JWT_SECRET` | `SENSAPP_AUTH_DISABLED` | Listens on | Result |
|---|---|---|---|
| set | any | any | Authentication on. |
| unset | `true` | any | **Open**: every endpoint answers anyone, with a warning. The explicit opt-out. |
| unset | unset | a loopback address (`127.0.0.1`, `::1`) | Authentication on with a **secret made for this run**. SensApp prints a token (all scopes, 24 hours) and a link to the UI that signs in with it. |
| unset | unset | any other address | **SensApp does not start** and says how to fix it. |

The last row is the point: a container or a server has no business answering everyone because nobody configured it. The secret made on a loopback address only lives in the process, so a restart refuses the tokens made before; it is for a developer's machine, where only the local user can connect. Several instances behind a load balancer need the same configured secret, which is why they never get one made for them.

```bash
# A local run: SensApp prints a token and a link
cargo run

# A secret that stays, for anything else
export SENSAPP_JWT_SECRET=$(sensapp generate-secret)

# Open, for a demo, a test, or a network that authenticates in front of SensApp
export SENSAPP_AUTH_DISABLED=true
```

The secret must be at least **32 characters**. `sensapp generate-secret` prints a random one of 48 characters (288 bits). Keep it private: anyone with the secret can make any token. In `settings.toml` it is `jwt_secret`, but an environment variable or a Secret is a better place than a file.

## Making tokens

### With the command line

The command needs the secret (`SENSAPP_JWT_SECRET`), so it runs where SensApp runs:

```bash
# Read + write access, 1 hour validity (default)
sensapp generate-token my-service

# Read-only access, valid for 24 hours
sensapp generate-token prometheus-scraper --scope read --duration 86400

# Write-only, restricted to specific sensors
sensapp generate-token edge-device --scope write --sensors "temperature,humidity,pressure"

# Permission to delete series and samples, never granted by default
sensapp generate-token cleanup --scope delete --duration 900

# Scopes can be combined
sensapp generate-token cleanup --scope readwrite,delete

# A token that makes tokens
sensapp generate-token me --scope admin
```

The token is printed to stdout. In a container: `docker exec <container> sensapp generate-token …`, in Kubernetes `kubectl exec deploy/<release> -- sensapp generate-token …`.

| Flag | Description | Default |
|------|-------------|---------|
| `--scope` | Comma-separated `read`, `write`, `delete`, `admin`, or `readwrite` (shorthand for `read,write`) | `read write` |
| `--duration` | Token validity in seconds, at most ten years | `3600` (1 hour) |
| `--sensors` | Comma-separated sensor name allow list | all sensors |

### With an admin token

A token with the `admin` scope makes other tokens, through the Credentials tab of the [web UI](FRONTEND.md#credentials) or `POST /api/v1/admin/tokens`:

```bash
curl -X POST http://localhost:3000/api/v1/admin/tokens \
  -H "Authorization: Bearer $ADMIN_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{"subject": "edge-device-7", "scope": ["write"], "sensors": ["temperature"], "duration_seconds": 86400}'
```

The answer holds the `token` and what it was made with (`subject`, `scope`, `sensors`, `expires_at`, `jti`). **The token is only shown there**: SensApp keeps nothing, so a token cannot be listed or looked up later. The answer carries `Cache-Control: no-store`.

- `scope` is a list of `read`, `write` and `delete`. **The endpoint never makes an `admin` token** (403): those come from the command line, which has the secret. A stolen admin token therefore cannot renew itself, and an admin token that expires is replaced with one command.
- `duration_seconds` is at most `SENSAPP_TOKEN_MAX_DURATION_SECONDS` (one year by default).
- An admin token reads and writes nothing: the scope is as separate as `delete` is. Whoever has it can still make a token for any scope, so keep it as private as the secret, and short. It lasts an hour by default.
- With authentication disabled there is nothing to sign with: the answer is `404`.
- Every creation is logged (who asked, for whom, the scope, the sensors, the expiry, the `jti`), never the token.

## Using tokens

Include the token in the `Authorization` header:

```
Authorization: Bearer <token>
```

`Authorization: Token <token>` is accepted too: it is how the InfluxDB clients (Telegraf's `outputs.influxdb_v2` `token`) send theirs.

```bash
# Write data with a write token
curl -X POST http://localhost:3000/publish \
  -H "Authorization: Bearer $TOKEN" \
  -H "Content-Type: application/json" \
  -d '[{"n":"temperature","u":"Cel","v":22.5,"t":1700000000}]'

# Read metrics with a read token
curl http://localhost:3000/metrics \
  -H "Authorization: Bearer $TOKEN"
```

## Scopes

| Scope | Allows | Default |
|-------|--------|---------|
| `read` | Reading series, samples and queries | yes |
| `write` | Sending samples | yes |
| `delete` | Deleting series and samples, and the maintenance (`vacuum`) | no |
| `admin` | Making tokens (`POST /api/v1/admin/tokens`) | no |

None implies another. A token without a `scope` claim has `read write`.

## Route Protection

| Endpoint | Auth Required | Scope |
|----------|--------------|-------|
| `GET /` | No | — |
| `GET /docs` | No | — |
| `GET /ui/*` (the web UI files) | No | — |
| `GET /health/live` | No | — |
| `GET /health/ready` | No | — |
| `GET /prometheus/metrics` | No for service metrics; yes when `include_latest_samples=true` | `read` for latest samples |
| `GET /metrics` | Yes | `read` |
| `GET /series` | Yes | `read` |
| `GET /series/{uuid}` | Yes | `read` |
| `DELETE /series/{uuid}` | Yes | `delete` |
| `DELETE /series/{uuid}/samples` | Yes | `delete` |
| `GET /api/v1/query` | Yes | `read` |
| `POST /api/v1/prometheus_remote_read` | Yes | `read` |
| `POST /publish` | Yes | `write` |
| `POST /api/v2/write` | Yes | `write` |
| `POST /api/v1/prometheus_remote_write` | Yes | `write` |
| `POST /api/v1/admin/vacuum` | Yes | `delete` (it removes duplicate samples) |
| `POST /api/v1/admin/tokens` | Yes | `admin` |

Health checks, documentation, the web UI files, and Prometheus scrape endpoints are always public so that orchestration tools (Kubernetes probes, Prometheus scraper) work without tokens.

## JWT Claims

Tokens use the HS256 (HMAC-SHA256) algorithm with the following claims, and a `kid` in the header that names the secret that signed them:

```json
{
  "sub": "my-service",
  "exp": 1700003600,
  "iat": 1700000000,
  "nbf": 1700000000,
  "iss": "sensapp",
  "aud": "sensapp",
  "jti": "6f9c2f5e-4b1e-4c52-9a43-2f1c8f0f3a11",
  "scope": "read write",
  "sensors": ["temperature", "humidity"]
}
```

| Claim | Required | Description |
|-------|----------|-------------|
| `sub` | Yes | Subject — identifies who/what the token was issued to |
| `exp` | Yes | Expiration time (Unix timestamp). Tokens are rejected after this time |
| `iss` | Yes | Issuer, `sensapp`. A token of another issuer is rejected, even signed with the right secret |
| `aud` | Yes | Audience, `sensapp`. Same |
| `iat` | No | Issued-at time (informational) |
| `nbf` | No | Not-before time. If present, the token is rejected before this time |
| `jti` | No | Unique id of the token, in the logs of its requests. Every token SensApp makes has one |
| `scope` | No | Space-separated among `"read"`, `"write"`, `"delete"` and `"admin"`. Defaults to `"read write"` |
| `sensors` | No | Array of allowed sensor names. If absent, all sensors are accessible |

A token made by hand (with a JWT library) needs `iss` and `aud` set to `sensapp`; without a `kid` it is checked against the current secret.

### Time Validation

- **`exp`** is always required and validated. Expired tokens are rejected with `401 Unauthorized`.
- **`nbf`** is validated when present. Tokens presented before their not-before time are rejected.
- The `jsonwebtoken` library applies a default leeway of 60 seconds for clock skew tolerance.

## Rotating the secret, and revoking tokens

SensApp keeps no list of tokens, so a token cannot be revoked one by one: it works until it expires. What can be revoked is **a secret**, and with it every token it signed. That is also how the secret is changed without cutting the clients off at once:

1. Generate a new secret (`sensapp generate-secret`).
2. Put it in `SENSAPP_JWT_SECRET`, and move the old one to `SENSAPP_JWT_PREVIOUS_SECRETS` (comma-separated, several are allowed). Restart the instances. New tokens are signed with the new secret; the tokens of the old one still verify.
3. Once those tokens have expired or the clients got new ones, remove the old secret from `SENSAPP_JWT_PREVIOUS_SECRETS`.

To **revoke at once** (a leaked token, a lost admin token), do step 2 and leave the old secret out: every token it signed is refused. That includes the ones of the clients you trust, which need new tokens. The `kid` of a token tells which secret signed it, and a token whose `kid` is unknown is refused. Short durations are the other defence: prefer a day to a year, and an hour for an admin.

Listing and revoking single tokens is a possible future feature (`ideas/token-registry-and-revocation.md`).

## Logs

The `subject` and the `token_id` (the `jti`) of an authenticated request are part of its log line, so the logs tell who did what:

```
INFO request{method=POST uri=/api/v2/write … subject="telegraf" token_id="0ff625ed-…"}: …
```

The token itself is never logged, and neither is the `Authorization` header.

## Security Notes

- The secret must be kept private — anyone with the secret can create valid tokens, an admin token included. Use `sensapp generate-secret`, or any cryptographically random string of at least 32 characters.
- Prefer short-lived tokens, rotate the secret now and then, and keep the admin token short.
- Run SensApp behind TLS when it is reachable over a network: a token is a bearer credential.
- The sensor allow list uses exact string matching on sensor names.
- When present, the sensor allow list applies to all API reads and writes, including Prometheus and InfluxDB compatibility endpoints. Catalog and selector results omit other sensors; direct series requests for another sensor return 404, and writes to another sensor return 403.
- With authentication enabled, `/prometheus/metrics?include_latest_samples=true` requires a read token and applies its sensor allow list. Plain `/prometheus/metrics` stays public for service monitoring.
- Sensor-scoped tokens cannot run the database-wide `/api/v1/admin/vacuum` operation.
- The `delete` and `admin` scopes are never implied by `read write`. `delete` only allows deleting, not reading. With a sensor allow list, series the token cannot access are reported as not found. See [DATA_LIFECYCLE.md](DATA_LIFECYCLE.md).

## Web UI

The UI (see [FRONTEND.md](FRONTEND.md)) is made of public static files. When authentication is on, its API calls get `401`, and it asks for a token: paste the output of `sensapp generate-token`, or open the link a local SensApp prints, which signs in by itself. The UI keeps the token for the browser tab only and sends it as `Authorization: Bearer`. A `read` token is enough to explore; a token without `read` gets `403`, which the UI reports the same way. The **Credentials** tab makes tokens for an admin token.

## Kubernetes

The [Helm chart](../charts/sensapp/README.md) makes a random secret on the first install and keeps it across upgrades; give `auth.jwtSecret` or `auth.existingSecret` to choose it, `auth.previousSecrets` to rotate it, and `auth.disabled=true` to run open.
