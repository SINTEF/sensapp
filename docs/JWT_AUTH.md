# JWT Authentication

SensApp supports **optional** JWT (JSON Web Token) authentication. By default, all endpoints are open — just like Prometheus. When enabled, JWT tokens control read and write access, and can optionally restrict which sensors a client may interact with.

## Enabling Authentication

Set the `SENSAPP_JWT_SECRET` environment variable (or `jwt_secret` in `settings.toml`) to a secret string of **at least 32 characters**:

```bash
export SENSAPP_JWT_SECRET="my-super-secret-key-at-least-32-characters-long"
```

When unset, authentication is disabled and all endpoints remain open.

## Generating Tokens

Use the built-in CLI command to generate signed tokens:

```bash
# Read + write access, 1 hour validity (default)
sensapp generate-token my-service

# Read-only access, valid for 24 hours
sensapp generate-token prometheus-scraper --scope read --duration 86400

# Write-only, restricted to specific sensors
sensapp generate-token edge-device --scope write --sensors "temperature,humidity,pressure"

# Read + write (explicit)
sensapp generate-token admin --scope readwrite --duration 3600
```

The token is printed to stdout.

### Options

| Flag | Description | Default |
|------|-------------|---------|
| `--scope` | `read`, `write`, or `readwrite` | `read write` (both) |
| `--duration` | Token validity in seconds | `3600` (1 hour) |
| `--sensors` | Comma-separated sensor name allow list | all sensors |

## Using Tokens

Include the token in the `Authorization` header:

```
Authorization: Bearer <token>
```

Example with curl:

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

## Route Protection

| Endpoint | Auth Required | Scope |
|----------|--------------|-------|
| `GET /` | No | — |
| `GET /docs` | No | — |
| `GET /health/live` | No | — |
| `GET /health/ready` | No | — |
| `GET /prometheus/metrics` | No | — |
| `GET /metrics` | Yes | `read` |
| `GET /series` | Yes | `read` |
| `GET /series/{uuid}` | Yes | `read` |
| `GET /api/v1/query` | Yes | `read` |
| `POST /api/v1/prometheus_remote_read` | Yes | `read` |
| `POST /publish` | Yes | `write` |
| `POST /api/v2/write` | Yes | `write` |
| `POST /api/v1/prometheus_remote_write` | Yes | `write` |
| `POST /api/v1/admin/vacuum` | Yes | `write` |

Health checks, documentation, and Prometheus scrape endpoints are always public so that orchestration tools (Kubernetes probes, Prometheus scraper) work without tokens.

## JWT Claims

Tokens use the HS256 (HMAC-SHA256) algorithm with the following claims:

```json
{
  "sub": "my-service",
  "exp": 1700003600,
  "iat": 1700000000,
  "nbf": 1700000000,
  "scope": "read write",
  "sensors": ["temperature", "humidity"]
}
```

| Claim | Required | Description |
|-------|----------|-------------|
| `sub` | Yes | Subject — identifies who/what the token was issued to |
| `exp` | Yes | Expiration time (Unix timestamp). Tokens are rejected after this time |
| `iat` | No | Issued-at time (informational) |
| `nbf` | No | Not-before time. If present, the token is rejected before this time |
| `scope` | No | Space-separated: `"read"`, `"write"`, or `"read write"`. Defaults to `"read write"` |
| `sensors` | No | Array of allowed sensor names. If absent, all sensors are accessible |

### Time Validation

- **`exp`** is always required and validated. Expired tokens are rejected with `401 Unauthorized`.
- **`nbf`** is validated when present. Tokens presented before their not-before time are rejected.
- The `jsonwebtoken` library applies a default leeway of 60 seconds for clock skew tolerance.

## Security Notes

- The JWT secret must be kept private — anyone with the secret can create valid tokens.
- Use a cryptographically random secret of at least 32 characters.
- For production deployments, consider short-lived tokens and rotate secrets periodically.
- The sensor allow list uses exact string matching on sensor names.
