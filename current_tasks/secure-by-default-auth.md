# Secure by default authentication, admin scope, credentials tab

## Goal

SensApp does not run open unless told to, an `admin` scope can mint tokens (CLI and `POST /api/v1/admin/tokens`), and the UI has a Credentials tab to create tokens.

## Context

Until now authentication was opt-in (`SENSAPP_JWT_SECRET`), tokens came only from `sensapp generate-token`, there was no audit of who did what, no secret rotation and InfluxDB clients' `Authorization: Token` was refused. The plan is in the session that created this file; decisions taken with the user:

- Jupyter-style default: no secret on a loopback address makes an in-memory secret and prints a token plus a UI link; no secret on another address refuses to start unless `SENSAPP_AUTH_DISABLED=true`.
- Stateless: no token registry, so no list or revoke in the UI. Revocation is secret rotation (`SENSAPP_JWT_PREVIOUS_SECRETS`). The registry is in `ideas/token-registry-and-revocation.md`.
- Admin tokens are minted on demand from the secret (CLI), short-lived by default. The endpoint never mints `admin` tokens.

## Steps (one commit each)

1. Startup rule: `resolve_auth_mode`, `SENSAPP_AUTH_DISABLED`, `generate-secret`.
2. Token model: `admin` scope, `jti`, `iss`/`aud`, `kid` and previous secrets, `Token` scheme, subject in the logs, shared token request validation.
3. `POST /api/v1/admin/tokens`, OpenAPI and the regenerated client.
4. Frontend: Credentials tab, `#token=` pickup, the Telegraf header workaround removed.
5. Helm, Docker, compose, scripts and docs.

## Progress

- Started.
