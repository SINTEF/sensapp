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

- Step 1 done: `resolve_auth_mode`, `SENSAPP_AUTH_DISABLED`, `generate-secret`. The dev token of a made secret has every scope (including `admin`) and lasts 24 hours, because the CLI cannot mint another one with a secret that only lives in the process.
- Step 2 done: `admin` scope, `jti`, `iss`/`aud` (tokens made by hand need them), `kid` and `SENSAPP_JWT_PREVIOUS_SECRETS`, the `Token` scheme, `subject` and `token_id` in the request log span, `TokenRequest` shared by the CLI and the future endpoint.
- Found: `http::server::tests::frontend_openapi_document_is_up_to_date` already failed before this task (the saved `frontend/openapi.json` is stale); step 3 regenerates it.
- Step 3 done: `POST /api/v1/admin/tokens` (admin scope; refuses `admin` tokens; duration capped by `SENSAPP_TOKEN_MAX_DURATION_SECONDS`, one year by default, held by `AuthConfig`; `Cache-Control: no-store`; 404 when authentication is disabled; audit log line without the token). OpenAPI and the frontend client regenerated.
