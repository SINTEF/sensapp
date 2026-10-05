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
5. Helm (a made secret, `existingSecret`, `previousSecrets`, `disabled`), the container, the compose file, the perf and live scripts and the notebook (explicit `SENSAPP_AUTH_DISABLED`), a CI step that the image refuses to start open, and the docs.

## Result

Done on the branch `frontend-good-enough`, in five commits. See `docs/JWT_AUTH.md` for the behaviour. What was verified, and what was not:

- Verified: the full Rust suite on SQLite and PostgreSQL (clippy and fmt clean), the frontend (typecheck, lint, 243 tests, `npm audit`), the real binary (the three startup modes, `Token` scheme, the subject in the logs), the UI in the browser pane against a local SensApp (the link signs in, the Credentials tab makes a token that is limited as asked), `helm lint` and `helm template` with the four auth setups.
- Not verified: the Helm `lookup` that keeps the made secret across upgrades (no cluster here), the new CI step that checks the image refuses to start (it only runs in CI; its command was replayed with the binary), and the other backends (TimescaleDB, DuckDB, ClickHouse), which the tests of this task do not depend on but CI runs.
- Left out on purpose: listing and revoking single tokens, in `ideas/token-registry-and-revocation.md`.
- The README (human-owned) still says authentication is optional and that all endpoints are open by default: it needs the wording proposed in the session.

## Progress

- Step 1 done: `resolve_auth_mode`, `SENSAPP_AUTH_DISABLED`, `generate-secret`. The dev token of a made secret has every scope (including `admin`) and lasts 24 hours, because the CLI cannot mint another one with a secret that only lives in the process.
- Step 2 done: `admin` scope, `jti`, `iss`/`aud` (tokens made by hand need them), `kid` and `SENSAPP_JWT_PREVIOUS_SECRETS`, the `Token` scheme, `subject` and `token_id` in the request log span, `TokenRequest` shared by the CLI and the future endpoint.
- Found: `http::server::tests::frontend_openapi_document_is_up_to_date` already failed before this task (the saved `frontend/openapi.json` is stale); step 3 regenerates it.
- Step 3 done: `POST /api/v1/admin/tokens` (admin scope; refuses `admin` tokens; duration capped by `SENSAPP_TOKEN_MAX_DURATION_SECONDS`, one year by default, held by `AuthConfig`; `Cache-Control: no-store`; 404 when authentication is disabled; audit log line without the token). OpenAPI and the frontend client regenerated.
- Step 4 done: Credentials tab (`frontend/src/pages/CredentialsPage.tsx`: open-server and not-admin states, the form, the token shown once), `#token=` pickup (`lib/tokenFromAddress.ts`), refused mutations ask for a token like refused queries, the Telegraf snippet uses its own `token`. Checked in the browser pane against a local SensApp: the printed link signs in and drops the fragment, the made token only sees its sensors and cannot write others nor make tokens. `ideas/influxdb-token-authorization-scheme.md` moved to `done/`.
- Step 5 done: chart, container, CI, scripts, notebook, docs (`JWT_AUTH.md` rewritten, `CONFIGURATION.md`, `FRONTEND.md`, chart README, `DATA_LIFECYCLE.md`), `TODO.md`.
