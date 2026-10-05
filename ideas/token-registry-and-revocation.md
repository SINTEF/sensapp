# Token registry: list and revoke single tokens

The authentication work of October 2026 kept tokens **stateless** on purpose: SensApp signs them and keeps nothing. The price is that a token cannot be listed, and cannot be revoked one by one: only rotating the secret (`SENSAPP_JWT_PREVIOUS_SECRETS`) revokes, and it revokes every token the secret signed. The Credentials tab can only make tokens.

The option that was left out, if one-by-one management is wanted later:

- Every token already carries a `jti` (and the logs have it), so the hook exists.
- A `credentials` table in the main storage: `jti`, subject, scope, sensors, `created_at`, `expires_at`, creator (the `jti` of the admin token that asked), `revoked_at`. `POST /api/v1/admin/tokens` inserts a row; `GET /api/v1/admin/tokens` lists them; `DELETE /api/v1/admin/tokens/{jti}` sets `revoked_at`.
- Checking a request must not cost a query: each instance keeps the set of revoked `jti` in memory and refreshes it every few seconds (a bounded delay, safe with several instances, which a per-process list would not be). Tokens of the command line have a `jti` too and are not registered: they are valid unless their `jti` is revoked, which works as a denylist.
- Backends: the SQL ones (PostgreSQL, TimescaleDB, SQLite, DuckDB) and ClickHouse (insert-only events, a `ReplacingMergeTree` or a revocations table) can hold it; BigQuery and rrdcached cannot (or should not), so the Credentials tab would fall back to create-only there, as now.
- Concurrent writers: revoking is idempotent and creating has a fresh UUID, so two instances cannot conflict; test it anyway with two routers on one database.
- UI: a table in the Credentials tab (name, scope, sensors, expiry, creator, a Revoke button).

Decide when someone needs to revoke a single client without cutting the others off.
