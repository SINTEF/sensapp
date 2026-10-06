# SQLite: concurrent `DELETE /series/{uuid}` fails with `database is locked`

Details and diagnosis: `ideas/sqlite-delete-series-database-is-locked.md`.

## Plan

1. [x] A backend-generic integration test: N series deleted concurrently, every call `Ok(true)`, catalog empty. Fails on SQLite.
2. [x] `delete_series` on SQLite starts its transaction with `BEGIN IMMEDIATE`.
3. [x] Mixed concurrent publishes and deletes test.
4. [x] `SQLITE_BUSY`/`SQLITE_LOCKED` answer `503` (no `Retry-After`: none of the other 503s sets one).
5. [x] Line in `docs/DATA_LIFECYCLE.md`, move this file and the idea to `done/`, tick the TODO.

## Progress

- Reproduced with a test (24 concurrent deletes): `database is locked`. `begin_with("BEGIN IMMEDIATE")` fixes it, as `publish` already did.
- Passes on SQLite (5 runs) and DuckDB. PostgreSQL, TimescaleDB and ClickHouse were not run locally (no containers up), they never had the bug.
- `app_error.rs`: busy/locked is detected through the extended result code's low byte (517 is `SQLITE_BUSY_SNAPSHOT`).
