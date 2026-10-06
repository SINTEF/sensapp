# SQLite: concurrent `DELETE /series/{uuid}` fails with `database is locked`

Details and diagnosis: `ideas/sqlite-delete-series-database-is-locked.md`.

## Plan

1. [ ] A backend-generic integration test: N series deleted concurrently, every call `Ok(true)`, catalog empty. Fails on SQLite.
2. [ ] `delete_series` on SQLite starts its transaction with `BEGIN IMMEDIATE`.
3. [ ] Mixed concurrent publishes and deletes test.
4. [ ] Decide on `503` + `Retry-After` for `SQLITE_BUSY`/`SQLITE_LOCKED`.
5. [ ] Line in `docs/DATA_LIFECYCLE.md`, move this file and the idea to `done/`, tick the TODO.

## Progress
