# SQLite: `DELETE /series/{uuid}` fails with `database is locked` under concurrent deletes

## Observation

Seen on 5 October 2026, reproduced locally. A client that deletes many series in parallel (6 at a time, one
`DELETE /series/{uuid}` each) gets `500 Internal Server Error` for almost all of them on the SQLite backend:
30 of 31 requests failed, the 31st worked. The response body is only `Internal Server Error`; the cause is in
the server log:

```
ERROR sensapp::http::app_error: Internal Server Error: error returned from database: (code: 5) database is locked
... response failed classification=Status code: 500 Internal Server Error latency_ms=0
```

`latency_ms=0` is the clue: the request does not wait. The connection already has
`busy_timeout(Duration::from_secs(5))` and WAL (`src/storage/sqlite/storage.rs`, in the connection options),
so a plain write that meets another writer would wait up to five seconds, not fail at once.

Not a ClickHouse problem: the same purge against ClickHouse 26.1 (14 full-size series, 6 in parallel) had no
error. The ClickHouse `500` that started this investigation is a separate, still unexplained, issue (needs the
`Internal Server Error:` line of the server log, see the end).

## Cause

`SqliteStorage::delete_series` (`src/storage/sqlite/storage.rs`, `self.pool.begin()`) opens a **deferred**
transaction: `BEGIN` takes no lock, the first statement is a `SELECT` (a read snapshot), and the `DELETE`s
that follow need the write lock. When another connection has committed a write since the snapshot was taken,
SQLite cannot upgrade the read transaction and returns `SQLITE_BUSY` (`SQLITE_BUSY_SNAPSHOT` in WAL mode)
**immediately**: the busy handler is not consulted, because waiting could not help, the snapshot is stale.
With several deletes in flight they invalidate each other's snapshots.

A single client deleting one series at a time never sees it, which is why the delete tests pass.

## Fix

Take the write lock when the transaction starts, so that a second writer waits (up to `busy_timeout`) instead
of failing: begin the transaction with `BEGIN IMMEDIATE`. In sqlx that means either a connection option
(check what the sqlx version in `Cargo.lock` offers, `begin_with("BEGIN IMMEDIATE")` on newer ones) or running
`BEGIN IMMEDIATE` / `COMMIT` by hand on an acquired connection. Keep the `forget_sensor_id` call after the commit.

Other places to look at while there:

- `cleanup_test_data` (`#[cfg(any(test, feature = "test-utils"))]`, same file) uses the same
  `pool.begin()` but only writes, so it is not affected; leave it.
- Any other read-then-write transaction on SQLite or DuckDB. `grep -rn "\.begin()" src/storage/sqlite src/storage/duckdb`
  found only the two above at the time of writing.
- The delete takes the whole database write lock for as long as the delete runs (8 value tables). Fine for
  SQLite, but worth a line in `docs/DATA_LIFECYCLE.md`: deletes and writes queue behind each other on SQLite.

## Tests

- An integration test on SQLite (it must be generic over backends like the others, and skipped where it
  cannot apply): create N series with some samples, delete them concurrently (`tokio::spawn`, N between 8 and
  32), assert every call returns `Ok(true)` and the catalog is empty afterwards. It fails today.
- The same test on the other backends, as a regression guard: it should pass everywhere.
- Concurrent writes (`/publish`) mixed with deletes: no `database is locked`, no lost write.

## Related

- A 500 hides the database error from the client on purpose (`AppError::InternalServerError` returns a fixed
  body). Consider answering `503` with `Retry-After` for `SQLITE_BUSY` and `SQLITE_LOCKED`, as
  `sqlx_error_is_unavailable` already does for the PostgreSQL errors that mean "try again", so that clients
  retry instead of failing. Decide together with this fix.
- The ClickHouse purge that returned `500`: the exception is only in the server log. Get the line
  `Internal Server Error: ... Failed to delete series rows from <table>: <ClickHouse exception>`
  (`kubectl logs deploy/sensapp | grep "Internal Server Error"`) and the ClickHouse version before touching
  `delete_series` in `src/storage/clickhouse/mod.rs`.
