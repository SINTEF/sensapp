# Remove duplicate samples with the vacuum operation

## Goal

Samples are stored at least once: a retried write, a client that sends a sample twice or a crash in the
middle of a request leaves duplicates. `POST /api/v1/admin/vacuum` is expected to remove them, and it
does not: it only runs `VACUUM` (PostgreSQL, SQLite, DuckDB), `OPTIMIZE TABLE` (ClickHouse) and nothing
on the other backends. The dedup code that exists is dead (`deduplicate()` of SQLite, commented out in
DuckDB). With the SDK retrying timeouts the duplicates become likelier.

## Design (from the former idea `sample-deduplication-in-maintenance`)

- Exact duplicates only: same sensor, same timestamp, same value. Two values at the same timestamp stay
  (no conflict policy to invent), the first one written is kept.
- Every value table, each backend with what is cheap for it: window functions with `ctid` (PostgreSQL),
  `rowid` (SQLite, DuckDB), `OPTIMIZE TABLE .. FINAL DEDUPLICATE` (ClickHouse), the hypertable for
  TimescaleDB. RRDCached and BigQuery: not supported, say so.
- It removes rows, so the endpoint requires the `delete` scope (today `write`).
- The call reports how many samples it removed, per backend where it can be known.
- Dedup is part of vacuum; no flag (vacuum is an explicit maintenance call).

## Done when

- [x] Trait method (default: not supported) and implementations, `vacuum` endpoint answers with the count.
- [x] Backend-generic tests: duplicates removed on every value type, distinct values at one timestamp
  kept, distinct sensors untouched, idempotent, series with nothing to remove.
- [x] `delete` scope required (JWT tests), OpenAPI and `docs/DATA_LIFECYCLE.md` updated, the idea moved to done.
- [x] Measured on a few million rows (PostgreSQL, ClickHouse) so that the cost is known and documented.
- [x] Full suites, clippy.

## Progress

Done 2 Oct 2026 for SQLite, PostgreSQL, TimescaleDB and ClickHouse. DuckDB is done in
`done/duckdb-sqlite-rrdcached-bulk-and-tests.md` (its columns may change with the precision fix);
BigQuery and RRDCached answer "unsupported".

- Trait method `StorageInstance::deduplicate_samples() -> Result<u64>` (default: `StorageError::Unsupported`),
  and `storage::common::duplicate_key_columns` (series, time, value; coordinates for locations).
- PostgreSQL and TimescaleDB: per table, `DELETE .. WHERE (tableoid, ctid) IN (.. row_number() OVER
  (PARTITION BY series, time, value ORDER BY ctid) .. > 1)`; `(tableoid, ctid)` identifies a row of a
  partitioned table (the chunks of a hypertable). SQLite: `DELETE .. WHERE rowid NOT IN (SELECT MIN(rowid) ..
  GROUP BY ..)`; the dead `deduplicate()` and its last `allow(dead_code)` are gone. ClickHouse: `OPTIMIZE TABLE ..
  FINAL DEDUPLICATE BY ..`, the count taken before and after. Each statement is atomic per table.
- `POST /api/v1/admin/vacuum` removes duplicates then vacuums, answers `{"status":"ok","duplicates_removed":N}`
  (`null` where unsupported), and now needs the `delete` scope (it left the write routes, so the write limit no
  longer applies to it). Sensor-scoped tokens stay refused.
- Tests: `tests/integration/deduplication.rs` (all eight value types written three times and counted, exact
  duplicates only: two values at one timestamp and one value at two timestamps stay, another series is untouched,
  idempotent, nothing to remove) on SQLite, PostgreSQL, TimescaleDB and ClickHouse; real router tests of the scopes
  (`401` without token, `403` for read, write, read write and for a sensor-scoped delete token, `200` for delete)
  and of a retried write (2 samples before, 1 after); OpenAPI contract test.
- Cost, measured on 3 million float rows with 300 000 duplicates (100 series): PostgreSQL removes exactly 300 000
  in 2.4 s, a second pass with nothing to remove 1.8 s (about 0.7 microsecond per row); ClickHouse 0.22 s and 0.16 s.
- Docs: `docs/DATA_LIFECYCLE.md` (Duplicate samples, backend column), `docs/JWT_AUTH.md`, `docs/CLICKHOUSE.md`,
  `docs/PYTHON_SDK.md` and `docs/CONFIGURATION.md` (which promised it), `docs/BACKENDS.md`.
- Full suites on the four backends and clippy on all features pass.
