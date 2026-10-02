# Remove duplicate samples with the vacuum operation

## Goal

Samples are stored at least once: a retried write, a client that sends a sample twice or a crash in the
middle of a request leaves duplicates. `POST /api/v1/admin/vacuum` is expected to remove them, and it
does not: it only runs `VACUUM` (PostgreSQL, SQLite, DuckDB), `OPTIMIZE TABLE` (ClickHouse) and nothing
on the other backends. The dedup code that exists is dead (`deduplicate()` of SQLite, commented out in
DuckDB). With the SDK retrying timeouts the duplicates become likelier.

## Design (from `ideas/sample-deduplication-in-maintenance.md`)

- Exact duplicates only: same sensor, same timestamp, same value. Two values at the same timestamp stay
  (no conflict policy to invent), the first one written is kept.
- Every value table, each backend with what is cheap for it: window functions with `ctid` (PostgreSQL),
  `rowid` (SQLite, DuckDB), `OPTIMIZE TABLE .. FINAL DEDUPLICATE` (ClickHouse), the hypertable for
  TimescaleDB. RRDCached and BigQuery: not supported, say so.
- It removes rows, so the endpoint requires the `delete` scope (today `write`).
- The call reports how many samples it removed, per backend where it can be known.
- Dedup is part of vacuum; no flag (vacuum is an explicit maintenance call).

## Done when

- [ ] Trait method (default: not supported) and implementations, `vacuum` endpoint answers with the count.
- [ ] Backend-generic tests: duplicates removed on every value type, distinct values at one timestamp
  kept, distinct sensors untouched, idempotent, series with nothing to remove.
- [ ] `delete` scope required (JWT tests), OpenAPI and `docs/DATA_LIFECYCLE.md` updated, the idea moved to done.
- [ ] Measured on a few million rows (PostgreSQL, ClickHouse) so that the cost is known and documented.
- [ ] Full suites, clippy.

## Progress

(none yet)
