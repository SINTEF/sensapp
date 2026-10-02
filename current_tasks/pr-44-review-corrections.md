# PR 44 review corrections

Source: `ideas/review-human-corrected.md` (the review of `storage-hardening-and-bulk-io`, with the
maintainer's notes). Each point is verified against the code first, then fixed or answered, with one
commit per point.

| # | Point | Verdict | Status |
|---|-------|---------|--------|
| 1 | PG: label changes on existing sensors dropped | Not a regression: labels are in the UUID hash and `main` also returned early. Documented and pinned by a test | done |
| 2 | `storage_error_is_unavailable` too broad | Confirmed, and worse: sqlx errors through `anyhow` were never classified | done |
| 3 | At-least-once writes, retries, manual dedup | Maintainer: describe vacuum as not automatic, no scheduler | |
| 4 | DuckDB legacy `timestamp_ms` column | Maintainer: no deployments exist, nothing to do | |
| 5 | `find_selector_sensors` without `LIMIT` | | |
| 6 | `admin/vacuum` unbounded | Maintainer: slow is expected, keep it simple | |
| M1 | Measure `register_sensors` on single-sample publishes | Maintainer: worth measuring | |
| M2 | Misleading BRIN comment in `postgresql/selector.rs` | | |
| M3 | ClickHouse reads without `FINAL`: one sentence in `CLICKHOUSE.md` | | |
| M4 | ClickHouse `deduplicate_samples()` count under load | | |
| M5 | `StdDev` / `Variance` removed | Maintainer asks what this breaks | |
| M6 | `MAX_SELECTOR_SERIES = 256` blocks aggregated selectors | Maintainer: major if true | |
| M7 | SQLite aggregated path: per-series `LIMIT/OFFSET` | | |
| M8 | `batch_query_samples` global `LIMIT` comment | | |
| L1 | Dedicated counter for shed requests | Maintainer: true | |
| L2 | ClickHouse selector: bind an array instead of interpolating ids | Maintainer: true | |

## Notes per point

### 1. Label changes on existing sensors

- The review says the cache path used to upsert labels. It did not: `get_sensor_id_or_create_sensor` on
  the merge base returned the id of an existing sensor before writing any label.
- The sensor UUID is a keyed Blake3 hash of the name, the type, the unit and the sorted labels
  (`src/datamodel/sensor.rs`). A changed label on a Prometheus, InfluxDB or SenML-by-name series is a new
  sensor, not an update.
- Only an explicit UUID (Arrow, SenML) reaches the case, and all four backends (SQLite, TimescaleDB,
  ClickHouse, DuckDB) keep the labels of the first publish.
- Done: `docs/DATAMODEL.md` says so, and `publish_robustness::labels_are_written_when_the_sensor_is_created`
  pins it on every backend.

### 2. "Unavailable" classification

- Confirmed: bare `"timed out"` and `"no such file or directory"` made a slow statement or a bad SQLite
  path a retryable 503.
- Worse than the review says: `StorageError::Database` is never built explicitly, and sqlx errors travel
  through `?` as plain `anyhow`, where only ClickHouse errors were downcast. A PG pool timeout reached the
  anonymous 500 path and the string matcher saw almost nothing.
- `OperationFailed` is only built by the ClickHouse classifier for errors it already judged not transient,
  so it is now always a 500.
- Done: `sqlx_error_is_unavailable` matches `PoolTimedOut`, `PoolClosed`, `WorkerCrashed`, `Io` and the
  PostgreSQL SQLSTATEs `08*`, `53300`, `57P01`, `57P02`, `57P03`, in both the `StorageError` path and the
  `anyhow` chain. No text matching is left. `57014` (statement timeout) is a 500.
