# PR 44 review corrections

Source: `ideas/review-human-corrected.md` (the review of `storage-hardening-and-bulk-io`, with the
maintainer's notes). Each point is verified against the code first, then fixed or answered, with one
commit per point.

| # | Point | Verdict | Status |
|---|-------|---------|--------|
| 1 | PG: label changes on existing sensors dropped | Not a regression: labels are in the UUID hash and `main` also returned early. Documented and pinned by a test | done |
| 2 | `storage_error_is_unavailable` too broad | Confirmed, and worse: sqlx errors through `anyhow` were never classified | done |
| 3 | At-least-once writes, retries, manual dedup | Maintainer: describe vacuum as not automatic, no scheduler. Docs only | done |
| 4 | DuckDB legacy `timestamp_ms` column | Maintainer: no deployments exist, no guard | done (decision) |
| 5 | `find_selector_sensors` without `LIMIT` | Confirmed on all five lookups | done |
| 6 | `admin/vacuum` unbounded | Maintainer: slow is expected, keep it simple. Longer timeout of its own | done |
| M1 | Measure `register_sensors` on single-sample publishes | Maintainer: worth measuring | |
| M2 | Misleading BRIN comment in `postgresql/selector.rs` | Confirmed with `EXPLAIN` | done |
| M3 | ClickHouse reads without `FINAL`: one sentence in `CLICKHOUSE.md` | | |
| M4 | ClickHouse `deduplicate_samples()` count under load | | |
| M5 | `StdDev` / `Variance` removed | False premise: they never existed. Nothing breaks | done (no change) |
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

### 3. At-least-once writes and manual deduplication

- Maintainer decision: no scheduler, describe the vacuum as not automatic.
- Done: `DATA_LIFECYCLE.md` states in bold that nothing removes duplicates, what the reader sees until
  then (`count`/`avg` count twice, the values list the sample twice) and when to run the vacuum. The SDK,
  CONFIGURATION and CLICKHOUSE pages that said "the vacuum removes them afterwards" now say "when it is
  run, nothing runs it automatically".
- Not done on purpose: a duplicate-count metric. Counting duplicates is a scan of every value table, the
  cost of the vacuum itself, so it does not belong on `/metrics`.

### 4. DuckDB legacy column

- Premise confirmed: the DuckDB init migration was edited in place (`timestamp_ms TIMESTAMP_MS` became
  `timestamp_us TIMESTAMP`) and the runner is a plain `CREATE TABLE IF NOT EXISTS` batch.
- Maintainer decision: breaking changes are fine, there is no existing deployment, so no
  `reject_legacy_metadata_tables()` equivalent. A `.duckdb` file from before this branch fails with a
  missing-column error: delete it.

### 5. `LIMIT` on the sensor lookup of a selector

- Confirmed: the bulk read looked up every matching sensor and all their labels, then compared with
  `max_series`. The shape is not a CTE as the review says, but a first query for the sensors (already
  `ORDER BY sensor_id`) and a second for the labels of the ids it returned, so a `LIMIT` on the first bounds
  both.
- On SQLite the labels query binds one variable per sensor, so the lookup relied on a small result for
  SQLite's variable limit. The limit now guarantees it (the failure itself was not reproduced).
- Done: `find_selector_sensors` takes `limit: Option<usize>`; the two bulk readers pass `max_series + 1`,
  which is enough to report `Series`. Five lookups changed: PostgreSQL, TimescaleDB (its own copy of the
  query), SQLite, ClickHouse, DuckDB. The label-query callers pass `None`.
- Not covered: the sequential fallback (RRDCached, token-filtered reads) goes through
  `query_sensors_by_labels`, where `limit` is a per-series sample limit. It reads series one by one and is
  not the "bulk" path of the review.
- Measured: TimescaleDB, release build, 30 000 series of 3 samples, `GET /api/v1/query` with a selector
  that exceeds the cap of 256 series (HTTP 400), five warm runs each, two rounds alternating the binaries:

  | Selector | before | after |
  |----------|--------|-------|
  | `{__name__=~".+"}` | 175 to 210 ms | 7 to 12 ms |
  | `{__name__="cpu usage",dc=~"d.*"}` | 290 to 310 ms | about 82 ms |

  The remaining 82 ms of the second one is the label sub-query scan, which a `LIMIT` on the outer query
  cannot shorten. The first measurement of this change compared a binary with itself (the shared target
  directory did not rebuild the second one): the binaries were compared by hash before the numbers above.

### 6. The vacuum

- Maintainer direction: a slow vacuum is expected, extend the timeout, keep it simple.
- The vacuum shared the 30 s request timeout, so on a large database the client got a `504` while the
  database carried on, which invites a second call.
- Done: `SENSAPP_HTTP_MAINTENANCE_TIMEOUT_SECONDS` (default 3600) applies to
  `POST /api/v1/admin/vacuum` only. The route keeps the same authentication; the timeout layer of the
  other routes no longer wraps it. `real_router::the_vacuum_has_a_timeout_of_its_own` covers both
  directions.
- Not done, on purpose: `statement_timeout`/`lock_timeout`, batches, and a guard against two concurrent
  vacuums. Left to the maintainer: a guard would have to outlive the request (a timed-out request drops its
  future while the statement runs on), so it means running the vacuum in a spawned task. `DATA_LIFECYCLE.md`
  tells operators not to start a second one.

### M5. `StdDev` / `Variance`

- The maintainer asked what this breaks. Nothing: `git log -S"StdDev"` and `-S"Variance"` over `src/` find no
  commit, so `Aggregation` never had these variants, on `main` or before.
- The only trace is `stddev(...)` on the PromQL endpoint, which answers "Aggregation 'stddev' is not
  supported" (400) identically on `main` and on this branch (`tests/integration/simple_promql.rs`).
- The `unreachable!("handled separately")` arms are for `Avg` and `Count` in the ClickHouse aggregation
  helpers, unrelated.
- If `stddev` should exist, it is a new feature: see `ideas/promql-rate-and-arithmetic.md`.

### M2. The BRIN comment

- Confirmed on a plain PostgreSQL table with the same DDL (2 000 interleaved series, 2 million rows):
  `ORDER BY sensor_id, timestamp_us LIMIT 10` on three sensors is a parallel bitmap heap scan of 6 448 lossy
  blocks (14 587 buffers, 665 667 rows rechecked) followed by a top-N sort. The `LIMIT` trims the output only.
- Only the PostgreSQL file made the claim. Done: the comment says what the plan does. No query change: the
  statements are correct, and series written in long runs make BRIN much tighter than this interleaved
  worst case.
