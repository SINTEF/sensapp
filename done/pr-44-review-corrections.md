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
| M1 | Measure `register_sensors` on single-sample publishes | Measured: about +0.6 ms per write, no LRU added | done |
| M2 | Misleading BRIN comment in `postgresql/selector.rs` | Confirmed with `EXPLAIN` | done |
| M3 | ClickHouse reads without `FINAL`: one sentence in `CLICKHOUSE.md` | Premise wrong (value tables are `MergeTree`), sentence still useful | done |
| M4 | ClickHouse `deduplicate_samples()` count under load | Confirmed for ClickHouse only; docs were wrong both ways | done |
| M5 | `StdDev` / `Variance` removed | False premise: they never existed. Nothing breaks | done (no change) |
| M6 | `MAX_SELECTOR_SERIES = 256` blocks aggregated selectors | Confirmed. Maintainer chose the pushdown | done |
| M7 | SQLite aggregated path: per-series `LIMIT/OFFSET` | Premise wrong (global `LIMIT`, no `OFFSET`); test module note added | done |
| M8 | `batch_query_samples` global `LIMIT` comment | Premise inverted: the limit is per sensor. Comments and a multi-sensor test added | done |
| L1 | Dedicated counter for shed requests | Maintainer: true | done |
| L2 | ClickHouse selector: bind an array instead of interpolating ids | Done as a server-side parameter, not `has()` | done |
| + | ClickHouse aggregated reads counted samples outside the window | Not in the review: found by the M6 parity test, fixed on its own | done |

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

### M3. ClickHouse reads and duplicates

- The review says the value tables are `ReplacingMergeTree`. They are plain `MergeTree` (the init migration):
  only `units`, `sensors` and `labels` are `ReplacingMergeTree`, and those are read with `FINAL`. So there is
  no background deduplication and no "worse than PostgreSQL" window: duplicates stay until the vacuum, as on
  every backend.
- Done: one bullet in `CLICKHOUSE.md` states that reads do not hide duplicates, and why.

### M4. The duplicate count on ClickHouse

- Confirmed for ClickHouse: `count()` before and after `OPTIMIZE .. FINAL DEDUPLICATE BY`, so a concurrent
  insert lowers the result (floored at 0 by `saturating_sub`) and a concurrent delete raises it.
- `DATA_LIFECYCLE.md` said "exact unless writes happen at the same time" for every backend. On PostgreSQL,
  TimescaleDB, SQLite and DuckDB the number is the rows affected by the `DELETE`, which is exact whatever else
  is written.
- Done: the doc says which is which. The algorithm is unchanged: an exact count needs a duplicate scan of
  every table on top of the merge. The full-merge cost of `OPTIMIZE .. FINAL` is already documented in
  `CLICKHOUSE.md` and in the code.

### M7. SQLite aggregated limits

- The review says SQLite uses a per-series `LIMIT/OFFSET` where PostgreSQL and ClickHouse use a global
  budget. `read_aggregated_bulk` in `src/storage/sqlite/selector.rs` is one statement over all the sensors
  with a single `LIMIT ?5`, and there is no `OFFSET` anywhere in `src/storage/sqlite`. The `LIMIT ?6` of
  `storage_query_helpers.rs` is the single-series read. The sequential read shares one budget too.
- The caveat of the review about the tests is right: they compare a backend with the portable read on itself.
  Done: both module docs say it, and state the shared contract (global limits).

### M8. `batch_query_samples` and its `LIMIT`

- The review says the `LIMIT $4` is global across the sensors of a type. It is per sensor: PostgreSQL runs it
  inside a `CROSS JOIN LATERAL` for each id, and SQLite applies the limit per sensor in Rust (it has no SQL
  `LIMIT` there). The trait documents "maximum number of samples per sensor". The global limit the reviewer
  describes is the one of `read_numeric_samples` (selector reads), which is documented.
- The review's worry is still right: the only test of the limit had one sensor, so it could not tell per-sensor
  from global. Done: comments on both `batch_query_samples` spell out the two semantics and say not to merge
  them, and `test_query_limit_is_per_sensor` (three sensors, limit 2) pins it.
- Side finding, not fixed: SQLite reads every row of the window for all the sensors before truncating per
  sensor. See the notes at the end.

### L1. A counter for shed writes

- Done: `sensapp_http_writes_shed_total`, incremented by the write limiter only when it turns a write away. The
  limiter holds a clone of the counter that the metrics registry owns (`WriteLimiter::with_shed_counter`), so
  the middleware keeps its own state type. `CONFIGURATION.md` says how to use it next to the 503 counter.
  `real_router::the_write_limit_sheds_writes_and_spares_everything_else` checks the count after a rejection.

### L2. ClickHouse sensor ids as a parameter

- Checked on the ClickHouse container before changing anything (2 million rows, the real DDL,
  `ORDER BY (sensor_id, timestamp_us)`, three ids out of 5 000):

  | Form | Granules read |
  |------|---------------|
  | `sensor_id IN (7,1500,1999)` | 6 of 245 |
  | `sensor_id IN [7,1500,1999]` | 6 of 245 |
  | `has([7,1500,1999], sensor_id)` | 245 of 245 |
  | `sensor_id IN {ids:Array(UInt64)}` with `param_ids` | 6 of 245 |

  So binding the array as `has(?, sensor_id)`, as `labels_of_sensors` does, would have lost the primary key
  on the value tables. That function reads the small `labels` table and is left as it is.
- The crate's `bind` is client-side text substitution, so it would not change what the server sees. The
  server-side `Query::param` does: the statement is the same whatever the ids, which is the point of the
  review item.
- Done: both bulk reads (`read_numeric_samples` and `read_aggregated_bulk`) use
  `sensor_id IN {ids:Array(UInt64)}` with `.param("ids", sensor_ids)`. Chunks are at most 256 ids, so the URL
  stays small. The selector, regex, PromQL, remote read, label and ClickHouse suites pass (100 tests).

### M1. Single-sample publishes without the sensor cache

- Method: TimescaleDB container, release builds of `main` (`aca6cbf`, with the sensor cache) and of this
  branch (hashes checked to differ), InfluxDB line protocol, steady state (the series exist and were warmed
  up), one new sample per request on a known series, persistent connections, two rounds alternating binaries.
  The script writes 1 500 requests from one client, then 3 200 from 8 clients on 40 series.

  | | main (cache) | this branch | difference |
  |---|---|---|---|
  | 1 client, mean / p95 | 2.05 / 2.7 ms | 2.6 to 2.7 / 3.3 to 3.6 ms | +0.6 ms (+30%) |
  | 1 client, writes/s | 485 | 367 to 382 | -22% |
  | 8 clients, mean / p95 | 4.4 to 4.7 / 5.9 to 6.5 ms | 5.9 to 6.1 / 8.1 to 9.0 ms | +1.4 ms (+30%) |
  | 8 clients, writes/s | 1 710 to 1 820 | 1 300 to 1 340 | -25% |

- Reading: the review is right that the cost exists (the sensor and dictionary round trips come back for every
  write), but it is about 0.6 ms of latency per request, and a single machine still takes more than 1 300
  single-sample writes per second from 8 clients. A device that sends one sample per second spends 0.06% of a
  second on it.
- Decision: no `Uuid -> (sensor_id, labels_hash)` LRU. It would win back part of 0.6 ms and bring back what the
  branch removed on purpose: a stale id after a rollback or a deletion by another instance. Revisit if
  somebody ingests mostly tiny requests at thousands per second. The script is the measure to repeat.

### M6. The 256-series cap on cross-series aggregations

- Confirmed, and a higher series cap alone would not have fixed it: the aggregation fetched raw samples, so the
  100 000-sample budget also bound (300 series over 24 hours at one sample a minute are 432 000 samples).
- Maintainer decision: push the aggregation down, by reusing the bulk aggregated reader. Delivered and measured
  in `done/cross-series-aggregation-pushdown.md` (moved from `ideas/`).
- Found on the way: a pre-existing ClickHouse bug that counted the samples outside of a window in aggregated
  reads, fixed in its own commit.

## Performance of the commits of this task

Measured on TimescaleDB with release builds (hashes checked to differ), or with `EXPLAIN` where a build per
change was not worth it:

| Commit | Effect |
|--------|--------|
| 5 `LIMIT max_series + 1` | Faster: 175 to 210 ms down to 7 to 12 ms for a selector over the cap, on 30 000 series |
| M1 no sensor cache (already on the branch) | +0.6 ms per single-sample write, measured, no change made |
| L2 ClickHouse ids as a parameter | None expected: `EXPLAIN` reads the same 6 of 245 granules as `IN (...)` |
| M6 pushdown | `sum`/`count` unchanged, 100 series x 500 samples 88 down to 54 ms, 300 000 samples answered instead of refused. `avg` on a small query over a large table costs about 13 ms more (it reads the window twice) |
| 2, 3, 4, 6, L1, M2 to M5, M7, M8 | No hot path touched (error mapping, a timeout layer on one route, an atomic increment only when shedding, docs, tests) |

## Observed, not fixed

- SQLite `batch_query_*_samples` (`src/storage/sqlite/batch_queries.rs`) have no SQL `LIMIT`: they read every
  row of the window for the given sensors and truncate per sensor in Rust. Their only callers are the
  sequential selector fallback (limit 1, to discover the series) and the label query, so it shows with a token
  that has a sensor allow list on SQLite. Not part of this review.
- The sequential fallback (RRDCached, token-filtered reads) still looks up every matching sensor, as the
  `query_sensors_by_labels` limit means samples per series. Item 5 covers the bulk path only.
- `README.md` was not touched, as AGENTS.md asks.
