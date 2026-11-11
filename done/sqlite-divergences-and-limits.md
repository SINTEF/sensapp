# SQLite: smaller divergences and limits

Source: code review of 10 November 2026. Reported from reading. The cache bug is in
`sqlite-id-cache-survives-rollback.md`, Numeric aggregation and NaN handling in
`numeric-and-non-finite-values-across-backends.md`.

## Problem

1. **Buckets truncate toward zero** (`src/storage/sqlite/storage_query_helpers.rs:11,44`,
   `src/storage/sqlite/selector.rs:173,205`): `(?5 + ((timestamp_us - ?5) / ?4) * ?4)`. Without `start`
   (origin 0) and with pre-1970 samples, `[-step+1, step-1]` collapses into bucket 0. The reference uses
   `div_euclid`.
2. **Unchecked step multiplication** (`src/storage/sqlite/storage.rs:586`, `step_ms * 1000`); every other
   site uses `checked_mul`.
3. **One bound variable per matching sensor** (`src/storage/sqlite/matchers.rs:203-220`, `IN (?1 … ?N)`).
   `query_sensors_by_labels` (`storage.rs:827-829`) and `query_selector_sequential`
   (`src/storage/selector.rs:36-38`) pass no cap, so a selector matching more than 32,766 sensors answers
   "too many SQL variables" (500) instead of the series-limit error.
4. **`list_metrics` splits label keys on `,`** (`storage.rs:403-411`), so a label name containing a comma is
   split.
5. **Latest and first on duplicate timestamps** pick an arbitrary row (`LIMIT 1` without a tie-break), on
   SQLite and DuckDB.
6. **Writers hold pooled connections while waiting**: each queued writer keeps one of the 10 connections for
   up to the 5 s `busy_timeout`, which can starve readers.
7. **Vacuum deduplication is one whole-table `DELETE` per table** (`storage.rs:109-126`): on a large file it
   holds the write lock past 5 s and concurrent writes get `SQLITE_BUSY` (503).
8. **No index on `labels(name, description)` or `sensors(name)`**: every matcher subquery scans `labels`.
9. **Statement cache**: each distinct row count of a multi-row INSERT is a separate cached prepared
   statement per connection (`sqlite_publishers.rs`).

## Suggested solution

1. Floor division in SQL: `(x - o) / s - ((x - o) % s < 0)` for the bucket index.
2. `checked_mul` and the existing "step is too large" error.
3. Pass the ids as one JSON array and use `json_each(?)`, as `batch_queries.rs` does.
4. Aggregate label keys with `json_group_array` and parse the JSON.
5. Add a deterministic tie-break (`rowid`) and write the rule in the trait documentation.
6. A dedicated write pool with `max_connections = 1`, so writers queue in process without taking reader
   connections.
7. Deduplicate in slices (per sensor, or by `rowid` ranges), one short transaction each.
8. One migration with the two indexes; measure a selector read before and after.
9. `.persistent(false)` on the multi-row inserts, or pad to a few fixed sizes.

## Tests

Generic tests for 1, 3 (a catalog of 40,000 sensors is quick on SQLite), 4 and 5. Measure 6 to 8 and note
the numbers here.

## Done when

The tests pass on SQLite and the reference backend gives the same answers.

## Outcome (11 November 2026)

Measurements with a temporary harness on a release build (not kept in the repository).

1. **Already fixed** (commit b4860c1, floor division), with the test
   `buckets_before_1970_start_at_or_before_their_samples`.
2. **Already guarded** by `validate()` and `MAX_STEP_MS`; `checked_mul` added anyway.
3. **Fixed.** The HTTP selector paths already capped at `max_series + 1`; only the internal
   `query_sensors_by_labels` was uncapped. The id lists are now one JSON array read with `json_each(?)` in
   `matchers.rs` and `selector.rs`, so there is no variable limit. No test with 33,000 sensors: tests stay
   small, and the variable limit cannot be reached with one bound parameter.
4. **Reproduced and fixed**: SQLite returned `["a", "b", "c"]` for the label names `a,b` and `c`. A
   migration rebuilds `metrics_summary` with `json_group_array(DISTINCT ...)`, parsed as JSON. DuckDB
   returned no keys at all and ClickHouse neither; both now list them (DuckDB also groups by name and type,
   not by unit). Generic test `selector_semantics::a_label_name_with_a_comma_is_one_label_key`. BigQuery
   still returns none: added to the BigQuery and RRDCached task, as it cannot be tested live here.
5. **Reproduced and fixed** on SQLite and DuckDB: `Last` over three samples at one timestamp returned the
   first written (1.0 instead of 3.0). Raw reads, first and last break ties by `rowid`, and the rule is
   written on `Aggregation` (SQLite and DuckDB keep the write order; the other backends may keep either
   sample). Test `aggregated_windows::equal_timestamps_follow_the_order_of_the_writes`.
6. **Measured and fixed** with a write pool of one connection (shared with the read pool for in-memory
   databases). 16 writers, reads during 3 s:

   | | read p50 | read max | writes |
   |---|---|---|---|
   | before | 43 ms | 153 ms | 1375 |
   | write pool | 0.2 ms | 4.9 ms | 1220 |

   Writers now queue in process, which costs about 10 % of throughput under this contention. Across
   instances the 5 s `busy_timeout` still applies, as before.
7. **Measured and fixed**: deduplication runs one `DELETE` per series. 1,000,000 rows with 100,000
   duplicates: the vacuum took 0.86 s and blocked a concurrent writer for 0.94 s; now 0.68 s and the
   writer waits at most 7.7 ms.
8. **Measured and fixed**: migration `20261111000001_selector_indexes.sql` adds
   `labels(name, description)` and `sensors(name)`. 10,000 sensors:

   | selector | before | after |
   |---|---|---|
   | `{site="s7"}` | 1.62 ms | 0.84 ms |
   | `temperature{site="s7"}` | 1.62 ms | 0.86 ms |
   | `{room="r4242"}` | 0.85 ms | 0.098 ms |
9. **Measured and fixed**: 300 inserts of varied sizes (1.2 M samples) grew the resident memory from 12 to
   143 MB with the statement cache, and to 19 MB with `persistent(false)` (1.63 s before, 1.38 s after).

Suites run: SQLite in full (369 integration tests), PostgreSQL in full (370), DuckDB targeted (51),
TimescaleDB targeted (8), ClickHouse targeted (32), the SQLite `data_lifecycle` race 3 times without the
pool limit, `cargo clippy` and `cargo clippy --all-features --tests` clean.
