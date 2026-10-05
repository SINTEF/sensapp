# Prometheus remote read with `step` hints in bulk

## Goal

`query_sensor_data_for_prometheus` (`src/http/prometheus_read.rs`) has a path for read requests that carry
hints with a `step`: it discovers the sensors (reading one sample each and throwing it away), then calls
`query_sensor_data_advanced` sequentially for each. Make it cost a handful of queries on the backends
that can, with the same results and the same limits.

## Design

Same pattern as `query_selector` (`done/bulk-selector-reads.md`): a storage method with a portable default
(today's loop), a bulk implementation for ClickHouse, PostgreSQL, TimescaleDB and SQLite: sensors in one
query, then per numeric type one aggregated query (`GROUP BY sensor_id, bucket`) with the shared budget.

## Done when

- [x] Characterise today's behaviour first (tests of the hints path against the loop, all aggregations,
  steps, windows, empty buckets).
- [x] Trait method, generic orchestration, backends; results identical to the loop.
- [x] Measured with 100 and 1000 series (before/after below).
- [x] Full suites, clippy; the idea moved to done.

## Progress

Done 2 Oct 2026 for ClickHouse, PostgreSQL, TimescaleDB and SQLite (DuckDB: see
`done/duckdb-sqlite-rrdcached-bulk-and-tests.md`; BigQuery and RRDCached keep the portable read).

- Characterised first: the existing remote read tests (14) pass before and after, and the new backend-generic
  tests compare the bulk read with the portable one.
- `StorageInstance::query_selector_aggregated` (default: `storage::selector::query_selector_aggregated_sequential`, the loop
  that was inline in `prometheus_read.rs`), the JWT wrapper override, and the generic
  `read_aggregated_selector_in_bulk` (series limit checked first, sensors read in doubling chunks, bucket budget
  as the LIMIT, series without data left out) with `BulkSelectorBackend::read_aggregated_samples`.
- Per backend, the SQL of the single-sensor aggregated read over many sensors, grouped by sensor and bucket: PostgreSQL
  (the CTE builder is shared with the single-sensor queries), TimescaleDB (`time_bucket`), SQLite (both query shapes, numerics as
  text) and ClickHouse (`IN` list, same bucket expression). The single-sensor SQL did not change.
- The remote read handler calls the storage method; storage outages in that path are now 503, not 500.
- Bug found by the equivalence tests and fixed: the aggregated read of an integer series with `sum` failed on TimescaleDB
  ("i64 is not compatible with NUMERIC"), also for the single-sensor read.
- `tests/integration/selector_aggregated.rs`: all 7 aggregations on 6 float, 4 integer and 2 numeric series, aligned and
  unaligned steps and windows, no start or end, steps shorter than the sampling, a late series and a string series left out,
  empty windows and selectors, and the exact limits (series, and buckets). 4 tests, on the four backends.
- Benchmark `tests/perf/remote_read.py` (hand-written protobuf and snappy literals, no dependency) and the three new lines
  of `tests/perf/scale.sh`. Release builds, 3000 series x 10 samples, median of 3:

| remote read with a 60 s step | before | after |
|---|---|---|
| PostgreSQL, 10 series | 0.117 s | 0.021 s |
| PostgreSQL, 100 series | 0.995 s | 0.031 s |
| TimescaleDB, 100 series | 0.726 s | 0.032 s |
| ClickHouse, 100 series | 1.227 s | 0.033 s |
| SQLite, 100 series | 0.018 s | 0.0044 s |

- Pitfall met while measuring: building the "before" binary in a `git worktree` with `CARGO_TARGET_DIR` pointing at the
  main `target/` made the next build of the main tree a no-op, and the "after" binary was byte-identical to the "before" one
  (same hash). Check the hash of the two binaries (or `strings` for a message that only the new code has) before comparing.
- Full suites on the four backends (269, 269, 269 and 282 integration tests) and `clippy -D warnings` on all features pass.
