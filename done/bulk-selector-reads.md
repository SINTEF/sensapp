# Bulk selector reads

## Goal

Reading the series of a selector (`/api/v1/query`, Prometheus remote read) must cost a handful of
queries whatever the number of series, on every backend that can do it, with the same limits and
the same results as today. Breaking changes to the storage trait are fine.

## Context (measured 1 Oct 2026, release builds, 3000 series x 10 samples, local databases)

`query_selector_bounded` (`src/http/limits.rs`) discovers the matching sensors with
`query_sensors_by_labels(.., limit = 1)`, then reads every sensor one after the other with
`query_sensor_data` so that one shared budget of 100 000 samples can be spent as it goes. About 3
to 4 round trips per series, for every backend:

| selector matches | ClickHouse | PostgreSQL |
|---|---|---|
| 10 series | 0.17 s | 0.06 s |
| 100 series | 1.2-1.5 s | 0.38 s |
| 300 series, rejected (limit 256) | 2.9 s | 0.65 s |

The discovery reads one sample per sensor and the result is thrown away. A selector over the
series limit costs as much as a valid one before it is rejected.

PostgreSQL and SQLite already fetch the samples of many sensors in bulk (`batch_query_samples`,
lateral joins with a limit per sensor), but that cannot enforce a total budget.

## Design

New method on `StorageInstance`, with a default implementation that is today's algorithm (so
DuckDB, BigQuery and RRDCached keep working unchanged):

```rust
async fn query_selector(&self, matchers, start, end, numeric_only, max_series, max_samples)
    -> Result<Result<Vec<SensorData>, SelectorLimitExceeded>>
```

`SelectorLimitExceeded` is `Series` or `Samples`; `limits.rs` turns it into the same 400 messages.

Backends that override it (PostgreSQL, TimescaleDB, SQLite, ClickHouse):

1. one query for the matching sensors, with their labels and units in bulk (exists);
2. more than `max_series` sensors: stop here, nothing else was read;
3. numeric sensors (Integer, Numeric, Float: all of Prometheus, and aggregations): one query per
   value type, `WHERE sensor_id IN (..) AND time range ORDER BY sensor_id, timestamp LIMIT
   remaining + 1`, so that the shared budget is enforced inside the database; more rows than the
   budget is `Samples`;
4. other types (string, boolean, location, json, blob) keep the per-sensor read, with what is left
   of the budget;
5. every matching sensor is returned, in the order of the matcher query, with an empty sample set
   when it has no data in the window (as today).

## Done when

- [x] Trait method, default implementation, `limits.rs` uses it; Prometheus remote read and
  `/api/v1/query` unchanged for callers.
- [x] Overrides for ClickHouse, PostgreSQL, TimescaleDB, SQLite.
- [x] Backend-generic tests (run on SQLite, PostgreSQL, TimescaleDB, ClickHouse): same results as
  the default implementation on a mix of numeric and non-numeric sensors, empty window, sensors
  without samples, `Series` and `Samples` limits at their exact boundaries, order of samples.
- [x] Measured again with the script of this task: 100 series under 0.1 s on ClickHouse and
  PostgreSQL, the 300 series rejection under 0.1 s; numbers recorded below.
- [x] No regression: default, ClickHouse and TimescaleDB test suites, clippy, Python SDK tests.
- [x] `docs/CLICKHOUSE.md` caveat about selector cost removed or updated.

## Out of scope

The Prometheus remote read path with read hints (`step` aggregation) still reads discovered
sensors one by one with `query_sensor_data_advanced`; note it in `ideas/` if it stays slow.

## Progress

Done 1 Oct 2026, one commit per step:

1. `StorageInstance::query_selector` with a portable default (`storage::selector::query_selector_sequential`);
   the JWT wrapper delegates when the token sees every sensor and reads through its filtered methods
   otherwise. No change of behaviour.
2. `storage::selector::read_selector_in_bulk`, generic for every backend, and the small
   `BulkSelectorBackend` trait (find the sensors, read numeric samples, read other types). It reads
   the numeric series in chunks of 8, 16, 32, ... sensors, each query limited to the budget left
   plus one, so a selector within the budget costs a few queries and one over the budget stops after
   the chunk that exceeds it. (A single query for all the series read every selected row before
   sorting on PostgreSQL, where the only index is a BRIN: 1.1 s for 5M rows against 0.2 s for the
   first chunk.)
3. Implemented for ClickHouse, PostgreSQL, TimescaleDB and SQLite. DuckDB, BigQuery and RRDCached keep
   the sequential default.
4. `tests/integration/selector_reads.rs`: the backend's `query_selector` against the sequential read on
   22 series of five types, windows (inside, late, empty), empty and single-series selectors, and the
   exact boundaries of both limits. Passes on the four backends.
5. `tests/perf/scale.sh`, the benchmark used below.

Measured (release build, local databases, 3000 series x 10 samples, `tests/perf/scale.sh`):

| selector | ClickHouse before | after | PostgreSQL before | after |
|---|---|---|---|---|
| 10 series | 0.17 s | 0.015 s | 0.06 s | 0.017 s |
| 100 series | 1.2-1.5 s | 0.022 s | 0.38 s | 0.020 s |
| 300 series, rejected | 2.9 s | 0.052 s | 0.65 s | 0.015 s |

Over budget with real volume (100 series x 5000 samples, 500 000 in total): rejected after 0.05 s
(ClickHouse) and 0.10 s (PostgreSQL); 10 series x 5000 samples (50 000, within the budget): 0.10 s
and 0.12 s, mostly serialisation of the response.

Full suites pass on SQLite, PostgreSQL, TimescaleDB and ClickHouse; `clippy -D warnings` on all
features.

Side findings, in `ideas/`: label regex matchers are not anchored (Prometheus anchors them);
Prometheus remote read with `step` hints still reads series one by one.
