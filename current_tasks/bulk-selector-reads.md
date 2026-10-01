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

- [ ] Trait method, default implementation, `limits.rs` uses it; Prometheus remote read and
  `/api/v1/query` unchanged for callers.
- [ ] Overrides for ClickHouse, PostgreSQL, TimescaleDB, SQLite.
- [ ] Backend-generic tests (run on SQLite, PostgreSQL, TimescaleDB, ClickHouse): same results as
  the default implementation on a mix of numeric and non-numeric sensors, empty window, sensors
  without samples, `Series` and `Samples` limits at their exact boundaries, order of samples.
- [ ] Measured again with the script of this task: 100 series under 0.1 s on ClickHouse and
  PostgreSQL, the 300 series rejection under 0.1 s; numbers recorded below.
- [ ] No regression: default, ClickHouse and TimescaleDB test suites, clippy, Python SDK tests.
- [ ] `docs/CLICKHOUSE.md` caveat about selector cost removed or updated.

## Out of scope

The Prometheus remote read path with read hints (`step` aggregation) still reads discovered
sensors one by one with `query_sensor_data_advanced`; note it in `ideas/` if it stays slow.

## Progress

(none yet)
