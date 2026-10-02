# DuckDB: bulk writes

## Goal

Writing many series, or many strings, to DuckDB must cost a handful of statements whatever the number of
series, like PostgreSQL, TimescaleDB and ClickHouse. DuckDB is not the main target (local analysis), but the
per-series path is slow and the `#[cached]` ids have the rollback problem the other backends lost.

## Baseline (2 Oct 2026, release build, macOS, `tests/perf/scale.sh 3000` and `tests/perf/strings.sh 3000`)

| write | before |
|---|---|
| 3000 new series (30 000 samples) | 2.69 s |
| the same series again | 0.37 s |
| strings: 3000 series x 10, 50 distinct strings | 2.51 s |
| strings: one series of 10 000 distinct strings | 1.50 s |
| strings: the first request again | 0.37 s |

Why: `get_sensor_id_or_create_sensor` is one `SELECT`, one `INSERT` and one `INSERT` per label (plus the
dictionaries) per sensor, the string dictionary is one statement per distinct string, and every sensor opens
its own appender on its value table. All of it behind `#[cached]` wrappers that are filled inside the
transaction, so a rollback leaves ids of sensors that were never committed (until the 120 s TTL ends).

## Plan

1. `duckdb_registration.rs`: register all the sensors of a batch with a few statements (ids by chunked
   `IN`, appenders for units, label dictionaries, sensors and labels). Nothing cached.
2. Bulk string dictionary for the whole batch.
3. One appender per value table for the whole batch instead of one per sensor.
4. Remove the `#[cached]` wrappers, `forget_sensor_id` and their cache clears.
5. Before/after numbers, tests on top of the backend-generic ones, docs.

## Done when

- [ ] Before/after numbers below.
- [ ] A failed batch leaves no sensor, label or string behind and a later write of the same series works.
- [ ] Backend-generic tests still pass on DuckDB, and on SQLite and TimescaleDB where they are generic.
- [ ] Full suites, clippy, and the DuckDB suite.

## Not in scope

The aggregated selector read (`BulkSelectorBackend::read_aggregated_samples`) is still one query per series on
DuckDB (0.30 s for a 100 series remote read with a step). The `time_bucket` SQL of `duckdb_bucketed_cte`
extends to many sensors with `GROUP BY sensor_id, bucket`. A read, not a write: separate task if wanted.

## Progress
