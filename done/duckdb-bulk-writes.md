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

- [x] Before/after numbers below.
- [x] A failed batch leaves no sensor, label or string behind and a later write of the same series works.
- [x] Backend-generic tests still pass on DuckDB, and on SQLite and TimescaleDB where they are generic.
- [x] Full suites, clippy, and the DuckDB suite.

## Not in scope

The aggregated selector read (`BulkSelectorBackend::read_aggregated_samples`) is still one query per series on
DuckDB (0.30 s for a 100 series remote read with a step). The `time_bucket` SQL of `duckdb_bucketed_cte`
extends to many sensors with `GROUP BY sensor_id, bucket`. A read, not a write: separate task if wanted.

## Progress

Done 2 Oct 2026.

- New `src/storage/duckdb/duckdb_registration.rs`: `register_sensors` and `ensure_string_ids`. The ids that exist
  are read with chunked `IN` lists (the crate cannot bind list parameters), the missing units, label names and
  descriptions, strings, sensors and labels go in through appenders with `add_column`, so the ids still come
  from the sequences of the tables. The same unit, label or string twice in a batch is one row; a unit that
  exists keeps its description; labels are written when the sensor is created, like on the other backends.
- `duckdb_publishers.rs`: `publish_batch` registers everything first, then writes the samples of every sensor
  through one appender per value table. The string samples use the ids of the batch dictionary.
- `duckdb_utilities.rs` is gone with its `#[cached]` wrappers, `forget_sensor_id` and the cache clears of
  `delete_series` and of the test cleanup. A rolled back batch cannot leave a stale sensor id any more.
- Tests: four unit tests on an in-memory database (one unit, labels and dictionaries written once, the same sensor
  twice in the input, a rollback leaves every table empty and the same batch can be written again, lookups and
  strings across the chunk size, empty input). The backend-generic tests already cover 2100 new sensors in one
  batch, string dictionaries, labels written once and concurrent first writes; the DuckDB suite (236 lib, 274
  integration) and the default build (239 lib, 287 integration) pass, `clippy --tests -D warnings` on both.

Numbers, same machine and scripts as the baseline (release build, DuckDB file, one run each, indicative):

| write | before | after |
|---|---|---|
| 3000 new series (30 000 samples) | 2.69 s | 0.23 s |
| the same series again | 0.37 s | 0.11 s |
| strings: 3000 series x 10, 50 distinct strings | 2.51 s | 0.20 s |
| strings: one series of 10 000 distinct strings | 1.50 s | 0.16 s |
| strings: the first request again | 0.37 s | 0.10 s |

Reads are unchanged (a 100 series remote read with a step is still about 0.3 s: one query per series).

Not done: the timestamps still go through the appender as RFC 3339 text, parsed by DuckDB. At 3.6 us per sample
it is not what is left.

Note for later: the macOS binary needs `libduckdb.dylib` next to it (`install_name_tool -add_rpath
@executable_path` and `codesign -f -s -` on a copy) to run the `tests/perf` scripts, and `cargo test --doc` cannot
find it locally. CI and the Docker image are not affected.
