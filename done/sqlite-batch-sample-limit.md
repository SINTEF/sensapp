# SQLite batch sample queries: apply the per-sensor limit in SQL

## Context

Found while answering M8 of the PR 44 review (`done/pr-44-review-corrections.md`). The eight
`batch_query_*_samples` in `src/storage/sqlite/batch_queries.rs` read every row of the window for the
given sensors, ordered by sensor and time, and kept the first `limit` per sensor in Rust.

Their callers: `query_sensors_by_labels`, which the sequential selector fallback calls with a limit of 1 to
discover the series (a token with a sensor allow list on SQLite), and the label query itself. Nothing
exposes an unlimited dump of it over HTTP.

## Delivered

- One statement per type, bounded by the `(sensor_id, timestamp_us)` index of each sensor:

  ```sql
  FROM json_each(?1) ids
  JOIN float_values v ON v.rowid IN (
      SELECT w.rowid FROM float_values w
      WHERE w.sensor_id = ids.value AND <window> ORDER BY w.timestamp_us ASC LIMIT ?4)
  ```

  `EXPLAIN QUERY PLAN` shows a covering index search per id and `rowid` lookups, no table scan.
- `test_query_limit_applies_to_every_sample_type` (all eight types, two sensors each, limit 2) on every
  backend. It passes with the old and the new code: it pins the behaviour, not the implementation.

## Measured

Release build, SQLite, `query_sensors_by_labels`, five runs each:

| Data | limit | before | after |
|---|---|---|---|
| 50 series x 20 000 samples | 1 | 765 to 790 ms | 0.5 ms |
| 50 series x 20 000 samples | 100 | 755 to 805 ms | 6 ms |
| 2 000 series x 5 samples | 1 | 32 ms | 19 ms |
| 2 000 series x 5 samples | 100 | 34 ms | 28 ms |
| 50 series x 20 000 samples | 100 000 (returns all 1 000 000 rows) | 950 ms | 1 300 ms |

The last line is the one shape that got slower (+37%): a dump of a million rows, where the lookups by
`rowid` cost more than one sequential scan. Nothing in production asks for it.

## Tried and rejected

- A `ROW_NUMBER() OVER (PARTITION BY sensor_id ...)` window: halves the heavy case (470 ms) but still scans
  every row, and doubles the many-sensors case.
- One indexed query per sensor from Rust: 0.5 ms on the heavy case, but 2.4x slower (79 against 33 ms) with
  2 000 sensors, because every query costs a round trip through sqlx.
