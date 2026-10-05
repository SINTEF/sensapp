# SQLite: a time window or the last sample scans the whole series

Fixed on 4 October 2026 (branch `write-timeouts`), see the Outcome at the end. Original report:

## Observation (4 October 2026)

One series of 1,331,266 float samples, SQLite file of 49 MB, release build. Tiny reads are not tiny:

| read | SQLite | PostgreSQL | TimescaleDB |
|---|---|---|---|
| `GET /series/{uuid}?limit=1` | 0.4 ms | 49 ms | 8 ms |
| raw, a window of 1 hour (120 samples) | **133 ms** | 4 ms | 5 ms |
| `GET /series/{uuid}/last` | **54 to 108 ms** | 40 ms | 11 ms |

The index of the table is `(sensor_id, timestamp_us)` and a window of one hour should cost microseconds.
The queries of `src/storage/sqlite/storage.rs` (`query_*_samples`, around lines 1134 and 1286) and
`src/storage/sqlite/selector.rs:62` are written as

```sql
WHERE sensor_id = ? AND (? IS NULL OR timestamp_us >= ?) AND (? IS NULL OR timestamp_us <= ?)
ORDER BY timestamp_us ASC LIMIT ?
```

With bound parameters SQLite cannot know that the bounds are not NULL when it plans, so the index is
used for `sensor_id` only and every row of the series is visited (`EXPLAIN QUERY PLAN`:
`SEARCH float_values USING INDEX index_float_values (sensor_id=?)`). The same window as a plain range uses
both columns (`(sensor_id=? AND timestamp_us>? AND timestamp_us<?)`). Measured with the `sqlite3` shell on
the same file: **175 ms with the `? IS NULL OR` form, 0.06 ms as a plain range**, same 120 rows.

The cost grows with the length of the series, not with the window, so a dashboard that polls the last
hour of a long series gets slower every day.

## Idea

Build the `WHERE` clause from the bounds that are present (`timestamp_us >= ?` only when there is a start),
or keep one statement per combination (none, start, end, both). Check the aggregated queries
(`sqlite_bucketed_cte` in `storage_query_helpers.rs`, `?2 IS NULL OR`) and the last-sample and
availability fast paths for the same pattern. Test: `EXPLAIN QUERY PLAN` shows both columns in the
`SEARCH`, and a timing test on a series of a few hundred thousand samples.

PostgreSQL has a different version of the same weakness: its value tables only have a BRIN index
(see `done/ingestion-deduplication.md`), so `limit=1` and `/last` read the series (40 to 50 ms here); TimescaleDB does
not (chunks, and an index per chunk). Check whether that matters before adding a B-tree.

## Outcome

- The 20 positional and 24 numbered time predicates of `src/storage/sqlite/` (`storage.rs`, `selector.rs`,
  `batch_queries.rs`, `storage_query_helpers.rs`) are now `timestamp_us >= COALESCE(?, -9223372036854775807)`
  and `timestamp_us <= COALESCE(?, 9223372036854775807)`: the same meaning for a missing bound, and the
  planner can use both columns of the index (`EXPLAIN QUERY PLAN`: `SEARCH ... (sensor_id=? AND
  timestamp_us>? AND timestamp_us<?)`). The positional queries take one bind per bound instead of two. The
  constants are exact integers (`-i64::MAX`, not `i64::MIN`, which SQLite would read as a float).
- Measured on the 1.33 M-sample series (release build, median of 9 runs through the HTTP API): `/last` 54 ms to
  **0.5 ms**, a week of 1-hour buckets 65 to **5.0 ms**, a raw week (20,000 samples) 78 to **26 ms**, a month
  of 1-minute buckets 131 to 76 ms. Whole-history aggregations did not change (they read every row): 0.45 to
  1 s. In the `sqlite3` shell, the one-hour window: 175 ms to 0.1 ms.
- Test: `tests/integration/time_window_reads.rs`, on every backend (SQLite, PostgreSQL and TimescaleDB were
  run): all eight sample types, no bound / a start / an end / both / between two samples / with a limit /
  outside the data, for the read of one series, the latest sample and the read by labels. Swapping two
  binds in `query_float_samples` or in `query_latest_timestamp_us` makes it fail. Before this, the only
  "time range" test of the suite (`test_time_range_queries`) asserted `is_some() || is_none()`.
- Not changed: `storage.rs` catalog listing (`?1 IS NULL OR name = ?1`, `sensor_id > ?2`), which is not on a
  time column.
