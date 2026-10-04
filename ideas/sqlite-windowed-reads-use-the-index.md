# SQLite: a time window or the last sample scans the whole series

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
