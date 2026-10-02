# String values: bulk dictionary on the SQL backends

## Goal

A series of string values (or many series of them) must not cost one dictionary lookup per distinct
string. On PostgreSQL and TimescaleDB `publish_string_values` calls `get_string_value_id_or_create` (a
`#[cached]` function with a `INSERT .. ON CONFLICT .. UNION ALL SELECT` per value) for every sample.

## Plan

1. Measure first (new script or `tests/perf/scale.sh` variant): a request with 3000 string series x 10
   samples, and one series with 10 000 distinct strings, on PostgreSQL, TimescaleDB, SQLite, DuckDB.
2. PostgreSQL and TimescaleDB: collect the distinct strings of the whole batch, one
   `INSERT .. unnest .. ON CONFLICT DO NOTHING` (sorted, like the other dictionaries) then one `SELECT` for the
   ids, then one `INSERT` of the samples for all the string sensors of the batch; drop the cached function
   and its cache clearing.
3. SQLite and DuckDB: same measurement; change only if it measures badly.

## Done when

- [x] Before/after numbers below.
- [x] Backend-generic test: many strings (repeated, unicode, empty, very long), several series, read back.
- [x] Concurrent writers of the same new strings (two processes) get consistent ids, no deadlock.
- [x] Full suites, clippy.

## Progress

PostgreSQL and TimescaleDB done 2 Oct 2026; DuckDB is measured in `current_tasks/duckdb-sqlite-rrdcached-bulk-and-tests.md`.

- New `src/storage/pg_strings.rs` (shared by both backends): `ensure_string_ids` inserts the sorted distinct
  strings of a batch with one `INSERT .. unnest .. ON CONFLICT DO NOTHING` and reads their ids back with one
  `SELECT .. = ANY`. `publish_string_samples` (in each publisher, `timestamp_us` or `time`) then writes the
  string samples of every sensor of the batch with one statement. The `#[cached]` per-value function, its
  cache clearing and the now empty `postgresql_utilities.rs` and `timescaledb_utilities.rs` are gone.
- SQLite measured and left alone (0.35 s for 3000 series of strings, 0.23 s for 10 000 distinct strings).
- `tests/perf/strings.sh` is the benchmark. Release build, local databases, 3000 series x 10 samples drawn
  from 50 strings, then one series of 10 000 distinct strings:

| write | PostgreSQL before | after | TimescaleDB before | after |
|---|---|---|---|---|
| 3000 string series, all new | 7.05 s | 1.24 s | 5.0 s | 1.03 s |
| one series, 10 000 distinct strings | 9.92 s | 0.38 s | 8.37 s | 0.33 s |
| the first request again | 7.31 s | 0.67 s | 1.86 s | 0.46 s |

- Backend-generic test `string_samples_round_trip_whatever_their_content` (empty, unicode, quotes, newlines, a
  20 000 character string, repeated values, three series sharing strings, two requests): it passed before the
  change and after it, on SQLite, PostgreSQL and TimescaleDB.
- Two SensApp processes writing 200 overlapping requests of 20 series with strings drawn from 300 shared
  values, in random order: all `204`, exactly 300 dictionary rows, 4000 samples, no deadlock or error, on both
  backends.
- SQLite, PostgreSQL and TimescaleDB suites and `clippy -D warnings` on all features pass.
