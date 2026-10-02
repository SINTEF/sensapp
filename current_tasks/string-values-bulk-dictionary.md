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

- [ ] Before/after numbers below.
- [ ] Backend-generic test: many strings (repeated, unicode, empty, very long), several series, read back.
- [ ] Concurrent writers of the same new strings (two processes) get consistent ids, no deadlock.
- [ ] Full suites, clippy.

## Progress

(none yet)
