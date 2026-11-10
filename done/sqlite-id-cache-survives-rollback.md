# SQLite: cached ids survive a rolled-back transaction

Source: code review of 10 November 2026. Confirmed by reading, not reproduced.

## Problem

`src/storage/sqlite/sqlite_utilities.rs` wraps five get-or-create functions in `#[cached(ttl_secs = 120)]`:
label names (`:8`), label descriptions (`:56`), units (`:104`), sensors (`:153`), string values (`:231`).
The macro stores every `Ok(id)` when the function returns, which is before `transaction.commit()` in
`SqliteStorage::publish` (`src/storage/sqlite/storage.rs:81-93`).

When the transaction rolls back (a later statement fails, or the future is dropped by the write timeout or a
client disconnect), the ids stay in the cache. All ids are `INTEGER PRIMARY KEY AUTOINCREMENT` and
`sqlite_sequence` rolls back too, so SQLite gives the same ids to the next rows.

Scenario:

1. Request R1 registers new sensors A, B, C as ids 100, 101, 102, then times out. Nothing is committed.
2. The client retries within 120 s with A, B, C, D, E. A, B, C are cache hits, so no `sensors` row is
   inserted. D gets id 100 and E gets 101.
3. A's samples and D's samples are now both under `sensor_id = 100`. A, B and C are missing from the catalog.

The same happens to string values (a `string_values` row reads back as another string), units and labels.

Related, same cause:

- `delete_series` commits, then calls `forget_sensor_id` (`storage.rs:163-164`): a publish in between writes
  under a deleted sensor.
- The caches are process-global statics keyed by UUID or string only, so two SQLite stores in one process
  (the tests) share them.
- `TtlCache` only evicts an expired key when the same key is read again, so a high-cardinality string series
  grows the string cache without bound.

## Suggested solution

Remove the five caches, as was done for PostgreSQL, TimescaleDB and DuckDB
(`done/bulk-sensor-registration-sql-backends.md`, `done/duckdb-bulk-writes.md`). The lookups run inside the
`BEGIN IMMEDIATE` transaction on indexed columns. Remove `forget_sensor_id` with them.

If the write rate drops measurably, do the bulk registration the other SQL backends use (one lookup per batch,
`json_each` for the id lists) instead of bringing a cache back.

## Tests

- Publish a batch that registers new sensors and then fails (for example a second sensor whose value violates
  a constraint, or a dropped future), then publish other new sensors: each sensor's samples must come back
  under its own UUID, and the first sensors must not exist.
- Same for string values: failed batch with string "foo", then a batch with "bar", then "foo" again.
- Write the test against the generic storage test harness so it runs on every backend.

## Done when

No `cached` import remains in `src/storage/sqlite/`, the tests above pass, and the write numbers before and
after are noted here.

## Outcome (10 November 2026)

**Confirmed by a test.** `tests/integration/rolled_back_publish.rs` has two generic tests (every backend but
RRDcached, which keys its files by UUID and stores no strings). Each cuts a publish of 100 new series with
`tokio::time::timeout` after 1, 2, … 25 ms, as the write timeout does, then:

- `series_of_a_cut_publish_are_registered_again_under_their_own_id`: sends the same series again with 100
  other new ones. Each series must exist and read back only its own value.
- `strings_of_a_cut_publish_are_stored_again_under_their_own_id`: new strings `foo_*` in the cut batch, then
  `bar_*`, then `foo_*` again, each time on new series so that only the string ids can go wrong.

Before the fix both failed on SQLite at the first cut (1 ms): a series of the cut batch was missing from the
catalog, and a `foo_*` series read back `bar_*`.

A failing batch made of valid data could not be built in a generic way (no input fails on every backend), so
the tests use the dropped future only. They do not assert that the series of the cut batch are absent: the cut
can land after the commit, and ClickHouse has no transaction.

**Fix, two commits.**

1. `3634f59`: the five `#[cached]` attributes, `forget_sensor_id` and the cache clearing in
   `cleanup_test_data` are removed. No `cached` import remains in `src/storage/sqlite/` (the crate is still used
   by `src/datamodel/sensor.rs`, out of scope).
2. `f108996`: removing the caches made writes measurably slower, so, as suggested, SQLite now does the bulk
   registration of the other SQL backends: `register_sensors` and `ensure_string_values` in
   `sqlite_utilities.rs` look up and insert the sensors, units, label dictionaries, labels and strings of a
   whole batch with one statement per table, the lists passed as JSON arrays to `json_each`. The
   `BEGIN IMMEDIATE` transaction holds the write lock, so no other writer can insert between the lookup and the
   insert. A dictionary value that does not come back from SQLite unchanged is an error, not a sample stored
   under a wrong id. Embedded NUL and non-ASCII text go through `json_each` unchanged (checked with
   `sqlite3` 3.54).

The related items are gone with the caches: no publish can use the id of a deleted sensor, the two SQLite
stores of a test process no longer share state, and the string cache cannot grow.

**Measurements.** Release builds, `--features sqlite`, `tests/perf/scale.sh 3000` and `tests/perf/strings.sh
3000` on a fresh SQLite file, three runs each, median, same machine:

| Write | With the caches (`a55e8ac`) | Caches removed (`3634f59`) | Bulk registration (`f108996`) |
| --- | --- | --- | --- |
| 3000 new series, 30 000 samples | 0.39 s | 0.61 s | 0.19 s |
| the same series again | 0.14 s | 0.17 s | 0.15 s |
| 3000 new series, new strings | 0.37 s | 0.78 s | 0.18 s |
| one series, 10 000 distinct strings | 0.24 s | 0.22 s | 0.07 s |
| the strings request again (all known) | 0.14 s | 0.42 s | 0.14 s |

The three binaries had different `shasum`s.

**Backends run.** SQLite: full suite (282 unit, 318 integration tests) and `cargo clippy --tests`. DuckDB: the two
new tests pass (30 s). The new tests on PostgreSQL, TimescaleDB and ClickHouse were not run: Docker (colima)
was not running.
