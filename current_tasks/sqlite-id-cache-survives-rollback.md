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
