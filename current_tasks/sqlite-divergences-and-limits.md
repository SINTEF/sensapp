# SQLite: smaller divergences and limits

Source: code review of 10 November 2026. Reported from reading. The cache bug is in
`sqlite-id-cache-survives-rollback.md`, Numeric aggregation and NaN handling in
`numeric-and-non-finite-values-across-backends.md`.

## Problem

1. **Buckets truncate toward zero** (`src/storage/sqlite/storage_query_helpers.rs:11,44`,
   `src/storage/sqlite/selector.rs:173,205`): `(?5 + ((timestamp_us - ?5) / ?4) * ?4)`. Without `start`
   (origin 0) and with pre-1970 samples, `[-step+1, step-1]` collapses into bucket 0. The reference uses
   `div_euclid`.
2. **Unchecked step multiplication** (`src/storage/sqlite/storage.rs:586`, `step_ms * 1000`); every other
   site uses `checked_mul`.
3. **One bound variable per matching sensor** (`src/storage/sqlite/matchers.rs:203-220`, `IN (?1 … ?N)`).
   `query_sensors_by_labels` (`storage.rs:827-829`) and `query_selector_sequential`
   (`src/storage/selector.rs:36-38`) pass no cap, so a selector matching more than 32,766 sensors answers
   "too many SQL variables" (500) instead of the series-limit error.
4. **`list_metrics` splits label keys on `,`** (`storage.rs:403-411`), so a label name containing a comma is
   split.
5. **Latest and first on duplicate timestamps** pick an arbitrary row (`LIMIT 1` without a tie-break), on
   SQLite and DuckDB.
6. **Writers hold pooled connections while waiting**: each queued writer keeps one of the 10 connections for
   up to the 5 s `busy_timeout`, which can starve readers.
7. **Vacuum deduplication is one whole-table `DELETE` per table** (`storage.rs:109-126`): on a large file it
   holds the write lock past 5 s and concurrent writes get `SQLITE_BUSY` (503).
8. **No index on `labels(name, description)` or `sensors(name)`**: every matcher subquery scans `labels`.
9. **Statement cache**: each distinct row count of a multi-row INSERT is a separate cached prepared
   statement per connection (`sqlite_publishers.rs`).

## Suggested solution

1. Floor division in SQL: `(x - o) / s - ((x - o) % s < 0)` for the bucket index.
2. `checked_mul` and the existing "step is too large" error.
3. Pass the ids as one JSON array and use `json_each(?)`, as `batch_queries.rs` does.
4. Aggregate label keys with `json_group_array` and parse the JSON.
5. Add a deterministic tie-break (`rowid`) and write the rule in the trait documentation.
6. A dedicated write pool with `max_connections = 1`, so writers queue in process without taking reader
   connections.
7. Deduplicate in slices (per sensor, or by `rowid` ranges), one short transaction each.
8. One migration with the two indexes; measure a selector read before and after.
9. `.persistent(false)` on the multi-row inserts, or pad to a few fixed sizes.

## Tests

Generic tests for 1, 3 (a catalog of 40,000 sensors is quick on SQLite), 4 and 5. Measure 6 to 8 and note
the numbers here.

## Done when

The tests pass on SQLite and the reference backend gives the same answers.
