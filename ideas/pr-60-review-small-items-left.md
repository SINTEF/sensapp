# Small items of the review of pull request 60 that are not done

From `review-followups-changelog-and-small-items.md` (11 November 2026). The changelog and docs part, items 1, 2 and
10 of the code list, and the `done/` wording fix are done. These remain, each with the reason:

3. **SQLite Numeric aggregation reads every row of the window** (`storage.rs`, `Some(usize::MAX)`): cap it at
   `DEFAULT_QUERY_LIMIT` and return an error above it. Needs a decision on the limit (the Numeric buckets are
   computed in Rust, so the cap is a memory bound).
4. **Tie-break on equal timestamps for String reads** (DuckDB `mod.rs`, SQLite `storage.rs` and
   `batch_queries.rs`): add `rowid ASC` and a String case to the test. Needs the test written on both backends.
5. **DuckDB `delete_series` commits twice**: try one transaction on the bundled DuckDB 2.0, else delete the sensor
   row first. Needs a DuckDB experiment.
6. **SQLite Numeric rows stored before this version are not normalised** (`1.0` and `1` are not duplicates): a
   migration that rewrites `numeric_values.value`, or "new values only" in the changelog.
7. **DuckDB reads are unbounded** (`ReaderPool::get`): a semaphore around the readers.
8. **The SQLite read pool is not read-only**: `read_only(true)`; check that the WAL setup of the pool still works.
9. **One bad remote-write series drops its batch** (a label value with 0x1F): skip the invalid series, store the
   rest, count them. Changes a 400 into a partial success, to decide with the metric name.
11. **RRDcached readiness fails when the one connection is busy for 3 s**: report busy as ready.
12. **The remote-read `Latest` grid assumes a 5-minute lookback**: make it a setting or fix the sentence of
    `docs/DATAMODEL.md`.
13. **Tests that assert less than they seem to** (`sensor_allow_list_names_are_not_patterns_in_selectors`, no decoy
    for `|` and prefixes; the "chunked responses must be sorted" claim has no source).

Also from the SenML part: **refuse to export a `bt` under 2^28** (or write absolute `t` there), since the importer
reads it as relative to now. The range is in `docs/SENML.md` now.
