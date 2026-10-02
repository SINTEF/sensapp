# DuckDB, SQLite and RRDCached: test them, and bring the bulk paths where they pay

## Goal

- DuckDB has never been run locally in this work (CI found a precision failure; the user is fixing the
  precision in parallel, do not collide with it: check `origin` before touching timestamps).
- SQLite got the bulk selector read but not a bulk write path; it measured fast (0.42 s for 3000 new series).
- RRDCached: run its tests against the new features (default selector read, anchored regexes).

## Plan

1. Run the full DuckDB suite and the RRDCached suite locally; record failures.
2. Measure SQLite and DuckDB with `tests/perf/scale.sh` (writes, selector reads), strings, dedup, hints.
3. For each result that is bad, apply the same pattern (bulk registration, bulk numeric samples, bulk
   selector read, dictionary in bulk, dedup, bulk hints), starting with the biggest gain. Keep new code in
   new files where possible (the DuckDB timestamp fix touches `duckdb/mod.rs`).
4. Anything not worth doing is recorded with its numbers, not silently skipped.

## Done when

- [ ] DuckDB and RRDCached suites pass (or failures are documented with a reason and an owner).
- [ ] Before/after numbers for SQLite and DuckDB, and the decision for each path.
- [ ] No regression on the other backends.

## Progress

(none yet)
