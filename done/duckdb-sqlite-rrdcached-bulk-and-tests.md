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

- [x] DuckDB and RRDCached suites pass (or failures are documented with a reason and an owner).
- [x] Before/after numbers for SQLite and DuckDB, and the decision for each path.
- [x] No regression on the other backends.

## Progress

- DuckDB suite (233 lib, 267 integration) and RRDCached module (12 tests, container built from
  `docker/rrdcached/Dockerfile`) pass locally. The backend-generic integration suite is not meant for
  RRDCached (no labels, no deletion): it fails there by design, as before; only its module runs in
  `cargo make test-rrdcached`.
- DuckDB bugs found and fixed on the way: numeric aggregation bound a stray parameter (every aggregated read of
  a numeric series failed), no duplicate removal (now `rowid`-based, vacuum reports the count).
- DuckDB reads: the time window and the limit are in the SQL (a test pins their semantics on four backends);
  labels come from one query per page; matchers are pushed into SQL with RE2 regexes; numeric selector samples
  use the shared bulk reader. Release build, 3000 series (`tests/perf/scale.sh`, `strings.sh`):

  | | before | after |
  |---|---|---|
  | selector, 1 series | 1.00 s | 0.005 s |
  | selector, 100 series | 1.22 s | 0.006 s |
  | GET /series?limit=1000 | 0.34 s | 0.020 s |
  | string series read back | 1.58 s | 0.012 s |
  | write, 3000 new series | 2.6 s | 3.0 s (noise, unchanged code path) |

- Decisions: SQLite is already fast (writes 0.42 s for 3000 new series, strings 0.35 s) and has the bulk
  reads, so nothing more. DuckDB bulk writes and aggregated bulk reads are recorded with their numbers in
  `ideas/duckdb-bulk-writes.md` (not an ingestion backend).
