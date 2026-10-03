# TimescaleDB: concurrent first writes of a new series deadlock

Branch `timescaledb-first-write-deadlock`, stacked on `dedup-at-ingestion` (PR 46 is not merged yet and the
reproducing test lives there). Idea file: `ideas/timescaledb-concurrent-first-write-deadlock.md`.

## Plan

1. Confirm the relation of the lock with `pg_locks` while the test runs.
2. Fix it with the simplest approach that keeps "a failed batch leaves no sensor behind".
3. Measure a normal write (`tests/perf/scale.sh`) before and after.
4. Remove the TimescaleDB exception of the concurrent writers test.
5. Full TimescaleDB suite, clippy.

## Progress

(nothing yet)
