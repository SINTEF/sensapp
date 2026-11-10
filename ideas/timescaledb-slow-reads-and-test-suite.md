# TimescaleDB: slow non-numeric selector reads, and a slow test suite

Noticed on 10 November 2026 while running the SQLite id cache task on every backend. Not investigated further.

## Non-numeric selector reads are one query per series

`read_selector_in_bulk` (`src/storage/selector.rs`) reads Integer, Numeric and Float series in bulk, but every
other type through `read_other_samples`, one query per series. On TimescaleDB, a selector matching 200 String
series of one sample each took about 0.9 s, against well under 0.1 s for 200 Float series. A bulk read per type
(strings joined to their dictionary, booleans, ...) would remove the loop. `query_sensors_by_labels` on
TimescaleDB has the same per-series loop for every type.

Check the other backends before changing it: the loop is in the shared code.

## The TimescaleDB test suite is slow

On the CI run of `main` at `1404d19`, the integration suites took: SQLite 22 s, PostgreSQL 59 s, DuckDB 61 s,
ClickHouse 95 s, TimescaleDB 205 s. Locally (colima) the TimescaleDB suite took 515 s and ClickHouse 331 s.
Measure which tests take the time (per-test timings) before guessing; the cleanup of every test deletes all
rows with `DELETE`, which leaves the hypertables and catalog tables full of dead rows, a possible cause.
