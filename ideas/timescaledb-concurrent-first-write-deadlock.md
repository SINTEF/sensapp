# TimescaleDB: concurrent first writes of a new series can deadlock

Found on 3 Oct 2026 while testing the ingestion deduplication (`current_tasks/ingestion-deduplication-experiment.md`),
but it does **not** depend on it: with the deduplication off the same test deadlocked 8 times out of 8.

## What happens

Eight writers send samples of the same **new** series at the same moment (two instances receiving the first
samples of a sensor, a client that retries while the first request is running). PostgreSQL reports
`deadlock detected` and one or more requests fail with a 500. The server log shows:

- the writers that lost are in `INSERT INTO sensors .. ON CONFLICT (uuid) DO NOTHING`, waiting for the
  uncommitted sensor row of the winner;
- the winner is in its `INSERT INTO float_values ..` waiting for a `ShareRowExclusiveLock` on a relation that the
  losers hold in row-exclusive mode. Most likely the hypertable insert takes that lock on `sensors` (the foreign
  key of a chunk), which conflicts with the `INSERT INTO sensors` of the others.

With the series registered first, 0 failures in 8 runs. PostgreSQL, SQLite and DuckDB are not affected (their
tests with the same scenario pass).

## To do

- Confirm which relation the lock is on (`pg_locks` while the test runs).
- A client retry makes it harmless in practice, but a 500 on a first write is not nice. Options: take an
  advisory lock on the sensor uuid hash before the registration (so that the registrations of the same sensor are
  serialized), or register the sensors in a first short transaction that commits before the samples are written.
- Test: the concurrent writers test of `tests/integration/deduplication.rs` without the TimescaleDB exception.
