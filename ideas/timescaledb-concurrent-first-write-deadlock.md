# TimescaleDB: concurrent first writes of a new series can deadlock

Found on 3 Oct 2026 while testing the ingestion deduplication (`done/ingestion-deduplication.md`),
but it does **not** depend on it: with the deduplication off the same test deadlocked 8 times out of 8.
Task: `current_tasks/timescaledb-first-write-deadlock.md`.

## What happens

Eight writers send samples of the same **new** series at the same moment (two instances receiving the first
samples of a sensor, a client that retries while the first request is running). PostgreSQL reports
`deadlock detected` and one or more requests fail with a 500. The server log shows:

- the writers that lost are in `INSERT INTO sensors .. ON CONFLICT (uuid) DO NOTHING`, waiting for the
  uncommitted sensor row of the winner;
- the winner is in its `INSERT INTO float_values ..` waiting for a `ShareRowExclusiveLock` on a relation that the
  losers hold in row-exclusive mode.

With the series registered first, 0 failures in 8 runs. PostgreSQL, SQLite and DuckDB are not affected (their
tests with the same scenario pass).

## Confirmed cause (3 Oct 2026, `pg_locks` and the server log)

The relation is **`sensors`**. The value hypertables are partitioned by time (7 days) **and** by
`by_hash('sensor_id', 2)`, so a series whose first sample lands in a chunk that does not exist yet creates it
(a new week, or the second hash partition of the week). Creating a chunk copies the foreign key of the
hypertable onto it: `ALTER TABLE <chunk> ADD CONSTRAINT .. FOREIGN KEY (sensor_id) REFERENCES sensors`, which
takes `ShareRowExclusiveLock` on `sensors`. Any open transaction that inserted a sensor holds
`RowExclusiveLock` on `sensors` until its commit, so the chunk creation waits for it. Chunk creators also
serialize on `ShareUpdateExclusiveLock` of the hypertable, which closes the cycle.

So it is **not limited to the same series**. Reproduced by hand with two sessions, on different series:

```
A: BEGIN; INSERT INTO sensors (a new one);  ..; INSERT INTO float_values (a, new week); COMMIT;
B: BEGIN; INSERT INTO float_values (an existing sensor, same new week);                 COMMIT;
```

B creates the chunk (holds `ShareUpdateExclusiveLock` on `float_values`) and waits for A on `sensors`; A waits
for B on `float_values`. Deadlock, PostgreSQL aborts one of them. In production this is a new week starting
while a first write of a new series is running, and the very first writes of an empty database.

## Options

- An advisory lock on the sensor uuids before the registration serializes the writers of the **same** new
  series only. It does not break the cycle above (different series).
- Registering in a first short transaction that commits would break the cycle but leaves the sensors of a
  failed batch behind, which `done/duckdb-bulk-writes.md` and `done/bulk-sensor-registration-sql-backends.md`
  rule out.
- Locking the hypertables before `sensors` for the batches that register: a consistent order across several
  value tables and the chunk creators is fragile, and it serializes the registering batches.
- Retrying the batch when PostgreSQL reports `deadlock_detected` (SQLSTATE 40P01), as it documents for
  applications: the transaction was rolled back whole, so nothing is left behind. Covers both cases.
