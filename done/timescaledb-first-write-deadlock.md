# TimescaleDB: concurrent first writes of a new series deadlock

Branch `timescaledb-first-write-deadlock`, stacked on `dedup-at-ingestion` (PR 46 was not merged when this was
done, and the reproducing test lives there). Found on 3 Oct 2026 while testing the ingestion deduplication
(`done/ingestion-deduplication.md`); it does **not** depend on it.

## Symptom

Eight writers that send samples of the same **new** series at the same moment (two instances receiving the
first samples of a sensor, a client that retries while the first request runs): PostgreSQL reports
`deadlock detected` and requests fail with a 500. The concurrent writers test of
`tests/integration/deduplication.rs` failed 4 times out of 4 when run alone, with the deduplication on or off.
PostgreSQL, SQLite and DuckDB are not affected.

## Cause (confirmed with `pg_locks`, the server log and `pg_class`)

The relation is **`sensors`**. The value hypertables are partitioned by time (7 days) **and** by
`by_hash('sensor_id', 2)`, so the first sample of a series in a chunk that does not exist yet (a new week, or the
second hash partition of the week) creates the chunk. Creating a chunk:

1. takes `ShareUpdateExclusiveLock` on the hypertable (chunk creators serialize), then
2. copies the foreign key of the hypertable onto the chunk
   (`ALTER TABLE <chunk> ADD CONSTRAINT .. FOREIGN KEY (sensor_id) REFERENCES sensors`), which takes
   `ShareRowExclusiveLock` on `sensors`. That waits for every transaction that inserted a sensor and has not
   committed (they hold `RowExclusiveLock`).

A batch that registered a new sensor and then needs a chunk takes the same two locks in the **other order**:
a lock-order inversion, so not limited to one series. Reproduced by hand with two sessions on different series
(a new sensor + a first sample in a new week, against an existing sensor + the same new week): deadlock. In
production: a new week starting while a new series is first written, and the first writes of an empty database.

## What was done

1. **Lock order** (`src/storage/pg_sensor_registration.rs`, `timescaledb/mod.rs`): a batch that has sensors to
   register first runs `LOCK TABLE ONLY <the hypertables of the batch> IN SHARE UPDATE EXCLUSIVE MODE`
   (alphabetical, one statement), then inserts the sensors. Same order as the chunk creators, so no cycle.
   `ONLY`, otherwise the lock goes to every chunk. Still **one transaction**: a failed batch leaves no sensor,
   label or string behind (the guarantee of `done/duckdb-bulk-writes.md` and
   `done/bulk-sensor-registration-sql-backends.md`). Batches of known series take no lock.
2. **Retry** (`TimeScaleDBStorage::publish`): the order above is alphabetical, the inserts meet the types in the
   order of the code (integers, numerics, floats, strings, then the others per series), so a writer holding
   `string_values` that creates a chunk of `boolean_values` against a registering boolean + string batch can
   still deadlock. PostgreSQL aborts one (SQLSTATE 40P01); the transaction is rolled back whole, so the batch is
   tried again (5 attempts, random 10 to 60 ms pause). The existing foreign-key retry shares the loop.
3. The TimescaleDB exception of the concurrent writers test is gone: all rounds run.

Rejected: an advisory lock on the sensor uuids (serializes the writers of the *same* series only, the
different-series cycle stays); registering in a first short transaction that commits (leaves the sensors of a
failed batch behind); retry alone (each deadlock costs the 1 s `deadlock_timeout`, and 8 writers on a new time
range kept colliding: 54 deadlocks, 26 s, still failing after 5 attempts).

## Tests

- `publish_robustness::concurrent_first_writes_of_different_series_in_a_new_time_range`: 8 writers, 4 types in a
  different order each, new time range every round. Fails 3 out of 3 without the lock.
- `publish_robustness::concurrent_first_writes_meeting_the_types_in_the_other_order`: fails 3 out of 3 without
  the lock.
- `timescale_deadlock::a_batch_aborted_by_a_deadlock_is_written_again`: a connection plays the chunk creator
  (holds `string_values`, then asks for `boolean_values`) against a boolean + string registration. Fails with one
  attempt, passes with five. The two tests above do not hit this hole by themselves (0 failures in 30 runs
  with a single attempt): it needs this exact interleaving.
- `deduplication::at_ingestion_concurrent_writers_of_the_same_samples_store_them_once`: alone, 4 failures out of 4
  before, 10 passes out of 10 after, 0 `deadlock detected` in the server log.
- Gate: `cargo test --no-default-features --features timescaledb` (243 unit, 292 integration, all pass),
  `cargo clippy --all-targets --features all-storage -- -D warnings`, `cargo clippy --tests` on timescaledb, `cargo fmt --check`.
  Not run: the PostgreSQL suite (no PostgreSQL container here; the only change there is the `&[]` argument).

## Cost (release binaries before and after, `shasum` 911b12f.. and 5dbec05.., same TimescaleDB)

`tests/perf/scale.sh 3000`, fresh database each time, 3 runs alternating:

| | before | after |
|---|---|---|
| write, 3000 new series (30 000 samples) | 3.05, 2.96, 2.93 s | 3.04, 3.01, 2.91 s |
| write, same series again | 1.17, 1.15, 1.14 s | 1.17, 1.17, 1.11 s |

No difference. A write of known series never takes the lock. **But** registering batches now take turns per value
type, which loses the parallelism of bulk loads of *new* series: 4 parallel writers of 3 000 new series each
(12 000 series, 120 000 samples, all HTTP 204): **5.3, 4.5, 4.7 s before, 11.9, 11.5, 11.6 s after**, which is
the sum of the four batches. One-time per series, steady state is unaffected. Follow-up in
`ideas/timescaledb-parallel-first-writes.md`.

Side effect on the dev container: `log_lock_waits` was switched on with `ALTER SYSTEM` while investigating
(switched off again at the end).
