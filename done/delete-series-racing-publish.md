# `delete_series` racing a publish of the same series

Source: code review of 10 November 2026. Reasoned from the code and from database semantics; nothing was
executed. Reproduce each case with a concurrent test first: SensApp runs as several instances, so these
interleavings are expected in production.

## Problem

**PostgreSQL and TimescaleDB** (`src/storage/postgresql/mod.rs:370-403`,
`src/storage/timescaledb/mod.rs:721-754`): a plain `SELECT sensor_id`, eight `DELETE`s, then
`DELETE FROM sensors`, with no lock on the sensor row. A writer that inserts a sample after the deleter's
`DELETE FROM <table>` makes `DELETE FROM sensors` fail on the foreign key. The delete rolls back and the
caller gets a 500. Nothing is orphaned. On TimescaleDB the deleter can also be the deadlock victim against a
writer creating a chunk. The opposite order is handled (the writer retries).

**ClickHouse** (`src/storage/clickhouse/mod.rs:338-367`, `:611-615`): no transactions. The order is the 8
value tables, `labels`, then `sensors`, each step "DELETE, count, DELETE again if rows remain".

- Zombie samples: a publish between the value-table delete and the `sensors` delete is acknowledged and its
  samples stay under a `sensor_id` with no sensor. The next publish of that series re-registers the same
  deterministic id and the old samples reappear.
- The verify loop deletes a fresh registration: a publish that registers the sensor between the deleter's
  first `DELETE FROM sensors` and its `count()` loses its sensor row while its samples and labels remain.
- False failure: a series written faster than one loop iteration makes the delete fail with "still has rows".

**Existing test gap**: `concurrent_publish_and_delete` (`tests/integration/data_lifecycle.rs:299`) publishes
to other series than the ones it deletes.

**SQLite, small window**: fixed by `sqlite-id-cache-survives-rollback.md`.

**TimescaleDB time-range delete on compressed chunks** (suspected, version-dependent,
`timescaledb/mod.rs:777-779`): `timescaledb.max_tuples_decompressed_per_dml_transaction` defaults to 100,000,
so a range delete over more compressed samples of one series may fail.

## Suggested solution

- PostgreSQL and TimescaleDB: start the delete with
  `SELECT sensor_id FROM sensors WHERE uuid = $1 FOR UPDATE`. Writers take `FOR KEY SHARE` on that row
  through the foreign key, so they wait, then get the violation and re-register, which `publish` already
  retries. Add a retry on deadlock (40P01) for the TimescaleDB deleter, as `publish` has.
- ClickHouse: delete the `sensors` row first, then labels, then the value tables. A concurrent publisher
  then either sees no sensor and registers it again (its data belongs to the new incarnation), or wrote
  before the delete. To keep new samples from being removed with the old ones, bound the value-table deletes
  to what existed when the delete started (for example `timestamp_us <= max seen` is not enough; use the
  insert time if the tables have one, otherwise accept and document that a publish during a delete may be
  removed). Remove the "count then delete again" loop for `sensors`, or make it compare what it deletes
  with what it first saw. Write the accepted semantics in `docs/` next to the ClickHouse delete notes.
- TimescaleDB range delete: test it on a compressed chunk with more than 100,000 samples; if it fails, set
  the setting with `SET LOCAL` for that transaction or delete in slices.

## Tests

A generic concurrent test: one task deletes series X in a loop while another publishes to X; afterwards
either the series exists with only samples published after the last delete, or it does not exist. No 500,
no samples without a sensor row (check with a direct count where the backend allows).

## Done when

The test passes on every backend that supports deletes, or the documented exception is asserted.

## Outcome (11 November 2026)

Test first: `data_lifecycle::<backend>::publish_racing_delete_of_the_same_series`. One task publishes 30
single samples to a series while another deletes that series again and again. The deleter waits for a
publish to return between deletes, so no publish meets two of them. Neither may fail; a sample whose
publish returned before the last delete started must be gone, and one published after it returned must
be kept. Then the series is deleted once more and published again, and must hold only the new sample,
which catches samples left without a sensor row.

Before the fix, 5 runs each, without the pool limit:

| Backend | Result |
| --- | --- |
| PostgreSQL | 5 of 5 failed: the delete fails on `float_values_sensor_id_fkey` |
| TimescaleDB | 5 of 5 failed: the same |
| ClickHouse | 5 of 5 failed: samples were left behind, minutes 14 to 30 came back with the series after the last delete |
| SQLite, DuckDB | pass (a single writer; on SQLite the delete waits for the publisher to finish) |

Note: the local suite runner set `SENSAPP_PG_POOL_MAX_CONNECTIONS=1`, which serialises a publish and a
delete, and the test passed there 15 times. CI uses the default pool. Race tests must run with a pool.

Fixed:

- PostgreSQL and TimescaleDB share `pg_sensor_registration::delete_series`, which starts with
  `SELECT .. FOR UPDATE` on the sensor row and retries a deadlock (5 attempts, random pause). 8 of 8 runs
  pass, with 8 to 13 deletes per run.
- ClickHouse deletes the samples of the sensor's table (once), then the labels and the sensor row (checked,
  as before), then `delete_samples_without_sensor`. That function deletes what is left while the sensor row
  is absent and stops when a publish registers it again. A delete of a UUID with no sensor row runs it too,
  so a later delete removes what an earlier one left. The value tables are no longer checked one by one:
  under a continuous writer that check failed with "still has rows". A lost delete is caught by the final
  pass. 8 of 8 runs pass.
- TimescaleDB, compressed chunks: confirmed with the limit lowered to 10 ("tuple decompression limit
  exceeded"). Range deletes and series deletes run `SET LOCAL
  timescaledb.max_tuples_decompressed_per_dml_transaction = 0`. Test
  `timescale_compressed::deletes_over_compressed_chunks_are_not_limited`, which lowers the limit for its
  database and resets it.

Accepted and documented in `docs/DATA_LIFECYCLE.md` ("Deleting a series that is being written to"):

- On ClickHouse, a write that read the sensor row before a delete and lands after it has finished leaves
  samples without a sensor row until the next delete of the UUID. They come back if the series is written
  again first.
- On ClickHouse, two concurrent deletes of a series plus a write can leave the series without its labels.

Also fixed: `sql_sum_overflow` (task 14) warned about dead code in a ClickHouse-only build.

Suites:

- Full runs: SQLite (367 integration tests) and PostgreSQL (368).
- `data_lifecycle` and `timescale_compressed` on TimescaleDB (13 tests).
- `data_lifecycle` on ClickHouse and on DuckDB (9 tests each).
- `data_lifecycle` 5 times each with a full pool on PostgreSQL, TimescaleDB, ClickHouse and DuckDB.
- `cargo check` of each single-backend build; `cargo clippy` and `cargo clippy --all-features --tests` are clean.
