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
