# Bulk sensor registration on the SQL backends

## Goal

Registering the sensors of a batch must cost a handful of statements whatever the number of
sensors, on PostgreSQL and TimescaleDB (and SQLite if it measures badly). ClickHouse already does
it (commit "register ClickHouse sensors and read their labels in bulk"). Breaking changes (schema
included) are fine.

## Context (measured 1 Oct 2026, release build, PostgreSQL 16, one InfluxDB request of 3000
series x 10 samples, 4 labels each)

| write | ClickHouse (after its fix) | PostgreSQL |
|---|---|---|
| 3000 new series | 0.8 s | 11.6 s |
| same series again, warm process cache | 0.7 s | 1.8 s |
| same series after a restart, cold cache | | 2.7 s |

About 3.9 ms per new series: `get_sensor_id_or_create_sensor` (`postgresql_utilities.rs`) runs, per
sensor, a SELECT, a unit lookup, an INSERT and, per label, two dictionary lookups and an INSERT,
all in one transaction. At that rate a request of about 7700 new series passes the 30 s timeout.
Prometheus remote write sends thousands of series per request. Existing series cost 0.6 ms each
even with the cache, because the cache is consulted per sensor behind an async lock.

Also: the function is wrapped in `#[cached]` while it runs inside a transaction, so a rollback
leaves a cached id of a sensor that was never committed (until the 120 s TTL ends).

## Design

One function shared by PostgreSQL and TimescaleDB (same schema: check) in a module compiled for
either feature, taking the transaction and the sensors of the batch:

1. ids from the process cache (a small explicit cache of uuid to id, filled only after commit);
2. `SELECT uuid, sensor_id FROM sensors WHERE uuid = ANY($1)` for the others;
3. for the new ones: units, label-name and label-description dictionaries with
   `INSERT .. SELECT FROM unnest(..) ON CONFLICT DO NOTHING` then one SELECT for their ids;
   sensors with `INSERT .. SELECT FROM unnest(..) ON CONFLICT (uuid) DO NOTHING RETURNING`,
   re-selecting the ones another writer inserted first; labels with one `INSERT .. unnest`
   for the sensors this call inserted;
4. `forget_sensor_id` and the test cleanup keep working.

SQLite: measure first (one connection, local statements), change only if it is slow.
DuckDB and BigQuery are out of scope.

## Done when

- [x] Shared bulk registration used by PostgreSQL and TimescaleDB, old per-sensor code and its
  `#[cached]` wrappers removed.
- [x] Concurrent first writes of the same series from several tasks (and from two processes)
  create one sensor with its labels once (backend-generic tests exist in
  `tests/integration/publish_robustness.rs`, add a cross-process drill).
- [x] A rolled back batch leaves no stale cached id.
- [x] Measured again: 3000 new series under 2 s, existing series under 1 s; numbers below.
- [x] No regression: default, TimescaleDB and ClickHouse suites, clippy, Python SDK tests.

## Progress

Done 1 Oct 2026.

- `src/storage/pg_sensor_registration.rs`, shared by PostgreSQL and TimescaleDB (same tables): the
  ids of a batch's sensors with one `SELECT .. WHERE uuid = ANY($1)`, then for the new ones one
  `INSERT .. unnest .. ON CONFLICT DO NOTHING` each for units, label names, label descriptions,
  sensors (`RETURNING uuid, sensor_id`) and labels, all in the transaction of the batch. Values
  are sorted before each insert so concurrent transactions lock in the same order. The ids a
  concurrent writer created are read again; labels are written only for the sensors this call
  created.
- No process cache any more: the per-sensor `#[cached]` functions, `forget_sensor_id` and the
  stale-id-after-rollback hazard are gone, and so is the reason TimescaleDB had a copy of the
  PostgreSQL utilities (only the string dictionary cache remains, in each backend). A foreign key
  violation (a sensor deleted by another instance between its registration and the insert of its
  samples) still triggers one retry.
- The measurement then showed the next cost: one `INSERT` per sensor for the samples (3000
  statements, 1.8 s even for existing series). The numeric samples (integer, numeric, float) of all
  the sensors of a batch are now written with one statement per type
  (`publish_numeric_samples`); strings (dictionary), booleans, locations, json and blobs stay per
  sensor.
- SQLite measured first and left alone: 0.42 s for 3000 new series, 0.14 s for existing ones.
  DuckDB and BigQuery out of scope.

Measured (release build, local PostgreSQL 16, one InfluxDB request, 3000 series x 10 samples,
`tests/perf/scale.sh`):

| write | PostgreSQL before | after | TimescaleDB before | after |
|---|---|---|---|---|
| 3000 new series | 11.6-14.0 s | 0.99 s | 13.7 s | 1.02 s |
| same series again | 1.8 s | 0.38 s | 2.0 s | 0.38 s |

(With only the bulk registration: 2.5 s and 1.8 s.)

Two SensApp processes writing 200 overlapping requests of 20 new series each, in random order
(the workload that can deadlock): all 204, exactly 100 sensors, 600 labels and 4000 samples, no
deadlock or error in the logs, on PostgreSQL and TimescaleDB.

The generic regression tests of `tests/integration/publish_robustness.rs` (concurrent first writes,
2100 sensors in a batch, a deleted series written again, republishing) pass on SQLite, PostgreSQL,
TimescaleDB and ClickHouse; full suites on the four backends, the Python SDK tests and
`clippy -D warnings` on all features pass.

Not done: a rolled back batch leaving no stale id needs no test any more (there is no cache); string
values still look up their dictionary id one by one (cached), which is slow for series of many
distinct strings.
