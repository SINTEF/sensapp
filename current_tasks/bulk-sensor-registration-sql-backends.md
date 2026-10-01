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

- [ ] Shared bulk registration used by PostgreSQL and TimescaleDB, old per-sensor code and its
  `#[cached]` wrappers removed.
- [ ] Concurrent first writes of the same series from several tasks (and from two processes)
  create one sensor with its labels once (backend-generic tests exist in
  `tests/integration/publish_robustness.rs`, add a cross-process drill).
- [ ] A rolled back batch leaves no stale cached id.
- [ ] Measured again: 3000 new series under 2 s, existing series under 1 s; numbers below.
- [ ] No regression: default, TimescaleDB and ClickHouse suites, clippy, Python SDK tests.

## Progress

(none yet)
