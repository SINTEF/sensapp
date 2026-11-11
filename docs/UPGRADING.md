# Upgrading SensApp

Pre-production: a release may need more than a new binary. This page lists what to do, version by version. The
breaking changes themselves are in [CHANGELOG.md](../CHANGELOG.md).

## From 0.4.2 on PostgreSQL or TimescaleDB: no rolling restart

The migrations of this release (`dictionary_md5` and `matcher_indexes`, for both backends) drop the unique
constraints on `strings.value`, `units.name` and `labels_name_dictionary.name` and replace them with unique
indexes on `md5(value)`, and add indexes. A SensApp 0.4.2 binary still writes with `ON CONFLICT (value)` or
`(name)`, which no longer has a constraint to match: PostgreSQL answers SQLSTATE `42P10`, and every write of a
String sample and every registration of a new sensor with a unit or labels is a `500` until that instance is
replaced. An old binary also cannot start once the migration is applied.

So, for this release:

1. Stop **all** instances of the old version.
2. Start one instance of the new version, and wait for `/health/ready`: it applies the migrations.
3. Start the others.

Do not run old and new instances together, and do not scale the Deployment while the first new pod migrates.
The migrations take an advisory lock, so two new instances starting together are fine.

**The migrations are long on a large database**: the eight `CREATE INDEX … (sensor_id, time DESC)` of
TimescaleDB scan every hypertable, and the unique `md5(value)` indexes scan the dictionaries. They run on a
connection of their own, without the `statement_timeout` of the requests (`SENSAPP_PG_STATEMENT_TIMEOUT_SECONDS`
is not applied), and with the maintenance timeout as `lock_timeout`. The index builds of one migration are one
transaction: each hypertable stays locked against writes until all eight are built, so plan a maintenance window
on a large TimescaleDB database. A liveness probe that fails while the migration runs restarts the pod: give the
Deployment a `startupProbe`, or raise the failure threshold, for the first start.

**Rolling back** means restoring the database from a backup taken before step 1: the migrations are not
reversible, and a 0.4.2 binary cannot use the migrated database. Take it.

## ClickHouse and BigQuery: reload the data

The key of a series changed (keyed BLAKE3 of the UUID, see the changelog): empty the database and load it again.
Set `SENSAPP_SENSOR_SALT` to your own value before you load.

## PostgreSQL behind PgBouncer, and other notes

- SensApp sends `statement_timeout` and `lock_timeout` as startup parameters (the `options` of the connection).
  PgBouncer refuses a startup parameter it does not know unless `ignore_startup_parameters = options` lists it. Or
  set `SENSAPP_PG_STATEMENT_TIMEOUT_SECONDS=0` and `SENSAPP_PG_LOCK_TIMEOUT_SECONDS=0`: with both at 0 SensApp
  sends no startup option, and the limits are the server's own.
- The dictionary tables are indexed by `md5(value)`. `md5()` is not available on a PostgreSQL in FIPS mode: the
  migration fails there.
- TimescaleDB 2.11 or later is needed: a delete on a compressed chunk lifts
  `timescaledb.max_tuples_decompressed_per_dml_transaction`, a setting that older versions do not have. The tests
  run on 2.31.
