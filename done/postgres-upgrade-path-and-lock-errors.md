# PostgreSQL and TimescaleDB: upgrade path, lock errors and race tests in CI

Source: review of pull request 60 on 11 November 2026 (HEAD `7d8b396`). Confirmed by reading; nothing was
run against a database.

## Problem

1. **Migrations run under the new statement timeout.** `create_or_migrate` runs `sqlx::migrate!` on the
   pool (`src/storage/postgresql/mod.rs:314-318`, `src/storage/timescaledb/mod.rs:637-641`), and the pool
   now carries `statement_timeout = 130 s` and `lock_timeout = 60 s` (`src/storage/pg_session.rs`). Two new
   migrations scale with the data: the eight `CREATE INDEX … (sensor_id, time DESC)` on the TimescaleDB
   hypertables (`timescaledb/migrations/20261110100000_matcher_indexes.sql`) and the unique indexes on
   `md5(value)` (`20261110000000_dictionary_md5.sql`). On a large database one statement passes 130 s, the
   migration rolls back, the process exits, and the next start fails the same way. The only way out
   (`SENSAPP_PG_STATEMENT_TIMEOUT_SECONDS=0`) is not documented.
   The eight index builds are also one transaction, so each hypertable stays locked against writes until
   all eight are built.
2. **Rolling upgrade breaks old instances, undocumented.** After the md5 migration drops the unique
   constraints, a 0.4.2 binary still issues `ON CONFLICT (value)` / `(name)` and gets SQLSTATE 42P10: every
   write with String samples and every registration of a new sensor with a unit or label is a 500 until
   that instance is replaced. An old binary also cannot start once the migration is applied.
3. **Race tests run with one connection in CI.** `Makefile.toml:172` and `:236` set
   `SENSAPP_PG_POOL_MAX_CONNECTIONS = "1"` for `test-postgres` and `test-timescaledb`, and CI runs
   `cargo make check-<feature>` (`.github/workflows/ci.yml:277`), which depends on those tasks.
   `done/delete-series-racing-publish.md` says CI uses the default pool; it does not.
   `publish_racing_delete_of_the_same_series` uses one storage for both sides, so in CI it is serialised and
   passes without the fix. No test covers the identity check on the concurrent-creation path.
4. **Lock timeout and deadlock answer 500.** `sqlstate_is_unavailable` (`src/http/app_error.rs:14-16`) does
   not include 55P03 (lock timeout, new with `lock_timeout`) or 40P01. `delete_series` now holds
   `FOR UPDATE` on the sensor row for the whole delete, so a batch that contains that series can wait 60 s
   and then fail entirely with a 500, which clients do not retry. `PostgresStorage::publish` retries once on
   a deleted sensor and has no deadlock retry (TimescaleDB has five attempts).
5. **TimescaleDB `latest` did not get the PostgreSQL fix** (`src/storage/timescaledb/mod.rs:1303-1384`):
   two statements, and `Some` with no sample after a concurrent delete. On PostgreSQL the fix returns `None`
   even when older samples remain in the window.
6. **Dictionary lookups are quadratic in the distinct strings of a batch (suspected, not measured)**
   (`src/storage/pg_strings.rs:32-37`, same shape in `pg_sensor_registration.rs:215-266`):
   `md5(value) = ANY(...) AND value = ANY($1)` rechecks each matched row against the whole bound array.
7. Notes: the `options` startup parameter is always sent, and PgBouncer refuses it unless
   `ignore_startup_parameters` lists it; `md5()` is unavailable on a FIPS-mode PostgreSQL;
   `SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction` needs a minimum TimescaleDB version
   that the docs do not state; the TimescaleDB Numeric average uses `COUNT(*)` although the comment at
   `timescaledb/mod.rs:532-536` says to use `COUNT(value)`.

## Suggested solution

1. Run migrations on a maintenance connection (the detached-connection helper in `pg_session.rs`) with no
   statement timeout and a long lock timeout. Split the eight index builds into separate migration files so
   each commits on its own, if the files are not released yet; otherwise leave them.
2. Write an upgrade section in `docs/` and the changelog: stop all instances, start the new version
   (no rolling restart for this release), and how to roll back.
3. Remove the pool cap from the two Makefile tasks, or make the race tests open their own storage with an
   explicit pool size and assert it is above 1. Add a concurrent test for the identity check (two tasks
   registering one UUID with two names: exactly one succeeds).
4. Map 55P03 and 40P01 to 503 with `Retry-After`. Give PostgreSQL `publish` the same retry loop as
   TimescaleDB (deadlock and deleted sensor, with jitter).
5. One statement for `latest` on both backends: `ORDER BY timestamp DESC LIMIT 1` within the window.
6. Measure a batch of 8,192 distinct strings before and after; if it is slow, join on
   `unnest($1) AS v` with `md5(d.value) = md5(v) AND d.value = v`.
7. Add a setting to not send the startup options, and state the PgBouncer, FIPS and minimum TimescaleDB
   version notes in the docs. Use `COUNT(value)`.

## Tests

- A migration that sleeps longer than the statement timeout succeeds at startup.
- The race tests fail on the commit before the delete fix when run with the CI command.
- A lock timeout answers 503.
- `latest` under a concurrent range delete returns the newest remaining sample, on both backends.

## Done when

The tests pass in CI with a pool above one connection, and the upgrade notes exist.

## Outcome (11 November 2026)

1. **Migrations under the statement timeout**: real. `pg_session::run_migrations` applies them on a connection
   detached from the pool, with `statement_timeout = 0` and the maintenance timeout as `lock_timeout`, for both
   backends (`run` does not compile inside an `async_trait` method, so it calls `run_direct`). Test: a migration
   that sleeps 3 s succeeds on a pool with a 1 s statement timeout, where the same sleep fails on the pool
   (PostgreSQL and TimescaleDB). **Not done**: splitting the eight hypertable index builds into one file each.
   The migration is unreleased, but the test databases that applied it would then refuse the changed checksum;
   the page tells to plan a window instead.
2. **Upgrade path**: `docs/UPGRADING.md` (stop every old instance, start the new one, rollback is a backup,
   `startupProbe`), a breaking entry in the changelog.
3. **Pool cap**: removed from `test-postgres` and `test-timescaledb` in `Makefile.toml`, so CI uses the default
   pool. New test `two_writers_registering_one_uuid_with_two_types_cannot_both_succeed` (10 rounds, exactly one
   writer wins): passes on SQLite, PostgreSQL, TimescaleDB and DuckDB. It does not on ClickHouse (both writers
   succeed, round 0), so it skips ClickHouse and BigQuery, see `ideas/clickhouse-concurrent-registration.md`.
   It uses two types, not two names, because task 7 removes the name half of the check. "The race tests fail on
   the commit before the delete fix with the CI command" was not re-run on that commit.
4. **Lock timeout and deadlock**: `55P03` and `40P01` are a 503 (no `Retry-After`: earlier batches of the
   request may be stored). `storage::common::retry_publish` is shared by PostgreSQL and TimescaleDB (deleted
   sensor and deadlock, five attempts, jitter), unit tested with a fake database error. A lock timeout is not
   retried: it waited 60 s already.
5. **`latest`**: real, on more backends than the review named. The new test
   `latest_finds_the_newest_remaining_sample_while_the_newest_ones_are_deleted` failed 3 of 3 with a single
   attempt on PostgreSQL, and on SQLite, DuckDB and ClickHouse too. A single statement per type would change
   sixteen SQL strings and risk the plans of the hot read path, so the read is tried again when the samples of
   the newest time vanished (`LatestRead`, `LATEST_ATTEMPTS = 5`): 15 runs of 15 on PostgreSQL, TimescaleDB and
   ClickHouse and SQLite pass. Three attempts were not enough against a deleter that never pauses. DuckDB still
   fails (`ideas/duckdb-latest-under-concurrent-delete.md`) and is skipped.
6. **Dictionary lookups, quadratic (suspected)**: not measured, not changed.
7. **Notes**: `COUNT(value)` in the TimescaleDB numeric average; `0` timeouts send no startup option (PgBouncer);
   the PgBouncer, FIPS and TimescaleDB 2.11 notes are in `docs/UPGRADING.md`. The TimescaleDB 2.11 minimum comes
   from memory of its release notes, not checked.
