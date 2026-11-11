# DuckDB: connection model and divergences from the reference

Source: code review of 10 November 2026. Reported from reading the code and the `duckdb` crate; nothing was
run.

## Problem

1. **One connection behind one mutex** (`src/storage/duckdb/mod.rs:34`, `Arc<Mutex<Connection>>`).
   - A long read, `deduplicate_samples` (`:94-115`) or `VACUUM ANALYZE` blocks every other read and write.
   - A timed-out write still runs: `publish` (`:68-81`) uses `spawn_blocking`, and when the request is
     dropped the closure stays queued on `blocking_lock()` and commits later. The client got a 504 and
     retries, so duplicates appear under overload.
   - `create_or_migrate` (`:61-66`), `vacuum` (`:83-86`), `health_check` (`:952-957`) run synchronous DuckDB
     calls on an async worker thread.
   - The health check waits on the same mutex, so `/health` hangs during maintenance.
2. **Reads compare the raw UUID text** (`mod.rs:420,434,1003`: `WHERE CAST(s.uuid AS TEXT) = ?` with the
   caller's string). An upper-case, braced or hyphen-less UUID is a 404 on DuckDB and works on SQLite, and
   `delete_series` normalises, so GET 404 and DELETE 200 can disagree. The cast also keeps DuckDB from using
   the unique index on `uuid`.
3. **Duplicate label names fail the batch** (`src/storage/duckdb/duckdb_registration.rs:107-121`): `labels`
   has `PRIMARY KEY (sensor_id, name)` and `sort_labels` does not deduplicate. SQLite and PostgreSQL ignore
   the duplicate. A unit id missing from the re-read silently registers the sensor without a unit (`:84-87`).
4. **No migration tracking** (`mod.rs:39,61-66`): `INIT_SQL` is `CREATE ... IF NOT EXISTS` run at each start,
   so a change to an existing table will not apply to existing files.
5. **Smaller divergences**: `query_sensor_data` with `limit = None` is unlimited (`mod.rs:489`) where SQLite
   and PostgreSQL cap at `DEFAULT_QUERY_LIMIT`; `list_metrics` returns empty label keys and groups by unit
   (`mod.rs:367-404`); value tables have no ordering by `(sensor_id, timestamp_us)`.
   *Update 11 November 2026: `list_metrics` was aligned with SQLite in the SQLite divergences task (label
   keys, grouped by name and type), and first and last on equal timestamps keep the write order (`rowid`).*

## Suggested solution

1. Use `Connection::try_clone()` for reads (a small pool of cloned connections, or one clone per
   operation), and keep one writer connection behind the mutex. Run every DuckDB call in `spawn_blocking`.
   Give the health check its own connection and a timeout. For timed-out writes: check a cancellation flag
   (a `CancellationToken` or an `Arc<AtomicBool>` set when the request future is dropped) after taking the
   lock and before starting the transaction, so a write whose client has gone is skipped.
2. Parse the UUID once at the HTTP layer and pass the canonical string (or a `Uuid`) to storage on every
   backend; in DuckDB compare with `s.uuid = ?::UUID`.
3. Reject duplicate label names at validation (in `Sensor` construction) with a 400, so every backend sees
   the same input; turn the missing unit id into an error.
4. Add a `schema_version` table and numbered migration steps, as small as possible.
5. Apply the default limit; align `list_metrics` with SQLite.

## Tests

Generic tests: a read completes while a long write holds the writer; UUID spellings behave the same on GET
and DELETE on every backend; duplicate label names are a 400; default limit applies.

## Done when

The tests pass, and the write and read numbers before and after the connection change are noted here.

## Outcome (11 November 2026)

1. **Fixed.** Writes go through one connection behind a mutex. Reads, and the health check, take a
   connection from a small pool of `try_clone()` connections of the same database (16 kept idle). Every
   DuckDB call runs in `spawn_blocking`, the migration, the vacuum and the test cleanup included. A publish
   whose request is dropped while it waits for the writer (a timeout, a client gone) is skipped when its
   turn comes, instead of committing after the client got its error. The health check has a 5 s timeout.
   Unit tests: `reads_and_the_health_check_do_not_wait_for_the_writer`, and
   `a_write_abandoned_while_it_waits_is_not_committed`, which fails with the check disabled.
   Measured on a release build, file database, with a temporary harness (not kept):

   | | before | after |
   |---|---|---|
   | reads while 16 writers publish, p50 / max | 31 ms / 47 ms | 2.7 ms / 15 ms |
   | writes of 2000 samples in 3 s | 1450 | 1441 |
   | health check while the vacuum deduplicates 1 M rows | 50 ms | 0.24 ms |

   DuckDB deduplicates 1 M rows in about 50 ms, so the wait behind a vacuum was short anyway; the reads
   queued behind the writes were the larger cost.
2. **Reproduced and fixed, on more than DuckDB**: an upper-case or hyphen-less UUID was a 404 on reads on
   SQLite and DuckDB, while the delete parsed it. The HTTP read handlers now pass the canonical UUID to
   storage, as the deletes did. DuckDB compares `uuid = CAST(? AS UUID)`, and answers "not found" to a text
   that is not a UUID. The same test found that availability without a step **panicked** on DuckDB (an
   untyped `NULL` column); it is `CAST(NULL AS BIGINT)` now. Generic test
   `identity_over_http::a_series_answers_to_every_spelling_of_its_uuid`.
3. **Reproduced and fixed**: `cpu,host=a,host=b` answered 500 on DuckDB and 204 on SQLite (one of the two
   values kept). `Sensor::new_without_uuid` refuses a label name given twice (`DuplicateLabelName`, a
   400). The paths with an explicit UUID read their labels from a JSON object, so they cannot have one
   twice. Generic test `influxdb_integration::a_tag_written_twice_is_refused`, and a unit test. A unit
   missing from the re-read is now an error instead of a series without its unit.
4. **Fixed**: a `schema_version` table and a list of steps; the first step is the existing schema, which
   is idempotent, so databases made before take it without change. Unit test `each_migration_runs_once`.
5. **Fixed**: `query_sensor_data` without a limit applies `DEFAULT_QUERY_LIMIT` (10 million, so no test:
   tests stay small). `list_metrics` and the tie-break were done in the SQLite divergences task. The value
   tables are not sorted: DuckDB's zone maps already skip by time, and nothing measured needs it.

Suites run: DuckDB, SQLite and PostgreSQL in full; ClickHouse and TimescaleDB targeted
(`identity_over_http`, `influxdb_integration`); `cargo clippy` and `cargo clippy --all-features --tests`.
