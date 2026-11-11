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
