# Batched sample inserts

## Status (1 Oct 2026)

Implemented for PostgreSQL, TimescaleDB and SQLite. ClickHouse was already batched (one streamed `Inserter` per value type).

- PostgreSQL and TimescaleDB: one `INSERT ... SELECT ... FROM unnest(arrays)` per call (`publish_*_values`), so one round trip per sensor batch instead of one per sample.
- SQLite: multi-row `INSERT` through `QueryBuilder::push_values`, 8 000 rows per statement (4 columns at most, under SQLite's 32 766 bound variables; `SENSAPP_BATCH_SIZE` is configurable, so the chunking is needed).
- Tests: `tests/integration/batched_inserts.rs` (20 000 samples of every type, backend-generic).
- Note: SQLite `json_values.value` is a BLOB column in a STRICT table, so the batched insert binds the JSON as bytes (the read path already reads `Vec<u8>`).

### Result (release build, 1 331 266 float samples, InfluxDB line protocol, 200 000 lines per request, fresh database)

| backend | before | after |
|---|---|---|
| SQLite | 17.0 s (78 000 / s) | 3.2 s (412 000 / s) |
| PostgreSQL 18, localhost | 442 s (3 000 / s) | 9.2 s (145 000 / s) |

PostgreSQL gains the most because each sample was a round trip. TimescaleDB uses the same code shape but was only covered by the integration tests, not benchmarked.

## Observation

Importing 1 331 266 samples of one float series (InfluxDB line protocol, 200 000 lines per request, SQLite, **debug build** on an idle laptop) took 70 to 74 s, about 18 000 samples/s. A server request timeout (30 s) already rejected a 200 000-line chunk once the machine was busy.

Both the SQLite and the PostgreSQL publishers insert one row per statement:

- `src/storage/sqlite/sqlite_publishers.rs`: `publish_float_values` loops over the samples and runs `INSERT INTO float_values (...) VALUES (?, ?, ?)` for each one inside the batch transaction. Same shape for integer, string, boolean, etc.
- `src/storage/postgresql/postgresql_publishers.rs`: the same loop, so on PostgreSQL every sample is also a client-server round trip. PostgreSQL is the main backend, so this is where it matters most.

## Measurements so far (1 Oct 2026, SQLite, empty database, one float series)

| upload | debug build | release build |
|---|---|---|
| InfluxDB line protocol, 200k lines per request | 73.7 s (18 000 / s) | 17.0 s (78 000 / s) |
| Arrow `/publish`, 200k rows per request | 50.3 s (26 500 / s) | not measured |
| Arrow `/publish`, one 1.33 M-row request | rejected by the 30 s request timeout | 18.5 s (72 000 / s) |

The release build is 4 times faster than debug, so most of the original 70 s was the debug build. Arrow is not faster than line protocol in release, and only 1.5 times faster in debug, so parsing is not the main cost. The per-row statements are the likely one, still to be confirmed with a profile. 78 000 samples/s is acceptable for SQLite on one laptop. The case to check is PostgreSQL over a network, where each row is also a round trip: measure it before doing anything.

## Ideas

- **SQLite:** multi-row `INSERT ... VALUES (...), (...)` through `sqlx::QueryBuilder::push_values`, in chunks that stay under `SQLITE_MAX_VARIABLE_NUMBER` (32 766 in current SQLite, so about 10 000 rows of 3 columns).
- **PostgreSQL:** one `INSERT ... SELECT * FROM unnest($1::bigint[], $2::bigint[], $3::float8[])` per sensor batch (the read paths already use `unnest`), or `COPY ... FROM STDIN` for the largest batches.
- The type-specific publishers (integer, numeric, float, string, boolean, location, blob, json) share the pattern, so start with float and integer, then see whether a small shared helper is worth it (KISS: only if the repetition hurts).
- Run the same benchmark as above before and after, with the Zeblab export (1.33 M samples, `zeb_320_001_338_320_001_Hpu001_AlmFl.parquet`).

## Related

- `ideas/sample-deduplication-in-maintenance.md`: re-importing the same samples duplicates them, so benchmarks must use a fresh series each time.
- `docs/HTTP_LIMITS.md` (64 MiB body limit) and `SENSAPP_HTTP_SERVER_TIMEOUT_SECONDS` (default 30) bound what one request can carry. The timeout is not documented in `HTTP_LIMITS.md` yet.
