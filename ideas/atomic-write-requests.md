# Atomic write requests

## Observation

A write request is stored batch by batch: `BatchBuilder` cuts it into batches of `SENSAPP_BATCH_SIZE`
(8,192) samples and each batch is its own transaction (`storage.publish`). When the request fails or times
out midway, the batches already committed stay, and a client that sends the request again (the SDK does,
Telegraf and Prometheus do) stores them twice. Seen on 4 October 2026: 1,351,680 rows for 1,331,266 sent
(`done/write-timeouts.md`). Deduplication at ingestion (`SENSAPP_DEDUPLICATE_ON_INGEST`) absorbs the
duplicates, but it is opt-in and not available on ClickHouse, BigQuery and RRDCached.

## Options

- **One transaction per request** on the transactional backends (PostgreSQL, TimescaleDB, SQLite, DuckDB):
  all or nothing, so a retry is safe. The body is already buffered whole (64 MiB at most), so memory is not
  the issue; the cost is a longer transaction (locks on TimescaleDB chunk creation, see
  `done/timescaledb-first-write-deadlock.md`, WAL size) and `unnest` arrays of up to 650,000
  elements per statement. Measure before deciding.
- **Idempotent retries**: an `Idempotency-Key` header (the SDK would send a UUID per `publish`), remembered
  for a while. Works on every backend but needs shared state across instances (see the multi-instance
  rule).
- **Keep it as it is** and rely on deduplication plus the vacuum, with the guidance of small requests that
  `docs/HTTP_LIMITS.md#timeouts` gives.

Decide with a concurrent-writers test and numbers: transaction time and lock waits for 1 M-sample requests
on TimescaleDB with the weekly chunks.
