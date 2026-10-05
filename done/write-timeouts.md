# Timeouts of their own for the writes, per-operation timeouts in the SDK

Branch `write-timeouts`. Found on 1 October 2026 and fixed on 4 October 2026.

## Problem

One 30 s timeout (`SENSAPP_HTTP_SERVER_TIMEOUT_SECONDS`) covered every request. Importing a 1.33 M-sample
series into TimescaleDB in **one** Arrow request hit it: the server answered 504 after 30 s, the Python SDK
retried (504 is retryable), and the series ended with **1,351,680 rows holding 688,128 distinct
timestamps**: half of the data, the first half twice. The cause is that a request is stored batch by batch
(`SENSAPP_BATCH_SIZE`, 8,192 samples, each batch one transaction, see `BatchBuilder`): the timeout cancels
the request after 27 or 28 batches were committed (the duplicates were 663,552 = 27 x 24,576 samples), and a
retry writes them again. The SDK had a single 10 s timeout for reads and writes, too small for a big upload.

## What was done

- `SENSAPP_HTTP_WRITE_TIMEOUT_SECONDS` (default **300 s**) for `/publish`, the InfluxDB write and the
  Prometheus remote write; `SENSAPP_HTTP_SERVER_TIMEOUT_SECONDS` (30 s) keeps the reads and the rest. Same
  mechanism as the vacuum's own timeout: the layer sits on the route group.
- Why 300 s: the body limit (64 MiB, about 650,000 samples of line protocol) bounds a write. Measured with
  200,000-sample requests: SQLite 4 s, PostgreSQL 17 s, TimescaleDB 60 s for 1.33 M samples (22,000
  samples/s while creating a chunk per week of history), so a full body is about 30 s in the slowest case
  measured. 300 s is ten times that and still ends a hung database. Proxies in front often have 60 s, which
  is why `docs/HTTP_LIMITS.md` also advises requests of 100,000 to 200,000 samples.
- SDK: `SensAppClient(timeout=35, write_timeout=330)`, a little above the server's, so that the server's 504
  (which says what happened) reaches the client before a client-side timeout. A write that took more than
  `RetryPolicy.total_timeout` (60 s) is not retried, so a write that timed out at 300 s is not sent again.
- OpenAPI 504 text of the three writes, `docs/CONFIGURATION.md`, `docs/HTTP_LIMITS.md#timeouts`,
  `docs/PYTHON_SDK.md#timeouts`.
- Tests: `real_router::writes_have_a_timeout_of_their_own` (a write slower than the standard timeout passes,
  a write is still bounded, the other requests keep theirs), the SDK tests on the timeouts. The other
  timeout tests now exercise the write timeout, which is the one that applies to their write.

## Validation

`cargo clippy --tests`, `cargo test` (SQLite; 246 + 299 tests), SDK `ruff` and `pytest` (69 passed). The
original failure, replayed against the TimescaleDB container: see the results at the end of this file.

## Left alone, on purpose

A timed-out or failed request still leaves its first batches stored. The SDK documents that retries can
duplicate (the vacuum, or `SENSAPP_DEDUPLICATE_ON_INGEST`, removes them). Making a request atomic is
`ideas/atomic-write-requests.md`.

`frontend/openapi.json` (branch `frontend-good-enough`) holds the old 504 text of the writes: after merging
both branches, its drift test says so, `UPDATE_OPENAPI=1 cargo test frontend_openapi_document` fixes it.

## Result of the replay (4 October 2026)

Release build, TimescaleDB 2.17.2 container, empty database, SDK and server defaults: the same single
Arrow request of 1,331,266 samples now answers 200 after 48.7 s on the server (51.6 s for the client).
The database holds **1,331,266 rows and 1,331,266 distinct timestamps**, and the log has no 504. Before: 504
after 30 s, a retry, and 1,351,680 rows for 688,128 distinct timestamps.

## Follow-up: the reads, 4 October 2026

30 s was also low for reads: a raw read is bounded (100,000 samples) but an aggregation scans its window.
Whole-history aggregations of the 1.33 M-sample series took 0.17 to 0.98 s, linear in the samples scanned,
so 120 s covers about 100 million samples on the slowest query measured. `SENSAPP_HTTP_SERVER_TIMEOUT_SECONDS`
now defaults to **120 s** and the SDK's `timeout` to **125 s** (writes: 300 s and 330 s). Docs:
`docs/CONFIGURATION.md`, `docs/HTTP_LIMITS.md#timeouts`, `docs/PYTHON_SDK.md#timeouts`.
