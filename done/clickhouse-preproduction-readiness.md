# ClickHouse Pre-Production Readiness

## Goal

Make ClickHouse the first operationally credible SensApp backend for pre-production deployments.

## Context

- `TODO.md` identifies ClickHouse as the main hardening target.
- Core ClickHouse query and pagination support already exist.
- Real-service tests already cover repeated migrations and basic health checks.
- The remaining work is now mostly about end-to-end validation, failure-path confidence, and deployment guidance.

## Current focus

1. ~~Add or strengthen tests for ClickHouse operational behaviors such as repeated migrations and health checks.~~
2. ~~Validate the ingest/query/export lifecycle against a real ClickHouse service.~~
3. ~~Add deployment and operating guidance for ClickHouse-based setups.~~

## Completed in this pass

- Added integration coverage for repeated `create_or_migrate()` runs.
- Added integration coverage for ClickHouse storage `health_check()`.
- Added end-to-end ClickHouse-backed HTTP lifecycle coverage for readiness, CSV ingestion, Influx ingestion, series listing, query, and CSV export.
- Refactored ClickHouse aggregated query helpers so the ClickHouse feature set passes `cargo clippy --tests`.
- Added `docs/CLICKHOUSE.md` with connection-string, Helm, validation, and operator guidance.
- Refreshed a small set of low-risk dependencies: `cached`, `hybridmap`, `once_cell`, and `tracing-subscriber`.
4. Define and document what "pre-production ready" means for SensApp with ClickHouse.

## Remaining release work (estimate)

See `docs/PREPRODUCTION_RELEASE_PLAN.md` for acceptance criteria and sequencing.
The ClickHouse deployment path is approximately **4–8 engineer-days** after CI
passes, for staging, recovery/backup exercises, and release preparation. A
frontend-inclusive release adds **1–3 days** for the remaining OpenAPI generator
security work. These are estimates, not observed durations; they assume an
available ClickHouse environment and someone who can operate it. BigQuery and
broad backend parity are separate follow-ups.

## Careful review (1 Oct 2026)

Done against a real ClickHouse 24.8 (local Docker), with the server built `--no-default-features --features clickhouse`, in throwaway databases and disposable containers. The first status ("tests pass, ready") was too optimistic: the tests never wrote the same series twice or concurrently, and nothing had been run at scale or through a failure. The second pass found more than the first.

### Fixed (one commit each, see `git log`)

| # | Finding | Fix |
| --- | --- | --- |
| 1 | Labels re-inserted on every write (4 labels written 5 times = 20 rows, label list repeated in the `/series` dataset id) | Written once, when the sensor is created, before the sensor row |
| 2 | 20 concurrent first writes of one series = 20 catalog entries | `ReplacingMergeTree` for `units`, `sensors`, `labels`, `FINAL` on reads; confirmed with 400 writes over two SensApp instances: 100 series |
| 3 | Sensor ids from `DefaultHasher`, which may change with the Rust release | XOR of the UUID halves, BLAKE3 for units, golden tests pin both |
| 4 | One request spanning more than 100 months rejected | `max_partitions_per_insert_block` = 2400 |
| 5 | Hyphenated database names failed at startup | Quoted |
| 6 | Connection string parsed by hand: no percent-decoding, `@:/` in a password broke startup | `url` crate |
| 7 | Outage = anonymous 500, insert errors = 400 "invalid data format" | `StorageError::Unavailable` (503) and central classification of client errors, other server errors are 500 |
| 8 | Hung ClickHouse: readiness took 30 s then 408 | Health check bounded to 3 s, request timeout answers 504 |
| 9 | Backup and restore undocumented | Procedure documented and exercised (`BACKUP` / `DROP DATABASE` / `RESTORE`), setting it needs documented |
| 10 | Unused materialized views with stale rows | Removed |
| 11 | Single node, not replicated | Documented |
| new | Partition expression depended on the server time zone | Pinned to UTC, pruning unchanged |
| new | `DoubleDelta` on `Float64` was 14 to 20 percent larger than `Gorilla` (and larger than raw data on noise) | `Gorilla, ZSTD(1)` |
| new | Private CA impossible (`UnknownIssuer`), TLS never tested end to end | System trust store (`SSL_CERT_FILE`), tested against a TLS ClickHouse with a private CA |
| new | 3000 new series in one write took 41.7 s (4 ms per existing series, thousands of tiny parts), listing 1000 series 6 s | Bulk registration and bulk label reads: 0.8 s, 0.7 s and 0.13 s |

Databases created by earlier builds are refused at startup with a clear message (the ids and the schema changed): there are no deployments yet.

### Evidence

- All backend-generic regression tests are in `tests/integration/publish_robustness.rs` and also pass on SQLite and TimescaleDB: 150 months in one request, dates from 1900 to 9999, the same series published five times, 16 concurrent first writes, 2100 new sensors in one batch, a deleted series written again.
- ClickHouse specific: legacy database refused, hyphenated database with a fully percent-encoded password, no materialized view, health check against a server that never answers, classification of client errors.
- Drills: ClickHouse stopped (503, ready 503, recovers by itself), paused (ready 503 after 3 s, writes 504), killed in the middle of a 500,000 sample write (503, nothing stored, the retry stores exactly 500,000), SensApp restarted (data intact), two SensApp instances writing the same new series, backup and restore.
- Full suites pass: default features, ClickHouse (255 integration tests), TimescaleDB, Python SDK; `clippy -D warnings` on the default, ClickHouse and all-features builds.

### Still open

- ~~Selector queries read each series with its own queries~~: fixed, `done/bulk-selector-reads.md` (100 series: 1.3 s to 0.02 s).
- Writes are at least once and not atomic across tables. A crash in the middle of a request can store part of it, and a retry then duplicates that part, which the vacuum operation removes (`done/sample-deduplication-in-vacuum.md`). In the kill drill nothing was stored, but a kill during the commit of the inserts could.
- Not tested: a replicated or clustered ClickHouse (SensApp does not create replicated tables), long soak runs, data volumes beyond millions of rows, a release build under load, a staging environment with real operations.
- The test harness migrates and truncates 11 tables for every test, about 0.4 s each: the ClickHouse integration suite takes over a minute.
- TLS and the new tests are not exercised by CI yet beyond what the existing ClickHouse job runs (the TLS check was manual).

### What "good enough" means here

No known data-correctness bug, failures reported with the right status and recovering without a restart, a documented backup and restore that was practised, documentation of what is and is not guaranteed, and evidence from a real service instead of mocks. That is met for the code. What remains of the release plan is evidence from a real environment: staging the image and the chart, exercising operations there, and publishing (steps 2 to 4 of `docs/PREPRODUCTION_RELEASE_PLAN.md`), plus a green CI run.

## Closed (2 Oct 2026)

Merged to `main` with PR 44, and the CI/CD Pipeline is green on the merge commit (backend matrix, frontend,
Python SDK, audit, Helm, Docker smoke, live Prometheus compatibility): step 1 of the release plan has its
evidence. What is left is not code: staging, operations drills and the release itself (steps 2 to 4 of
`docs/PREPRODUCTION_RELEASE_PLAN.md`), plus the frontend generator upgrade (step 5).

## Notes

- Keep tests generic where practical, but accept backend-specific validation where operational behavior differs.
- Prefer simple, explicit checks over adding new abstractions.
