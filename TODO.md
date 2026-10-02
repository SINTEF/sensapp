# SensApp TODO

This file tracks the main remaining work for SensApp. Detailed task history lives in `current_tasks/`, `ideas/` and `done/`.

The core is in place: HTTP-only ingestion, DCAT/query/export endpoints, Prometheus and InfluxDB compatibility, health endpoints, Prometheus service metrics, JWT sensor authorization, HTTP resource limits, data lifecycle controls, cross-series aggregation, a Python SDK, Helm chart and container images, and a broad integration test suite run against every backend in CI.

The next phase is not to add more features. It is to make the existing system solid enough for pre-production, with ClickHouse as the reference backend. The acceptance criteria and sequencing are in [docs/PREPRODUCTION_RELEASE_PLAN.md](docs/PREPRODUCTION_RELEASE_PLAN.md).

## Current Priorities

### 1. Pre-production release (ClickHouse)

Code, tests and docs for this exist (see `docs/CLICKHOUSE.md`, `done/clickhouse-*.md`). What remains is evidence from a real environment:

- [ ] Verify the revised CI on GitHub: backend matrix, DuckDB and Docker smoke durations, cache sizes (`done/ci-build-time.md`)
- [x] ClickHouse review: duplicated labels and sensors, unstable ids, 100-month insert limit, connection string, outage statuses, TLS with private CAs, bulk registration (`current_tasks/clickhouse-preproduction-readiness.md`)
- [ ] Stage a deployment of the image and Helm chart against an external persistent ClickHouse
- [ ] Exercise operations: restart SensApp, interrupt and recover ClickHouse, practise backup and restore on disposable data
- [ ] Review dependency audit findings, align Cargo and chart versions, write the changelog, publish from a reviewed tag, smoke-test the published artifacts

### 2. Observability and resilience

Done: Prometheus scrape endpoint with HTTP request count, duration and in-flight gauges, per-operation counts and durations (queries and writes), processed sample and series counts, and storage readiness; request tracing through `tower-http`; request timeout; request body and read-size limits.

- [x] Backpressure on writes: concurrency limit, `503` with `Retry-After` (`docs/CONFIGURATION.md`). Rate limiting is left to a reverse proxy on purpose
- [x] Request correlation identifiers (`x-request-id`) in logs and response headers
- [ ] Review structured logging for enough context to diagnose backend failures (credentials are already redacted)

### 3. Documentation

- [x] Configuration reference: `docs/CONFIGURATION.md`
- [x] Document backend trade-offs clearly, and which backends are maintained versus experimental. The release plan already positions ClickHouse as the reference, the other backends as tested compatibility paths, and BigQuery and RRDCached as experimental
- [x] Document which features are production-oriented and which remain research-oriented

### 4. Codebase cleanup

- [x] Remove the module duplication between the library crate (`src/lib.rs`) and the binary crate (`src/main.rs` declares the same modules again)
- [ ] Keep module boundaries simple and avoid reintroducing architectural complexity
- [x] Drop the unused `migrate-*` and `setup-dev` cargo-make tasks (`ideas/ci-followups.md`)

## Deferred Work

These are valid tasks, but not the current focus. Most have a note in `ideas/`.

- [ ] Bring BigQuery back in sync with the current storage trait and query model (`ideas/bigquery-backend-reconciliation.md`)
- [ ] Add benchmark tooling for storage backend comparison (a first script exists: `tests/perf/scale.sh`, writes and selector reads of thousands of series through the HTTP API)
- [ ] Add research-specific comparison endpoints and reporting helpers
- [ ] Add storage-space and latency comparison reports across backends
- [ ] Data retention (`ideas/data-retention.md`)
- [ ] Cross-series aggregation pushdown, composite sensors, a minimal PromQL `rate()`

## Working Position

The project sits between two goals:

- a real tool that should be good enough for pre-production use
- a research platform that supports storage backend comparison

The practical rule for now:

- prefer making the existing core reliable over adding new capabilities
- treat ClickHouse as the first backend that must feel operationally credible
- keep other backends working, but accept that not all of them need the same maturity at the same time
