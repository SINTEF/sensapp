# SensApp TODO

This file tracks the main remaining work for SensApp.

Most of the large refactoring work is done: HTTP-only ingestion, direct storage calls, DCAT/query/export endpoints, Prometheus and InfluxDB compatibility, health endpoints, Prometheus service metrics, and a substantial integration test suite are already in place.

The next phase is not to add more features. It is to make the existing system solid enough for pre-production, with ClickHouse as the first serious target backend, while keeping the other backends reasonably healthy.

## Current Priorities

### 1. Pre-production readiness

- [ ] Make ClickHouse the reference pre-production backend
- [ ] Validate the full ingestion/query/export lifecycle against a real ClickHouse service
- [ ] Harden ClickHouse operational behavior: migrations, health checks, error messages, and recovery paths
- [ ] Add deployment and operating guidance for a ClickHouse-based setup
- [ ] Define what pre-production ready means for SensApp and document the acceptance criteria

### 2. Backend quality and consistency

- [ ] Keep PostgreSQL, SQLite, TimescaleDB, DuckDB, RRDCached, and ClickHouse aligned with the current `StorageInstance` trait
- [ ] Add cross-backend consistency tests for the core workflows: publish, list metrics, list series, query by UUID, query by labels, export formats
- [ ] Make feature-gated backend tests run explicitly in CI instead of relying on the default test path
- [ ] Decide which backends are actively maintained versus experimental

### 3. Codebase cleanup

- [ ] Remove structural duplication between the library crate and the binary crate where practical
- [ ] Ensure the project builds cleanly without duplicated code paths, duplicated tests, or unnecessary warnings
- [ ] Keep module boundaries simple and avoid reintroducing architectural complexity

### 4. Observability and resilience

- [ ] Add query latency metrics
- [ ] Add write latency metrics
- [ ] Add ingestion rate and error rate metrics
- [ ] Add structured logging with enough context to diagnose backend failures
- [ ] Add request correlation or tracing identifiers where useful
- [ ] Add ingestion rate limiting and backpressure handling

### 5. Documentation

- [ ] Write a pre-production deployment guide for ClickHouse
- [ ] Document backend trade-offs clearly
- [ ] Write a short configuration reference for common deployments
- [ ] Document which features are production-oriented and which remain research-oriented

## Deferred Work

These are valid tasks, but not the current focus.

- [ ] Bring BigQuery back in sync with the current storage trait and query model
- [ ] Add benchmark tooling for storage backend comparison
- [ ] Add research-specific comparison endpoints and reporting helpers
- [ ] Add storage-space and latency comparison reports across backends

## Working Position

The project currently sits between two goals:

- a real tool that should be good enough for pre-production use
- a research platform that supports storage backend comparison

That balance is useful, but it also creates maintenance pressure. The practical rule for now should be:

- prefer making the existing core reliable over adding new capabilities
- treat ClickHouse as the first backend that must feel operationally credible
- keep other backends working, but accept that not all of them need the same maturity at the same time
