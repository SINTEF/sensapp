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

## Notes

- Keep tests generic where practical, but accept backend-specific validation where operational behavior differs.
- Prefer simple, explicit checks over adding new abstractions.
