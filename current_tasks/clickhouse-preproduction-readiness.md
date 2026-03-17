# ClickHouse Pre-Production Readiness

## Goal

Make ClickHouse the first operationally credible SensApp backend for pre-production deployments.

## Context

- `TODO.md` identifies ClickHouse as the main hardening target.
- Core ClickHouse query and pagination support already exist.
- The remaining work is less about features and more about operational confidence: migrations, health checks, lifecycle validation, and deployment guidance.

## Current focus

1. Add or strengthen tests for ClickHouse operational behaviors such as repeated migrations and health checks.
2. Validate the ingest/query/export lifecycle against a real ClickHouse service.
3. Add deployment and operating guidance for ClickHouse-based setups.

## Notes

- Keep tests generic where practical, but accept backend-specific validation where operational behavior differs.
- Prefer simple, explicit checks over adding new abstractions.
