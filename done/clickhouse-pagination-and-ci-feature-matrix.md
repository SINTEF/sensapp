# ClickHouse Pagination And CI Feature Matrix

## Goal

Finish ClickHouse cursor pagination and make CI exercise the ClickHouse backend through its explicit feature-gated check path.

## Context

- ClickHouse is the current reference backend for pre-production hardening.
- `list_series` was ignoring `limit` and `bookmark` for ClickHouse.
- GitHub Actions only exercised SQLite and PostgreSQL in the main test matrix even though backend-specific cargo-make tasks already existed.

## Completed Work

- Implemented real `list_series` cursor pagination for ClickHouse using `sensor_id` bookmarks
- Added ClickHouse integration coverage for pagination behavior and invalid bookmarks
- Added ClickHouse to the GitHub Actions backend matrix with the ClickHouse service and test environment
- Fixed ClickHouse numeric decimal storage to use a supported fixed-scale mapping
- Hardened config and pagination tests so the repo-defined ClickHouse check passes cleanly

## Validation

- `cargo test --no-default-features --features clickhouse,test-utils --test clickhouse_integration`
- `TEST_DATABASE_URL=clickhouse://default:password@localhost:8123/sensapp_test cargo test --no-default-features --features clickhouse,test-utils --test crud_dcat_api test_series_pagination_basic -- --nocapture`
- `TEST_DATABASE_URL=clickhouse://default:password@localhost:8123/sensapp_test cargo test --no-default-features --features clickhouse,test-utils --test crud_dcat_api test_series_pagination_with_bookmark -- --nocapture`
- `cargo make check-clickhouse`

## Status

Completed.
