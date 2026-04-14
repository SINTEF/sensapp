# Cross-Backend Pagination And CI Expansion

## Goal

Bring `list_series` pagination semantics in line across the maintained backends and expand the explicit CI backend matrix beyond ClickHouse.

## Context

- ClickHouse now uses proper cursor pagination semantics.
- SQLite still ignores `limit` and `bookmark`.
- PostgreSQL and DuckDB still use the old `len == limit` heuristic for next-page detection.
- GitHub Actions only exercises a subset of the backend-specific check tasks that already exist in `cargo-make`.

## Scope

- Implement proper `list_series` pagination for SQLite
- Fix next-page detection in PostgreSQL and DuckDB
- Make the generic pagination tests backend-agnostic where needed
- Extend the CI backend matrix to additional supported backends

## Outcome

- SQLite now implements cursor pagination for `list_series`
- PostgreSQL, DuckDB, and TimescaleDB now use `limit + 1` next-page detection
- TimescaleDB now implements label-based queries needed by Prometheus remote read
- TimescaleDB timestamp conversion now round-trips at the intended microsecond precision
- CI now exercises `duckdb` and `timescaledb`, including a dedicated TimescaleDB migration step
- Local PostgreSQL 18 test compose configuration now uses a compatible data mount path

## Status

Completed.