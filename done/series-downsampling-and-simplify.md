# Series Downsampling And Simplify

## Goal

Add explicit query-time downsampling and simplification for series reads.

## Delivered

- Extended `GET /series/{series_uuid}` with explicit query parameters for temporal bucketing, aggregation selection, and explicit simplify enablement.
- Added shared query option types and Rust-side fallback processing for aggregation and simplify.
- Added Prometheus-style duration parsing for `step` values.
- Added native aggregation pushdown for PostgreSQL, SQLite, TimescaleDB, DuckDB, and ClickHouse.
- Kept simplify as a Rust-side post-processing step across all backends so the API semantics stay consistent.

## Validation

- `cargo check`
- `cargo check --features "sqlite timescaledb clickhouse"`
- `cargo check --features "duckdb"`
- `cargo test --test query_export advanced_series_query_tests`
- `cargo test --test advanced_backend_queries --features "sqlite timescaledb clickhouse"`
- `cargo test --test advanced_backend_queries --features "duckdb"`

## Follow-Ups

- Consider a dedicated last-sample API or a storage-level fast path for `aggregation=last` without requiring a caller-selected bucket.
- Consider an availability/existence API for a time window, returning either boolean presence or window coverage ratio.
