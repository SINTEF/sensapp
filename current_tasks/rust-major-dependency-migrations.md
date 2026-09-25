# Rust Major Dependency Migrations

## Goal

Upgrade Arrow, SQLx, cached, jsonwebtoken, and utoipa to their current major versions, then review their effects on the API and storage backends.

## Changes

- Arrow 60, SQLx 0.9, cached 4, jsonwebtoken 11, utoipa 6, and matching utoipa-scalar 0.4.
- Adapt cached macro settings and generated cache locks.
- Audit SQLx dynamic queries: matcher values remain bound parameters; generated placeholders, table names, aggregation expressions, and limits come from internal fixed choices or numeric values. The test database identifier is quoted with embedded quotes escaped.
- Add an OpenAPI document smoke test for core routes.
- DuckDB still brings Arrow 58 transitively; SensApp's direct Arrow APIs use 60.
- Raise the Docker builder to Rust 1.96 so it can build SQLx 0.9 (requires Rust 1.94) and cached 4 (requires Rust 1.92).

## Validation

- `cargo check --locked` and `cargo check --locked --all-features` pass.
- `cargo test --locked` passes with SQLite-backed integration tests.
- `cargo test --locked --no-default-features --features sqlite` passes.
- `cargo test --locked --no-default-features --features postgres` passes against temporary local PostgreSQL 18.
- `cargo clippy --locked --all-features --tests -- -D warnings` and `cargo fmt --all -- --check` pass.
- TimescaleDB service tests need CI because the local Docker daemon and TimescaleDB service are unavailable.

## Remaining

- Review the CI backend matrix on the draft pull request, especially TimescaleDB and DuckDB.
