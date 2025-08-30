# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Building and Running

- `cargo build` - Build the project
- `cargo run` - Run SensApp with default configuration
- `cargo check` - Check code for errors without building
- `cargo test` - Run all tests
- `cargo clippy` - Run linter for code quality checks
- `cargo clippy --tests` - Run linter on tests
- `cargo fmt` - Format code according to Rust style guide

### Quality Assurance

- `pre-commit install` - Install pre-commit hooks for code quality
- `pre-commit run --all-files` - Run all pre-commit checks manually
- All commits must follow [Conventional Commits](https://www.conventionalcommits.org/) format

## Architecture Overview

SensApp is a **sensor data platform** built with Rust that scales from edge deployments (SQLite) to big data clusters (ClickHouse). It uses an **event-driven architecture** with an internal message bus for component communication.

### Core Components

#### Data Ingestion (`src/ingestors/`)

- **HTTP REST API** with Axum web framework
- **MQTT client** for IoT device integration
- **Multi-format support**: JSON, CSV, SenML, InfluxDB line protocol, Prometheus remote write and Remote Read
- **Compatibility gateways**: InfluxDB and Prometheus.

#### Storage Abstraction (`src/storage/`)

- **Unified storage trait** supporting multiple backends:
  - SQLite (edge/small deployments)
  - PostgreSQL/TimescaleDB (medium deployments)
  - DuckDB (experimental analytics)
  - BigQuery (cloud analytics)
  - ClickHouse (large-scale deployments)
- **Storage factory pattern** for runtime backend selection via connection strings

#### Data Model (`src/datamodel/`)

- **Type-safe sensor data** with strongly typed samples (Integer, Float, String, Boolean, Location, JSON)
- **UUID v7 identifiers** for time-ordered sensor IDs
- **Microsecond precision timestamps** for time-series data
- **String deduplication** using dictionary tables for efficiency

#### Data Processing (`src/infer/`, `src/parsing/`)

- **CSV auto-inference** for column types, headers, datetime parsing
- **Geolocation detection** for spatial data
- **Prometheus remote write** protocol parsing
- **Data type inference** for schema-on-write scenarios

### Configuration

- Primary config file: `settings.toml`
- Database connections configured via connection strings
- MQTT configurations support multiple endpoints
- Sentry integration for error tracking and monitoring

### Key Design Patterns

- **Async-first** using Tokio runtime throughout
- **Event-driven** architecture with message passing
- **Storage-agnostic** design with trait-based abstractions
- **Type safety** for sensor data with compile-time guarantees
- **Scalable deployment** patterns from single-node to distributed clusters, but the complexity is handled by the storage layer (the databases).

## Important Notes for Development

- postgresql database name is sensapp
- DATABASE_URL="postgres://postgres:postgres@localhost:5432/sensapp" sqlx migrate run --source src/storage/postgresql/migrations
- DATABASE_URL="postgres://postgres:postgres@localhost:5432/sensapp" cargo sqlx prepare
- do focus on postgresql, AND NOT OTHER STORAGE BACKENDS FOR NOW.
- You are an excellent and experienced software engineer.
- code used for unit tests and integration tests should be marked with #[cfg(any(test, feature = "test-utils"))]
- I truly hate #[allow(dead_code)], so avoid it as much as possible. If the code is unused, delete it. If the code is used only conditionally, mark the conditions correctly. For example, in integration tests it should be marked with #[cfg(feature = "test-utils")]
- unit tests and integrations tests are very helpful and appreciated. Consider doing them even when not actively requested.
- Rust best practices should be followed.
- KISS: Keep It Simple Stupid. Avoid over-engineering.
- Good Enough is the target, not perfection.
- Professionalism and pragmatism are expected. This is a professional project, not a hobby project.
- Backward compatibility is not a concern. The project is not deployed in production yet.
