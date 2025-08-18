# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Development Commands

### Building and Running

- `cargo build` - Build the project
- `cargo run` - Run SensApp with default configuration
- `cargo check` - Check code for errors without building
- `cargo test` - Run all tests
- `cargo clippy` - Run linter for code quality checks
- `cargo fmt` - Format code according to Rust style guide

### Database Testing

- Use different storage backends by modifying the hardcoded connection string in `main.rs:107`
- SQLite: `"sqlite://test.db"`
- PostgreSQL: `"postgres://postgres:postgres@localhost:5432/postgres"`
- DuckDB: `"duckdb://sensapp.db"`
- BigQuery: `"bigquery://key.json?project_id=PROJECT&dataset_id=DATASET"`
- RRDCached: `"rrdcached://localhost:42217?preset=munin"`

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
- **Multi-format support**: JSON, CSV, SenML, InfluxDB line protocol, Prometheus remote write

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
- **Scalable deployment** patterns from single-node to distributed clusters
