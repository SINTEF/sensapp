# SensApp Build & Test Automation

This project uses `cargo-make` for automated testing and quality checks across different database backends.

## Installation

```bash
cargo install cargo-make
```

## Available Tasks

### Quick Tasks
- `cargo make check-all` - Run all checks for PostgreSQL and SQLite (default)
- `cargo make ci` - Full CI pipeline with formatting and checks
- `cargo make clean` - Clean build artifacts and test databases

### Individual Database Testing
- `cargo make check-sqlite` - SQLite only (build, test, clippy)
- `cargo make check-postgres` - PostgreSQL only (build, test, clippy)

### Extended Testing
- `cargo make check-working-features` - Test PostgreSQL + SQLite combined
- `cargo make ci-extended` - Full CI with all working features

### Specific Operations
- `cargo make fmt-check` - Code formatting check
- `cargo make migrate-sqlite` - Run SQLite migrations
- `cargo make migrate-postgres` - Run PostgreSQL migrations

## Environment Variables

The build system automatically configures database connections:
- SQLite tests: Uses `sqlite://test.db`
- PostgreSQL tests: Uses `postgres://postgres:postgres@localhost:5432/sensapp`

Override with `TEST_DATABASE_URL` if needed.

## CI Usage

For continuous integration, use:
```bash
cargo make ci
```

This runs:
1. Code formatting check
2. SQLite build, test, and clippy
3. PostgreSQL build, test, and clippy

## List All Tasks

```bash
cargo make --list-all-steps
```
