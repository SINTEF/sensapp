# Live Prometheus compatibility test in CI

## Goal

Exercise SensApp's remote write and remote read endpoints through a real Prometheus process on every pull request.

## Implemented

- Added a dedicated CI job with PostgreSQL and Prometheus v2.55.1. The job gates image and chart release jobs.
- Added a local runner, pinned Prometheus configuration, and a Python live test.
- Remote write check waits for Prometheus to scrape SensApp's storage readiness gauge and send it back to SensApp with a job label.
- Remote read check publishes a distinct probe only to SensApp, queries it through Prometheus, and verifies that SensApp handled a remote read request.
- Added bounded readiness checks and service logs on failure.

## Validation

- Live local run against a fresh PostgreSQL database: both directions passed.
- `cargo check --locked`: passed.
- `cargo test --locked`: passed against the disposable PostgreSQL database.
- `cargo clippy --locked --tests -- -D warnings`: passed.
- `cargo fmt --all -- --check`, Python syntax check, shell syntax check, and YAML parsing: passed.
