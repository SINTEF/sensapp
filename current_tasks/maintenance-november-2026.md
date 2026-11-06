# Maintenance Pass, November 2026

## Goal

Bring Rust, frontend, and Python SDK dependencies to their latest versions, majors included, clear
security advisories, and refresh the image and CI pins. Upgrades that need real migration work go to
`ideas/` with the reason, not into this pass.

## Starting point (6 November 2026)

- npm audit: 1 high in production deps (vite 8.3.0 to 8.3.2), 13 findings with dev deps.
- Rust compatible: arrow 60.1, tokio 1.54, uuid 1.29, rustls 0.23.46, clickhouse 0.15.3, cached 4.1,
  duckdb 1.10505 to 1.20000 (DuckDB 2.0 engine), and patch releases.
- Rust majors: base64 0.23, sha2 0.11, getrandom 0.4, tower-http 0.7, prometheus-client 0.25,
  sentry 0.49, geo 0.33, serial_test 4.
- Python SDK: polars 2, pyarrow 26, pandas 3.1, pytest 9.2, ruff 0.17.
- Images: `rust:1.98` builder, ClickHouse 24.8 in tests (end of life), `timescaledb:2.30.2-pg16`.

## Progress

- [ ] Rust compatible updates
- [ ] Rust majors
- [ ] Rust security audit
- [ ] Frontend to latest
- [ ] Python SDK to latest
- [ ] Images and CI pins
- [ ] Checks: clippy, tests, live smoke test, CI

## Deferred

None yet.
