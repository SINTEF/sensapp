# CI and Release Checks

The main workflow runs on pull requests, pushes to `main`/`develop`, version
tags, manual dispatch, and every Monday at 03:17 UTC. The scheduled run catches
dependency and GitHub runner drift even when there are no code changes.

## What is checked

- Rust formatting, a blocking `cargo audit` (with the documented RSA and unused
  optional rkyv advisory exceptions in `Makefile.toml`), frontend
  lint/typecheck/tests/build, and Python
  SDK unit tests, lint, and package build.
- Separate build, test, and Clippy checks for SQLite, PostgreSQL, ClickHouse,
  DuckDB, TimescaleDB, and RRDCached. The service-backed jobs use real database
  containers. BigQuery remains an experimental compile-only Docker variant until
  an isolated CI dataset and credentials are available.
- Helm lint/template/package and Docker builds. The normal runtime image is
  actually started with ClickHouse, then `tests/clickhouse_container_smoke.py`
  checks readiness, publish, query, and service metrics. Live Python SDK tests
  also run against this image. CI stops ClickHouse and verifies that readiness
  becomes unhealthy, restarts it, and verifies recovery. The separate SDK
  workflow also starts a SQLite-backed server for its live tests whenever SDK
  paths change.

The image push depends on these checks. Publishing a GitHub Release for a
`vX.Y.Z` tag is the release trigger; a tag push alone runs CI but does not
publish packages. CI first checks that the tag, Cargo package version, and Helm
`appVersion` agree. The release job waits for the image and chart jobs, verifies
the crate package, and publishes it to crates.io. The Python SDK package is
built and checked, but is not yet published by this workflow.

## Local reproduction

Use `cargo make check-local-matrix` with Docker for the backend matrix. For the
container smoke, run a ClickHouse service and a SensApp image configured with
`SENSAPP_STORAGE_CONNECTION_STRING=clickhouse://...`; then run:

```bash
python3 tests/clickhouse_container_smoke.py
```

The script defaults to `http://127.0.0.1:3000` and accepts `--base-url` for
another endpoint. It uses only Python's standard library.
