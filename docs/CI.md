# CI and Release Checks

The main workflow runs on pull requests, pushes to `main`/`develop`, version
tags, published releases, and manual dispatch.

## What is checked

- Rust formatting, a blocking `cargo audit` (with the documented RSA advisory
  exception in `Makefile.toml`), frontend lint/typecheck/tests/build and a
  high-severity npm audit, and Python 3.14 SDK unit tests, lint, package build and
  wheel install smoke test.

The OpenAPI generator has been upgraded to 0.99.0 and its client regenerated.
The generator still depends on a vulnerable `js-yaml`, so `package.json`
overrides that nested dependency to 4.3.2 or newer. The refreshed lockfile has
no npm audit findings. The regenerated client also passed a live API smoke test;
see `ideas/remove-frontend-js-yaml-override.md` for the override follow-up.
- Separate build, test, and Clippy checks for SQLite, PostgreSQL, ClickHouse,
  DuckDB, TimescaleDB, and RRDCached. The service-backed jobs use real database
  containers. BigQuery remains an experimental compile-only Docker variant on
  manual runs until an isolated CI dataset and credentials are
  available; it does not block the ClickHouse release path.
- Helm lint/template/package and Docker builds. The normal runtime image is
  actually started with ClickHouse, then `tests/clickhouse_container_smoke.py`
  checks readiness, publish, query, and service metrics. Live Python SDK tests
  also run against this image. CI stops ClickHouse and verifies that readiness
  becomes unhealthy, restarts it, and verifies recovery. The image smoke job is
  skipped on published releases: the tagged commit already passed it on `main`,
  and the publish job builds the image again anyway.

The image push depends on these checks. Publishing a GitHub Release for a
`vX.Y.Z` tag is the release trigger; a tag push alone runs CI but does not
publish packages. CI first checks that the tag, Cargo package version, and Helm
`appVersion` agree. The release then publishes the image and the Helm chart. The
crate is not published to crates.io for now. The Python SDK package is
built and checked, but is not yet published by this workflow.

## Build time and caching

- Rust jobs use `Swatinem/rust-cache` with one cache per storage feature. Only pushes to
  `main`/`develop` save it; pull requests restore from `main`. The first `main` run after a
  `Cargo.lock` change is cold. Jobs save the cache even when they fail, so a retry starts warm.
- `duckdb` links the prebuilt `libduckdb` (`DUCKDB_DOWNLOAD_LIB=1`, see `.cargo/config.toml`)
  and must not be given the `bundled` feature, which compiles DuckDB from C++ (~20 min) and
  ignores that variable. The Docker runtime image ships `libduckdb.so` in `/usr/local/lib`.
- `cargo make check-<feature>` runs the tests and one `clippy --all-targets` pass; the build is
  a by-product of the test run.
- Tools come prebuilt (`taiki-e/install-action`, cargo-chef release binaries). No sqlx-cli is
  needed: every backend runs its own migrations (`create_or_migrate`) when the tests connect.
- All integration tests are one binary (`tests/integration/main.rs`, one module per former
  file), so the dependency tree is linked once. Filter by module:
  `cargo test --test integration jwt_auth::`.

## Local reproduction

Use `cargo make check-local-matrix` with Docker for the backend matrix. For the
container smoke, run a ClickHouse service and a SensApp image configured with
`SENSAPP_STORAGE_CONNECTION_STRING=clickhouse://...`; then run:

```bash
python3 tests/clickhouse_container_smoke.py
```

The script defaults to `http://127.0.0.1:3000` and accepts `--base-url` for
another endpoint. It uses only Python's standard library.
