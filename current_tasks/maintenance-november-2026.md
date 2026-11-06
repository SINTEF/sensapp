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

- [x] Rust compatible updates, DuckDB 2.0 included, without code changes.
- [x] Rust majors: all eight, only sentry needed a change (`ClientOptions` is non-exhaustive, built
  with its builder now). `cargo outdated --root-deps-only` is empty.
- [x] Rust security audit: a new advisory, RUSTSEC-2026-0387 (crypto-bigint 0.5), came with the old
  RSA one through the `rust_crypto` backend of jsonwebtoken. SensApp only uses HS256, and aws-lc-rs is
  already compiled in for rustls, so jsonwebtoken now uses `aws_lc_rs`. `cargo audit` is clean without
  ignores, and the ignore is gone from `Makefile.toml`.
- [x] Frontend to latest, TypeScript 6.0 (see deferred), vite 8.3.6, jsdom 31. `minimatch` is
  overridden to 11 under the lint tooling. npm audit is clean. Lint (80 files), typecheck, 287 tests,
  and the build pass.
- [x] Python SDK on polars 2 and pyarrow 26 without code changes. 69 unit tests and the 2 live tests
  pass. The pytest-asyncio fixture loop scope is explicit, to silence its new warning.
- [x] Images and CI pins: ClickHouse 26.8 LTS, TimescaleDB 2.31.0 on pg18 (new volume name and
  mount path), `rust:1.99`, actions/checkout v7, setup-python v7, setup-uv v10, pre-commit/action 3.0.1.
- [x] Local matrix after all upgrades: default, SQLite, DuckDB, PostgreSQL, TimescaleDB, ClickHouse,
  RRDCached, all green. Live smoke test on SQLite: write, series catalog, `/ui`, Prometheus
  exposition, readiness, and the SDK live tests.
- [x] The 15 open Dependabot alerts on `main` are fixed on the branch. The critical one is `zip` 6,
  a build dependency of `libduckdb-sys` 1.10505 that DuckDB 2.0 no longer has. The others are undici
  (8.12.4 now), vite (8.3.6), and rustls (0.23.46). `cargo audit` did not report `zip` or rustls.
- [ ] GitHub CI on the branch. `astral-sh/setup-uv` has no floating `v10` tag, so it is pinned to v10.3.3.

## Deferred

- TypeScript 7: typescript-eslint 8.72 accepts `<6.1.0`. See `ideas/frontend-typescript-7.md`.
- Tailwind 4.3.5: published during the pass without its `@tailwindcss/oxide` package. The lock holds
  4.3.4, and the `^4.3.4` range takes 4.3.5 on the next refresh.
- `@types/node` stays on 24, to match the Node 24 runtime.
- npm overrides for `js-yaml` and `minimatch`: see `ideas/remove-frontend-npm-overrides.md`.
- `pre-commit autoupdate` wanted to move gitleaks from v8.30.1 to v8.30.0, a tag-ordering quirk,
  not applied.

## Notes

- The DuckDB doctest fails locally on macOS, on `main` too: the doctest binary does not find the
  downloaded `libduckdb.dylib`. Not a regression.
- ClickHouse 26.8 may no longer lose lightweight deletes like 24.8 did. The check-and-repeat in
  `src/storage/clickhouse/mod.rs` stays, it is cheap.
