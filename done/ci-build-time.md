# CI build time and cache thrash

## Goal
Stop CI from spending its time recompiling. Diagnosis (Sep 2026, from `gh` run data):

- `test-matrix (duckdb)` and both Docker builds took 25+ min because `duckdb` had the
  `bundled` feature, which compiles DuckDB from C++ and silently ignores `DUCKDB_DOWNLOAD_LIB`.
- `moonrepo/setup-rust` saved ~9 GiB per feature per branch (quota is 10 GB), so caches,
  including the tiny cargo-make one, were evicted constantly. Failed jobs never saved.
- `check-<feature>` ran build + test + clippy + clippy --tests (four compile passes).
- cargo-make, cargo-audit, sqlx-cli and cargo-chef were compiled from source.

## Done
- `duckdb`: dropped `bundled`; prebuilt libduckdb is downloaded and shipped in the Docker runtime image.
- `Swatinem/rust-cache` (per-feature key, PRs restore only, `cache-on-failure`), `CARGO_INCREMENTAL=0`,
  slimmer dev debuginfo.
- `check-*` = `test-*` + `clippy --all-targets`.
- Prebuilt cargo-make / cargo-audit (`taiki-e/install-action`), prebuilt cargo-chef,
  fmt job no longer needs cargo-make.
- sqlx-cli and the `migrate-*` CI steps removed (backends self-migrate; verified from an empty
  SQLite, PostgreSQL and TimescaleDB).
- The 19 `tests/*.rs` are one `tests/integration` binary (231 tests pass on PostgreSQL and
  TimescaleDB, 230 on SQLite). RRDCached runs as `--test integration rrdcached_integration::`.
- `docker-smoke` skipped on release events (publish/release jobs tolerate the skipped need).
- Duplicate `python-sdk-check.yaml` removed; wheel smoke test folded into the `python-sdk` job.
  The live SDK tests still run against the runtime image in `docker-smoke`; the extra
  SQLite-backed live run is gone.
- Do not add `[profile.dev.package."*"] debug = false`: on macOS it makes the `sqlx_macros`
  proc-macro dylib fail to load ("mis-aligned LINKEDIT").

## To verify on GitHub
- duckdb job and docker smoke duration on a cold and a warm run.
- `gh cache list`: entry sizes and total (was ~9 GiB each).
- The first run on `main` after merge seeds the caches; PR runs before that start cold.

## Follow-ups
See `ideas/ci-followups.md`.
