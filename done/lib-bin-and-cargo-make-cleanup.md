# One crate tree, no dead cargo-make tasks

## Goal

- `src/main.rs` declares the same modules as `src/lib.rs` (`mod config; mod http; ...`), so everything is
  compiled twice, unit tests run twice, and dead-code warnings differ between the two. SensApp is not
  meant to be used as a library: make the binary a thin client of the library crate, without keeping
  two copies of the module tree.
- `setup-dev`, `migrate-postgres`, `migrate-sqlite`, `migrate-timescaledb` and `migrate-clickhouse`
  (cargo-make) are not used by CI: the backends migrate themselves at startup. Remove them, and the
  mentions in `CONTRIBUTING.md` and `.github/workflows/ci.yml` (commented block).

## Checklist

- [x] The binary uses `sensapp::...` paths, with no `mod` declarations of its own; `cargo build` and every
  feature combination still compile; the `#[allow(dead_code)]` made necessary by the double compilation
  are dropped where they are no longer needed.
- [x] Unit tests run once (count before and after in the log below).
- [x] cargo-make tasks removed, docs and CI comments updated, `cargo make` still lists the rest.
- [x] Full suites, clippy on all features, `cargo build --release` for the default and all-storage builds.

## Progress

Done 2 Oct 2026.

- `src/main.rs` imports from the `sensapp` library and declares no module. Unit tests: 235 + 232 before
  (library and binary), 235 once after. Smoke test of the binary: starts, `/health/ready`, a write,
  `generate-token`.
- 53 `#[allow(dead_code)]` removed. Method: turn all 54 into `#[expect(dead_code)]`, run `cargo check
  --all-targets` on the default build, each of the six backends alone and `--all-features`, keep an
  attribute only if it was not reported unfulfilled everywhere it is compiled. One is left
  (`deduplicate()` of SQLite, used by `current_tasks/sample-deduplication-in-vacuum.md`), and the two
  `cfg_attr(not(test), allow(dead_code))` of `http/auth.rs` went too. `clippy -D warnings` shows 0 on the
  eight configurations.
- cargo-make: `setup-dev`, the four `migrate-*` and the three `prepare-*` tasks removed (no `.sqlx`
  directory, no compile-time query macro, ClickHouse is not sqlx), with the mentions in `CONTRIBUTING.md`
  and the commented block of the CI workflow. The Makefile still parses, no dependency dangles
  (cargo-make itself is not installed on this machine, so the tasks were not run). The `build-*`,
  `ci-*` and `stop-local-test-stack` tasks that nothing references are developer entry points and stay.
