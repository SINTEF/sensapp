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

- [ ] The binary uses `sensapp::...` paths, with no `mod` declarations of its own; `cargo build` and every
  feature combination still compile; the `#[allow(dead_code)]` made necessary by the double compilation
  are dropped where they are no longer needed.
- [ ] Unit tests run once (count before and after in the log below).
- [ ] cargo-make tasks removed, docs and CI comments updated, `cargo make` still lists the rest.
- [ ] Full suites, clippy on all features, `cargo build --release` for the default and all-storage builds.

## Progress

(none yet)
