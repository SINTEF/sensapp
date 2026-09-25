# CI Release Readiness

## Goal

Make the existing CI pipeline verify the current release candidate against real storage services and a running container, with a repeatable release gate.

## Scope

- Reconcile the older local checkouts against upstream `main` before carrying changes forward.
- Run a scheduled validation pass so toolchain and dependency drift is visible.
- Make the security audit an actual gate and validate the runtime container against ClickHouse.
- Require the relevant checks before publishing a release.

## Progress

- [x] Record decisions for each older checkout and preserve deferred BigQuery work.
- [x] Update CI and add a container smoke test.
- [x] Validate workflow syntax and run available local checks.
- [x] Resolve the compatible audit findings and document the unused optional exception.
- [ ] Run the revised workflow in GitHub Actions and resolve any runner-only failures.
