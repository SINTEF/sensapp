# CI Release Readiness (September 2026)

The older local checkouts were reviewed against upstream `main`, with unfinished
BigQuery work preserved as a separate follow-up. CI now runs for repository
activity or explicit manual dispatch. It gates releases on Rust, frontend,
Python SDK, security audit, Helm, backend service tests, and a ClickHouse-backed
runtime image smoke test.

The revised workflow passed in GitHub Actions on PR #31 and that PR was merged.
The optional `rkyv` audit exception documented during that work was removed in
the subsequent dependency refresh after `rkyv` left the lockfile.
