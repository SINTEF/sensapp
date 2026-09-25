# SensApp Pre-Production Release Plan

## Starting point

Use the clean `sensapp-sep-26` clone of upstream `main`. The older local
checkouts have been reviewed in `done/local-work-reconciliation-september-2026.md`;
none is a safe release base. The useful unfinished BigQuery work is recorded in
`ideas/bigquery-backend-reconciliation.md` and is outside the first ClickHouse
release.

ClickHouse already has migration, health, HTTP lifecycle, and query coverage.
The remaining release risk is operational: running the packaged image against a
persistent service, handling outages and restarts, and confirming that publishing
is repeatable.

The September 2026 audit found eight lockfile vulnerabilities. Compatible
updates resolved seven. The last is an optional `rkyv` edge listed by
`rust_decimal` but absent from every active SensApp feature graph; its precise
exception is documented in `Makefile.toml`. The audit still reports nonblocking
unmaintained/unsound/yanked dependency warnings, including `core2` and `spin`;
review those before choosing the release date.

## Acceptance criteria and estimate

| Step | Acceptance evidence | Estimate |
| --- | --- | ---: |
| 1. Run the revised CI on a branch/PR | Rust/backend matrix, frontend, Python SDK, audit, Helm, Docker, and live ClickHouse smoke all pass in GitHub Actions. Fix any runner-only failures. | 0.5–2 days |
| 2. Stage a ClickHouse deployment | Deploy the built image and chart with external persistent ClickHouse; document configuration, credentials, resource limits, and logs for the actual environment. | 1–2 days |
| 3. Exercise operations | Verify ingest, series listing, query, and export with representative data; restart SensApp; interrupt/recover ClickHouse; practice backup and restore on disposable data. Confirm readiness and data survival. | 1–2 days |
| 4. Prepare the first release | Review dependency audit findings, version and changelog; align Cargo and chart versions; publish the GitHub Release from a reviewed tag; verify the pushed image and crate, then repeat a smoke check using those artifacts. | 0.5–2 days |

**Planning range: 4–8 engineer-days** for this pre-production release, assuming
a usable staging environment and access to its operations. Calendar time may be
longer if infrastructure or approvals are unavailable. This estimate excludes
new features, full production security review, and BigQuery support.

## Release decision

Ship the ClickHouse-backed pre-production build when all four steps have
evidence. Treat PostgreSQL, SQLite, TimescaleDB, DuckDB, and RRDCached as tested
compatibility paths rather than equal deployment promises for this release.
Keep BigQuery experimental until its older read work is ported and tested with a
real isolated dataset.
