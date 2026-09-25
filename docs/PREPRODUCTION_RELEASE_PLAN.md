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

The September 2026 audit found eight lockfile vulnerabilities. A subsequent
compatible dependency refresh removed the optional `rkyv` edge and `core2` from
the lockfile. The RSA advisory exception remains documented in `Makefile.toml`
because SensApp uses HS256 JWTs while `jsonwebtoken` still brings in RSA. The
blocking CI audit must confirm the refreshed lockfile before release; review
remaining nonblocking unmaintained/unsound/yanked warnings, including `spin`.

The frontend dependency refresh and OpenAPI generator upgrade reduced npm audit
findings from 22 to zero. The generated client passes lint, typecheck, tests,
build, and a live API compatibility check. CI now blocks on high npm
advisories. The nested `js-yaml` override can be removed when the generator's
parser accepts a patched version itself.

## Acceptance criteria and estimate

| Step | Acceptance evidence | Estimate |
| --- | --- | ---: |
| 1. Run the revised CI on a branch/PR | Rust/backend matrix, frontend, Python SDK, audit, Helm, Docker, and live ClickHouse smoke all pass in GitHub Actions. Fix any runner-only failures. | 0.5–2 days |
| 2. Stage a ClickHouse deployment | Deploy the built image and chart with external persistent ClickHouse; document configuration, credentials, resource limits, and logs for the actual environment. | 1–2 days |
| 3. Exercise operations | Verify ingest, series listing, query, and export with representative data; restart SensApp; interrupt/recover ClickHouse; practice backup and restore on disposable data. Confirm readiness and data survival. | 1–2 days |
| 4. Prepare the first release | Review dependency audit findings, version and changelog; align Cargo and chart versions; publish the GitHub Release from a reviewed tag; verify the pushed image and crate, then repeat a smoke check using those artifacts. | 0.5–2 days |
| 5. Clear frontend security findings | Upgrade the OpenAPI generator, regenerate its client, verify API compatibility, and make the high-severity npm audit a release gate if the frontend ships with this release. | 1–3 days |

**Planning range: 5–11 engineer-days** for a frontend-inclusive pre-production
release, assuming
a usable staging environment and access to its operations. Calendar time may be
longer if infrastructure or approvals are unavailable. This estimate excludes
new features, full production security review, and BigQuery support. A
backend-only release could defer step 5.

## Release decision

Ship the ClickHouse-backed pre-production build when all four steps have
evidence. Treat PostgreSQL, SQLite, TimescaleDB, DuckDB, and RRDCached as tested
compatibility paths rather than equal deployment promises for this release.
Keep BigQuery experimental until its older read work is ported and tested with a
real isolated dataset.
