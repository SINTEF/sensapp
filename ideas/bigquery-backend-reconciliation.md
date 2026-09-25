# Port the Local BigQuery Read Work

## Source to preserve

`/Users/antoinep/work/sensapp-vibe-prom-read` contains `storage-update` commit
`31d7a3d` and uncommitted changes in `src/storage/bigquery/`. It also contains
`tests/bigquery_integration.rs`. Do not reset or delete that checkout before the
port is complete.

## Why a focused port is needed

Current `main` uses paginated `list_series` and advanced query methods that the
old branch did not implement. BigQuery is currently marked experimental/deferred
in `TODO.md`, and the CI environment has no BigQuery credentials. Copying the
older module directly would replace newer interfaces and leave live behavior
unverified.

## Proposed work

1. Adapt the old matcher and typed sample queries to the current storage trait,
   pagination contract, and current `gcp-bigquery-client` APIs.
2. Review the local identifier-validation patch against real Google Cloud project
   and dataset naming rules before adopting it. Keep all dynamic values
   parameterized; validate identifiers used in SQL.
3. Review bounded cache expiry and parallel queries for correctness under
   concurrent writes, not just speed.
4. Port the old integration tests, add pagination and error-path cases, and run
   them against an isolated real BigQuery dataset. Keep compile checks in CI even
   when live credentials are unavailable.

This is separate from the first ClickHouse pre-production release gate.
