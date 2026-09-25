# Local Work Reconciliation (September 2026)

## Baseline

The clean `sensapp-sep-26` checkout starts at upstream `main` commit
`667901899828432924986ca1d169c46ebb304194` (14 April 2026). It is a
shallow clone, so comparisons below use the older worktrees and current file
contents rather than ancestry counts against this checkout.

## Decisions

| Local folder | State and decision |
| --- | --- |
| `sensapp` | `sprint-antoine` at `bdf58f2`, with untracked experiments and databases. Upstream's `done/merge-stale-branches-march-2026.md` examined the branch and explicitly rejected its divergent architecture. No code copied. Preserve the folder. |
| `sensapp-fuck` | Scratch Rust project, not a Git repository. No SensApp changes to carry. |
| `sensapp-vibe-1` | `vibe-coding-2` at `bc8b822` with an unfinished merge and 16 conflicted files. The branch's useful pagination and query work was subsequently merged into upstream by PR #30. No unresolved merge state copied. Preserve the folder until its staged changes have been separately reviewed if needed. |
| `sensapp-vibe-rrdcached` | Clean `rrdcached` checkout at `3236511`. Upstream PR #30 merged this backend and extended its tests. No code copied. |
| `sensapp-vibe-prom-read` | `storage-update` at `31d7a3d`, seven commits after its December 2025 main, plus uncommitted BigQuery changes and two stashes. Reviewed by backend below. Preserve the folder, including its untracked database files; never copy those into the release checkout. |

## `storage-update` backend review

- **ClickHouse, DuckDB, TimescaleDB, RRDCached:** upstream has continued these
  implementations, changed the `StorageInstance` contract, and added modern
  integration and HTTP lifecycle coverage. The old modules and migrations cannot
  be taken as a whole without regressing current behavior. The release path uses
  upstream code and its service-backed tests.
- **BigQuery:** this is the important unfinished carry-forward candidate. The
  old branch implements matcher queries and sample reads, and has
  `tests/bigquery_integration.rs`; current upstream still has explicit TODOs for
  pagination, metric filtering, and label queries, and returns metadata without
  samples for UUID reads. The local uncommitted changes add bounded/expiring
  caches, identifier validation, and parallel sample queries. The old code uses
  the pre-pagination storage trait and an older BigQuery client; it needs a
  deliberate port with real-service tests. See `ideas/bigquery-backend-reconciliation.md`.
- **Old TODO files:** `TODO_EXPORTERS.md` and `TODO_PROMQL.md` are planning
  notes. Current upstream implements multi-format query responses and has
  `tests/simple_promql.rs`; they are not code patches to import.

## Result

No old branch was merged wholesale. The release checkout remains based on
upstream `main`, and BigQuery work is retained as a scoped follow-up. CI now
validates the current supported backends and runtime behavior so future drift is
caught automatically.
