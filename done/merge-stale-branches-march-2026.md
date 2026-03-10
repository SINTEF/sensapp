# Merge Stale Remote Branches (March 2026)

## Summary

Analyzed and resolved three stale remote branches: `rrdcached`, `vibe-coding-2`, and `sprint-antoine`.

## Branch: rrdcached (MERGED)

**2 commits ahead of main, 0 behind.**

Changes merged:
- Complete rrdcached storage backend implementation (`src/storage/rrdcached/mod.rs`)
  - TCP and Unix socket client abstraction
  - Create, batch insert, flush operations
  - List series via `LIST` command
  - Basic fetch support (WIP)
- RRDCached documentation (`docs/RRDCACHED.md`)
- Integration test framework (`tests/rrdcached_integration.rs`)
- Feature-gated behind `rrdcached` cargo feature

Conflicts resolved: `Cargo.toml` and `Cargo.lock` (dependency versions).

## Branch: vibe-coding-2 (MERGED)

**10 commits ahead, 5 behind main.**

Changes merged:
- **Directory restructure**: `src/ingestors/http/` → `src/http/` (simplification)
- **CRUD DCAT API**: New comprehensive CRUD endpoints with format negotiation (SenML, CSV, JSONL, Arrow)
- **Pagination**: `ListSeriesResult` with cursor-based pagination (`list_series` now supports `limit` + `bookmark`)
- **Storage trait update**: All backends updated for new `list_series` signature
- **PostgreSQL improvements**: Labels fetched via catalog view (JOIN in SQL) instead of N+1 queries
- **PromQL label selectors**: Parsing `{env="prod",region=~"us.*"}` style selectors
- **Metrics endpoints**: Summary views for sensor metadata
- **CRUD DCAT API tests** (`tests/crud_dcat_api.rs`)

Conflicts resolved: 17 files including storage backends, HTTP routes, Cargo.toml.

## Branch: sprint-antoine (NOT MERGED — too divergent)

**10 commits ahead, 61 behind main.**

This branch represents an older development path that was superseded by main's more recent work. Merging would have:
- Deleted ALL 17 test files
- Deleted all exporters (arrow, csv, jsonl, senml)
- Deleted the modern Prometheus read/write implementation
- Reverted the `src/http` rename back to `src/ingestors/http`
- Removed query.rs matchers, clickhouse support, and recent migration files

**Valuable ideas extracted** (moved to `ideas/`):
- MQTT ingestor integration
- OPC-UA ingestor
- Event bus system
- Geobuf parsing support

## Verification

- `cargo check` — passes (1 warning: unused function `publish_prometheus`)
- `cargo clippy --tests` — clean (same 1 warning)
- `cargo test` — 169 passed, 3 failed (PostgreSQL connection timeouts, expected without a running database)
