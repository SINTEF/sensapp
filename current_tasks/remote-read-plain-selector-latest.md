# Remote read: answer a plain selector with one sample per step

## Goal

Prometheus graphing `my_metric` over 8 weeks at a coarse step got HTTP 400 `Query exceeds 100000 samples in total`
from `prometheus_remote_read` (ClickHouse, but the same on every backend). A plain selector (hints `func=""`,
`range_ms=0`) is answered with raw samples today, and the raw path is capped at 100,000 samples.

## Context

Prometheus evaluates an instant selector at each step `t` with the latest sample in `(t - lookback, t]`. Returning,
per step bucket `(t_k - step, t_k]`, the last sample **with its own timestamp** gives Prometheus the same answer as
the raw samples, with one sample per step. Plan: `~/.claude/plans/while-querying-a-lot-generic-island.md`.

## Steps

- [x] 1. `SENSAPP_HTTP_MAX_QUERY_SAMPLES` (default 100,000) replaces the three hard-coded sample constants; limit tests at the boundary with a low value.
- [x] 2-5. `Aggregation::Latest` (internal): value of the last sample of a bucket, stamped with its real timestamp, in every backend (common, SQLite, DuckDB, PostgreSQL, TimescaleDB, ClickHouse, BigQuery). Tested live on PostgreSQL, SQLite, DuckDB, TimescaleDB and ClickHouse (`selector_aggregated.rs`); BigQuery by its SQL unit test.
- [ ] 6. Hint routing: `range_ms == 0` and aligned grid answers with `Latest`; clearer error when the raw fallback exceeds the limit.
- [ ] 7. Live Prometheus harness cases, docs (`DATAMODEL.md`, `HTTP_LIMITS.md`, `CONFIGURATION.md`, `BACKENDS.md`), move to `done/`.

## Progress

Started 5 Oct 2026.
