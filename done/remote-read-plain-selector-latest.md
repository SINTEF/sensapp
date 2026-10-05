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
- [x] 6. Hint routing: `range_ms == 0` and aligned grid answers with `Latest`; clearer error when the raw fallback exceeds the limit.
- [x] 7. Live Prometheus harness cases, docs (`DATAMODEL.md`, `HTTP_LIMITS.md`, `CONFIGURATION.md`), moved to `done/`.

## Progress

Done 5 Oct 2026.

## Results

- Prometheus v3.8.0, v3.13.4 and v3.15.0 against SensApp on PostgreSQL (`tests/prometheus_live/run.sh`): the 22 hint
  queries give the answer of the raw samples (a plain selector at a 1 h and a 15 min step, `sum/avg/min/max/count` across
  two series, a subquery that stays on raw samples), and a plain selector over a 120,000-sample series (one sample a
  minute, 83 days; over the limit of the raw read) at a one hour step, evaluated on the hour and on the half hour, gives
  the raw answer. A series with samples only between minutes 10 and 19 of each hour keeps its gaps.
- Backend-generic tests (`selector_aggregated`, `prometheus_remote_read_integration`) pass on PostgreSQL, SQLite, DuckDB,
  TimescaleDB 2.30 and ClickHouse 24.8. BigQuery: the SQL is unit tested, not run against BigQuery.

## Limits

- The steps are anchored on the end of the hints and checked against the start with the default 5 minute lookback; another
  `--query.lookback-delta` with an unaligned range falls back to the raw samples (rejected over the limit).
- `rate`, `increase`, subqueries and `count_over_time` still need the raw samples.

