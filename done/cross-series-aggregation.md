# Cross-Series Aggregation

## Goal

Answer "average temperature across a room" without a full PromQL engine: let `GET /api/v1/query` combine series at query time.

## Delivered

- `sum|avg|min|max|count [by (labels) | without (labels)] (selector)` in the simple PromQL endpoint, with an optional `step` bucket width (`src/http/simple_promql.rs`).
- A matrix selector is accepted inside an aggregation to choose the window (a documented deviation from strict PromQL).
- `src/storage/cross_series.rs`: groups the fetched raw samples by label subset and time bucket. `avg` is computed from sum and count over all samples, never as an average of per-series averages.
- Aggregations only consider numeric sensors. One output series per group, with a deterministic uuid.
- Everything else (`rate`, arithmetic, `topk`, `quantile`, nested aggregations) is still rejected with a clear error.

## Known Limits

- Raw samples were fetched, so the selector limits applied (256 series, 100k samples in total). Superseded: the aggregation is pushed down to the database, see `done/cross-series-aggregation-pushdown.md` and `docs/HTTP_LIMITS.md`.
- `first` and `last` are not defined across series and are not offered.

## Follow-Ups

- Done: `done/cross-series-aggregation-pushdown.md`.
- See `ideas/promql-rate-and-arithmetic.md` and `ideas/composite-sensors.md` for what was deliberately not built.

## Validation

- `cargo test --lib cross_series`, `cargo test --lib simple_promql`, `cargo test --test simple_promql`, `cargo test --test jwt_auth` (sensor allow lists only aggregate the readable series)
