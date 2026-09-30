# Cross-Series Aggregation Pushdown

## Status

Nice to have. Cross-series aggregation in `/api/v1/query` (see `done/cross-series-aggregation.md`) fetches raw samples, so it is bound by the selector limits (256 series, 100,000 samples in total), documented in `docs/HTTP_LIMITS.md`.

## Idea

Have each series compute per-bucket `sum` and `count` in the database with `query_sensor_data_advanced` (`step` + `Aggregation::Sum` / `Count`), then merge those buckets across series in Rust. `avg` is total sum over total count, `min` and `max` merge directly. All series already share the window start as bucket origin, so the buckets line up. The limit would then apply to the number of buckets instead of raw samples.

## Revisit If

Someone needs a long window over high-frequency sensors and hits the sample limit.
