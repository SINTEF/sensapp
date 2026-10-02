# Prometheus remote read with `step` hints in bulk

## Goal

`query_sensor_data_for_prometheus` (`src/http/prometheus_read.rs`) has a path for read requests that carry
hints with a `step`: it discovers the sensors (reading one sample each and throwing it away), then calls
`query_sensor_data_advanced` sequentially for each. Make it cost a handful of queries on the backends
that can, with the same results and the same limits.

## Design

Same pattern as `query_selector` (`done/bulk-selector-reads.md`): a storage method with a portable default
(today's loop), a bulk implementation for ClickHouse, PostgreSQL, TimescaleDB and SQLite: sensors in one
query, then per numeric type one aggregated query (`GROUP BY sensor_id, bucket`) with the shared budget.

## Done when

- [ ] Characterise today's behaviour first (tests of the hints path against the loop, all aggregations,
  steps, windows, empty buckets).
- [ ] Trait method, generic orchestration, backends; results identical to the loop.
- [ ] Measured with 100 and 1000 series (before/after below).
- [ ] Full suites, clippy; the idea moved to done.

## Progress

(none yet)
