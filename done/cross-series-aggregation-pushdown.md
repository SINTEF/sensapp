# Cross-Series Aggregation Pushdown

## Goal

`avg by (room) (temperature[24h])` over a few hundred series was impossible on every backend, although its output is tiny: the aggregation fetched the raw samples, so it was bound by the selector limits (256 series, 100 000 samples in total), and a larger `step` did not help. Raised by the review of the `storage-hardening-and-bulk-io` branch (see `done/pr-44-review-corrections.md`, item M6).

## Delivered

- `read_cross_series` in `src/storage/cross_series.rs` asks the storage for per-series buckets with `query_selector_aggregated` (the bulk aggregated reader that Prometheus remote read hints already use) and merges them in Rust: `sum`, `count`, `min` and `max` merge directly, `avg` reads `count` and `sum` at the same time. All the buckets start at the start of the window; without a `step` the whole window is one bucket.
- `/api/v1/query` uses it for every aggregation. Plain selectors keep the raw read and its limits.
- Limits of an aggregation: 10 000 series (`MAX_AGGREGATED_SELECTOR_SERIES`, from the memory held by sensors and labels, not from sample volume) and 100 000 buckets in total. A larger `step` now helps, since it makes fewer buckets.
- `aggregate_across_series` (the raw implementation) stays as the reference: `tests/integration/cross_series_reads.rs` compares the two on every backend for each aggregation, grouping, step and window (including a window that starts between samples), with decimal, integer and float series in one group, and probes the series and bucket limits at exactly N and N-1 with 300 series.
- The test found a bug in ClickHouse: a windowed aggregated read also counted the samples outside of the window (the bucket was selected under the name of the time column, and ClickHouse resolves aliases in `WHERE`). Fixed in its own commit, with `tests/integration/aggregated_windows.rs`.

## Measured

TimescaleDB, release builds, same data, before (raw read) and after:

| Query | before | after |
|---|---|---|
| `sum by (room)`, 10 series x 20 samples, in a database of 400 000 rows | 27.7 ms | 28.5 ms |
| `avg by (room)`, same | 27.7 ms | about 41 ms |
| `sum by (room)`, 100 series x 500 samples (50 000) | 88 ms | 54 ms |
| `avg by (room)`, 3 000 series x 100 samples (300 000) | `400`, over the limits | 0.2 to 1.7 s (noisy on this machine: the database scans 300 000 rows) |

`avg` costs more on a small query because it reads the window twice (count and sum). Run at the same time, the two reads cost 13 ms more than the old single read on that database, instead of 26 ms one after the other.

## Known differences

- A series without any sample in the window is left out of the name of the output series (`avg(a)` and not `avg` when a second series of the group has no data in the window). The values are the same.
- `avg` reads `count` and `sum` separately: a sample written between the two can make a bucket slightly off, and a bucket seen by only one of them is left out.
- The sequential fallback (RRDCached, and reads of a token with a sensor allow list) reads series one at a time and aggregates each in the same way, so it gets the same limits but not the speed.
