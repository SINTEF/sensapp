# Prometheus remote read: debug output, hints and ordering

Source: code review of 10 November 2026. Item 1 was confirmed by reading. Items 2 to 4 depend on Prometheus
behaviour recalled from memory: check each against the Prometheus source or the live harness
(`tests/prometheus_live`) before changing code.

## Problem

1. **`println!` on the request path**: 9 in `src/http/prometheus_read.rs` (`:323-571`) and 8 in
   `src/parsing/prometheus/stream_writer.rs` (`:28-133`), outside the test modules. Every request prints
   matcher values, label sets and a hex dump per chunk to stdout.
2. **`*_over_time` buckets are anchored on `query.start_timestamp_ms`, not `hints.start_ms`**
   (`prometheus_read.rs:131-142`). `Query.start_timestamp_ms` is believed to be the minimum over all
   selectors of the expression, while hints are per selector. For `max_over_time(x[2m]) > y` at step 2m the
   buckets of `x` would start 3 minutes before the first evaluation and be misaligned by one minute.
   `done/remote-read-hints-semantics.md` holds for single-selector queries only.
3. **The `Latest` path is taken for an aggregation inside a subquery** (`prometheus_read.rs:54-70`, `:88-93`).
   For `max_over_time(sum(x)[1h:5m])` the hint is believed to be `func = "sum"`, `range_ms = 0`,
   `step_ms = 5m`, and subquery evaluation times are aligned to absolute multiples of the interval, not to
   `hints.end_ms`. The grid check passes for every instant query, and SensApp returns samples on the wrong
   grid.
4. **Chunked responses are not sorted by label set** (`src/storage/postgresql/matchers.rs:159`,
   `ORDER BY s.sensor_id`). Prometheus is believed to sort only SAMPLES responses itself; with
   `read_recent: true` and a local TSDB the merge can produce duplicate series.
5. **An encode error drops a series silently** (`prometheus_read.rs:533-543`).
6. **Unchecked arithmetic on hint fields** (`prometheus_read.rs:92,98-99`): wraps in release for a crafted
   request.
7. Notes: one frame per series (Prometheus caps a frame at 50 MB, relevant only if
   `SENSAPP_HTTP_MAX_QUERY_SAMPLES` is raised a lot); the comment at
   `src/parsing/prometheus/converter.rs:58` is wrong (uppercase sorts before `_`), the behaviour is right;
   `test_calculate_crc32` only asserts non-zero, pin `crc32c("123456789") == 0xE3069283`.

## Suggested solution

1. Replace the `println!` calls with `tracing::debug!` (or remove the hex dumps). Add
   `#![deny(clippy::print_stdout)]` for the library crate, allowing it in `main.rs` and the CLI.
2. Anchor the buckets on `hints.start_ms`, or fall back to raw samples when
   `hints.start_ms != query.start_timestamp_ms`. The fallback is the safe choice.
3. Additionally require `hints.end_ms % hints.step_ms == 0` only when that is what Prometheus does for
   subqueries; otherwise fall back to raw samples when `func` is an aggregation operator and not a function.
   Returning raw samples is always correct, so when in doubt, fall back.
4. Sort the series by label set (byte order of the sorted label pairs) before streaming.
5. Fail the request on an encode error.
6. `checked_*` arithmetic, falling back to raw samples.

## Tests

Add to the live harness: a two-selector query (`max_over_time(x[2m]) > y`), a subquery with an inner
aggregation, both compared with the same query on a Prometheus that has the data locally. Unit tests for
the hint decisions.

## Done when

The live tests pass and `done/remote-read-hints-semantics.md` is amended with what was learnt.

## Outcome (11 November 2026)

Items 2 and 3 were checked in the Prometheus 3.13.4 source before any change, and every item had a failing
test first. Details of the Prometheus behaviour: [remote-read-hints-semantics.md](remote-read-hints-semantics.md).

Fixed:

1. The 17 `println!` calls are gone (the useful ones were already `debug!`), the per-request `info!` logs
   are `debug!`, and `src/lib.rs` has `#![cfg_attr(not(test), deny(clippy::print_stdout))]`. The binary
   (`main.rs`, which holds the CLI) is a separate crate and keeps its prints.
2. Confirmed: the query bounds are those of the whole expression. The buckets now start at
   `hints.start_ms` and stop at `min(hints.end_ms, query.end_timestamp_ms)`. Anchoring on the hints is
   what the single-selector analysis already relied on, so it was chosen over the raw fallback.
3. Confirmed, and wider than stated: a subquery of a `*_over_time` (`max_over_time(max_over_time(x[15m])[1h:15m])`)
   was wrong too. `*_over_time` buckets now need the first evaluation on a multiple of the step, and the
   latest samples answer the grid of the end and the grid of the step when they differ (two reads,
   merged per series). Cost: the Prometheus UI, which does not align its queries, gets raw samples for
   `*_over_time`.
4. Both response types sort the series of a query by label set (label by label, name then value, byte
   order). Integration test with an uppercase label name, on both response types.
5. An encode error answers 500 instead of dropping the series.
6. `checked_*` arithmetic on the hints; out-of-range hints get the raw samples (unit test with
   `i64::MIN`/`i64::MAX`, which panicked before).
7. The comment of `build_prometheus_labels` is right; `test_calculate_crc32` pins
   `crc32c("123456789") == 0xE3069283`. One frame per series is kept.

Tests: 4 unit tests of the hint decisions, 2 backend-generic integration tests (both grids, label set
order). Two existing integration tests sent hints Prometheus never sends (a range starting on a multiple
of the step instead of 1 ms after it); they now send the real ones.

Suites: SQLite full (366 integration), PostgreSQL full (367), the 18 remote read tests on TimescaleDB,
DuckDB and ClickHouse, the live harness against Prometheus 3.13.4 (passes; the previous commit fails 3
of the 7 new queries), `cargo clippy` and `cargo clippy --all-features --tests` clean.
