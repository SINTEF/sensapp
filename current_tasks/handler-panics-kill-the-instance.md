# Request-reachable panics, and the exit-on-panic hook

Source: code review of 10 November 2026. Items marked (read) were confirmed by reading; none was executed.

## Problem

`src/main.rs:94-98` installs a panic hook that calls `std::process::exit(1)`. Any panic in a request handler
therefore stops the whole instance, and a retrying client can take down every instance in turn. These inputs
panic today:

| Input | Where | Status |
| --- | --- | --- |
| `GET /series/{uuid}/availability?...&step=0s` with no sample in the window | `div_euclid(0)` at `src/http/crud.rs:1381` (PostgreSQL returns a 500 instead when there are samples) | read |
| CSV sensor name or header containing 0x0B or 0x1C-0x1F | `.unwrap()` at `src/importers/csv.rs:134` and `:156` | read |
| Wide CSV with the same header twice and different inferred types (`time,a,a` / `...,1,1.5`) | `unreachable!("Sensor type mismatch")` in `convert_samples_to_typed_samples`, `csv.rs:314-383` | reported, check first |
| Arrow `Decimal128` with scale > 28, a negative scale, or a value over 96 bits | `Decimal::from_i128_with_scale` at `src/importers/arrow.rs:228` | read |
| ClickHouse Numeric read or `sum` over 96 bits | `decimal_from_clickhouse_raw`, `src/storage/clickhouse/clickhouse_utilities.rs:36-38` | read |
| SenML `[{"n":"x","t":-1e13,"v":1}]` | chrono `Duration::seconds` / `DateTime + TimeDelta` inside `sindit-senml` (`time.rs:60`), called from `src/importers/senml.rs:17` | reported, check first |
| Three-way race: sensor created by B, deleted by C, during A's registration | map index `sensor_ids[&uuid]` at `src/storage/postgresql/mod.rs:1118`, `src/storage/timescaledb/mod.rs:1610` | reported |
| DuckDB label re-read not returning the key | `label_names[name.as_str()]`, `src/storage/duckdb/duckdb_registration.rs:107-121` | reported |
| In-memory Numeric sum overflow | `sum += value`, `src/storage/common.rs:559` | reported |

## Suggested solution

1. Fix each site: return an error instead of panicking.
   - Availability: reject `step <= 0` with a 400 in the handler; use `checked_sub` for `end_us - start_us`.
   - CSV: `?` instead of `unwrap()`; reject duplicate header names with a 400.
   - Decimals: `Decimal::try_from_i128_with_scale`, a 400 on import and a `StorageError` on read.
   - SenML: validate the time range before it reaches chrono (the parse call itself may panic, so check the
     raw JSON numbers or patch `sindit-senml`).
   - Map indexes: `.get(..).context(..)`. On PostgreSQL the missing id should be retryable like the
     foreign-key violation.
   - Decimal sum: `checked_add` and the existing overflow error.
2. Contain what is left: add `tower_http::catch_panic::CatchPanicLayer` so a handler panic is a logged 500,
   and decide whether the exit-on-panic hook stays. Suggested: keep the hook's log line, drop the `exit(1)`
   for panics caught by the layer (panics in background tasks can still exit). Note that a stack overflow is
   an abort and is not contained by this; see `request-input-limits.md`.

## Tests

One test per row above, through the real router (`tests/integration/real_router.rs` style), asserting a 4xx
or 5xx answer and that a second request still works.

## Done when

Every row has a test, and a grep for `unwrap()`, `expect(`, `unreachable!` and map indexing in `src/importers`,
`src/http` and `src/infer` shows only sites that request input cannot reach.
