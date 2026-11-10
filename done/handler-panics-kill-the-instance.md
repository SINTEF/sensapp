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

## Outcome (10 November 2026)

Every row was checked first. Seven panicked as reported in a test; the PostgreSQL race is confirmed by reading only, and the DuckDB row was not reachable. The tests are in
`tests/integration/request_panics.rs` (through the real router, on the backend of `TEST_DATABASE_URL`) and in
the unit tests named below.

| Row | Result | Commit |
| --- | --- | --- |
| Availability `step=0s` | Confirmed. `parse_duration_millis` refuses a zero step (400); `end - start` uses `checked_sub`. Test `a_zero_step_is_refused` (availability and data). | `9f39490` |
| CSV name with 0x0B / 0x1C-0x1F | Confirmed, long and wide formats. `series_entry` returns the error. Test `a_csv_name_with_a_control_character_is_refused`. | `0ad1dd0` |
| Wide CSV, same header twice | Confirmed (`unreachable!`). Duplicate headers are refused before reading the rows; the type mismatch in `convert_samples_to_typed_samples` is an error too, no longer `unreachable!`. Test `a_wide_csv_with_the_same_header_twice_is_refused`. | `0ad1dd0` |
| Arrow `Decimal128` | Confirmed (scale > 28, > 96 bits, negative scale cast to a huge `u32`). `arrow_decimal` refuses what `Decimal` cannot hold, and now understands a negative scale (12e2 → 1200). Unit test `test_arrow_decimals_outside_the_numeric_range_are_refused`. | `96ee4a1` |
| ClickHouse decimals | Confirmed on read, and **a worse bug found on write**: `decimal_to_clickhouse_raw` called `rescale(8)`, which silently keeps a smaller scale when the mantissa would pass 96 bits. 10^25 was stored as 10^20. It is now refused (400, `InvalidDataFormat`); a read or a `sum` over 96 bits is a `StorageError`. Unit test `decimals_outside_the_clickhouse_range_are_errors`. | `3173ae4` |
| SenML `t = -1e13` | Confirmed (`DateTime + TimeDelta overflowed` in `sindit-senml` 0.3.0). `SenMLImporter::check_times` refuses a `t` or `bt` below -10^11 s before the parser; the raw JSON is now parsed once for this and for the exported names. Large positive times were already refused by the parser. Test `a_senml_time_out_of_range_is_refused`. The fix in the crate is `ideas/sindit-senml-checked-relative-time.md`. | `0f64eea` |
| PostgreSQL / TimescaleDB three-way race | Confirmed by reading: a sensor created by B and deleted by C before A's re-read had no id. `register_sensors` now returns `SensorDeletedDuringWrite`, and both backends retry it like the foreign-key violation (`sensor_deleted_during_write`). The publish loops use `.get(..).context(..)`. **No test**: the window is between two statements of one function and cannot be hit without a hook in the code. | `b8be0fe` |
| DuckDB label re-read | **Not reachable** as far as I can tell: the connection is behind a mutex, the dictionaries are only cleared by the test cleanup, and a NUL character in a label round-trips (tried, then the probe was removed). Replaced the map index with an error anyway. | `cec0c27` |
| In-memory Numeric sum | Confirmed (`Addition overflowed`) in `aggregate_numeric_samples`, used by DuckDB, BigQuery, RRDcached and the fallbacks. The sum is checked; Sum and Avg fail, Min/Max/Count still work. Unit test `a_numeric_sum_over_the_decimal_range_is_an_error`; the HTTP test `a_numeric_sum_over_the_decimal_range_is_refused` checks every backend answers an error (SQLite and PostgreSQL aggregate in SQL and already failed to decode the result, ClickHouse refuses the values). | `11828d4` |

### Catching what is left, and the exit hook

`CatchPanicLayer` sits inside the metrics and trace layers: a handler panic answers
`500 {"error":"Internal Server Error"}` with the request id, is logged with the panic message in the request span,
and the message is not sent to the client. Test `real_router::a_panic_in_a_handler_is_a_500_and_the_server_keeps_answering`
(the test storage panics in `list_metrics`); it fails without the layer.

**Decision: the `exit(1)` hook is gone**, not kept for background tasks. A panic hook runs before the unwinding,
so with `exit(1)` the layer never gets a turn. There is no long-lived background task in the production code:
the `tokio::spawn` calls (PostgreSQL batch queries, ClickHouse publishers) are part of a request and awaited, so a
panic there becomes a `JoinError` and an error answer. No lock can be poisoned: DuckDB uses a tokio mutex and the
BigQuery cache recovers from poisoning. The default hook still prints the panic.

### Payload errors

CSV parse errors (`parse_csv_data_grid`) now answer 400, so the CSV tests can tell a refusal from a crash. The rest
of `payload-errors-answer-400.md` (task 11) is left there; it says what was done.

### Grep check

`unwrap()`, `expect(`, `unreachable!`, `panic!` outside the test modules of `src/importers`, `src/http`, `src/infer`:

- `src/http/crud.rs` availability: `expect` on start and end, which `parse_optional_time_bounds` returns as given.
- `src/http/server.rs`: the CTRL+C handler at startup.
- `src/importers/senml.rs`: `first().unwrap()` on a list checked non-empty just above.
- `src/infer/geo_guesser.rs`: two constant regexes.
- `src/infer/columns.rs` and `is_header.rs`: inference cannot fail, `parse_string` accepts anything. The
  `unreachable!` in the numeric branches is reachable with `infer_column(.., numeric = true)` and a column mixing
  `1` and `1.5`, but no request path uses `numeric = true` (CSV passes `false`). Latent, left as is.

The map indexes found in the storage modules are covered above. Request-level stack overflows (deep JSON) are
aborts, not panics: `request-input-limits.md`.

### Suites run (10 November 2026, after `8209941`)

Full suites, all green: SQLite (331 integration tests, 28 s), PostgreSQL (332, 42 s), DuckDB (328, 61 s; the
doctest binary cannot load `libduckdb.dylib` on macOS, environmental), ClickHouse (344, 204 s), TimescaleDB (336,
174 s). `cargo clippy --all-features --tests` and per backend feature: clean. RRDcached and BigQuery code did not
change and were left to CI.
