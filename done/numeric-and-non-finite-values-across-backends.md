# Numeric and non-finite values: same results on every backend

Source: code review of 10 November 2026. Items 1, 2 and 5 were confirmed by reading; the others were reported
and depend on database formatting that was not checked.

## Problem

1. **SQLite `MIN`/`MAX` on Numeric compare text** (`src/storage/sqlite/storage_query_helpers.rs:94-103`).
   `numeric_values.value` is `TEXT`. The minimum of 9 and 10 is "10"; negatives are wrong too. `AVG`/`SUM`
   convert to REAL and lose digits past about 15, and a large result printed as `1.0e+20` fails
   `Decimal::from_str` with a 500 (`storage.rs:1657`, `selector.rs:258`).
2. **Arrow export of Numeric** (`src/exporters/arrow/mod.rs:236-238`, `:449`):
   `mantissa * 10^(18 - scale)` with a saturating exponent. For scale > 18 the raw mantissa is written as if
   its scale were 18 (off by `10^(scale-18)`), and the multiply wraps for |value| above about 1.7e20.
3. **DuckDB Numeric** (`src/storage/duckdb/migrations/20240223133248_init.sql:68`, `mod.rs:1529`):
   `DECIMAL(38,9)` rounds or rejects more than 9 decimals, and `CAST(AVG(value) AS VARCHAR)` is a DOUBLE that
   can print with an exponent and fail to parse.
4. **PostgreSQL aggregates**: `AVG`/`SUM` over `numeric_values` can exceed 28 decimals or 96 bits and fail to
   decode into `rust_decimal` (500); `SUM(value)::bigint` overflow is a 500.
5. **SQLite drops NaN and ±Infinity floats** with a success answer
   (`src/storage/sqlite/sqlite_publishers.rs:114-119`). Only NaN becomes NULL in SQLite; infinities are
   storable. `publish_location_values` (`:203-230`) has no filter, so a NaN coordinate fails the batch.
6. **NaN and Infinity become JSON `null`** in the SenML and JSONL exports and `/last` (invalid SenML, value
   lost). Prometheus stale markers are NaN.
7. **`simplify` returns 500 when the first or last sample is NaN** (`src/storage/common.rs:326-330`: the
   mapping back uses `==`).
8. **NaN in min/max differs**: Rust `f64::min/max` ignore NaN, PostgreSQL treats NaN as the greatest value;
   an all-NaN bucket in the cross-series merge yields ±Infinity (`src/storage/cross_series.rs:89-118`).
9. **SQLite Numeric duplicates**: "1.0" and "1.00" are different for deduplication; parsing uses
   `from_str` in one path and `from_str_exact` in two others.

## Suggested solution

Decide the rules once, write them in `docs/DATAMODEL.md`, and test them generically:

- **Numeric aggregation on SQLite**: aggregate in Rust with `rust_decimal` (fetch the bucket rows and reuse
  `aggregate_numeric_samples` from `common.rs`). Exact and simple; Numeric series are rare.
  The same fallback can serve DuckDB `AVG` and bound the PostgreSQL result: round the SQL result to 28
  decimals (`ROUND(AVG(value), 28)`) and return the overflow error when it does not fit.
- **Arrow export**: rescale with `Decimal::round_dp(18)` first, then `checked_mul`, and return an error (or
  null) when it does not fit precision 38.
- **Non-finite floats**: store them where the backend can, and reject with a 400 where it cannot, instead of
  dropping them silently. For SQLite: store ±Infinity, and either reject NaN or store it in a way that reads
  back as NaN. State what happens to Prometheus stale markers (suggested: drop them explicitly at the remote
  write handler, with a counter, since they are not samples).
- **JSON exports**: emit NaN and infinities as the strings `"NaN"`, `"+Inf"`, `"-Inf"` in JSONL and `/last`,
  and skip them (or fail) in SenML, which has no representation.
- **simplify**: map points back by index, not by value; drop non-finite samples before simplifying.
- **min/max with NaN**: pick the PostgreSQL rule or the "ignore NaN" rule and apply it in Rust and SQL.
- **SQLite Numeric text**: normalise on write (`Decimal::normalize().to_string()`) and parse with
  `from_str_exact` everywhere.

## Tests

Generic storage tests: Numeric min/max/avg/sum over 9, 10, -5, -10 and a 25-digit value; NaN and infinities
written and read back; an all-NaN bucket; Arrow export of a scale-22 value; simplify over a window ending in
NaN.

## Done when

The tests pass on all backends, or a backend's documented exception is asserted by its test.

## Outcome (11 November 2026)

The rules are written in `docs/DATAMODEL.md` ("Aggregating Numeric values", "Non-finite floats"). Each
item was reproduced by a failing test first.

1. **Numeric `MIN`/`MAX`/`SUM`/`AVG` on SQLite.** Before the fix, SQLite gave a max of `["9", "9"]` for
   9, 10, -5, -10 and a 25-digit value, and a sum rounded to 15 digits. SQLite now computes those buckets
   in Rust, from the stored values, with `aggregate_numeric_samples`; the bulk selector path does the
   same. The stored text is parsed with `from_str_exact`.
2. **Arrow export.** The scale is now 18, or `38 − integer digits` when a value needs more than 20 digits
   before the point (at least 9: `Decimal::MAX` fits). Values are rounded half away from zero. Unit test:
   `numerics_fit_their_decimal128_column`, with scale 22 and `Decimal::MAX`, on the single-series and the
   multi-series exports.
3. **DuckDB.** Decision: DuckDB keeps `DECIMAL(38, 9)` as a documented exception, like ClickHouse
   (`Decimal(38, 8)`) and BigQuery (`NUMERIC`). Test: `numerics_keep_their_digits_after_the_point` asserts
   the rounding of each. The average is the sum and the count as text, divided in Rust; it was a DOUBLE
   printed with an exponent. The Integer sum is read as a HUGEINT and checked, and "Overflow in SUM" is a
   400.
4. **PostgreSQL and TimescaleDB aggregates.** The Numeric aggregates are read as text, the average as
   `sum/count`, and parsed by `common::numeric_aggregate_from_text`. Before the fix:
   - the average of the 25-digit value was `246913578024691357802470`, because PostgreSQL divides with 16
     significant digits;
   - the sum of two `Decimal::MAX` failed to decode, with a 500;
   - `SUM(value)::bigint` failed with "bigint out of range", with a 500.

   `common::sql_sum_overflow` now maps SQLSTATE 22003, and SQLite's "integer overflow", to a 400
   (`aggregation_overflow`). The same function covers the single-series and the bulk selector reads.
   - Rule: an average whose sum does not fit is a 400, even if the average itself would fit, the same on
     every backend. ClickHouse's average now checks its sum with `throwIf`. ClickHouse error codes 70
     (`accurateCast`) and 395 (`throwIf`) are mapped to a 400 by `classify_clickhouse_error`, including
     when a cursor fails mid-read.
5. **NaN and the infinities on SQLite.** The infinities are stored (a REAL holds them). NaN is refused in
   `BatchBuilder::add`, for Float values and coordinates on every backend: `nan_is_refused`.
   - Prometheus remote write drops its NaN samples, the stale markers included
     (`remote_write_drops_nan_samples`). A series of only stale markers is not created.
   - Decision: there is no counter. The drop is documented.
6. **JSON exports.**
   - `exporters::float_to_json` writes `"NaN"`, `"+Inf"` and `"-Inf"`, in JSON Lines and in `/last`.
   - SenML leaves out the non-finite samples, which it wrote as `null` before.
7. **`simplify`.** The mapping was already by index since task 9. `simplify` now runs over the finite
   values, and keeps the non-finite samples. An infinity made every scaled point NaN before.
8. **NaN in `min`/`max`.** With NaN refused on write, the backends cannot differ any more. Infinities give
   the same `min` and `max` on every backend (tested).
9. **SQLite Numeric duplicates.** The values are stored with `Decimal::normalize()`. Test:
   `deduplication::at_ingestion_equal_numerics_are_duplicates`. 1.0, 1.00 and 1 gave 3 samples before the
   fix, and give 1 now.

Also fixed:
- `rrdcached_integration::prometheus_remote_write_then_read_the_series` computed its UUID with
  `__name__` among the labels. That broke with task 13, whose suites did not include RRDcached.
- The test `batched_inserts::sqlite_skips_non_finite_floats`, which asserted the silent drop, is removed.

**Not done.** BigQuery's own Numeric average and sum SQL are unchanged: it is documented to round to
9 digits, and was not run live. Its sums over the range of the type are not mapped to 400.

**Suites run.**
- Full: SQLite (364 integration tests) and PostgreSQL (365).
- TimescaleDB, DuckDB and ClickHouse: `non_finite`, `numeric_values`, `batched_inserts`, `selector_*`,
  `cross_series`, `aggregated`, `deduplication`, `query_export`, `prometheus` and `simplify`.
- RRDcached as CI runs it: the lib tests and `rrdcached_integration::`.
- `cargo clippy --all-features --tests` is clean.
