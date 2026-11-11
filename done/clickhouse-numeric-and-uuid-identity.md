# ClickHouse: Numeric scale, UUID identity and aggregation divergences

Source: code review of 10 November 2026. Items 1 and 2 were confirmed by reading (code and `rust_decimal`
source); the others were reported by a reviewer. Nothing was run against ClickHouse.

## Problem

1. **Large Numeric values are stored divided by a power of ten**
   (`src/storage/clickhouse/clickhouse_utilities.rs:23-34`). `Decimal::rescale` never fails: when the 96-bit
   mantissa cannot hold the value at scale 8, it stops at a smaller scale, and `mantissa()` is then written as
   if it were scale 8. Publishing `1e21` stores `1e20`. Affects |value| ≥ about 7.9e20; `Decimal(38,8)` could
   hold these values.
2. **The UUID is never compared after folding to 64 bits.** `uuid_to_sensor_id`
   (`src/storage/common.rs:67-70`) is the XOR of the two UUID halves. Its comment assumes hashed UUIDs, but
   SenML and Arrow accept client-chosen ones, and collisions are then trivial (`(a,b)` and `(b,a)`).
   - Registration keys on the id only (`clickhouse_utilities.rs:104-115`): a second UUID with the same id is
     never registered and its samples are appended to the first series.
   - `get_sensor_metadata` (`mod.rs:240-251`), `query_sensor_data` (`mod.rs:898-909`) and `delete_series`
     (`mod.rs:600-603`) match `WHERE s.sensor_id = ?` only, so `GET` and `DELETE` on a colliding UUID act on
     the other series.
   - BigQuery checks `sensor_id` and `uuid` on reads (`bigquery/reads.rs:302-306`) but merges on write
     (`bigquery/publishers.rs:183-197`).
3. **Panic on a Numeric over 96 bits when reading** (`clickhouse_utilities.rs:36-38`): covered by
   `handler-panics-kill-the-instance.md`.
4. **Aggregation divergences from PostgreSQL** (`src/storage/clickhouse/mod.rs`):
   - Integer `sum(value)` wraps silently (`:1776`); the reference returns an overflow error.
   - Buckets use `intDiv(timestamp_us - origin, step)` (`:82-87`), which truncates; the reference floors.
     Differs only without `start` and with samples before 1970.
   - `step_us: step_ms * 1000` is unchecked (`:1011`); every other path uses `checked_mul`.
   - Numeric `avg` goes through Float64 (`toDecimal128(avg(value), 8)`, `:1797`).
5. **Full scans on registration and label lookups**: `WHERE has(?, {id_column})`
   (`clickhouse_utilities.rs:53-55`) and `FROM labels FINAL WHERE has(?, sensor_id)` (`matchers.rs:33-34`).
   `selector.rs:70-71` already records that `has(?, sensor_id)` does not use the primary key and uses
   `sensor_id IN {ids:Array(UInt64)}` instead.

## Suggested solution

1. Compute the raw value as `mantissa * 10^(8 - scale)` in `i128` with `checked_mul`, and return an error
   when it does not fit `Decimal(38,8)`. Add unit tests for `decimal_to_clickhouse_raw` (there are none).
2. Add `AND s.uuid = ?` to the ClickHouse lookups and the delete. In registration, read the stored UUID of
   each existing id and return an error when it differs from the incoming one (ClickHouse and BigQuery).
   Do not change `uuid_to_sensor_id`: it is part of the on-disk format. Fix its comment.
4. Integer sum: sum as `Int128` (`sum(toInt128(value))`) and return the overflow error in Rust when the
   result does not fit `i64`. Buckets: a floor division, `intDiv(x - origin, step)` minus 1 when
   `x < origin` and the remainder is not 0. Step: `checked_mul`. Check each expression against ClickHouse
   before relying on it; they were not run.
5. Switch the two `has(?, ...)` queries to the `IN {ids:Array(UInt64)}` form, and check with `EXPLAIN` that
   the primary key is used.

## Tests

Generic storage tests where possible: a Numeric of 1e21 round trips or is refused; two UUIDs with the same
fold are refused at registration and never read each other's data; integer sum overflow is an error;
aggregation of pre-1970 samples without `start` gives the reference buckets.

## Done when

The tests pass in the ClickHouse CI job and `docs/` notes the Numeric range of the ClickHouse backend.

## Outcome (10 November 2026)

Every expression was checked against ClickHouse 26.8 before use.

1. **Numeric scale: right, fixed beyond the suggestion.** A `Decimal` mantissa is under 2^96, and
   2^96 × 10^8 is under 10^38, so every `Decimal` fits `Decimal(38, 8)`: `decimal_to_clickhouse_raw` is
   `mantissa × 10^(8 − scale)` after rounding to 8 decimals, and can no longer fail (task 3 had made it refuse
   the large values instead of storing them divided by a power of ten). `decimal_from_clickhouse_raw` strips
   the trailing zeros, so a stored value comes back exactly, and rounds the last decimals of an aggregate
   with more digits than a `Decimal` (as `Decimal` arithmetic does); over the range it is an error.
   Tests: unit `decimals_round_trip_through_clickhouse` (10^21, `Decimal::MAX`, `MIN`, 28 digits, rounding),
   generic `numeric_values::large_numerics_round_trip` (failed on ClickHouse before: refused).
2. **UUID identity: right, but narrower than reported.** A colliding UUID with *another* name was already
   refused, by the name check against the stored sensor of that key, with a misleading "another name or
   type" message. With the same name (or only the UUID, as SenML and Arrow allow) its samples went to the
   first series: measured 2 samples in the first series instead of 1.
   - New `StorageError::SensorKeyCollision` (409). ClickHouse and BigQuery registration refuse a UUID whose
     key is the key of a stored sensor with another UUID, or of another sensor of the same batch.
   - ClickHouse reads by UUID (`get_sensor_metadata`, `query_sensor_data`) compare the stored UUID, and
     `delete_series` goes through `get_sensor_metadata`. No SQL change was needed: the sensor row has its
     UUID. Since a colliding UUID is never registered, one key has one UUID, and the sample reads by key
     stay as they are.
   - BigQuery: the in-process registration cache fingerprint now includes the UUID (a colliding UUID with
     the same name and type hit the cache and was written under the other key).
   - `uuid_to_sensor_id` is unchanged; its comment no longer says every UUID is a hash.
   - Test: `sensor_identity::two_uuids_with_the_same_storage_key_never_share_a_series` (stored sensor and
     same batch; on the other backends both series are stored).
3. Covered by task 3.
4. **Aggregations: right.** Measured on ClickHouse: `intDiv(-5, 3)` is -1; `sum` of `Int64` wraps
   (`i64::MAX + 1` gave `i64::MIN`); `sum` of `Decimal(38, 8)` wraps too (30 × `Decimal::MAX` came back
   negative); `avg` of a decimal is a `Float64` (`…567.12345679` came back `…568.24539794`).
   - Buckets: `timestamp_us - positiveModulo(timestamp_us - origin, step)`.
   - Sums: `accurateCast(sum(toInt128(value)), 'Int64')` and
     `accurateCast(sum(toDecimal256(value, 8)), 'Decimal(38, 8)')`, which fail on overflow (as PostgreSQL's
     `SUM(value)::bigint`) instead of wrapping.
   - Numeric average: `accurateCast(sum(toDecimal256(value, 8)) / count(), 'Decimal(38, 8)')`, exact and
     truncated to 8 decimals.
   - Step: `checked_mul`.
   - **SQLite had the same truncated buckets** (not in the review): `/` truncates towards zero. Fixed with
     the positive remainder PostgreSQL already uses. DuckDB (`time_bucket`), BigQuery and PostgreSQL floor.
     SQLite's availability count also divides, but always from its start and only over samples after it.
   - Tests: generic `aggregated_windows::an_integer_sum_over_i64_is_an_error` and
     `buckets_before_1970_start_at_or_before_their_samples` (single series and selector), and ClickHouse
     `test_clickhouse_numeric_average_is_exact` and `test_clickhouse_numeric_sum_past_38_digits_is_an_error`.
5. **Full scans: wrong on ClickHouse 26.8.** `EXPLAIN indexes = 1` on a 200,000-row `ReplacingMergeTree`
   with `FINAL` reads 1 of 24 granules for both `has([5, 7], sensor_id)` and `sensor_id IN [5, 7]`: the
   server rewrites `has` into a set lookup. The three lookups were switched to `IN {ids:Array(UInt64)}`
   anyway, for consistency with `selector.rs` and older servers.

`docs/DATAMODEL.md` gives the ClickHouse Numeric range and the key collision rule.

Suites, all green: ClickHouse full (311 unit, 356 integration), SQLite full (296, 341), PostgreSQL full
(302, 342); TimescaleDB and DuckDB targeted (`aggregated_windows`, `sensor_identity`, `numeric_values`: 10
each); BigQuery unit tests with SQLite (329; with the `bigquery` feature alone, three HTTP unit tests cannot
build their SQLite storage, unrelated); clippy `--all-features --tests`. BigQuery itself was not run live.
