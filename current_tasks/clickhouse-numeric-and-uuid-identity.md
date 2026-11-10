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
