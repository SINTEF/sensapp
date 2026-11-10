# Timestamps: integer conversions everywhere

Source: code review of 10 November 2026. The code was read; the arithmetic was simulated in Python, not run
in Rust.

## Problem

Several conversions go through `f64` seconds and then truncate or floor. The result is a timestamp one
microsecond (or more) off, with a success answer.

1. **`datetime_to_micros`** (`src/storage/common.rs:82-86`, same formula at
   `src/datamodel/sensapp_datetime.rs:28-33`): `datetime.to_unix(Unit::Microsecond).floor() as i64`.
   Exact for 2005-09 to 2038-01 by coincidence. Simulated: about 24 % of timestamps one microsecond low in
   January 2005 and January 2039, 0 % in 1990 and November 2026. Used by every backend when writing.
   `datetime_to_millis` (`src/parsing/prometheus/converter.rs:128-130`) has the same shape.
2. **Arrow import** (`src/importers/arrow.rs:437-442`): `from_unix_seconds(micros as f64 / 1_000_000.0)`.
   Reported: about 3 % of today's timestamps land one microsecond early, so export then import is not
   idempotent and ingest deduplication misses those samples.
3. **SenML import** (`src/importers/senml.rs:133-138`): f64 to chrono nanoseconds to f64 to hifitime.
   Reported: about 2.9 % of millisecond timestamps one microsecond early.
4. **SenML export** (`src/exporters/senml.rs:7-9`): a plain bug, not rounding.
   `(to_unix_seconds() * 1000.0) as i64 + milliseconds()` adds the millisecond part twice: a sample at
   `.250` is exported at `.500`, and samples at `X.900` and `X+1.100` come out in inverted order. This is the
   default export format.
5. **TimescaleDB reads** of numeric, string, boolean, location, json and blob series
   (`src/storage/timescaledb/mod.rs:2027-2388`): timestamps rebuilt through f64 seconds, and the bounds
   computed as `(t * 1000) as i128` (an `i64` multiply that wraps after year 2262). The integer and float
   paths use the exact helpers at `:38-45`.
6. **TimescaleDB out-of-range bound** (`timescaledb/mod.rs:38-41`): `unwrap_or(UNIX_EPOCH)` turns a bound
   outside years ±9999 into 1970. A remote read with an open end (`i64::MAX` ms) then asks for
   `[start, 1970]` and returns nothing with a 200. PostgreSQL answers correctly.

## Suggested solution

- One integer conversion in `src/datamodel/sensapp_datetime.rs`, used everywhere:
  `to_unix_duration().total_nanoseconds().div_euclid(1000)` for microseconds (and `/ 1_000_000` for
  milliseconds), saturating to `i64`. Make `datetime_to_micros` and `datetime_to_millis` call it.
- Importers build datetimes with `SensAppDateTime::from_unix_microseconds_i64` (Arrow) and from chrono's
  `timestamp()` + `timestamp_subsec_nanos()` rounded to microseconds (SenML).
- SenML export: compute milliseconds from the integer microseconds.
- TimescaleDB: use `micros_to_offset_datetime` and `offset_datetime_to_sensapp` in the six paths, and make
  `micros_to_offset_datetime` clamp to the minimum and maximum `OffsetDateTime` instead of returning the
  epoch.
- Add a range check at ingestion for timestamps that would saturate (decide the range, for example years
  1 to 9999) and answer 400.

## Tests

- Unit test: round trip `micros -> SensAppDateTime -> micros` for random microsecond values across 1900 to
  2200, zero mismatches. Include `1073741824001000` (10 January 2004), which fails today.
- SenML export: samples at `.250`, `.900`, `.999` come out at the same instants; export then import gives the
  same microseconds.
- Arrow: export then import gives the same microseconds for 100,000 random timestamps.
- Generic storage test: a string series written at millisecond timestamps reads back at exactly those
  timestamps (catches the TimescaleDB paths).
- Remote read with `end_timestamp_ms = i64::MAX` returns the data on every backend.

## Done when

No `from_unix_seconds(... as f64)` and no `to_unix(...).floor()` remain on a read or write path, and the tests
pass on all backends.
