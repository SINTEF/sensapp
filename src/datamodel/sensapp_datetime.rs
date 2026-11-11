pub type SensAppDateTime = hifitime::Epoch;
use anyhow::Result;

pub trait SensAppDateTimeExt {
    fn from_unix_nanoseconds_i64(timestamp: i64) -> Self;
    fn from_unix_microseconds_i64(timestamp: i64) -> Self;
    fn from_unix_milliseconds_i64(timestamp: i64) -> Self;
    fn from_unix_seconds_i64(timestamp: i64) -> Self;
    /// Nanoseconds since the Unix epoch, exact.
    fn to_unix_nanoseconds_i128(&self) -> i128;
    /// Microseconds since the Unix epoch, rounded down, saturating to `i64`.
    fn to_unix_microseconds_i64(&self) -> i64;
    /// Milliseconds since the Unix epoch, rounded down, saturating to `i64`.
    fn to_unix_milliseconds_i64(&self) -> i64;
    /// Seconds since the Unix epoch, rounded down, saturating to `i64`.
    fn to_unix_seconds_i64(&self) -> i64;
}

/// 0001-01-01T00:00:00Z in Unix microseconds. With [`LATEST_UNIX_US`], the times every backend
/// stores: BigQuery's `TIMESTAMP` and TimescaleDB (through `OffsetDateTime`) stop at years 1 and
/// 9999. Writes outside are refused.
pub const EARLIEST_UNIX_US: i64 = -62_135_596_800_000_000;
/// 9999-12-31T23:59:59.999999Z in Unix microseconds.
pub const LATEST_UNIX_US: i64 = 253_402_300_799_999_999;

/// A read bound in Unix microseconds, clamped to the times that can be stored. Every stored
/// sample is inside, so the read finds the same samples, and an open end such as `i64::MAX`
/// milliseconds (a Prometheus remote read) stays something the databases accept.
pub fn clamp_to_storable_micros(micros: i64) -> i64 {
    micros.clamp(EARLIEST_UNIX_US, LATEST_UNIX_US)
}

/// `value` / `unit` rounded down, clamped to the `i64` range.
fn floor_div_saturating(value: i128, unit: i128) -> i64 {
    let value = value.div_euclid(unit);
    i64::try_from(value).unwrap_or(if value < 0 { i64::MIN } else { i64::MAX })
}

impl SensAppDateTimeExt for SensAppDateTime {
    fn from_unix_nanoseconds_i64(timestamp: i64) -> Self {
        Self::from_unix_duration(hifitime::Duration::from_truncated_nanoseconds(timestamp))
    }
    fn from_unix_microseconds_i64(timestamp: i64) -> Self {
        Self::from_utc_duration(UNIX_REF_EPOCH.to_utc_duration() + timestamp * Unit::Microsecond)
    }
    fn from_unix_milliseconds_i64(timestamp: i64) -> Self {
        Self::from_utc_duration(UNIX_REF_EPOCH.to_utc_duration() + timestamp * Unit::Millisecond)
    }
    fn from_unix_seconds_i64(timestamp: i64) -> Self {
        Self::from_utc_duration(UNIX_REF_EPOCH.to_utc_duration() + timestamp * Unit::Second)
    }
    // Integer arithmetic on hifitime durations, the reverse of the constructors above. Going
    // through `to_unix(..)`, an `f64`, lost a microsecond on some timestamps.
    fn to_unix_nanoseconds_i128(&self) -> i128 {
        (self.to_utc_duration() - UNIX_REF_EPOCH.to_utc_duration()).total_nanoseconds()
    }
    fn to_unix_microseconds_i64(&self) -> i64 {
        floor_div_saturating(self.to_unix_nanoseconds_i128(), 1_000)
    }
    fn to_unix_milliseconds_i64(&self) -> i64 {
        floor_div_saturating(self.to_unix_nanoseconds_i128(), 1_000_000)
    }
    fn to_unix_seconds_i64(&self) -> i64 {
        floor_div_saturating(self.to_unix_nanoseconds_i128(), 1_000_000_000)
    }
}

use hifitime::{UNIX_REF_EPOCH, Unit};
use sqlx::types::time::OffsetDateTime;
pub fn sensapp_datetime_to_offset_datetime(datetime: &SensAppDateTime) -> Result<OffsetDateTime> {
    let unix_timestamp_us = datetime.to_unix_microseconds_i64() as i128;
    Ok(OffsetDateTime::from_unix_timestamp_nanos(
        unix_timestamp_us * 1_000_i128,
    )?)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn assert_send<T: Send>() {}

    #[test]
    fn test_send() {
        assert_send::<SensAppDateTime>();
    }

    #[test]
    fn test_milliseconds_roundtrip() {
        // Test that from_unix_milliseconds_i64 -> to_unix_milliseconds roundtrips correctly
        let test_cases: &[i64] = &[
            1000,          // Small value
            1704067200000, // Jan 1, 2024 00:00:00 UTC
            1704067200123, // With subsecond precision
        ];

        for &input_ms in test_cases {
            let epoch = SensAppDateTime::from_unix_milliseconds_i64(input_ms);
            let output_ms = epoch.to_unix_milliseconds_i64();
            assert_eq!(
                input_ms, output_ms,
                "from_unix_milliseconds_i64 should roundtrip for {}",
                input_ms
            );
        }
    }

    #[test]
    fn test_sensapp_datetime_to_offset_datetime() {
        // Integer arithmetic on both sides: going through f64 seconds rounds the
        // last microsecond differently from the integer conversion, which made
        // a test of the current time fail now and then.
        for microseconds in [
            1_794_400_612_316_943_i64,
            1_794_400_612_316_942,
            0,
            -1,
            -1_500_000,
        ] {
            let datetime = SensAppDateTime::from_unix_microseconds_i64(microseconds);
            let offset = sensapp_datetime_to_offset_datetime(&datetime).unwrap();
            assert_eq!(
                (offset.unix_timestamp_nanos() / 1_000) as i64,
                microseconds,
                "{microseconds} microseconds"
            );
        }

        let now = hifitime::Epoch::now().unwrap();
        let offset_now = sensapp_datetime_to_offset_datetime(&now).unwrap();
        assert_eq!(
            (offset_now.unix_timestamp_nanos() / 1_000) as i64,
            now.to_unix_microseconds_i64()
        );
    }
}
