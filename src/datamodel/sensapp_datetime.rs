pub type SensAppDateTime = hifitime::Epoch;
use anyhow::Result;

pub trait SensAppDateTimeExt {
    fn from_unix_nanoseconds_i64(timestamp: i64) -> Self;
    fn from_unix_microseconds_i64(timestamp: i64) -> Self;
    fn from_unix_milliseconds_i64(timestamp: i64) -> Self;
    fn from_unix_seconds_i64(timestamp: i64) -> Self;
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
}

use hifitime::{UNIX_REF_EPOCH, Unit};
use sqlx::types::time::OffsetDateTime;
#[allow(dead_code)]
pub fn sensapp_datetime_to_offset_datetime(datetime: &SensAppDateTime) -> Result<OffsetDateTime> {
    let unix_timestamp = datetime.to_unix_seconds().floor() as i128;

    let duration = datetime.to_et_duration();
    let (_sign, _days, _hours, _minutes, _seconds, miliseconds, microseconds, ns_left) =
        duration.decompose();
    let sum_after_seconds: i128 = (miliseconds as i128) * 1_000_000_i128
        + (microseconds as i128) * 1_000_i128
        + ns_left as i128;

    let sum = unix_timestamp * 1_000_000_000_i128 + sum_after_seconds;
    Ok(OffsetDateTime::from_unix_timestamp_nanos(sum)?)
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
            let output_ms = epoch.to_unix_milliseconds().floor() as i64;
            assert_eq!(
                input_ms, output_ms,
                "from_unix_milliseconds_i64 should roundtrip for {}",
                input_ms
            );
        }
    }

    #[test]
    fn test_sensapp_datetime_to_offset_datetime() {
        let hifitime_now = hifitime::Epoch::now().unwrap();

        let offset_now = sensapp_datetime_to_offset_datetime(&hifitime_now).unwrap();

        println!("hifitime_now: {:?}", hifitime_now);
        println!("offset_now: {:?}", offset_now);
        assert_eq!(
            hifitime_now.to_unix_seconds().floor() as i64,
            offset_now.unix_timestamp(),
        );

        // Compare sub-second precision: extract nanoseconds within the current second
        let hifitime_total_nanoseconds = hifitime_now.to_et_duration().total_nanoseconds();
        let hifitime_subsec_nanoseconds = (hifitime_total_nanoseconds % 1_000_000_000) as u32;

        assert_eq!(hifitime_subsec_nanoseconds, offset_now.nanosecond());
    }
}
