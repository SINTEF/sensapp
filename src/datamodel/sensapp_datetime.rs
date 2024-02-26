pub type SensAppDateTime = hifitime::Epoch;
use anyhow::Result;

use sqlx::types::time::OffsetDateTime;
pub fn sensapp_datetime_to_offset_datetime(datetime: &SensAppDateTime) -> Result<OffsetDateTime> {
    let unix_timestamp = datetime.to_unix_seconds().floor() as i128;

    let duration = datetime.to_duration();
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
    fn test_sensapp_datetime_to_offset_datetime() {
        let hifitime_now = hifitime::Epoch::now().unwrap();

        let offset_now = sensapp_datetime_to_offset_datetime(&hifitime_now).unwrap();

        println!("hifitime_now: {:?}", hifitime_now);
        println!("offset_now: {:?}", offset_now);
        assert_eq!(
            hifitime_now.to_unix_seconds().floor() as i64,
            offset_now.unix_timestamp(),
        );

        assert_eq!(
            hifitime_now.milliseconds(),
            (offset_now.nanosecond() / 1_000_000) as u64
        );

        assert_eq!(
            hifitime_now.nanoseconds(),
            (offset_now.nanosecond() % 1000) as u64
        );
    }
}
