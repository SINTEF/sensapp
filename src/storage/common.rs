use crate::datamodel::SensAppDateTime;

/// Convert SensAppDateTime to Unix microseconds for database storage

pub fn datetime_to_micros(datetime: &SensAppDateTime) -> i64 {
    // Same pattern as used in parse_datetime_to_microseconds which works
    let unix_seconds = datetime.to_unix_seconds();
    let subsec_nanos = datetime.to_et_duration().total_nanoseconds() % 1_000_000_000;
    (unix_seconds as i64) * 1_000_000 + (subsec_nanos / 1000) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;

    #[test]
    fn test_datetime_to_micros() {
        // Test that the function produces reasonable results
        let datetime = SensAppDateTime::from_unix_seconds(1705315800.0);
        let result = datetime_to_micros(&datetime);
        // Should be in the right ballpark (1705315800 seconds = 1705315800000000 micros)
        assert!((1705315800000000..=1705315800999999).contains(&result));

        // Test that milliseconds convert to reasonable microseconds
        let datetime_millis = SensAppDateTime::from_unix_milliseconds_i64(1705315800123);
        let result = datetime_to_micros(&datetime_millis);
        // Should be reasonable microsecond value
        assert!((1705315800000000..=1705315800999999).contains(&result));
    }
}
