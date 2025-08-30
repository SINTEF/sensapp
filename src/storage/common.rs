use crate::datamodel::SensAppDateTime;

/// Convert SensAppDateTime to Unix microseconds for database storage
pub fn datetime_to_micros(datetime: &SensAppDateTime) -> i64 {
    // Get total Unix seconds as a float (including fractional seconds)
    let unix_seconds = datetime.to_unix_seconds();
    // Convert to microseconds by multiplying by 1,000,000
    (unix_seconds * 1_000_000.0) as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;

    #[test]
    fn test_datetime_to_micros() {
        // Test with seconds
        let datetime = SensAppDateTime::from_unix_seconds(1705315800.0);
        let result = datetime_to_micros(&datetime);
        assert_eq!(result, 1705315800000000);

        // Test with milliseconds (the case that was failing)
        let datetime_millis = SensAppDateTime::from_unix_milliseconds_i64(1500);
        let result = datetime_to_micros(&datetime_millis);
        assert_eq!(result, 1500000); // 1500ms = 1500000µs

        // Test with milliseconds including sub-second precision
        let datetime_millis = SensAppDateTime::from_unix_milliseconds_i64(1705315800123);
        let result = datetime_to_micros(&datetime_millis);
        assert_eq!(result, 1705315800123000); // 1705315800123ms = 1705315800123000µs
    }
}
