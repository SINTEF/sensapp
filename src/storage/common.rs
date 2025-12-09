use crate::datamodel::SensAppDateTime;
use hifitime::Unit;

/// Convert SensAppDateTime to Unix microseconds for database storage
#[allow(dead_code)] // Used by SQLite backend when enabled
pub fn datetime_to_micros(datetime: &SensAppDateTime) -> i64 {
    // Use to_unix with Microsecond unit to get a f64 in microseconds,
    // then convert to i64. This properly handles the Unix time reference.
    datetime.to_unix(Unit::Microsecond).floor() as i64
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

    #[test]
    fn test_datetime_roundtrip_with_microseconds() {
        // Test: from_unix_milliseconds_i64 -> datetime_to_micros -> from_unix_microseconds_i64 -> to_unix_milliseconds
        // This is the path data takes: test creates samples -> storage writes micros -> storage reads micros -> converter to millis
        let input_ms: i64 = 1704067200000; // Jan 1, 2024 00:00:00 UTC

        // Step 1: Create datetime from milliseconds (like test helper does)
        let datetime_in = SensAppDateTime::from_unix_milliseconds_i64(input_ms);

        // Step 2: Convert to microseconds for storage (like postgres publisher does)
        let micros_stored = datetime_to_micros(&datetime_in);

        // Step 3: Read back from storage as microseconds (like postgres queries do)
        let datetime_out = SensAppDateTime::from_unix_microseconds_i64(micros_stored);

        // Step 4: Convert back to milliseconds (like prometheus converter does)
        let output_ms = datetime_out.to_unix_milliseconds().floor() as i64;

        println!("Input ms: {}", input_ms);
        println!("Stored micros: {}", micros_stored);
        println!("Expected micros: {}", input_ms * 1000);
        println!("Output ms: {}", output_ms);
        println!("Diff: {} ms", output_ms - input_ms);

        assert_eq!(
            input_ms, output_ms,
            "Roundtrip should preserve milliseconds"
        );
    }
}
