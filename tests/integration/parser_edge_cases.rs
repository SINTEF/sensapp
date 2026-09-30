use rust_decimal::Decimal;
use sensapp::infer::columns::{InferedColumn, infer_column};
use sensapp::infer::datetime_guesser::likely_datetime_column;
use sensapp::infer::geo_guesser::likely_geo_columns;
use sensapp::infer::parsing::{InferedValue, infer_type, infer_type_with_trim};
use std::str::FromStr;
use std::sync::Arc;

/// Test edge cases for CSV parsing and type inference
mod parsing_edge_cases {
    use super::*;

    #[test]
    fn test_empty_strings_and_whitespace() {
        // Empty strings should be inferred as strings
        assert_eq!(
            infer_type(""),
            Ok(("", InferedValue::String("".to_string())))
        );

        // Whitespace-only strings
        assert_eq!(
            infer_type("   "),
            Ok(("", InferedValue::String("   ".to_string())))
        );
        assert_eq!(
            infer_type("\t\n"),
            Ok(("", InferedValue::String("\t\n".to_string())))
        );

        // With trimming enabled, whitespace around numbers should work
        assert_eq!(
            infer_type_with_trim("  42  "),
            Ok(("", InferedValue::Integer(42)))
        );
        assert_eq!(
            infer_type_with_trim("\t3.15\n"),
            Ok(("", InferedValue::Float(3.15)))
        );
    }

    #[test]
    fn test_special_numeric_values() {
        // Very large integers
        assert_eq!(
            infer_type("9223372036854775807"),
            Ok(("", InferedValue::Integer(9223372036854775807)))
        );

        // Very small integers
        assert_eq!(
            infer_type("-9223372036854775808"),
            Ok(("", InferedValue::Integer(-9223372036854775808)))
        );

        // Scientific notation (should be parsed as floats)
        assert_eq!(infer_type("1.5e10"), Ok(("", InferedValue::Float(1.5e10))));
        assert_eq!(infer_type("2.5E-3"), Ok(("", InferedValue::Float(2.5e-3))));

        // Infinity and NaN (handled by Rust's float parsing)
        assert_eq!(
            infer_type("inf"),
            Ok(("", InferedValue::String("inf".to_string())))
        );
        assert_eq!(
            infer_type("NaN"),
            Ok(("", InferedValue::String("NaN".to_string())))
        );
    }

    #[test]
    fn test_boolean_variations() {
        // Standard boolean values
        assert_eq!(infer_type("true"), Ok(("", InferedValue::Boolean(true))));
        assert_eq!(infer_type("false"), Ok(("", InferedValue::Boolean(false))));

        // Case variations
        assert_eq!(infer_type("TRUE"), Ok(("", InferedValue::Boolean(true))));
        assert_eq!(infer_type("FALSE"), Ok(("", InferedValue::Boolean(false))));
        assert_eq!(infer_type("True"), Ok(("", InferedValue::Boolean(true))));
        assert_eq!(infer_type("False"), Ok(("", InferedValue::Boolean(false))));

        // Non-boolean strings that might look like booleans
        assert_eq!(
            infer_type("yes"),
            Ok(("", InferedValue::String("yes".to_string())))
        );
        assert_eq!(
            infer_type("no"),
            Ok(("", InferedValue::String("no".to_string())))
        );
        assert_eq!(infer_type("1"), Ok(("", InferedValue::Integer(1))));
        assert_eq!(infer_type("0"), Ok(("", InferedValue::Integer(0))));
    }

    #[test]
    fn test_datetime_edge_cases() {
        // Valid ISO8601 variations
        let datetime1 = sensapp::infer::parsing::parse_iso8601_datetime("2024-01-01T00:00:00Z");
        assert!(datetime1.is_ok());

        let datetime2 =
            sensapp::infer::parsing::parse_iso8601_datetime("2024-12-31T23:59:59+01:00");
        assert!(datetime2.is_ok());

        // Invalid dates
        let invalid_date = sensapp::infer::parsing::parse_iso8601_datetime("2024-02-30T00:00:00Z");
        assert!(invalid_date.is_err());

        let invalid_time = sensapp::infer::parsing::parse_iso8601_datetime("2024-01-01T25:00:00Z");
        assert!(invalid_time.is_err());

        // Ambiguous date formats (should be strings)
        assert_eq!(
            infer_type("01/01/2024"),
            Ok(("", InferedValue::String("01/01/2024".to_string())))
        );
        assert_eq!(
            infer_type("2024-01-01"),
            Ok(("", InferedValue::String("2024-01-01".to_string())))
        );
    }

    #[test]
    fn test_json_edge_cases() {
        // Valid JSON variations
        assert!(matches!(infer_type("{}"), Ok((_, InferedValue::Json(_)))));
        assert!(matches!(infer_type("[]"), Ok((_, InferedValue::Json(_)))));
        assert!(matches!(
            infer_type("{\"key\": \"value\"}"),
            Ok((_, InferedValue::Json(_)))
        ));
        assert!(matches!(
            infer_type("[1, 2, 3]"),
            Ok((_, InferedValue::Json(_)))
        ));

        // Invalid JSON (should be strings)
        assert_eq!(
            infer_type("{key: value}"),
            Ok(("", InferedValue::String("{key: value}".to_string())))
        );
        assert_eq!(
            infer_type("{\"key\":}"),
            Ok(("", InferedValue::String("{\"key\":}".to_string())))
        );
        assert_eq!(
            infer_type("[1,2,3,]"),
            Ok(("", InferedValue::String("[1,2,3,]".to_string())))
        );
    }

    #[test]
    fn test_unicode_and_special_characters() {
        // Unicode strings
        assert_eq!(
            infer_type("café"),
            Ok(("", InferedValue::String("café".to_string())))
        );
        assert_eq!(
            infer_type("北京"),
            Ok(("", InferedValue::String("北京".to_string())))
        );
        assert_eq!(
            infer_type("🚀"),
            Ok(("", InferedValue::String("🚀".to_string())))
        );

        // Special characters that might interfere with parsing
        assert_eq!(
            infer_type("data,with,commas"),
            Ok(("", InferedValue::String("data,with,commas".to_string())))
        );
        assert_eq!(
            infer_type("data\nwith\nnewlines"),
            Ok(("", InferedValue::String("data\nwith\nnewlines".to_string())))
        );
        assert_eq!(
            infer_type("data\"with\"quotes"),
            Ok(("", InferedValue::String("data\"with\"quotes".to_string())))
        );
    }

    #[test]
    fn test_mixed_type_precedence() {
        // When mixing types, string should win
        let mixed_column = vec!["42".to_string(), "hello".to_string(), "3.14".to_string()];

        let inferred = infer_column(mixed_column, false, false);
        assert!(matches!(inferred, InferedColumn::String(_)));

        // When mixing numbers, float should win over integer
        let mixed_numbers = vec!["42".to_string(), "3.15".to_string(), "100".to_string()];

        let inferred_numbers = infer_column(mixed_numbers, false, false);
        if let InferedColumn::Float(values) = inferred_numbers {
            assert_eq!(values, vec![42.0, 3.15, 100.0]);
        } else {
            panic!("Expected float column");
        }
    }
}

/// Test edge cases for column type inference
mod column_inference_edge_cases {
    use super::*;

    #[test]
    fn test_empty_columns() {
        let empty_column: Vec<String> = vec![];
        let inferred = infer_column(empty_column, false, false);
        assert_eq!(inferred, InferedColumn::Integer(vec![]));
    }

    #[test]
    fn test_single_value_columns() {
        // Single integer
        let single_int = vec!["42".to_string()];
        let inferred = infer_column(single_int, false, false);
        assert_eq!(inferred, InferedColumn::Integer(vec![42]));

        // Single float
        let single_float = vec!["3.15".to_string()];
        let inferred = infer_column(single_float, false, false);
        assert_eq!(inferred, InferedColumn::Float(vec![3.15]));

        // Single boolean
        let single_bool = vec!["true".to_string()];
        let inferred = infer_column(single_bool, false, false);
        assert_eq!(inferred, InferedColumn::Boolean(vec![true]));
    }

    #[test]
    fn test_null_and_missing_values() {
        // Columns with empty strings (common in CSV for missing values)
        let with_empty = vec!["42".to_string(), "".to_string(), "84".to_string()];

        let inferred = infer_column(with_empty, false, false);
        // Should be treated as string because of the empty string
        assert!(matches!(inferred, InferedColumn::String(_)));

        // Test with trimming - empty strings after trimming whitespace
        let with_whitespace = vec![
            "  42  ".to_string(),
            "   ".to_string(),
            "  84  ".to_string(),
        ];

        let inferred_trimmed = infer_column(with_whitespace, true, false);
        // Still should be string due to whitespace-only value
        assert!(matches!(inferred_trimmed, InferedColumn::String(_)));
    }

    #[test]
    fn test_numeric_with_decimal_precision() {
        let precise_decimals = vec![
            "123.456789".to_string(),
            "987.654321".to_string(),
            "0.000001".to_string(),
        ];

        // With numeric inference enabled
        let inferred = infer_column(precise_decimals.clone(), false, true);
        if let InferedColumn::Numeric(values) = inferred {
            assert_eq!(values[0], Decimal::from_str("123.456789").unwrap());
            assert_eq!(values[1], Decimal::from_str("987.654321").unwrap());
            assert_eq!(values[2], Decimal::from_str("0.000001").unwrap());
        } else {
            panic!("Expected numeric column");
        }

        // Without numeric inference, should be float
        let inferred_float = infer_column(precise_decimals, false, false);
        assert!(matches!(inferred_float, InferedColumn::Float(_)));
    }
}

/// Test edge cases for datetime column detection
mod datetime_detection_edge_cases {
    use super::*;

    #[test]
    fn test_timestamp_column_detection() {
        let column_names = vec![
            "id".to_string(),
            "timestamp_ms".to_string(),
            "value".to_string(),
        ];

        let columns = vec![
            InferedColumn::Integer(vec![1, 2, 3]),
            InferedColumn::Integer(vec![1609459200000, 1609459260000, 1609459320000]), // Unix timestamps in ms
            InferedColumn::Float(vec![20.5, 21.0, 21.5]),
        ];

        let detected = likely_datetime_column(&column_names, &columns);
        assert_eq!(detected, Some("timestamp_ms".to_string()));
    }

    #[test]
    fn test_ambiguous_datetime_columns() {
        let column_names = vec![
            "created_at".to_string(),
            "updated_at".to_string(),
            "value".to_string(),
        ];

        // Both created_at and updated_at have similar datetime scores
        let columns = vec![
            InferedColumn::Integer(vec![1609459200, 1609459260, 1609459320]), // Unix timestamps
            InferedColumn::Integer(vec![1609459210, 1609459270, 1609459330]), // Unix timestamps
            InferedColumn::Float(vec![20.5, 21.0, 21.5]),
        ];

        let detected = likely_datetime_column(&column_names, &columns);
        // Should pick one of them (probably created_at due to ordering)
        assert!(detected.is_some());
    }

    #[test]
    fn test_no_datetime_columns() {
        let column_names = vec![
            "sensor_id".to_string(),
            "temperature".to_string(),
            "humidity".to_string(),
        ];

        let columns = vec![
            InferedColumn::String(vec!["temp_01".to_string(), "temp_02".to_string()]),
            InferedColumn::Float(vec![20.5, 21.0]),
            InferedColumn::Float(vec![65.0, 64.5]),
        ];

        let detected = likely_datetime_column(&column_names, &columns);
        assert_eq!(detected, None);
    }
}

/// Test edge cases for geolocation detection
mod geo_detection_edge_cases {
    use super::*;

    #[test]
    fn test_perfect_geo_columns() {
        let column_names = vec![
            "latitude".to_string(),
            "longitude".to_string(),
            "altitude".to_string(),
        ];

        let columns = vec![
            InferedColumn::Float(vec![59.9139, 59.9140, 59.9141]), // Oslo latitude
            InferedColumn::Float(vec![10.7522, 10.7523, 10.7524]), // Oslo longitude
            InferedColumn::Float(vec![10.0, 11.0, 12.0]),
        ];

        let detected = likely_geo_columns(&column_names, &columns);
        if let Some(geo_cols) = detected {
            assert_eq!(geo_cols.lat, "latitude");
            assert_eq!(geo_cols.lon, "longitude");
        } else {
            panic!("Expected geo columns to be detected");
        }
    }

    #[test]
    fn test_abbreviated_geo_columns() {
        let column_names = vec!["lat".to_string(), "lng".to_string(), "speed".to_string()];

        let columns = vec![
            InferedColumn::Float(vec![40.7128, 40.7129, 40.7130]), // NYC latitude
            InferedColumn::Float(vec![-74.0060, -74.0061, -74.0062]), // NYC longitude
            InferedColumn::Float(vec![50.0, 55.0, 60.0]),
        ];

        let detected = likely_geo_columns(&column_names, &columns);
        if let Some(geo_cols) = detected {
            assert_eq!(geo_cols.lat, "lat");
            assert_eq!(geo_cols.lon, "lng");
        } else {
            panic!("Expected geo columns to be detected");
        }
    }

    #[test]
    fn test_invalid_coordinate_ranges() {
        let column_names = vec!["lat".to_string(), "lon".to_string(), "value".to_string()];

        // Invalid latitude (> 90)
        let columns = vec![
            InferedColumn::Float(vec![95.0, 96.0, 97.0]),
            InferedColumn::Float(vec![10.0, 11.0, 12.0]),
            InferedColumn::Float(vec![1.0, 2.0, 3.0]),
        ];

        let detected = likely_geo_columns(&column_names, &columns);
        assert!(detected.is_none(), "Expected no geo columns to be detected");
    }

    #[test]
    fn test_mismatched_geo_columns() {
        let column_names = vec![
            "latitude".to_string(),
            "speed".to_string(), // Not longitude
            "altitude".to_string(),
        ];

        let columns = vec![
            InferedColumn::Float(vec![59.9139, 59.9140, 59.9141]),
            InferedColumn::Float(vec![50.0, 55.0, 60.0]), // Speed, not longitude
            InferedColumn::Float(vec![10.0, 11.0, 12.0]),
        ];

        let detected = likely_geo_columns(&column_names, &columns);
        assert!(detected.is_none(), "Expected no geo columns to be detected");
    }
}

#[cfg(test)]
mod integration_tests {
    use super::*;

    #[test]
    fn test_complex_mixed_data_inference() {
        // Simulate a complex CSV with multiple data types
        let column_names = vec![
            "timestamp".to_string(),
            "sensor_id".to_string(),
            "latitude".to_string(),
            "longitude".to_string(),
            "temperature".to_string(),
            "active".to_string(),
            "metadata".to_string(),
        ];

        let columns = vec![
            // Unix timestamps
            InferedColumn::Integer(vec![1609459200, 1609459260, 1609459320]),
            // Sensor IDs (strings)
            InferedColumn::String(vec![
                "TEMP_001".to_string(),
                "TEMP_002".to_string(),
                "TEMP_003".to_string(),
            ]),
            // GPS coordinates
            InferedColumn::Float(vec![59.9139, 59.9140, 59.9141]),
            InferedColumn::Float(vec![10.7522, 10.7523, 10.7524]),
            // Temperature readings
            InferedColumn::Float(vec![20.5, 21.0, 19.8]),
            // Boolean status
            InferedColumn::Boolean(vec![true, true, false]),
            // JSON metadata
            InferedColumn::Json(vec![
                Arc::new(serde_json::json!({"battery": 85})),
                Arc::new(serde_json::json!({"battery": 80})),
                Arc::new(serde_json::json!({"battery": 90})),
            ]),
        ];

        // Test datetime detection
        let datetime_col = likely_datetime_column(&column_names, &columns);
        assert_eq!(datetime_col, Some("timestamp".to_string()));

        // Test geo detection
        let geo_cols = likely_geo_columns(&column_names, &columns);
        if let Some(detected_geo) = geo_cols {
            assert_eq!(detected_geo.lat, "latitude");
            assert_eq!(detected_geo.lon, "longitude");
        } else {
            panic!("Expected geo columns to be detected in integration test");
        }
    }
}
