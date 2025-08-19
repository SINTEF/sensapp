use crate::datamodel::{SensAppDateTime, SensorData, TypedSamples};
use anyhow::Result;
use base64::{Engine as _, engine::general_purpose};

/// Helper function to format datetime to RFC3339/ISO 8601
fn datetime_to_rfc3339(datetime: &SensAppDateTime) -> String {
    datetime.to_rfc3339()
}

/// Converter for SensorData to CSV format
pub struct CsvConverter;

impl CsvConverter {
    /// Convert SensorData to CSV format
    pub fn to_csv(sensor_data: &SensorData) -> Result<String> {
        let mut csv_output = String::new();

        match &sensor_data.samples {
            TypedSamples::Integer(samples) => {
                csv_output.push_str("timestamp,value\n");
                for sample in samples.iter() {
                    csv_output.push_str(&format!(
                        "{},{}\n",
                        datetime_to_rfc3339(&sample.datetime),
                        sample.value
                    ));
                }
            }
            TypedSamples::Numeric(samples) => {
                csv_output.push_str("timestamp,value\n");
                for sample in samples.iter() {
                    csv_output.push_str(&format!(
                        "{},{}\n",
                        datetime_to_rfc3339(&sample.datetime),
                        sample.value
                    ));
                }
            }
            TypedSamples::Float(samples) => {
                csv_output.push_str("timestamp,value\n");
                for sample in samples.iter() {
                    csv_output.push_str(&format!(
                        "{},{}\n",
                        datetime_to_rfc3339(&sample.datetime),
                        sample.value
                    ));
                }
            }
            TypedSamples::String(samples) => {
                csv_output.push_str("timestamp,value\n");
                for sample in samples.iter() {
                    // Escape quotes and wrap in quotes if needed
                    let escaped_value = if sample.value.contains(',')
                        || sample.value.contains('"')
                        || sample.value.contains('\n')
                    {
                        format!("\"{}\"", sample.value.replace("\"", "\"\""))
                    } else {
                        sample.value.clone()
                    };
                    csv_output.push_str(&format!(
                        "{},{}\n",
                        datetime_to_rfc3339(&sample.datetime),
                        escaped_value
                    ));
                }
            }
            TypedSamples::Boolean(samples) => {
                csv_output.push_str("timestamp,value\n");
                for sample in samples.iter() {
                    csv_output.push_str(&format!(
                        "{},{}\n",
                        datetime_to_rfc3339(&sample.datetime),
                        sample.value
                    ));
                }
            }
            TypedSamples::Location(samples) => {
                csv_output.push_str("timestamp,latitude,longitude\n");
                for sample in samples.iter() {
                    csv_output.push_str(&format!(
                        "{},{},{}\n",
                        datetime_to_rfc3339(&sample.datetime),
                        sample.value.y(), // latitude
                        sample.value.x()  // longitude
                    ));
                }
            }
            TypedSamples::Json(samples) => {
                csv_output.push_str("timestamp,value\n");
                for sample in samples.iter() {
                    // Convert JSON to string and escape for CSV
                    let json_str = sample.value.to_string();
                    let escaped_value = format!("\"{}\"", json_str.replace("\"", "\"\""));
                    csv_output.push_str(&format!(
                        "{},{}\n",
                        datetime_to_rfc3339(&sample.datetime),
                        escaped_value
                    ));
                }
            }
            TypedSamples::Blob(samples) => {
                csv_output.push_str("timestamp,value\n");
                for sample in samples.iter() {
                    // Encode binary data as base64
                    let encoded = general_purpose::STANDARD.encode(&sample.value);
                    csv_output.push_str(&format!(
                        "{},{}\n",
                        datetime_to_rfc3339(&sample.datetime),
                        encoded
                    ));
                }
            }
        }

        Ok(csv_output)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datamodel::unit::Unit;
    use crate::datamodel::{Sample, SensAppDateTime, Sensor, SensorData, SensorType, TypedSamples};
    use smallvec::smallvec;
    use uuid::Uuid;

    #[test]
    fn test_integer_samples_to_csv() {
        let sensor = Sensor::new(
            Uuid::new_v4(),
            "test_sensor".to_string(),
            SensorType::Integer,
            Some(Unit::new("Celsius".to_string(), None)),
            None,
        );

        let samples = TypedSamples::Integer(smallvec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1609459200.0), // 2021-01-01 00:00:00 UTC
                value: 23,
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1609459260.0), // 2021-01-01 00:01:00 UTC
                value: 24,
            }
        ]);

        let sensor_data = SensorData::new(sensor, samples);
        let csv_output = CsvConverter::to_csv(&sensor_data).unwrap();

        assert!(csv_output.starts_with("timestamp,value\n"));
        // Check for RFC3339 formatted timestamps
        assert!(csv_output.contains("2021-01-01T00:00:00"));
        assert!(csv_output.contains(",23\n"));
        assert!(csv_output.contains("2021-01-01T00:01:00"));
        assert!(csv_output.contains(",24\n"));
    }

    #[test]
    fn test_string_samples_to_csv() {
        let sensor = Sensor::new(
            Uuid::new_v4(),
            "test_sensor".to_string(),
            SensorType::String,
            None,
            None,
        );

        let samples = TypedSamples::String(smallvec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1609459200.0),
                value: "simple value".to_string(),
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1609459260.0),
                value: "value, with comma".to_string(),
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1609459320.0),
                value: "value with \"quotes\"".to_string(),
            }
        ]);

        let sensor_data = SensorData::new(sensor, samples);
        let csv_output = CsvConverter::to_csv(&sensor_data).unwrap();

        assert!(csv_output.contains("simple value\n"));
        assert!(csv_output.contains("\"value, with comma\"\n"));
        assert!(csv_output.contains("\"value with \"\"quotes\"\"\"\n"));
    }

    #[test]
    fn test_location_samples_to_csv() {
        let sensor = Sensor::new(
            Uuid::new_v4(),
            "test_sensor".to_string(),
            SensorType::Location,
            None,
            None,
        );

        let samples = TypedSamples::Location(smallvec![Sample {
            datetime: SensAppDateTime::from_unix_seconds(1609459200.0),
            value: geo::Point::new(2.3522, 48.8566), // Paris coordinates (lon, lat)
        }]);

        let sensor_data = SensorData::new(sensor, samples);
        let csv_output = CsvConverter::to_csv(&sensor_data).unwrap();

        assert!(csv_output.starts_with("timestamp,latitude,longitude\n"));
        assert!(csv_output.contains("48.8566,2.3522\n")); // latitude, longitude
    }

    #[test]
    fn test_boolean_samples_to_csv() {
        let sensor = Sensor::new(
            Uuid::new_v4(),
            "test_sensor".to_string(),
            SensorType::Boolean,
            None,
            None,
        );

        let samples = TypedSamples::Boolean(smallvec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1609459200.0),
                value: true,
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1609459260.0),
                value: false,
            }
        ]);

        let sensor_data = SensorData::new(sensor, samples);
        let csv_output = CsvConverter::to_csv(&sensor_data).unwrap();

        assert!(csv_output.contains("true\n"));
        assert!(csv_output.contains("false\n"));
    }

    #[test]
    fn test_blob_samples_to_csv() {
        let sensor = Sensor::new(
            Uuid::new_v4(),
            "test_sensor".to_string(),
            SensorType::Blob,
            None,
            None,
        );

        let samples = TypedSamples::Blob(smallvec![Sample {
            datetime: SensAppDateTime::from_unix_seconds(1609459200.0),
            value: vec![0x48, 0x65, 0x6c, 0x6c, 0x6f], // "Hello" in bytes
        }]);

        let sensor_data = SensorData::new(sensor, samples);
        let csv_output = CsvConverter::to_csv(&sensor_data).unwrap();

        // Base64 encoding of "Hello"
        let expected_base64 = general_purpose::STANDARD.encode("Hello");
        assert!(csv_output.contains(&expected_base64));
    }
}
