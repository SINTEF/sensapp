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
                        format!("\"{}\"", sample.value.replace('"', "\"\""))
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
                    let escaped_value = format!("\"{}\"", json_str.replace('"', "\"\""));
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

    /// Convert multiple SensorData to CSV format (long format with sensor_name column)
    /// Columns: timestamp,sensor_name,value,type
    /// This format is more robust for sparse/misaligned data from multiple sensors.
    pub fn to_csv_multi(sensor_data_list: &[SensorData]) -> Result<String> {
        let mut csv_output = String::new();
        csv_output.push_str("timestamp,sensor_name,value,type\n");

        for sensor_data in sensor_data_list {
            let sensor_name = &sensor_data.sensor.name;
            Self::append_samples_to_csv(&mut csv_output, sensor_name, &sensor_data.samples)?;
        }

        Ok(csv_output)
    }

    /// Internal helper to append samples from a single sensor to CSV output
    fn append_samples_to_csv(
        csv_output: &mut String,
        sensor_name: &str,
        samples: &TypedSamples,
    ) -> Result<()> {
        // Escape sensor name if needed
        let escaped_name =
            if sensor_name.contains(',') || sensor_name.contains('"') || sensor_name.contains('\n')
            {
                format!("\"{}\"", sensor_name.replace('"', "\"\""))
            } else {
                sensor_name.to_string()
            };

        match samples {
            TypedSamples::Integer(samples) => {
                for sample in samples.iter() {
                    csv_output.push_str(&format!(
                        "{},{},{},integer\n",
                        datetime_to_rfc3339(&sample.datetime),
                        escaped_name,
                        sample.value
                    ));
                }
            }
            TypedSamples::Numeric(samples) => {
                for sample in samples.iter() {
                    csv_output.push_str(&format!(
                        "{},{},{},numeric\n",
                        datetime_to_rfc3339(&sample.datetime),
                        escaped_name,
                        sample.value
                    ));
                }
            }
            TypedSamples::Float(samples) => {
                for sample in samples.iter() {
                    csv_output.push_str(&format!(
                        "{},{},{},float\n",
                        datetime_to_rfc3339(&sample.datetime),
                        escaped_name,
                        sample.value
                    ));
                }
            }
            TypedSamples::String(samples) => {
                for sample in samples.iter() {
                    let escaped_value = if sample.value.contains(',')
                        || sample.value.contains('"')
                        || sample.value.contains('\n')
                    {
                        format!("\"{}\"", sample.value.replace('"', "\"\""))
                    } else {
                        sample.value.clone()
                    };
                    csv_output.push_str(&format!(
                        "{},{},{},string\n",
                        datetime_to_rfc3339(&sample.datetime),
                        escaped_name,
                        escaped_value
                    ));
                }
            }
            TypedSamples::Boolean(samples) => {
                for sample in samples.iter() {
                    csv_output.push_str(&format!(
                        "{},{},{},boolean\n",
                        datetime_to_rfc3339(&sample.datetime),
                        escaped_name,
                        sample.value
                    ));
                }
            }
            TypedSamples::Location(samples) => {
                for sample in samples.iter() {
                    // Format location as "lat,lon" string for CSV
                    let value = format!("\"{},{}\"", sample.value.y(), sample.value.x());
                    csv_output.push_str(&format!(
                        "{},{},{},location\n",
                        datetime_to_rfc3339(&sample.datetime),
                        escaped_name,
                        value
                    ));
                }
            }
            TypedSamples::Json(samples) => {
                for sample in samples.iter() {
                    let json_str = sample.value.to_string();
                    let escaped_value = format!("\"{}\"", json_str.replace('"', "\"\""));
                    csv_output.push_str(&format!(
                        "{},{},{},json\n",
                        datetime_to_rfc3339(&sample.datetime),
                        escaped_name,
                        escaped_value
                    ));
                }
            }
            TypedSamples::Blob(samples) => {
                for sample in samples.iter() {
                    let encoded = general_purpose::STANDARD.encode(&sample.value);
                    csv_output.push_str(&format!(
                        "{},{},{},blob\n",
                        datetime_to_rfc3339(&sample.datetime),
                        escaped_name,
                        encoded
                    ));
                }
            }
        }

        Ok(())
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

    #[test]
    fn test_multi_sensor_to_csv() {
        // Create first sensor with integer samples
        let sensor1 = Sensor::new(
            Uuid::new_v4(),
            "temperature".to_string(),
            SensorType::Integer,
            None,
            None,
        );
        let samples1 = TypedSamples::Integer(smallvec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1609459200.0),
                value: 23,
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1609459260.0),
                value: 24,
            }
        ]);
        let sensor_data1 = SensorData::new(sensor1, samples1);

        // Create second sensor with integer samples
        let sensor2 = Sensor::new(
            Uuid::new_v4(),
            "humidity".to_string(),
            SensorType::Integer,
            None,
            None,
        );
        let samples2 = TypedSamples::Integer(smallvec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1609459200.0),
                value: 45,
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1609459260.0),
                value: 46,
            }
        ]);
        let sensor_data2 = SensorData::new(sensor2, samples2);

        // Test multi-sensor conversion
        let sensor_data_list = vec![sensor_data1, sensor_data2];
        let csv_output = CsvConverter::to_csv_multi(&sensor_data_list).unwrap();

        // Should have header row and 4 data rows
        let lines: Vec<&str> = csv_output.lines().collect();
        assert_eq!(lines.len(), 5); // 1 header + 4 data rows

        // Header should include sensor_name and type columns
        let header = lines[0].to_lowercase();
        assert!(header.contains("timestamp"));
        assert!(header.contains("sensor_name"));
        assert!(header.contains("value"));
        assert!(header.contains("type"));

        // Check that both sensors are in the output
        assert!(csv_output.contains("temperature"));
        assert!(csv_output.contains("humidity"));

        // Check for actual values
        assert!(csv_output.contains("23"));
        assert!(csv_output.contains("24"));
        assert!(csv_output.contains("45"));
        assert!(csv_output.contains("46"));
    }

    #[test]
    fn test_multi_sensor_csv_empty_list() {
        let sensor_data_list: Vec<SensorData> = vec![];
        let csv_output = CsvConverter::to_csv_multi(&sensor_data_list).unwrap();

        // Should just have the header row
        let lines: Vec<&str> = csv_output.lines().collect();
        assert_eq!(lines.len(), 1);
        assert!(lines[0].to_lowercase().contains("timestamp"));
    }
}
