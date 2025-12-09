use crate::datamodel::{SensAppDateTime, SensorData, TypedSamples};
use anyhow::Result;
use base64::{Engine as _, engine::general_purpose};
use std::collections::{BTreeSet, HashMap};

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
    /// Columns: timestamp,sensor_name,value,type[,label1,label2,...]
    /// This format is more robust for sparse/misaligned data from multiple sensors.
    /// Label columns are added at the end, sorted alphabetically by key.
    /// If a sensor doesn't have a particular label, the column value is empty.
    pub fn to_csv_multi(sensor_data_list: &[SensorData]) -> Result<String> {
        let mut csv_output = String::new();

        // Collect all unique label keys from all sensors (using BTreeSet for sorted order)
        let mut all_label_keys: BTreeSet<&str> = BTreeSet::new();
        for sensor_data in sensor_data_list {
            for (key, _) in &sensor_data.sensor.labels {
                all_label_keys.insert(key.as_str());
            }
        }

        // Convert to Vec for indexed access
        let label_keys: Vec<&str> = all_label_keys.into_iter().collect();

        // Write header
        csv_output.push_str("timestamp,sensor_id,sensor_name,value,type");
        for key in &label_keys {
            csv_output.push(',');
            csv_output.push_str(key);
        }
        csv_output.push('\n');

        // Write data rows
        for sensor_data in sensor_data_list {
            let sensor_id = &sensor_data.sensor.uuid;
            let sensor_name = &sensor_data.sensor.name;

            // Build a map of label key -> value for this sensor
            let labels_map: HashMap<&str, &str> = sensor_data
                .sensor
                .labels
                .iter()
                .map(|(k, v)| (k.as_str(), v.as_str()))
                .collect();

            Self::append_samples_to_csv(
                &mut csv_output,
                sensor_id,
                sensor_name,
                &sensor_data.samples,
                &label_keys,
                &labels_map,
            )?;
        }

        Ok(csv_output)
    }

    /// Internal helper to append samples from a single sensor to CSV output
    fn append_samples_to_csv(
        csv_output: &mut String,
        sensor_id: &uuid::Uuid,
        sensor_name: &str,
        samples: &TypedSamples,
        label_keys: &[&str],
        labels_map: &HashMap<&str, &str>,
    ) -> Result<()> {
        // Escape sensor name if needed
        let escaped_name =
            if sensor_name.contains(',') || sensor_name.contains('"') || sensor_name.contains('\n')
            {
                format!("\"{}\"", sensor_name.replace('"', "\"\""))
            } else {
                sensor_name.to_string()
            };

        // Pre-compute the label values suffix for this sensor (same for all samples)
        let label_suffix = Self::build_label_suffix(label_keys, labels_map);

        match samples {
            TypedSamples::Integer(samples) => {
                for sample in samples.iter() {
                    csv_output.push_str(&format!(
                        "{},{},{},{},integer{}\n",
                        datetime_to_rfc3339(&sample.datetime),
                        sensor_id,
                        escaped_name,
                        sample.value,
                        label_suffix
                    ));
                }
            }
            TypedSamples::Numeric(samples) => {
                for sample in samples.iter() {
                    csv_output.push_str(&format!(
                        "{},{},{},{},numeric{}\n",
                        datetime_to_rfc3339(&sample.datetime),
                        sensor_id,
                        escaped_name,
                        sample.value,
                        label_suffix
                    ));
                }
            }
            TypedSamples::Float(samples) => {
                for sample in samples.iter() {
                    csv_output.push_str(&format!(
                        "{},{},{},{},float{}\n",
                        datetime_to_rfc3339(&sample.datetime),
                        sensor_id,
                        escaped_name,
                        sample.value,
                        label_suffix
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
                        "{},{},{},{},string{}\n",
                        datetime_to_rfc3339(&sample.datetime),
                        sensor_id,
                        escaped_name,
                        escaped_value,
                        label_suffix
                    ));
                }
            }
            TypedSamples::Boolean(samples) => {
                for sample in samples.iter() {
                    csv_output.push_str(&format!(
                        "{},{},{},{},boolean{}\n",
                        datetime_to_rfc3339(&sample.datetime),
                        sensor_id,
                        escaped_name,
                        sample.value,
                        label_suffix
                    ));
                }
            }
            TypedSamples::Location(samples) => {
                for sample in samples.iter() {
                    // Format location as "lat,lon" string for CSV
                    let value = format!("\"{},{}\"", sample.value.y(), sample.value.x());
                    csv_output.push_str(&format!(
                        "{},{},{},{},location{}\n",
                        datetime_to_rfc3339(&sample.datetime),
                        sensor_id,
                        escaped_name,
                        value,
                        label_suffix
                    ));
                }
            }
            TypedSamples::Json(samples) => {
                for sample in samples.iter() {
                    let json_str = sample.value.to_string();
                    let escaped_value = format!("\"{}\"", json_str.replace('"', "\"\""));
                    csv_output.push_str(&format!(
                        "{},{},{},{},json{}\n",
                        datetime_to_rfc3339(&sample.datetime),
                        sensor_id,
                        escaped_name,
                        escaped_value,
                        label_suffix
                    ));
                }
            }
            TypedSamples::Blob(samples) => {
                for sample in samples.iter() {
                    let encoded = general_purpose::STANDARD.encode(&sample.value);
                    csv_output.push_str(&format!(
                        "{},{},{},{},blob{}\n",
                        datetime_to_rfc3339(&sample.datetime),
                        sensor_id,
                        escaped_name,
                        encoded,
                        label_suffix
                    ));
                }
            }
        }

        Ok(())
    }

    /// Build the label suffix string for a row
    /// Returns a string like ",value1,value2,value3" or empty string if no labels
    fn build_label_suffix(label_keys: &[&str], labels_map: &HashMap<&str, &str>) -> String {
        if label_keys.is_empty() {
            return String::new();
        }

        let mut suffix = String::new();
        for key in label_keys {
            suffix.push(',');
            if let Some(value) = labels_map.get(key) {
                // Escape value if needed
                if value.contains(',') || value.contains('"') || value.contains('\n') {
                    suffix.push_str(&format!("\"{}\"", value.replace('"', "\"\"")));
                } else {
                    suffix.push_str(value);
                }
            }
            // If no value, just leave it empty (the comma is already added)
        }
        suffix
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
        let uuid1 = Uuid::new_v4();
        let sensor1 = Sensor::new(
            uuid1,
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
        let uuid2 = Uuid::new_v4();
        let sensor2 = Sensor::new(
            uuid2,
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

        // Header should include sensor_id, sensor_name and type columns
        let header = lines[0].to_lowercase();
        assert!(header.contains("timestamp"));
        assert!(header.contains("sensor_id"));
        assert!(header.contains("sensor_name"));
        assert!(header.contains("value"));
        assert!(header.contains("type"));

        // Check that both sensor UUIDs are in the output
        assert!(csv_output.contains(&uuid1.to_string()));
        assert!(csv_output.contains(&uuid2.to_string()));

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

    #[test]
    fn test_multi_sensor_csv_with_labels() {
        use crate::datamodel::sensapp_vec::SensAppLabels;
        use smallvec::smallvec as labels_smallvec;

        // Create first sensor with labels
        let mut labels1: SensAppLabels = labels_smallvec![];
        labels1.push(("location".to_string(), "office".to_string()));
        labels1.push(("floor".to_string(), "2".to_string()));

        let sensor1 = Sensor {
            uuid: Uuid::new_v4(),
            name: "temperature".to_string(),
            sensor_type: SensorType::Float,
            unit: None,
            labels: labels1,
        };
        let samples1 = TypedSamples::Float(smallvec![Sample {
            datetime: SensAppDateTime::from_unix_seconds(1609459200.0),
            value: 23.5,
        },]);
        let sensor_data1 = SensorData::new(sensor1, samples1);

        // Create second sensor with different labels (only location)
        let mut labels2: SensAppLabels = labels_smallvec![];
        labels2.push(("location".to_string(), "warehouse".to_string()));

        let sensor2 = Sensor {
            uuid: Uuid::new_v4(),
            name: "humidity".to_string(),
            sensor_type: SensorType::Float,
            unit: None,
            labels: labels2,
        };
        let samples2 = TypedSamples::Float(smallvec![Sample {
            datetime: SensAppDateTime::from_unix_seconds(1609459260.0),
            value: 65.0,
        },]);
        let sensor_data2 = SensorData::new(sensor2, samples2);

        // Create third sensor with no labels
        let sensor3 = Sensor {
            uuid: Uuid::new_v4(),
            name: "pressure".to_string(),
            sensor_type: SensorType::Float,
            unit: None,
            labels: labels_smallvec![],
        };
        let samples3 = TypedSamples::Float(smallvec![Sample {
            datetime: SensAppDateTime::from_unix_seconds(1609459320.0),
            value: 1013.25,
        },]);
        let sensor_data3 = SensorData::new(sensor3, samples3);

        // Test multi-sensor conversion with labels
        let sensor_data_list = vec![sensor_data1, sensor_data2, sensor_data3];
        let csv_output = CsvConverter::to_csv_multi(&sensor_data_list).unwrap();

        // Parse CSV output
        let lines: Vec<&str> = csv_output.lines().collect();
        assert_eq!(lines.len(), 4); // 1 header + 3 data rows

        // Header should include label columns (alphabetically sorted)
        let header = lines[0];
        assert!(header.contains("timestamp"));
        assert!(header.contains("sensor_name"));
        assert!(header.contains("value"));
        assert!(header.contains("type"));
        assert!(header.contains("floor"));
        assert!(header.contains("location"));

        // Verify label columns are at the end, after the standard columns
        let header_parts: Vec<&str> = header.split(',').collect();
        let type_idx = header_parts.iter().position(|&s| s == "type").unwrap();
        let floor_idx = header_parts.iter().position(|&s| s == "floor").unwrap();
        let location_idx = header_parts.iter().position(|&s| s == "location").unwrap();
        assert!(floor_idx > type_idx, "floor should come after type");
        assert!(location_idx > type_idx, "location should come after type");

        // Check data rows - temperature sensor has both labels
        let temp_row = lines.iter().find(|l| l.contains("temperature")).unwrap();
        assert!(temp_row.contains("office"));
        assert!(temp_row.contains("2")); // floor value

        // Check humidity sensor has location but empty floor
        let humidity_row = lines.iter().find(|l| l.contains("humidity")).unwrap();
        assert!(humidity_row.contains("warehouse"));

        // Check pressure sensor has empty labels
        let pressure_row = lines.iter().find(|l| l.contains("pressure")).unwrap();
        assert!(pressure_row.contains("1013.25"));
    }

    #[test]
    fn test_multi_sensor_csv_labels_with_special_characters() {
        use crate::datamodel::sensapp_vec::SensAppLabels;
        use smallvec::smallvec as labels_smallvec;

        // Create sensor with label values containing special CSV characters
        let mut labels: SensAppLabels = labels_smallvec![];
        labels.push(("description".to_string(), "value, with comma".to_string()));
        labels.push(("notes".to_string(), "contains \"quotes\" here".to_string()));

        let sensor = Sensor {
            uuid: Uuid::new_v4(),
            name: "test_sensor".to_string(),
            sensor_type: SensorType::Float,
            unit: None,
            labels,
        };
        let samples = TypedSamples::Float(smallvec![Sample {
            datetime: SensAppDateTime::from_unix_seconds(1609459200.0),
            value: 42.0,
        }]);
        let sensor_data = SensorData::new(sensor, samples);

        let csv_output = CsvConverter::to_csv_multi(&[sensor_data]).unwrap();

        // Values with special characters should be properly escaped
        assert!(csv_output.contains("\"value, with comma\""));
        assert!(csv_output.contains("\"contains \"\"quotes\"\" here\""));
    }

    #[test]
    fn test_multi_sensor_csv_no_labels() {
        // Test that when no sensors have labels, no extra columns are added
        let sensor1 = Sensor::new(
            Uuid::new_v4(),
            "sensor1".to_string(),
            SensorType::Integer,
            None,
            None,
        );
        let samples1 = TypedSamples::Integer(smallvec![Sample {
            datetime: SensAppDateTime::from_unix_seconds(1609459200.0),
            value: 100,
        }]);
        let sensor_data1 = SensorData::new(sensor1, samples1);

        let sensor2 = Sensor::new(
            Uuid::new_v4(),
            "sensor2".to_string(),
            SensorType::Integer,
            None,
            None,
        );
        let samples2 = TypedSamples::Integer(smallvec![Sample {
            datetime: SensAppDateTime::from_unix_seconds(1609459260.0),
            value: 200,
        }]);
        let sensor_data2 = SensorData::new(sensor2, samples2);

        let csv_output = CsvConverter::to_csv_multi(&[sensor_data1, sensor_data2]).unwrap();

        let lines: Vec<&str> = csv_output.lines().collect();
        let header = lines[0];

        // Header should only have the standard 5 columns (including sensor_id)
        let header_parts: Vec<&str> = header.split(',').collect();
        assert_eq!(header_parts.len(), 5);
        assert_eq!(
            header_parts,
            vec!["timestamp", "sensor_id", "sensor_name", "value", "type"]
        );
    }
}
