use crate::datamodel::{SensorData, TypedSamples};
use anyhow::Result;
use base64::{Engine as _, engine::general_purpose};
use serde_json::{Map, Value, json};

/// Helper function to format datetime to RFC3339/ISO 8601
fn datetime_to_rfc3339(datetime: &crate::datamodel::SensAppDateTime) -> String {
    datetime.to_rfc3339()
}

/// Helper function to convert sensor labels to a JSON object
fn labels_to_json(sensor_data: &SensorData) -> Value {
    let mut labels_obj = Map::new();
    for (key, value) in &sensor_data.sensor.labels {
        labels_obj.insert(key.clone(), json!(value));
    }
    json!(labels_obj)
}

/// Converter for SensorData to JSON Lines format
pub struct JsonlConverter;

impl JsonlConverter {
    /// Convert SensorData to JSON Lines format (one JSON object per line)
    pub fn to_jsonl(sensor_data: &SensorData) -> Result<String> {
        let mut jsonl_output = String::new();
        Self::append_sensor_to_jsonl(&mut jsonl_output, sensor_data)?;
        Ok(jsonl_output)
    }

    /// Convert multiple SensorData to JSON Lines format (one JSON object per line)
    /// Simply iterates through all sensors and appends their lines to the output.
    pub fn to_jsonl_multi(sensor_data_list: &[SensorData]) -> Result<String> {
        let mut jsonl_output = String::new();

        for sensor_data in sensor_data_list {
            Self::append_sensor_to_jsonl(&mut jsonl_output, sensor_data)?;
        }

        Ok(jsonl_output)
    }

    /// Internal helper to append a single sensor's data to JSONL output
    fn append_sensor_to_jsonl(jsonl_output: &mut String, sensor_data: &SensorData) -> Result<()> {
        let labels = labels_to_json(sensor_data);

        match &sensor_data.samples {
            TypedSamples::Integer(samples) => {
                for sample in samples.iter() {
                    let line = json!({
                        "sensor_uuid": sensor_data.sensor.uuid,
                        "sensor_name": sensor_data.sensor.name,
                        "timestamp": datetime_to_rfc3339(&sample.datetime),
                        "value": sample.value,
                        "type": "integer",
                        "labels": labels
                    });
                    jsonl_output.push_str(&line.to_string());
                    jsonl_output.push('\n');
                }
            }
            TypedSamples::Numeric(samples) => {
                for sample in samples.iter() {
                    let line = json!({
                        "sensor_uuid": sensor_data.sensor.uuid,
                        "sensor_name": sensor_data.sensor.name,
                        "timestamp": datetime_to_rfc3339(&sample.datetime),
                        "value": sample.value.to_string(),
                        "type": "numeric",
                        "labels": labels
                    });
                    jsonl_output.push_str(&line.to_string());
                    jsonl_output.push('\n');
                }
            }
            TypedSamples::Float(samples) => {
                for sample in samples.iter() {
                    let line = json!({
                        "sensor_uuid": sensor_data.sensor.uuid,
                        "sensor_name": sensor_data.sensor.name,
                        "timestamp": datetime_to_rfc3339(&sample.datetime),
                        "value": sample.value,
                        "type": "float",
                        "labels": labels
                    });
                    jsonl_output.push_str(&line.to_string());
                    jsonl_output.push('\n');
                }
            }
            TypedSamples::String(samples) => {
                for sample in samples.iter() {
                    let line = json!({
                        "sensor_uuid": sensor_data.sensor.uuid,
                        "sensor_name": sensor_data.sensor.name,
                        "timestamp": datetime_to_rfc3339(&sample.datetime),
                        "value": sample.value,
                        "type": "string",
                        "labels": labels
                    });
                    jsonl_output.push_str(&line.to_string());
                    jsonl_output.push('\n');
                }
            }
            TypedSamples::Boolean(samples) => {
                for sample in samples.iter() {
                    let line = json!({
                        "sensor_uuid": sensor_data.sensor.uuid,
                        "sensor_name": sensor_data.sensor.name,
                        "timestamp": datetime_to_rfc3339(&sample.datetime),
                        "value": sample.value,
                        "type": "boolean",
                        "labels": labels
                    });
                    jsonl_output.push_str(&line.to_string());
                    jsonl_output.push('\n');
                }
            }
            TypedSamples::Location(samples) => {
                for sample in samples.iter() {
                    let line = json!({
                        "sensor_uuid": sensor_data.sensor.uuid,
                        "sensor_name": sensor_data.sensor.name,
                        "timestamp": datetime_to_rfc3339(&sample.datetime),
                        "latitude": sample.value.y(),
                        "longitude": sample.value.x(),
                        "type": "location",
                        "labels": labels
                    });
                    jsonl_output.push_str(&line.to_string());
                    jsonl_output.push('\n');
                }
            }
            TypedSamples::Json(samples) => {
                for sample in samples.iter() {
                    let line = json!({
                        "sensor_uuid": sensor_data.sensor.uuid,
                        "sensor_name": sensor_data.sensor.name,
                        "timestamp": datetime_to_rfc3339(&sample.datetime),
                        "value": sample.value,
                        "type": "json",
                        "labels": labels
                    });
                    jsonl_output.push_str(&line.to_string());
                    jsonl_output.push('\n');
                }
            }
            TypedSamples::Blob(samples) => {
                for sample in samples.iter() {
                    let encoded = general_purpose::STANDARD.encode(&sample.value);
                    let line = json!({
                        "sensor_uuid": sensor_data.sensor.uuid,
                        "sensor_name": sensor_data.sensor.name,
                        "timestamp": datetime_to_rfc3339(&sample.datetime),
                        "value": encoded,
                        "type": "blob",
                        "labels": labels
                    });
                    jsonl_output.push_str(&line.to_string());
                    jsonl_output.push('\n');
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
    fn test_integer_samples_to_jsonl() {
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
        let jsonl_output = JsonlConverter::to_jsonl(&sensor_data).unwrap();

        // Check that we have two lines
        let lines: Vec<&str> = jsonl_output.trim().split('\n').collect();
        assert_eq!(lines.len(), 2);

        // Each line should be valid JSON
        for line in lines {
            let parsed: serde_json::Value = serde_json::from_str(line).unwrap();
            assert!(parsed.get("sensor_name").is_some());
            assert!(parsed.get("timestamp").is_some());
            assert!(parsed.get("value").is_some());
            assert_eq!(parsed.get("type").unwrap(), "integer");
        }
    }

    #[test]
    fn test_location_samples_to_jsonl() {
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
        let jsonl_output = JsonlConverter::to_jsonl(&sensor_data).unwrap();

        let line = jsonl_output.trim();
        let parsed: serde_json::Value = serde_json::from_str(line).unwrap();

        assert_eq!(parsed.get("latitude").unwrap(), 48.8566);
        assert_eq!(parsed.get("longitude").unwrap(), 2.3522);
        assert_eq!(parsed.get("type").unwrap(), "location");
    }

    #[test]
    fn test_multi_sensor_to_jsonl() {
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

        // Create second sensor with float samples
        let sensor2 = Sensor::new(
            Uuid::new_v4(),
            "humidity".to_string(),
            SensorType::Float,
            None,
            None,
        );
        let samples2 = TypedSamples::Float(smallvec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1609459200.0),
                value: 45.5,
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1609459260.0),
                value: 46.2,
            }
        ]);
        let sensor_data2 = SensorData::new(sensor2, samples2);

        // Test multi-sensor conversion
        let sensor_data_list = vec![sensor_data1, sensor_data2];
        let jsonl_output = JsonlConverter::to_jsonl_multi(&sensor_data_list).unwrap();

        // Should have 4 lines (2 sensors * 2 samples each)
        let lines: Vec<&str> = jsonl_output.lines().filter(|l| !l.is_empty()).collect();
        assert_eq!(lines.len(), 4);

        // Each line should be valid JSON
        for line in &lines {
            let parsed: serde_json::Value = serde_json::from_str(line).unwrap();
            assert!(parsed.get("timestamp").is_some());
            assert!(parsed.get("sensor_name").is_some());
        }

        // Check that both sensors are represented
        let has_temperature = lines.iter().any(|l| l.contains("temperature"));
        let has_humidity = lines.iter().any(|l| l.contains("humidity"));
        assert!(has_temperature, "Should have temperature sensor records");
        assert!(has_humidity, "Should have humidity sensor records");
    }

    #[test]
    fn test_multi_sensor_jsonl_empty_list() {
        let sensor_data_list: Vec<SensorData> = vec![];
        let jsonl_output = JsonlConverter::to_jsonl_multi(&sensor_data_list).unwrap();

        // Should be empty
        assert!(jsonl_output.is_empty());
    }

    #[test]
    fn test_jsonl_with_labels() {
        use crate::datamodel::sensapp_vec::SensAppLabels;
        use smallvec::smallvec as labels_smallvec;

        // Create sensor with labels
        let mut labels: SensAppLabels = labels_smallvec![];
        labels.push(("environment".to_string(), "production".to_string()));
        labels.push(("region".to_string(), "europe".to_string()));

        let sensor = Sensor {
            uuid: Uuid::new_v4(),
            name: "cpu_usage".to_string(),
            sensor_type: SensorType::Float,
            unit: None,
            labels,
        };

        let samples = TypedSamples::Float(smallvec![Sample {
            datetime: SensAppDateTime::from_unix_seconds(1609459200.0),
            value: 75.5,
        }]);

        let sensor_data = SensorData::new(sensor, samples);
        let jsonl_output = JsonlConverter::to_jsonl(&sensor_data).unwrap();

        let line = jsonl_output.trim();
        let parsed: serde_json::Value = serde_json::from_str(line).unwrap();

        // Should have a labels object
        let labels_obj = parsed.get("labels").expect("Should have labels field");
        assert!(labels_obj.is_object(), "labels should be an object");

        // Check label values
        assert_eq!(labels_obj.get("environment").unwrap(), "production");
        assert_eq!(labels_obj.get("region").unwrap(), "europe");
    }

    #[test]
    fn test_jsonl_without_labels() {
        // Create sensor without labels
        let sensor = Sensor::new(
            Uuid::new_v4(),
            "simple_sensor".to_string(),
            SensorType::Integer,
            None,
            None,
        );

        let samples = TypedSamples::Integer(smallvec![Sample {
            datetime: SensAppDateTime::from_unix_seconds(1609459200.0),
            value: 42,
        }]);

        let sensor_data = SensorData::new(sensor, samples);
        let jsonl_output = JsonlConverter::to_jsonl(&sensor_data).unwrap();

        let line = jsonl_output.trim();
        let parsed: serde_json::Value = serde_json::from_str(line).unwrap();

        // Should have an empty labels object
        let labels_obj = parsed.get("labels").expect("Should have labels field");
        assert!(labels_obj.is_object(), "labels should be an object");
        assert!(
            labels_obj.as_object().unwrap().is_empty(),
            "labels should be empty"
        );
    }
}
