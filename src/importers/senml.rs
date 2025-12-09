use crate::datamodel::{Sample, SensAppDateTime, Sensor, SensorData, SensorType, TypedSamples};
use anyhow::Result;
use base64::{Engine as _, engine::general_purpose};
use serde_json::{Map, Value};
use smallvec::SmallVec;
use uuid::Uuid;

/// Parser for SenML JSON format (RFC 8428)
pub struct SenMLImporter;

impl SenMLImporter {
    /// Parse SenML JSON and create sensor data grouped by sensor name
    pub fn from_senml_json(json_str: &str) -> Result<Vec<(String, SensorData)>> {
        let json_value: Value = serde_json::from_str(json_str)?;
        let senml_array = json_value
            .as_array()
            .ok_or_else(|| anyhow::anyhow!("SenML must be a JSON array"))?;

        if senml_array.is_empty() {
            return Ok(Vec::new());
        }

        // Group records by base name (sensor name)
        let mut sensors_map: std::collections::HashMap<String, Vec<&Map<String, Value>>> =
            std::collections::HashMap::new();
        let mut current_base_name = String::new();
        let mut current_base_time: Option<f64> = None;

        for record in senml_array {
            let obj = record
                .as_object()
                .ok_or_else(|| anyhow::anyhow!("Each SenML record must be an object"))?;

            // Update base name if present
            if let Some(bn) = obj.get("bn").and_then(|v| v.as_str()) {
                current_base_name = bn.to_string();
            }

            // Update base time if present
            if let Some(bt) = obj.get("bt").and_then(|v| v.as_f64()) {
                current_base_time = Some(bt);
            }

            // Use base name or individual name
            let sensor_name = if let Some(name) = obj.get("n").and_then(|v| v.as_str()) {
                format!("{}{}", current_base_name, name)
            } else {
                current_base_name.clone()
            };

            if sensor_name.is_empty() {
                return Err(anyhow::anyhow!("SenML record must have a name (bn or n)"));
            }

            sensors_map.entry(sensor_name).or_default().push(obj);
        }

        let mut result = Vec::new();

        for (sensor_name, records) in sensors_map {
            if records.is_empty() {
                continue;
            }

            // Determine sensor type from first record with a value
            let sensor_type = Self::infer_sensor_type(records.first().unwrap())?;

            // Parse samples based on type
            let samples = match sensor_type {
                SensorType::Integer => {
                    let int_samples: Result<SmallVec<[Sample<i64>; 4]>> = records
                        .iter()
                        .map(|record| Self::parse_integer_sample(record, current_base_time))
                        .collect();
                    TypedSamples::Integer(int_samples?)
                }
                SensorType::Float => {
                    let float_samples: Result<SmallVec<[Sample<f64>; 4]>> = records
                        .iter()
                        .map(|record| Self::parse_float_sample(record, current_base_time))
                        .collect();
                    TypedSamples::Float(float_samples?)
                }
                SensorType::String => {
                    let string_samples: Result<SmallVec<[Sample<String>; 4]>> = records
                        .iter()
                        .map(|record| Self::parse_string_sample(record, current_base_time))
                        .collect();
                    TypedSamples::String(string_samples?)
                }
                SensorType::Boolean => {
                    let bool_samples: Result<SmallVec<[Sample<bool>; 4]>> = records
                        .iter()
                        .map(|record| Self::parse_boolean_sample(record, current_base_time))
                        .collect();
                    TypedSamples::Boolean(bool_samples?)
                }
                SensorType::Blob => {
                    let blob_samples: Result<SmallVec<[Sample<Vec<u8>>; 4]>> = records
                        .iter()
                        .map(|record| Self::parse_blob_sample(record, current_base_time))
                        .collect();
                    TypedSamples::Blob(blob_samples?)
                }
                _ => return Err(anyhow::anyhow!("Unsupported sensor type for SenML import")),
            };

            let sensor = Sensor {
                uuid: Uuid::new_v4(),
                name: sensor_name.clone(),
                sensor_type,
                unit: None, // Could be extracted from SenML "u" field if needed
                labels: SmallVec::new(),
            };

            let sensor_data = SensorData::new(sensor, samples);
            result.push((sensor_name, sensor_data));
        }

        Ok(result)
    }

    fn infer_sensor_type(record: &Map<String, Value>) -> Result<SensorType> {
        if record.contains_key("v") {
            // Numeric value
            if let Some(value) = record.get("v") {
                if value.is_i64() {
                    Ok(SensorType::Integer)
                } else if value.is_f64() {
                    Ok(SensorType::Float)
                } else {
                    Err(anyhow::anyhow!("Invalid numeric value in SenML"))
                }
            } else {
                Err(anyhow::anyhow!("Missing value in SenML record"))
            }
        } else if record.contains_key("vs") {
            Ok(SensorType::String)
        } else if record.contains_key("vb") {
            Ok(SensorType::Boolean)
        } else if record.contains_key("vd") {
            Ok(SensorType::Blob)
        } else {
            Err(anyhow::anyhow!(
                "SenML record must contain a value (v, vs, vb, etc.)"
            ))
        }
    }

    fn parse_timestamp(
        record: &Map<String, Value>,
        base_time: Option<f64>,
    ) -> Result<SensAppDateTime> {
        let time = if let Some(t) = record.get("t").and_then(|v| v.as_f64()) {
            // Relative time
            base_time.unwrap_or(0.0) + t
        } else {
            // Use base time or current time
            base_time.unwrap_or_else(|| {
                std::time::SystemTime::now()
                    .duration_since(std::time::UNIX_EPOCH)
                    .unwrap()
                    .as_secs() as f64
            })
        };

        // Convert Unix timestamp to SensAppDateTime
        Ok(SensAppDateTime::from_unix_seconds(time))
    }

    fn parse_integer_sample(
        record: &Map<String, Value>,
        base_time: Option<f64>,
    ) -> Result<Sample<i64>> {
        let datetime = Self::parse_timestamp(record, base_time)?;
        let value = record
            .get("v")
            .and_then(|v| v.as_i64())
            .ok_or_else(|| anyhow::anyhow!("Missing or invalid integer value"))?;
        Ok(Sample { datetime, value })
    }

    fn parse_float_sample(
        record: &Map<String, Value>,
        base_time: Option<f64>,
    ) -> Result<Sample<f64>> {
        let datetime = Self::parse_timestamp(record, base_time)?;
        let value = record
            .get("v")
            .and_then(|v| v.as_f64())
            .ok_or_else(|| anyhow::anyhow!("Missing or invalid float value"))?;
        Ok(Sample { datetime, value })
    }

    fn parse_string_sample(
        record: &Map<String, Value>,
        base_time: Option<f64>,
    ) -> Result<Sample<String>> {
        let datetime = Self::parse_timestamp(record, base_time)?;
        let value = record
            .get("vs")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing or invalid string value"))?
            .to_string();
        Ok(Sample { datetime, value })
    }

    fn parse_boolean_sample(
        record: &Map<String, Value>,
        base_time: Option<f64>,
    ) -> Result<Sample<bool>> {
        let datetime = Self::parse_timestamp(record, base_time)?;
        let value = record
            .get("vb")
            .and_then(|v| v.as_bool())
            .ok_or_else(|| anyhow::anyhow!("Missing or invalid boolean value"))?;
        Ok(Sample { datetime, value })
    }

    fn parse_blob_sample(
        record: &Map<String, Value>,
        base_time: Option<f64>,
    ) -> Result<Sample<Vec<u8>>> {
        let datetime = Self::parse_timestamp(record, base_time)?;
        let value_str = record
            .get("vd")
            .and_then(|v| v.as_str())
            .ok_or_else(|| anyhow::anyhow!("Missing or invalid blob value"))?;

        let value = general_purpose::URL_SAFE_NO_PAD
            .decode(value_str)
            .map_err(|e| anyhow::anyhow!("Failed to decode blob value: {}", e))?;

        Ok(Sample { datetime, value })
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datamodel::{Sample, SensAppDateTime, Sensor, SensorType, TypedSamples};
    use crate::exporters::senml::SenMLConverter;
    use smallvec::smallvec;
    use uuid::Uuid;

    #[test]
    fn test_blob_samples_senml_roundtrip() {
        let uuid = Uuid::new_v4();
        let sensor = Sensor::new(
            uuid,
            "blob_sensor".to_string(),
            SensorType::Blob,
            None,
            None,
        );

        let data = vec![1u8, 2, 3, 4, 255];
        let samples = TypedSamples::Blob(smallvec![Sample {
            datetime: SensAppDateTime::from_unix_seconds(1609459200.0),
            value: data.clone(),
        }]);

        let sensor_data = SensorData::new(sensor, samples);
        let senml_json = SenMLConverter::to_senml_json(&sensor_data).unwrap();

        // Verify JSON structure
        let json_str = senml_json.to_string();
        assert!(json_str.contains("vd"));
        assert!(json_str.contains("_name")); // Sensor name is in _name field
        assert!(json_str.contains("blob_sensor"));

        // Verify roundtrip - note: bn is now the UUID, so imported "name" will be the UUID
        let imported = SenMLImporter::from_senml_json(&json_str).unwrap();
        assert_eq!(imported.len(), 1);
        let (name, imported_data) = &imported[0];
        // The "name" from import is actually the bn field, which is now the UUID
        assert_eq!(name, &uuid.to_string());

        if let TypedSamples::Blob(samples) = &imported_data.samples {
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, data);
        } else {
            panic!("Wrong sample type");
        }
    }

    #[test]
    fn test_integer_import() {
        let json_str = r#"[
            {"bn": "temp_sensor", "bt": 1609459200.0, "bver": 10, "v": 23, "t": 0},
            {"v": 24, "t": 60.0}
        ]"#;

        let imported = SenMLImporter::from_senml_json(json_str).unwrap();
        assert_eq!(imported.len(), 1);
        let (name, data) = &imported[0];
        assert_eq!(name, "temp_sensor");
        assert_eq!(data.sensor.sensor_type, SensorType::Integer);

        if let TypedSamples::Integer(samples) = &data.samples {
            assert_eq!(samples.len(), 2);
            assert_eq!(samples[0].value, 23);
            assert_eq!(samples[1].value, 24);
        } else {
            panic!("Wrong sample type");
        }
    }

    #[test]
    fn test_string_import() {
        let json_str = r#"[
            {"bn": "status_sensor", "bt": 1609459200.0, "vs": "active", "t": 0}
        ]"#;

        let imported = SenMLImporter::from_senml_json(json_str).unwrap();
        assert_eq!(imported.len(), 1);
        let (name, data) = &imported[0];
        assert_eq!(name, "status_sensor");
        assert_eq!(data.sensor.sensor_type, SensorType::String);

        if let TypedSamples::String(samples) = &data.samples {
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, "active");
        } else {
            panic!("Wrong sample type");
        }
    }

    #[test]
    fn test_boolean_import() {
        let json_str = r#"[
            {"bn": "door_sensor", "bt": 1609459200.0, "vb": true, "t": 0}
        ]"#;

        let imported = SenMLImporter::from_senml_json(json_str).unwrap();
        assert_eq!(imported.len(), 1);
        let (name, data) = &imported[0];
        assert_eq!(name, "door_sensor");
        assert_eq!(data.sensor.sensor_type, SensorType::Boolean);

        if let TypedSamples::Boolean(samples) = &data.samples {
            assert_eq!(samples.len(), 1);
            assert!(samples[0].value);
        } else {
            panic!("Wrong sample type");
        }
    }

    #[test]
    fn test_empty_array_import() {
        let json_str = "[]";
        let imported = SenMLImporter::from_senml_json(json_str).unwrap();
        assert!(imported.is_empty());
    }

    #[test]
    fn test_invalid_json() {
        let json_str = "not valid json";
        let result = SenMLImporter::from_senml_json(json_str);
        assert!(result.is_err());
    }

    #[test]
    fn test_not_array() {
        let json_str = r#"{"bn": "test"}"#;
        let result = SenMLImporter::from_senml_json(json_str);
        assert!(result.is_err());
    }
}
