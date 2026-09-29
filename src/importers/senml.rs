use crate::datamodel::unit::Unit;
use crate::datamodel::{Sample, SensAppDateTime, Sensor, SensorData, SensorType, TypedSamples};
use anyhow::Result;
use sindit_senml::time::datetime_to_timestamp;
use sindit_senml::{SenMLResolvedRecord, SenMLValueField, parse_json};
use smallvec::SmallVec;
use std::collections::HashMap;
use uuid::Uuid;

/// Parser for SenML JSON format (RFC 8428)
/// Uses the sindit-senml crate for RFC-compliant parsing
pub struct SenMLImporter;

impl SenMLImporter {
    /// Parse SenML JSON and create sensor data grouped by sensor name
    pub fn from_senml_json(json_str: &str) -> Result<Vec<(String, SensorData)>> {
        let records = parse_json(json_str, None)?;

        if records.is_empty() {
            return Ok(Vec::new());
        }

        // Group records by sensor name
        let mut sensors_map: HashMap<String, Vec<&SenMLResolvedRecord>> = HashMap::new();

        for record in &records {
            sensors_map
                .entry(record.name.clone())
                .or_default()
                .push(record);
        }

        let export_names = Self::exported_sensor_names(json_str);
        let mut result = Vec::new();

        for (sensor_name, sensor_records) in sensors_map {
            if sensor_records.is_empty() {
                continue;
            }

            // Determine sensor type from first record with a value
            let sensor_type = Self::infer_sensor_type(sensor_records.first().unwrap())?;

            // Parse samples based on type
            let samples = match sensor_type {
                SensorType::Float => {
                    let float_samples: SmallVec<[Sample<f64>; 4]> = sensor_records
                        .iter()
                        .map(|record| Self::create_float_sample(record))
                        .collect();
                    TypedSamples::Float(float_samples)
                }
                SensorType::String => {
                    let string_samples: SmallVec<[Sample<String>; 4]> = sensor_records
                        .iter()
                        .map(|record| Self::create_string_sample(record))
                        .collect();
                    TypedSamples::String(string_samples)
                }
                SensorType::Boolean => {
                    let bool_samples: SmallVec<[Sample<bool>; 4]> = sensor_records
                        .iter()
                        .map(|record| Self::create_boolean_sample(record))
                        .collect();
                    TypedSamples::Boolean(bool_samples)
                }
                SensorType::Blob => {
                    let blob_samples: SmallVec<[Sample<Vec<u8>>; 4]> = sensor_records
                        .iter()
                        .map(|record| Self::create_blob_sample(record))
                        .collect();
                    TypedSamples::Blob(blob_samples)
                }
                _ => return Err(anyhow::anyhow!("Unsupported sensor type for SenML import")),
            };

            // Extract unit from first record if present
            let unit = sensor_records
                .first()
                .and_then(|r| r.unit.as_ref())
                .map(|u| Unit::new(u.clone(), None));

            // A UUID as the name is the identity written by the SenML exporter, so importing an
            // export goes back to the original series. Any other name derives a reproducible
            // UUID, so publishing the same sensor again appends to the same series.
            let sensor = match Uuid::parse_str(&sensor_name) {
                Ok(uuid) => Sensor::new(
                    uuid,
                    export_names
                        .get(&uuid)
                        .cloned()
                        .unwrap_or_else(|| sensor_name.clone()),
                    sensor_type,
                    unit,
                    None,
                ),
                Err(_) => Sensor::new_without_uuid(sensor_name.clone(), sensor_type, unit, None)?,
            };

            let sensor_data = SensorData::new(sensor, samples);
            result.push((sensor_name, sensor_data));
        }

        Ok(result)
    }

    /// Map the UUID base names to the `_name` extension field written by the SenML exporter.
    ///
    /// The SenML parser does not expose extension fields, so the raw JSON is scanned again.
    /// Base names are sticky in SenML: `_name` belongs to the `bn` in effect for its record.
    fn exported_sensor_names(json_str: &str) -> HashMap<Uuid, String> {
        let mut names = HashMap::new();
        let Ok(serde_json::Value::Array(records)) = serde_json::from_str(json_str) else {
            return names;
        };

        let mut base_name = String::new();
        for record in &records {
            if let Some(bn) = record.get("bn").and_then(|value| value.as_str()) {
                base_name = bn.to_string();
            }
            if let (Ok(uuid), Some(name)) = (
                Uuid::parse_str(&base_name),
                record.get("_name").and_then(|value| value.as_str()),
            ) {
                names.insert(uuid, name.to_string());
            }
        }
        names
    }

    /// Convert sindit-senml DateTime to SensAppDateTime using the time module
    fn record_time_to_sensapp_datetime(record: &SenMLResolvedRecord) -> SensAppDateTime {
        let (timestamp, precise_timestamp) = datetime_to_timestamp(&record.time);
        // Use precise timestamp if available (has sub-second precision), otherwise use integer timestamp
        let seconds = precise_timestamp.unwrap_or(timestamp as f64);
        SensAppDateTime::from_unix_seconds(seconds)
    }

    fn infer_sensor_type(record: &SenMLResolvedRecord) -> Result<SensorType> {
        match &record.value {
            Some(SenMLValueField::FloatingPoint(_)) => Ok(SensorType::Float),
            Some(SenMLValueField::StringValue(_)) => Ok(SensorType::String),
            Some(SenMLValueField::BooleanValue(_)) => Ok(SensorType::Boolean),
            Some(SenMLValueField::DataValue(_)) => Ok(SensorType::Blob),
            None => {
                // SenML defaults to 0.0 when no value is present
                Ok(SensorType::Float)
            }
        }
    }

    fn create_float_sample(record: &SenMLResolvedRecord) -> Sample<f64> {
        let datetime = Self::record_time_to_sensapp_datetime(record);
        let value = record.get_float_value().unwrap_or(0.0);
        Sample { datetime, value }
    }

    fn create_string_sample(record: &SenMLResolvedRecord) -> Sample<String> {
        let datetime = Self::record_time_to_sensapp_datetime(record);
        let value = record.get_string_value().cloned().unwrap_or_default();
        Sample { datetime, value }
    }

    fn create_boolean_sample(record: &SenMLResolvedRecord) -> Sample<bool> {
        let datetime = Self::record_time_to_sensapp_datetime(record);
        let value = record.get_bool_value().unwrap_or(false);
        Sample { datetime, value }
    }

    fn create_blob_sample(record: &SenMLResolvedRecord) -> Sample<Vec<u8>> {
        let datetime = Self::record_time_to_sensapp_datetime(record);
        let value = record.get_data_value().cloned().unwrap_or_default();
        Sample { datetime, value }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datamodel::{Sample, SensAppDateTime, Sensor, SensorType, TypedSamples};
    use crate::exporters::senml::SenMLConverter;
    use smallvec::smallvec;
    use uuid::Uuid;

    fn import(json_str: &str) -> Vec<(String, SensorData)> {
        _ = crate::config::load_configuration();
        SenMLImporter::from_senml_json(json_str).unwrap()
    }

    #[test]
    fn test_same_name_gives_same_uuid() {
        let first = import(r#"[{"bn": "temp", "bt": 1609459200.0, "v": 1.0}]"#);
        let second = import(r#"[{"bn": "temp", "bt": 1609459260.0, "v": 2.0}]"#);
        assert_eq!(first[0].1.sensor.uuid, second[0].1.sensor.uuid);
        assert_eq!(first[0].1.sensor.name, "temp");
    }

    #[test]
    fn test_different_names_or_units_give_different_uuids() {
        let temp = import(r#"[{"n": "temp", "t": 1609459200.0, "v": 1.0}]"#);
        let humidity = import(r#"[{"n": "humidity", "t": 1609459200.0, "v": 1.0}]"#);
        let temp_cel = import(r#"[{"n": "temp", "u": "Cel", "t": 1609459200.0, "v": 1.0}]"#);
        assert_ne!(temp[0].1.sensor.uuid, humidity[0].1.sensor.uuid);
        assert_ne!(temp[0].1.sensor.uuid, temp_cel[0].1.sensor.uuid);
    }

    #[test]
    fn test_uuid_base_name_is_the_series_uuid() {
        let uuid = Uuid::new_v4();
        let json = format!(r#"[{{"bn": "{uuid}", "bt": 1609459200.0, "v": 1.0}}]"#);
        let imported = import(&json);
        assert_eq!(imported[0].1.sensor.uuid, uuid);
        // Without `_name`, the name is the UUID itself
        assert_eq!(imported[0].1.sensor.name, uuid.to_string());
    }

    #[test]
    fn test_exported_name_is_used_with_uuid_base_name() {
        let uuid = Uuid::new_v4();
        let json = format!(
            r#"[{{"bn": "{uuid}", "_name": "kitchen_temp", "bt": 1609459200.0, "v": 1.0}},
                {{"t": 60.0, "v": 2.0}}]"#
        );
        let imported = import(&json);
        assert_eq!(imported.len(), 1);
        assert_eq!(imported[0].1.sensor.uuid, uuid);
        assert_eq!(imported[0].1.sensor.name, "kitchen_temp");
        assert_eq!(imported[0].1.samples.len(), 2);
    }

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

        // Verify roundtrip: bn is the UUID, so the import goes back to the original series
        let imported = SenMLImporter::from_senml_json(&json_str).unwrap();
        assert_eq!(imported.len(), 1);
        let (name, imported_data) = &imported[0];
        assert_eq!(name, &uuid.to_string());
        assert_eq!(imported_data.sensor.uuid, uuid);
        assert_eq!(imported_data.sensor.name, "blob_sensor");

        if let TypedSamples::Blob(samples) = &imported_data.samples {
            assert_eq!(samples.len(), 1);
            assert_eq!(samples[0].value, data);
        } else {
            panic!("Wrong sample type");
        }
    }

    #[test]
    fn test_float_import() {
        // Note: sindit-senml treats all numeric values as floats (per RFC 8428)
        let json_str = r#"[
            {"bn": "temp_sensor", "bt": 1609459200.0, "bver": 10, "v": 23, "t": 0},
            {"v": 24, "t": 60.0}
        ]"#;

        let imported = SenMLImporter::from_senml_json(json_str).unwrap();
        assert_eq!(imported.len(), 1);
        let (name, data) = &imported[0];
        assert_eq!(name, "temp_sensor");
        assert_eq!(data.sensor.sensor_type, SensorType::Float);

        if let TypedSamples::Float(samples) = &data.samples {
            assert_eq!(samples.len(), 2);
            assert_eq!(samples[0].value, 23.0);
            assert_eq!(samples[1].value, 24.0);
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
    fn test_unit_extraction() {
        let json_str = r#"[
            {"n": "temperature", "u": "Cel", "v": 25.5, "t": 1609459200}
        ]"#;

        let imported = SenMLImporter::from_senml_json(json_str).unwrap();
        assert_eq!(imported.len(), 1);
        let (name, data) = &imported[0];
        assert_eq!(name, "temperature");
        assert!(data.sensor.unit.is_some());
        assert_eq!(data.sensor.unit.as_ref().unwrap().name, "Cel");
    }

    #[test]
    fn test_multiple_sensors() {
        let json_str = r#"[
            {"n": "sensor1", "v": 10.0, "t": 1609459200},
            {"n": "sensor2", "v": 20.0, "t": 1609459200},
            {"n": "sensor1", "v": 11.0, "t": 1609459260}
        ]"#;

        let imported = SenMLImporter::from_senml_json(json_str).unwrap();
        assert_eq!(imported.len(), 2);

        // Find sensor1 and sensor2
        let sensor1 = imported.iter().find(|(n, _)| n == "sensor1");
        let sensor2 = imported.iter().find(|(n, _)| n == "sensor2");

        assert!(sensor1.is_some());
        assert!(sensor2.is_some());

        if let TypedSamples::Float(samples) = &sensor1.unwrap().1.samples {
            assert_eq!(samples.len(), 2);
        } else {
            panic!("Wrong sample type for sensor1");
        }

        if let TypedSamples::Float(samples) = &sensor2.unwrap().1.samples {
            assert_eq!(samples.len(), 1);
        } else {
            panic!("Wrong sample type for sensor2");
        }
    }
}
