use crate::datamodel::{Sample, SensAppDateTime, SensorData, TypedSamples};
use anyhow::Result;
use base64::{Engine as _, engine::general_purpose};
use serde_json::{Map, Value, json};

/// Helper function to get milliseconds from SensAppDateTime
fn datetime_to_ms(datetime: &SensAppDateTime) -> i64 {
    (datetime.to_unix_seconds() * 1000.0) as i64 + datetime.milliseconds() as i64
}

/// Converter for SensorData to SenML JSON format
pub struct SenMLConverter;

impl SenMLConverter {
    /// Convert SensorData to SenML JSON format according to RFC 8428
    pub fn to_senml_json(sensor_data: &SensorData) -> Result<Value> {
        let mut senml_records = Vec::new();

        // Get the first timestamp to use as base time
        let (base_time, samples_iter) = match &sensor_data.samples {
            TypedSamples::Integer(samples) => {
                let first_time = samples
                    .first()
                    .map(|s| datetime_to_ms(&s.datetime))
                    .unwrap_or(0);
                (
                    first_time,
                    Self::process_integer_samples(samples, first_time),
                )
            }
            TypedSamples::Numeric(samples) => {
                let first_time = samples
                    .first()
                    .map(|s| datetime_to_ms(&s.datetime))
                    .unwrap_or(0);
                (
                    first_time,
                    Self::process_numeric_samples(samples, first_time),
                )
            }
            TypedSamples::Float(samples) => {
                let first_time = samples
                    .first()
                    .map(|s| datetime_to_ms(&s.datetime))
                    .unwrap_or(0);
                (first_time, Self::process_float_samples(samples, first_time))
            }
            TypedSamples::String(samples) => {
                let first_time = samples
                    .first()
                    .map(|s| datetime_to_ms(&s.datetime))
                    .unwrap_or(0);
                (
                    first_time,
                    Self::process_string_samples(samples, first_time),
                )
            }
            TypedSamples::Boolean(samples) => {
                let first_time = samples
                    .first()
                    .map(|s| datetime_to_ms(&s.datetime))
                    .unwrap_or(0);
                (
                    first_time,
                    Self::process_boolean_samples(samples, first_time),
                )
            }
            TypedSamples::Location(samples) => {
                let first_time = samples
                    .first()
                    .map(|s| datetime_to_ms(&s.datetime))
                    .unwrap_or(0);
                (
                    first_time,
                    Self::process_location_samples(samples, first_time),
                )
            }
            TypedSamples::Json(samples) => {
                let first_time = samples
                    .first()
                    .map(|s| datetime_to_ms(&s.datetime))
                    .unwrap_or(0);
                (first_time, Self::process_json_samples(samples, first_time))
            }
            TypedSamples::Blob(samples) => {
                let first_time = samples
                    .first()
                    .map(|s| datetime_to_ms(&s.datetime))
                    .unwrap_or(0);
                (first_time, Self::process_blob_samples(samples, first_time))
            }
        };

        // Create base record with sensor metadata
        let mut base_record = Map::new();
        base_record.insert("bn".to_string(), json!(sensor_data.sensor.name));
        base_record.insert("bt".to_string(), json!(base_time as f64 / 1000.0)); // Convert to seconds
        base_record.insert("bver".to_string(), json!(10)); // SenML version

        if let Some(ref unit) = sensor_data.sensor.unit {
            base_record.insert("bu".to_string(), json!(unit.name));
        }

        // Add labels as extensions (non-standard but useful)
        if !sensor_data.sensor.labels.is_empty() {
            let mut labels_obj = Map::new();
            for (key, value) in &sensor_data.sensor.labels {
                labels_obj.insert(key.clone(), json!(value));
            }
            base_record.insert("_labels".to_string(), json!(labels_obj));
        }

        // Add the first sample to the base record if we have samples
        if let Some(first_sample) = samples_iter.first() {
            base_record.extend(first_sample.as_object().unwrap().clone());
            senml_records.push(json!(base_record));

            // Add remaining samples
            senml_records.extend(samples_iter.into_iter().skip(1));
        } else {
            // No samples, just the base record
            senml_records.push(json!(base_record));
        }

        Ok(json!(senml_records))
    }

    fn process_integer_samples(
        samples: &crate::datamodel::SensAppVec<Sample<i64>>,
        base_time_ms: i64,
    ) -> Vec<Value> {
        samples
            .iter()
            .enumerate()
            .map(|(i, sample)| {
                let mut record = Map::new();
                if i == 0 {
                    record.insert("t".to_string(), json!(0)); // First sample at base time
                } else {
                    let relative_time =
                        (datetime_to_ms(&sample.datetime) - base_time_ms) as f64 / 1000.0;
                    record.insert("t".to_string(), json!(relative_time));
                }
                record.insert("v".to_string(), json!(sample.value));
                json!(record)
            })
            .collect()
    }

    fn process_numeric_samples(
        samples: &crate::datamodel::SensAppVec<Sample<rust_decimal::Decimal>>,
        base_time_ms: i64,
    ) -> Vec<Value> {
        samples
            .iter()
            .enumerate()
            .map(|(i, sample)| {
                let mut record = Map::new();
                if i == 0 {
                    record.insert("t".to_string(), json!(0));
                } else {
                    let relative_time =
                        (datetime_to_ms(&sample.datetime) - base_time_ms) as f64 / 1000.0;
                    record.insert("t".to_string(), json!(relative_time));
                }
                record.insert(
                    "v".to_string(),
                    json!(sample.value.to_string().parse::<f64>().unwrap_or(0.0)),
                );
                json!(record)
            })
            .collect()
    }

    fn process_float_samples(
        samples: &crate::datamodel::SensAppVec<Sample<f64>>,
        base_time_ms: i64,
    ) -> Vec<Value> {
        samples
            .iter()
            .enumerate()
            .map(|(i, sample)| {
                let mut record = Map::new();
                if i == 0 {
                    record.insert("t".to_string(), json!(0));
                } else {
                    let relative_time =
                        (datetime_to_ms(&sample.datetime) - base_time_ms) as f64 / 1000.0;
                    record.insert("t".to_string(), json!(relative_time));
                }
                record.insert("v".to_string(), json!(sample.value));
                json!(record)
            })
            .collect()
    }

    fn process_string_samples(
        samples: &crate::datamodel::SensAppVec<Sample<String>>,
        base_time_ms: i64,
    ) -> Vec<Value> {
        samples
            .iter()
            .enumerate()
            .map(|(i, sample)| {
                let mut record = Map::new();
                if i == 0 {
                    record.insert("t".to_string(), json!(0));
                } else {
                    let relative_time =
                        (datetime_to_ms(&sample.datetime) - base_time_ms) as f64 / 1000.0;
                    record.insert("t".to_string(), json!(relative_time));
                }
                record.insert("vs".to_string(), json!(sample.value));
                json!(record)
            })
            .collect()
    }

    fn process_boolean_samples(
        samples: &crate::datamodel::SensAppVec<Sample<bool>>,
        base_time_ms: i64,
    ) -> Vec<Value> {
        samples
            .iter()
            .enumerate()
            .map(|(i, sample)| {
                let mut record = Map::new();
                if i == 0 {
                    record.insert("t".to_string(), json!(0));
                } else {
                    let relative_time =
                        (datetime_to_ms(&sample.datetime) - base_time_ms) as f64 / 1000.0;
                    record.insert("t".to_string(), json!(relative_time));
                }
                record.insert("vb".to_string(), json!(sample.value));
                json!(record)
            })
            .collect()
    }

    fn process_location_samples(
        samples: &crate::datamodel::SensAppVec<Sample<geo::Point>>,
        base_time_ms: i64,
    ) -> Vec<Value> {
        // For location, we create separate lat/lon records
        let mut records = Vec::new();

        for (i, sample) in samples.iter().enumerate() {
            let relative_time = if i == 0 {
                0.0
            } else {
                (datetime_to_ms(&sample.datetime) - base_time_ms) as f64 / 1000.0
            };

            // Latitude record
            let mut lat_record = Map::new();
            lat_record.insert("t".to_string(), json!(relative_time));
            lat_record.insert("n".to_string(), json!("lat"));
            lat_record.insert("v".to_string(), json!(sample.value.y()));
            records.push(json!(lat_record));

            // Longitude record
            let mut lon_record = Map::new();
            lon_record.insert("t".to_string(), json!(relative_time));
            lon_record.insert("n".to_string(), json!("lon"));
            lon_record.insert("v".to_string(), json!(sample.value.x()));
            records.push(json!(lon_record));
        }

        records
    }

    fn process_json_samples(
        samples: &crate::datamodel::SensAppVec<Sample<serde_json::Value>>,
        base_time_ms: i64,
    ) -> Vec<Value> {
        samples
            .iter()
            .enumerate()
            .map(|(i, sample)| {
                let mut record = Map::new();
                if i == 0 {
                    record.insert("t".to_string(), json!(0));
                } else {
                    let relative_time =
                        (datetime_to_ms(&sample.datetime) - base_time_ms) as f64 / 1000.0;
                    record.insert("t".to_string(), json!(relative_time));
                }
                // Convert JSON value to string for SenML
                record.insert("vs".to_string(), json!(sample.value.to_string()));
                json!(record)
            })
            .collect()
    }

    fn process_blob_samples(
        samples: &crate::datamodel::SensAppVec<Sample<Vec<u8>>>,
        base_time_ms: i64,
    ) -> Vec<Value> {
        samples
            .iter()
            .enumerate()
            .map(|(i, sample)| {
                let mut record = Map::new();
                if i == 0 {
                    record.insert("t".to_string(), json!(0));
                } else {
                    let relative_time =
                        (datetime_to_ms(&sample.datetime) - base_time_ms) as f64 / 1000.0;
                    record.insert("t".to_string(), json!(relative_time));
                }
                // Encode binary data as base64 for SenML
                record.insert(
                    "vd".to_string(),
                    json!(general_purpose::STANDARD.encode(&sample.value)),
                );
                json!(record)
            })
            .collect()
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
    fn test_integer_samples_to_senml() {
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
        let senml_json = SenMLConverter::to_senml_json(&sensor_data).unwrap();

        assert!(senml_json.is_array());
        let records = senml_json.as_array().unwrap();
        assert_eq!(records.len(), 2);

        // Check base record
        let base_record = &records[0];
        assert_eq!(base_record["bn"], "test_sensor");
        assert_eq!(base_record["bu"], "Celsius");
        assert_eq!(base_record["bver"], 10);
        assert_eq!(base_record["bt"], 1609459200.0);
        assert_eq!(base_record["v"], 23);
        assert_eq!(base_record["t"], 0);

        // Check second record
        let second_record = &records[1];
        assert_eq!(second_record["v"], 24);
        assert_eq!(second_record["t"], 60.0); // 1 minute later
    }
}
