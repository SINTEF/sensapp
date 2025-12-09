//! Conversion utilities between SensApp data types and Prometheus protocol types.
//!
//! This module provides functions to convert SensApp `SensorData` into Prometheus
//! `TimeSeries` format for the remote read API response.

use crate::datamodel::{SensorData, TypedSamples};

use super::remote_write_models::{Label, Sample as PromSample, TimeSeries};

/// Converts a SensApp `SensorData` into a Prometheus `TimeSeries`.
///
/// Only numeric sensor types (Float, Integer, Numeric) can be converted to Prometheus format.
/// Other types (String, Boolean, Location, Blob, Json) are silently skipped and return `None`.
///
/// # Arguments
/// * `sensor_data` - The SensApp sensor data to convert
///
/// # Returns
/// * `Some(TimeSeries)` - If the sensor type is numeric and conversion succeeded
/// * `None` - If the sensor type cannot be represented in Prometheus format
pub fn sensor_data_to_timeseries(sensor_data: &SensorData) -> Option<TimeSeries> {
    // Convert samples to Prometheus format - only numeric types are supported
    let samples = typed_samples_to_prom_samples(&sensor_data.samples)?;

    // Build labels from sensor name and labels
    let labels = build_prometheus_labels(&sensor_data.sensor);

    Some(TimeSeries { labels, samples })
}

/// Builds Prometheus labels from a SensApp Sensor.
///
/// Creates a `__name__` label from the sensor name, plus any additional labels
/// from the sensor's label map.
///
/// # Arguments
/// * `sensor` - The SensApp sensor to extract labels from
///
/// # Returns
/// A vector of Prometheus Label structs, sorted by name with `__name__` first.
pub fn build_prometheus_labels(sensor: &crate::datamodel::Sensor) -> Vec<Label> {
    let mut labels = Vec::with_capacity(1 + sensor.labels.len());

    // Add __name__ label from sensor name
    labels.push(Label {
        name: "__name__".to_string(),
        value: sensor.name.clone(),
    });

    // Add all other labels
    for (key, value) in sensor.labels.iter() {
        labels.push(Label {
            name: key.clone(),
            value: value.clone(),
        });
    }

    // Sort labels by name (Prometheus convention, __name__ will sort first due to underscore)
    labels.sort_by(|a, b| a.name.cmp(&b.name));

    labels
}

/// Converts SensApp TypedSamples to Prometheus Sample format.
///
/// Only numeric types (Float, Integer, Numeric) are supported.
/// Other types return None.
///
/// # Arguments
/// * `typed_samples` - The SensApp typed samples to convert
///
/// # Returns
/// * `Some(Vec<Sample>)` - For numeric types
/// * `None` - For non-numeric types
fn typed_samples_to_prom_samples(typed_samples: &TypedSamples) -> Option<Vec<PromSample>> {
    match typed_samples {
        TypedSamples::Float(samples) => {
            let prom_samples = samples
                .iter()
                .map(|s| PromSample {
                    value: s.value,
                    timestamp: datetime_to_millis(&s.datetime),
                })
                .collect();
            Some(prom_samples)
        }
        TypedSamples::Integer(samples) => {
            let prom_samples = samples
                .iter()
                .map(|s| PromSample {
                    value: s.value as f64,
                    timestamp: datetime_to_millis(&s.datetime),
                })
                .collect();
            Some(prom_samples)
        }
        TypedSamples::Numeric(samples) => {
            let prom_samples = samples
                .iter()
                .filter_map(|s| {
                    // rust_decimal::Decimal to f64 conversion
                    use rust_decimal::prelude::ToPrimitive;
                    s.value.to_f64().map(|value| PromSample {
                        value,
                        timestamp: datetime_to_millis(&s.datetime),
                    })
                })
                .collect();
            Some(prom_samples)
        }
        // Non-numeric types cannot be represented in Prometheus format
        TypedSamples::String(_)
        | TypedSamples::Boolean(_)
        | TypedSamples::Location(_)
        | TypedSamples::Blob(_)
        | TypedSamples::Json(_) => None,
    }
}

/// Converts a SensAppDateTime to Unix milliseconds.
///
/// # Arguments
/// * `datetime` - The SensApp datetime (hifitime::Epoch)
///
/// # Returns
/// Unix timestamp in milliseconds (i64)
#[inline]
fn datetime_to_millis(datetime: &crate::datamodel::SensAppDateTime) -> i64 {
    (datetime.to_unix_milliseconds()).floor() as i64
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datamodel::{Sample, SensAppDateTime, Sensor, SensorType};
    use smallvec::smallvec;

    fn create_test_sensor(name: &str, labels: Vec<(&str, &str)>) -> Sensor {
        use crate::datamodel::sensapp_vec::SensAppLabels;
        let sensor_labels: SensAppLabels = labels
            .into_iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();

        Sensor {
            uuid: uuid::Uuid::new_v4(),
            name: name.to_string(),
            sensor_type: SensorType::Float,
            unit: None,
            labels: sensor_labels,
        }
    }

    #[test]
    fn test_build_prometheus_labels() {
        let sensor =
            create_test_sensor("cpu_usage", vec![("instance", "server1"), ("job", "node")]);

        let labels = build_prometheus_labels(&sensor);

        // Should have 3 labels: __name__, instance, job
        assert_eq!(labels.len(), 3);

        // Labels should be sorted
        assert_eq!(labels[0].name, "__name__");
        assert_eq!(labels[0].value, "cpu_usage");
        assert_eq!(labels[1].name, "instance");
        assert_eq!(labels[1].value, "server1");
        assert_eq!(labels[2].name, "job");
        assert_eq!(labels[2].value, "node");
    }

    #[test]
    fn test_build_prometheus_labels_no_extra_labels() {
        let sensor = create_test_sensor("memory_usage", vec![]);

        let labels = build_prometheus_labels(&sensor);

        assert_eq!(labels.len(), 1);
        assert_eq!(labels[0].name, "__name__");
        assert_eq!(labels[0].value, "memory_usage");
    }

    #[test]
    fn test_datetime_to_millis() {
        let datetime = SensAppDateTime::from_unix_seconds(1000.0);
        let millis = datetime_to_millis(&datetime);
        assert_eq!(millis, 1_000_000);

        let datetime = SensAppDateTime::from_unix_seconds(1000.5);
        let millis = datetime_to_millis(&datetime);
        assert_eq!(millis, 1_000_500);
    }

    #[test]
    fn test_typed_samples_to_prom_samples_float() {
        let samples = TypedSamples::Float(smallvec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: 1.5,
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(2.0),
                value: 2.5,
            },
        ]);

        let prom_samples = typed_samples_to_prom_samples(&samples).unwrap();
        assert_eq!(prom_samples.len(), 2);
        assert_eq!(prom_samples[0].value, 1.5);
        assert_eq!(prom_samples[0].timestamp, 1000);
        assert_eq!(prom_samples[1].value, 2.5);
        assert_eq!(prom_samples[1].timestamp, 2000);
    }

    #[test]
    fn test_typed_samples_to_prom_samples_integer() {
        let samples = TypedSamples::Integer(smallvec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: 42,
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(2.0),
                value: 100,
            },
        ]);

        let prom_samples = typed_samples_to_prom_samples(&samples).unwrap();
        assert_eq!(prom_samples.len(), 2);
        assert_eq!(prom_samples[0].value, 42.0);
        assert_eq!(prom_samples[1].value, 100.0);
    }

    #[test]
    fn test_typed_samples_to_prom_samples_numeric() {
        use rust_decimal::Decimal;
        let samples = TypedSamples::Numeric(smallvec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: Decimal::new(123, 2), // 1.23
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(2.0),
                value: Decimal::new(456, 2), // 4.56
            },
        ]);

        let prom_samples = typed_samples_to_prom_samples(&samples).unwrap();
        assert_eq!(prom_samples.len(), 2);
        assert!((prom_samples[0].value - 1.23).abs() < 0.001);
        assert!((prom_samples[1].value - 4.56).abs() < 0.001);
    }

    #[test]
    fn test_typed_samples_to_prom_samples_string_returns_none() {
        let samples = TypedSamples::String(smallvec![Sample {
            datetime: SensAppDateTime::from_unix_seconds(1.0),
            value: "hello".to_string(),
        }]);

        assert!(typed_samples_to_prom_samples(&samples).is_none());
    }

    #[test]
    fn test_typed_samples_to_prom_samples_boolean_returns_none() {
        let samples = TypedSamples::Boolean(smallvec![Sample {
            datetime: SensAppDateTime::from_unix_seconds(1.0),
            value: true,
        }]);

        assert!(typed_samples_to_prom_samples(&samples).is_none());
    }

    #[test]
    fn test_sensor_data_to_timeseries_float() {
        let sensor = create_test_sensor("test_metric", vec![("env", "prod")]);
        let samples = TypedSamples::Float(smallvec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: 10.0,
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(2.0),
                value: 20.0,
            },
        ]);

        let sensor_data = SensorData::new(sensor, samples);
        let timeseries = sensor_data_to_timeseries(&sensor_data).unwrap();

        // Check labels
        assert_eq!(timeseries.labels.len(), 2);
        assert_eq!(timeseries.labels[0].name, "__name__");
        assert_eq!(timeseries.labels[0].value, "test_metric");
        assert_eq!(timeseries.labels[1].name, "env");
        assert_eq!(timeseries.labels[1].value, "prod");

        // Check samples
        assert_eq!(timeseries.samples.len(), 2);
        assert_eq!(timeseries.samples[0].value, 10.0);
        assert_eq!(timeseries.samples[0].timestamp, 1000);
        assert_eq!(timeseries.samples[1].value, 20.0);
        assert_eq!(timeseries.samples[1].timestamp, 2000);
    }

    #[test]
    fn test_sensor_data_to_timeseries_non_numeric_returns_none() {
        let sensor = create_test_sensor("string_metric", vec![]);
        let samples = TypedSamples::String(smallvec![Sample {
            datetime: SensAppDateTime::from_unix_seconds(1.0),
            value: "hello".to_string(),
        }]);

        let sensor_data = SensorData::new(sensor, samples);
        assert!(sensor_data_to_timeseries(&sensor_data).is_none());
    }
}
