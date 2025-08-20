mod common;

use anyhow::Result;
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch::{Batch, SingleSensorBatch};
use sensapp::datamodel::batch_builder::BatchBuilder;
use sensapp::datamodel::sensapp_vec::{SensAppLabels, SensAppVec};
use sensapp::datamodel::unit::Unit;
use sensapp::datamodel::*;
use std::sync::Arc;
use uuid::Uuid;

// Ensure configuration is loaded once for all tests in this module
static INIT: std::sync::Once = std::sync::Once::new();
fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

/// Test core data model functionality
mod core_datamodel_tests {
    use super::*;

    #[test]
    fn test_sensor_creation_and_display() {
        let uuid = Uuid::new_v4();
        let sensor = Sensor {
            uuid,
            name: "temperature_sensor".to_string(),
            sensor_type: SensorType::Float,
            unit: Some(Unit::new(
                "°C".to_string(),
                Some("Celsius temperature".to_string()),
            )),
            labels: SensAppLabels::new(),
        };

        assert_eq!(sensor.name, "temperature_sensor");
        assert_eq!(sensor.sensor_type, SensorType::Float);
        assert!(sensor.unit.is_some());
        assert_eq!(sensor.unit.as_ref().unwrap().name, "°C");

        // Test display formatting
        let display_string = format!("{}", sensor);
        assert!(display_string.contains("temperature_sensor"));
        assert!(display_string.contains("Float"));
        assert!(display_string.contains("°C"));
    }

    #[test]
    fn test_sensor_with_labels() {
        let mut labels = SensAppLabels::new();
        labels.push(("location".to_string(), "room1".to_string()));
        labels.push(("device".to_string(), "esp32".to_string()));

        let sensor = Sensor {
            uuid: Uuid::new_v4(),
            name: "temperature".to_string(),
            sensor_type: SensorType::Float,
            unit: None,
            labels,
        };

        assert_eq!(sensor.labels.len(), 2);
        let display_string = format!("{}", sensor);
        assert!(display_string.contains("labels"));
    }

    #[test]
    fn test_unit_creation_and_display() {
        let unit = Unit::new("°C".to_string(), Some("Celsius temperature".to_string()));
        assert_eq!(unit.name, "°C");
        assert_eq!(unit.description, Some("Celsius temperature".to_string()));
        assert_eq!(format!("{}", unit), "°C");

        let unit_no_desc = Unit::new("m/s".to_string(), None);
        assert_eq!(unit_no_desc.name, "m/s");
        assert_eq!(unit_no_desc.description, None);
    }

    #[test]
    fn test_typed_samples_creation() {
        // Integer samples
        let integer_samples = vec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(0.0),
                value: 42i64,
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: 43i64,
            },
        ];
        let typed_int = TypedSamples::Integer(integer_samples.into());

        if let TypedSamples::Integer(samples) = typed_int {
            assert_eq!(samples.len(), 2);
            assert_eq!(samples[0].value, 42);
        } else {
            panic!("Expected integer samples");
        }

        // Float samples
        let float_samples = vec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(0.0),
                value: 20.5f64,
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: 21.0f64,
            },
        ];
        let typed_float = TypedSamples::Float(float_samples.into());

        if let TypedSamples::Float(samples) = typed_float {
            assert_eq!(samples.len(), 2);
            assert_eq!(samples[0].value, 20.5);
        } else {
            panic!("Expected float samples");
        }

        // Boolean samples
        let bool_samples = vec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(0.0),
                value: true,
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: false,
            },
        ];
        let typed_bool = TypedSamples::Boolean(bool_samples.into());

        if let TypedSamples::Boolean(samples) = typed_bool {
            assert_eq!(samples.len(), 2);
            assert!(samples[0].value);
            assert!(!samples[1].value);
        } else {
            panic!("Expected boolean samples");
        }

        // String samples
        let string_samples = vec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(0.0),
                value: "hello".to_string(),
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: "world".to_string(),
            },
        ];
        let typed_string = TypedSamples::String(string_samples.into());

        if let TypedSamples::String(samples) = typed_string {
            assert_eq!(samples.len(), 2);
            assert_eq!(samples[0].value, "hello");
            assert_eq!(samples[1].value, "world");
        } else {
            panic!("Expected string samples");
        }
    }

    #[test]
    fn test_sensapp_datetime_operations() {
        let datetime1 = SensAppDateTime::from_unix_seconds(1609459200.0); // 2021-01-01
        let datetime2 = SensAppDateTime::from_unix_seconds(1609459260.0); // 2021-01-01 + 1 min

        assert_eq!(datetime1.to_unix_seconds(), 1609459200.0);
        assert_eq!(datetime2.to_unix_seconds(), 1609459260.0);

        // Test ordering
        assert!(datetime1 < datetime2);
        assert!(datetime2 > datetime1);
        assert_ne!(datetime1, datetime2);

        let datetime1_copy = SensAppDateTime::from_unix_seconds(1609459200.0);
        assert_eq!(datetime1, datetime1_copy);
    }

    #[test]
    fn test_sensor_type_variations() {
        let types = vec![
            SensorType::Integer,
            SensorType::Float,
            SensorType::Numeric,
            SensorType::String,
            SensorType::Boolean,
            SensorType::Location,
            SensorType::Blob,
            SensorType::Json,
        ];

        for sensor_type in types {
            let sensor = Sensor {
                uuid: Uuid::new_v4(),
                name: format!("{:?}_sensor", sensor_type),
                sensor_type,
                unit: None,
                labels: SensAppLabels::new(),
            };

            assert_eq!(sensor.sensor_type, sensor_type);
        }
    }
}

/// Test batch operations and batch builder
mod batch_tests {
    use super::*;
    use common::fixtures;

    #[tokio::test]
    async fn test_single_sensor_batch_creation() {
        let sensor = Arc::new(Sensor {
            uuid: Uuid::new_v4(),
            name: "test_sensor".to_string(),
            sensor_type: SensorType::Float,
            unit: None,
            labels: SensAppLabels::new(),
        });

        let samples = fixtures::create_test_float_samples(5, 20.0);
        let batch = SingleSensorBatch::new(sensor.clone(), samples);

        assert_eq!(batch.sensor.name, "test_sensor");
        assert_eq!(batch.len().await, 5);
    }

    #[tokio::test]
    async fn test_single_sensor_batch_append() -> Result<()> {
        let sensor = Arc::new(Sensor {
            uuid: Uuid::new_v4(),
            name: "test_sensor".to_string(),
            sensor_type: SensorType::Float,
            unit: None,
            labels: SensAppLabels::new(),
        });

        let initial_samples = fixtures::create_test_float_samples(3, 20.0);
        let mut batch = SingleSensorBatch::new(sensor.clone(), initial_samples);

        assert_eq!(batch.len().await, 3);

        // Append more samples of the same type
        let additional_samples = fixtures::create_test_float_samples(2, 25.0);
        batch.append(additional_samples).await?;

        assert_eq!(batch.len().await, 5);
        Ok(())
    }

    #[tokio::test]
    async fn test_single_sensor_batch_append_type_mismatch() {
        let sensor = Arc::new(Sensor {
            uuid: Uuid::new_v4(),
            name: "test_sensor".to_string(),
            sensor_type: SensorType::Float,
            unit: None,
            labels: SensAppLabels::new(),
        });

        let float_samples = fixtures::create_test_float_samples(3, 20.0);
        let mut batch = SingleSensorBatch::new(sensor.clone(), float_samples);

        // Try to append integer samples to float batch - should fail
        let integer_samples = fixtures::create_test_integer_samples(2, 42);
        let result = batch.append(integer_samples).await;

        assert!(
            result.is_err(),
            "Should fail when appending incompatible types"
        );
    }

    #[tokio::test]
    async fn test_batch_creation_and_length() {
        let sensor1 = Arc::new(Sensor {
            uuid: Uuid::new_v4(),
            name: "sensor1".to_string(),
            sensor_type: SensorType::Float,
            unit: None,
            labels: SensAppLabels::new(),
        });

        let sensor2 = Arc::new(Sensor {
            uuid: Uuid::new_v4(),
            name: "sensor2".to_string(),
            sensor_type: SensorType::Integer,
            unit: None,
            labels: SensAppLabels::new(),
        });

        let samples1 = fixtures::create_test_float_samples(5, 20.0);
        let samples2 = fixtures::create_test_integer_samples(3, 100);

        let batch1 = SingleSensorBatch::new(sensor1, samples1);
        let batch2 = SingleSensorBatch::new(sensor2, samples2);

        let mut sensors = SensAppVec::new();
        sensors.push(batch1);
        sensors.push(batch2);

        let batch = Batch::new(sensors);
        assert_eq!(batch.len().await, 8); // 5 + 3 samples total
    }

    #[tokio::test]
    async fn test_empty_batch() {
        let batch = Batch::default();
        assert_eq!(batch.len().await, 0);
    }
}

/// Test batch builder functionality
mod batch_builder_tests {
    use super::*;
    use common::fixtures;

    #[tokio::test]
    async fn test_batch_builder_creation() -> Result<()> {
        ensure_config();

        let batch_builder = BatchBuilder::new();
        assert!(
            batch_builder.is_ok(),
            "Batch builder should be created successfully"
        );
        Ok(())
    }

    #[tokio::test]
    async fn test_batch_builder_add_samples() -> Result<()> {
        ensure_config();

        let mut batch_builder = BatchBuilder::new()?;
        let sensor = fixtures::create_test_sensor("temperature", SensorType::Float);
        let samples = fixtures::create_test_float_samples(5, 20.0);

        batch_builder.add(sensor.clone(), samples).await?;

        // Add more samples for the same sensor
        let more_samples = fixtures::create_test_float_samples(3, 25.0);
        batch_builder.add(sensor.clone(), more_samples).await?;

        Ok(())
    }

    #[tokio::test]
    async fn test_batch_builder_multiple_sensors() -> Result<()> {
        ensure_config();

        let mut batch_builder = BatchBuilder::new()?;

        let temp_sensor = fixtures::create_test_sensor("temperature", SensorType::Float);
        let humidity_sensor = fixtures::create_test_sensor("humidity", SensorType::Float);

        let temp_samples = fixtures::create_test_float_samples(5, 20.0);
        let humidity_samples = fixtures::create_test_float_samples(5, 65.0);

        batch_builder.add(temp_sensor, temp_samples).await?;
        batch_builder.add(humidity_sensor, humidity_samples).await?;

        Ok(())
    }
}

/// Test typed samples edge cases and conversions
mod typed_samples_tests {
    use super::*;
    use geo::Point;
    use rust_decimal::Decimal;
    use std::str::FromStr;

    #[test]
    fn test_location_samples() {
        let location1 = Point::new(10.7522, 59.9139); // Oslo coordinates
        let location2 = Point::new(-74.0060, 40.7128); // NYC coordinates

        let location_samples = vec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(0.0),
                value: location1,
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: location2,
            },
        ];

        let typed_location = TypedSamples::Location(location_samples.into());

        if let TypedSamples::Location(samples) = typed_location {
            assert_eq!(samples.len(), 2);
            assert_eq!(samples[0].value.x(), 10.7522);
            assert_eq!(samples[0].value.y(), 59.9139);
            assert_eq!(samples[1].value.x(), -74.0060);
            assert_eq!(samples[1].value.y(), 40.7128);
        } else {
            panic!("Expected location samples");
        }
    }

    #[test]
    fn test_numeric_samples_precision() {
        let precise_values = [
            Decimal::from_str("123.456789").unwrap(),
            Decimal::from_str("987.654321").unwrap(),
            Decimal::from_str("0.000001").unwrap(),
        ];

        let numeric_samples = vec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(0.0),
                value: precise_values[0],
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: precise_values[1],
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(2.0),
                value: precise_values[2],
            },
        ];

        let typed_numeric = TypedSamples::Numeric(numeric_samples.into());

        if let TypedSamples::Numeric(samples) = typed_numeric {
            assert_eq!(samples.len(), 3);
            assert_eq!(samples[0].value.to_string(), "123.456789");
            assert_eq!(samples[1].value.to_string(), "987.654321");
            assert_eq!(samples[2].value.to_string(), "0.000001");
        } else {
            panic!("Expected numeric samples");
        }
    }

    #[test]
    fn test_json_samples() {
        let json1 = serde_json::json!({"temperature": 20.5, "humidity": 65});
        let json2 = serde_json::json!({"temperature": 21.0, "humidity": 64});

        let json_samples = vec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(0.0),
                value: json1,
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: json2,
            },
        ];

        let typed_json = TypedSamples::Json(json_samples.into());

        if let TypedSamples::Json(samples) = typed_json {
            assert_eq!(samples.len(), 2);
            assert_eq!(samples[0].value["temperature"], 20.5);
            assert_eq!(samples[1].value["humidity"], 64);
        } else {
            panic!("Expected JSON samples");
        }
    }

    #[test]
    fn test_blob_samples() {
        let blob1 = vec![0x01, 0x02, 0x03, 0x04];
        let blob2 = vec![0xFF, 0xFE, 0xFD, 0xFC];

        let blob_samples = vec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(0.0),
                value: blob1.clone(),
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: blob2.clone(),
            },
        ];

        let typed_blob = TypedSamples::Blob(blob_samples.into());

        if let TypedSamples::Blob(samples) = typed_blob {
            assert_eq!(samples.len(), 2);
            assert_eq!(samples[0].value, blob1);
            assert_eq!(samples[1].value, blob2);
        } else {
            panic!("Expected blob samples");
        }
    }
}

/// Test SensAppVec functionality
mod sensapp_vec_tests {
    use super::*;

    #[test]
    fn test_sensapp_vec_basic_operations() {
        let mut vec = SensAppVec::new();
        assert_eq!(vec.len(), 0);
        assert!(vec.is_empty());

        vec.push("item1".to_string());
        vec.push("item2".to_string());
        assert_eq!(vec.len(), 2);
        assert!(!vec.is_empty());

        // Test iteration
        let items: Vec<String> = vec.iter().cloned().collect();
        assert_eq!(items, vec!["item1".to_string(), "item2".to_string()]);
    }

    #[test]
    fn test_sensapp_vec_from_iter() {
        let items = vec!["a", "b", "c"];
        let vec = SensAppVec::from_iter(items.into_iter().map(|s| s.to_string()));

        assert_eq!(vec.len(), 3);
        assert_eq!(vec.iter().next().unwrap(), "a");
    }

    #[test]
    fn test_sensapp_labels() {
        let mut labels = SensAppLabels::new();
        assert_eq!(labels.len(), 0);

        labels.push(("key1".to_string(), "value1".to_string()));
        labels.push(("key2".to_string(), "value2".to_string()));

        assert_eq!(labels.len(), 2);

        // Test that we can find our labels
        let label_pairs: Vec<_> = labels.iter().collect();
        assert!(label_pairs.contains(&&("key1".to_string(), "value1".to_string())));
        assert!(label_pairs.contains(&&("key2".to_string(), "value2".to_string())));
    }
}

/// Test edge cases and error conditions
mod edge_cases_tests {
    use super::*;

    #[test]
    fn test_sensor_with_very_long_name() {
        let long_name = "a".repeat(1000);
        let sensor = Sensor {
            uuid: Uuid::new_v4(),
            name: long_name.clone(),
            sensor_type: SensorType::String,
            unit: None,
            labels: SensAppLabels::new(),
        };

        assert_eq!(sensor.name.len(), 1000);
        assert_eq!(sensor.name, long_name);
    }

    #[test]
    fn test_samples_with_extreme_timestamps() {
        // Very old timestamp (Unix epoch start)
        let old_sample = Sample {
            datetime: SensAppDateTime::from_unix_seconds(0.0),
            value: 42i64,
        };

        // Far future timestamp
        let future_sample = Sample {
            datetime: SensAppDateTime::from_unix_seconds(4000000000.0), // Year 2096
            value: 84i64,
        };

        let samples = vec![old_sample, future_sample];
        let typed_samples = TypedSamples::Integer(samples.into());

        if let TypedSamples::Integer(samples) = typed_samples {
            assert_eq!(samples.len(), 2);
            assert_eq!(samples[0].datetime.to_unix_seconds(), 0.0);
            assert_eq!(samples[1].datetime.to_unix_seconds(), 4000000000.0);
        } else {
            panic!("Expected integer samples");
        }
    }

    #[test]
    fn test_empty_sensor_values() {
        // Test sensors with empty string values
        let empty_samples = vec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(0.0),
                value: "".to_string(),
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(1.0),
                value: "non-empty".to_string(),
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(2.0),
                value: "".to_string(),
            },
        ];

        let typed_string = TypedSamples::String(empty_samples.into());

        if let TypedSamples::String(samples) = typed_string {
            assert_eq!(samples.len(), 3);
            assert_eq!(samples[0].value, "");
            assert_eq!(samples[1].value, "non-empty");
            assert_eq!(samples[2].value, "");
        } else {
            panic!("Expected string samples");
        }
    }
}
