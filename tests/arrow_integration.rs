mod common;

use anyhow::Result;
use axum::http::StatusCode;
use common::db::DbHelpers;
use common::http::TestApp;
use common::{TestDb, TestHelpers, fixtures};
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::{Sample, SensAppDateTime, Sensor, SensorData, SensorType, TypedSamples};
use sensapp::exporters::ArrowConverter;
use serial_test::serial;
use smallvec::smallvec;
use uuid::Uuid;

// Ensure configuration is loaded once for all tests in this module
static INIT: std::sync::Once = std::sync::Once::new();
fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

/// Test Arrow data export functionality
mod export_tests {
    use super::*;

    #[tokio::test]
    #[serial]
    async fn test_export_integer_data_as_arrow() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Ingest some integer sensor data
        let (csv_data, sensor_name) = fixtures::temperature_sensor_csv_with_name();
        app.post_csv("/sensors/publish", &csv_data).await?;

        // Get the sensor UUID
        let sensor = DbHelpers::get_sensor_by_name(&storage, &sensor_name)
            .await?
            .expect("Temperature sensor should exist");

        // Export as Arrow format
        let query_path = format!("/series/{}?format=arrow", sensor.uuid);
        let response = app.get(&query_path).await?;

        // Verify response
        response
            .assert_status(StatusCode::OK)
            .assert_content_type("application/vnd.apache.arrow.file");

        // Verify Arrow file format
        let body_bytes = response.body_bytes();
        assert!(!body_bytes.is_empty());
        assert_eq!(&body_bytes[0..6], b"ARROW1"); // Arrow magic number

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_export_multiple_data_types_as_arrow() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Ingest mixed sensor data
        let (csv_data, temperature_name, humidity_name) = fixtures::multi_sensor_csv_with_names();
        app.post_csv("/sensors/publish", &csv_data).await?;

        // Export temperature sensor as Arrow
        let temp_sensor = DbHelpers::get_sensor_by_name(&storage, &temperature_name)
            .await?
            .expect("Temperature sensor should exist");

        let query_path = format!("/series/{}?format=arrow", temp_sensor.uuid);
        let response = app.get(&query_path).await?;

        response.assert_status(StatusCode::OK);
        let body_bytes = response.body_bytes();
        assert!(!body_bytes.is_empty());
        assert_eq!(&body_bytes[0..6], b"ARROW1");

        // Export humidity sensor as Arrow
        let humidity_sensor = DbHelpers::get_sensor_by_name(&storage, &humidity_name)
            .await?
            .expect("Humidity sensor should exist");

        let query_path = format!("/series/{}?format=arrow", humidity_sensor.uuid);
        let response = app.get(&query_path).await?;

        response.assert_status(StatusCode::OK);
        let body_bytes = response.body_bytes();
        assert!(!body_bytes.is_empty());
        assert_eq!(&body_bytes[0..6], b"ARROW1");

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_export_arrow_with_time_range() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Ingest sensor data
        let (csv_data, sensor_name) = fixtures::temperature_sensor_csv_with_name();
        app.post_csv("/sensors/publish", &csv_data).await?;

        // Get the sensor UUID
        let sensor = DbHelpers::get_sensor_by_name(&storage, &sensor_name)
            .await?
            .expect("Temperature sensor should exist");

        // Export with time range
        let start_time = "2024-01-01T00:00:00Z";
        let end_time = "2024-12-31T23:59:59Z";
        let query_path = format!(
            "/series/{}?format=arrow&start={}&end={}",
            sensor.uuid, start_time, end_time
        );
        let response = app.get(&query_path).await?;

        response.assert_status(StatusCode::OK);
        let body_bytes = response.body_bytes();
        assert!(!body_bytes.is_empty());
        assert_eq!(&body_bytes[0..6], b"ARROW1");

        Ok(())
    }
}

/// Test Arrow data import functionality
mod import_tests {
    use super::*;

    #[tokio::test]
    #[serial]
    async fn test_import_arrow_integer_data() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Create test Arrow data
        let sensor = Sensor {
            uuid: Uuid::new_v4(),
            name: "test_integer_sensor".to_string(),
            sensor_type: SensorType::Integer,
            unit: None,
            labels: smallvec::SmallVec::new(),
        };

        let datetime1 = SensAppDateTime::now().unwrap();
        let datetime2 = datetime1 + hifitime::Duration::from_seconds(60.0);
        let datetime3 = datetime2 + hifitime::Duration::from_seconds(60.0);

        let samples = TypedSamples::Integer(smallvec![
            Sample {
                datetime: datetime1,
                value: 10
            },
            Sample {
                datetime: datetime2,
                value: 20
            },
            Sample {
                datetime: datetime3,
                value: 30
            },
        ]);

        let sensor_data = SensorData::new(sensor, samples);
        let arrow_bytes = ArrowConverter::to_arrow_file(&sensor_data)?;

        // Upload Arrow data
        let response = app
            .post_binary("/sensors/publish", "application/vnd.apache.arrow.file", &arrow_bytes)
            .await?;

        assert_eq!(response.status(), 200);

        // Verify data was ingested
        storage.expect_sensor_count(1).await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_import_arrow_float_data() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Create test Arrow data with float values
        let sensor = Sensor {
            uuid: Uuid::new_v4(),
            name: "test_float_sensor".to_string(),
            sensor_type: SensorType::Float,
            unit: None,
            labels: smallvec::SmallVec::new(),
        };

        let datetime = SensAppDateTime::now().unwrap();
        let samples = TypedSamples::Float(smallvec![
            Sample {
                datetime,
                value: std::f64::consts::PI
            },
            Sample {
                datetime: datetime + hifitime::Duration::from_seconds(1.0),
                value: std::f64::consts::E
            },
        ]);

        let sensor_data = SensorData::new(sensor, samples);
        let arrow_bytes = ArrowConverter::to_arrow_file(&sensor_data)?;

        // Upload Arrow data
        let response = app
            .post_binary("/sensors/publish", "application/vnd.apache.arrow.file", &arrow_bytes)
            .await?;

        assert_eq!(response.status(), 200);

        // Verify data was ingested
        storage.expect_sensor_count(1).await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_import_arrow_string_data() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Create test Arrow data with string values
        let sensor = Sensor {
            uuid: Uuid::new_v4(),
            name: "test_string_sensor".to_string(),
            sensor_type: SensorType::String,
            unit: None,
            labels: smallvec::SmallVec::new(),
        };

        let datetime = SensAppDateTime::now().unwrap();
        let samples = TypedSamples::String(smallvec![
            Sample {
                datetime,
                value: "hello".to_string()
            },
            Sample {
                datetime: datetime + hifitime::Duration::from_seconds(1.0),
                value: "world".to_string()
            },
        ]);

        let sensor_data = SensorData::new(sensor, samples);
        let arrow_bytes = ArrowConverter::to_arrow_file(&sensor_data)?;

        // Upload Arrow data
        let response = app
            .post_binary("/sensors/publish", "application/vnd.apache.arrow.file", &arrow_bytes)
            .await?;

        assert_eq!(response.status(), 200);

        // Verify data was ingested
        storage.expect_sensor_count(1).await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_import_arrow_invalid_format() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Upload invalid Arrow data
        let invalid_data = b"not arrow data";
        let response = app
            .post_binary("/sensors/publish", "application/vnd.apache.arrow.file", invalid_data)
            .await?;

        assert_eq!(response.status(), 400); // Should return bad request

        // Verify no data was ingested
        storage.expect_sensor_count(0).await?;

        Ok(())
    }
}

/// Test Arrow round-trip functionality (export then import)
mod roundtrip_tests {
    use super::*;

    #[tokio::test]
    #[serial]
    async fn test_arrow_roundtrip_integer_data() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Initial data ingestion via CSV
        let (csv_data, sensor_name) = fixtures::temperature_sensor_csv_with_name();
        app.post_csv("/sensors/publish", &csv_data).await?;

        // Get original sensor
        let original_sensor = DbHelpers::get_sensor_by_name(&storage, &sensor_name)
            .await?
            .expect("Original sensor should exist");

        // Export as Arrow
        let query_path = format!("/series/{}?format=arrow", original_sensor.uuid);
        let export_response = app.get(&query_path).await?;
        export_response.assert_status(StatusCode::OK);

        let arrow_bytes = export_response.body_bytes();

        // Clear database
        test_db.cleanup().await?;

        // Re-import the Arrow data
        let import_response = app
            .post_binary("/sensors/publish", "application/vnd.apache.arrow.file", arrow_bytes)
            .await?;

        assert_eq!(import_response.status(), 200);

        // Verify data integrity
        storage.expect_sensor_count(1).await?;

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_arrow_roundtrip_multiple_sensors() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new().await?;
        let storage = test_db.storage();
        let app = TestApp::new(storage.clone()).await;

        // Initial data ingestion
        let (csv_data, temp_name, humidity_name) = fixtures::multi_sensor_csv_with_names();
        app.post_csv("/sensors/publish", &csv_data).await?;

        // Export both sensors as Arrow
        let temp_sensor = DbHelpers::get_sensor_by_name(&storage, &temp_name)
            .await?
            .expect("Temperature sensor should exist");
        let humidity_sensor = DbHelpers::get_sensor_by_name(&storage, &humidity_name)
            .await?
            .expect("Humidity sensor should exist");

        // Export temperature sensor
        let temp_query = format!("/series/{}?format=arrow", temp_sensor.uuid);
        let temp_response = app.get(&temp_query).await?;
        temp_response.assert_status(StatusCode::OK);
        let temp_arrow_bytes = temp_response.body_bytes();

        // Export humidity sensor
        let humidity_query = format!("/series/{}?format=arrow", humidity_sensor.uuid);
        let humidity_response = app.get(&humidity_query).await?;
        humidity_response.assert_status(StatusCode::OK);
        let humidity_arrow_bytes = humidity_response.body_bytes();

        // Clear database
        test_db.cleanup().await?;

        // Re-import both Arrow datasets
        let temp_import_response = app
            .post_binary("/sensors/publish", "application/vnd.apache.arrow.file", temp_arrow_bytes)
            .await?;

        let humidity_import_response = app
            .post_binary("/sensors/publish", "application/vnd.apache.arrow.file", humidity_arrow_bytes)
            .await?;

        assert_eq!(temp_import_response.status(), 200);
        assert_eq!(humidity_import_response.status(), 200);

        // Verify both sensors were recreated
        storage.expect_sensor_count(2).await?;

        Ok(())
    }
}

/// Test Arrow converter unit functionality
mod converter_tests {
    use super::*;

    #[test]
    fn test_arrow_converter_integer_samples() {
        let sensor = Sensor {
            uuid: Uuid::new_v4(),
            name: "test_sensor".to_string(),
            sensor_type: SensorType::Integer,
            unit: None,
            labels: smallvec::SmallVec::new(),
        };

        let datetime = SensAppDateTime::now().unwrap();
        let samples = TypedSamples::Integer(smallvec![
            Sample {
                datetime,
                value: 42
            },
            Sample {
                datetime: datetime + hifitime::Duration::from_seconds(1.0),
                value: 84
            },
        ]);

        let sensor_data = SensorData::new(sensor, samples);
        let batch = ArrowConverter::to_record_batch(&sensor_data).unwrap();

        assert_eq!(batch.num_rows(), 2);
        assert_eq!(batch.num_columns(), 4); // timestamp, value, sensor_id, sensor_name

        // Verify schema
        let schema = batch.schema();
        assert_eq!(schema.field(0).name(), "timestamp");
        assert_eq!(schema.field(1).name(), "value");
        assert_eq!(schema.field(2).name(), "sensor_id");
    }

    #[test]
    fn test_arrow_converter_mixed_types() {
        // Test different data types
        let test_cases = vec![
            (
                SensorType::Integer,
                TypedSamples::Integer(smallvec![Sample {
                    datetime: SensAppDateTime::now().unwrap(),
                    value: 42
                }]),
            ),
            (
                SensorType::Float,
                TypedSamples::Float(smallvec![Sample {
                    datetime: SensAppDateTime::now().unwrap(),
                    value: std::f64::consts::PI
                }]),
            ),
            (
                SensorType::String,
                TypedSamples::String(smallvec![Sample {
                    datetime: SensAppDateTime::now().unwrap(),
                    value: "test".to_string()
                }]),
            ),
            (
                SensorType::Boolean,
                TypedSamples::Boolean(smallvec![Sample {
                    datetime: SensAppDateTime::now().unwrap(),
                    value: true
                }]),
            ),
        ];

        for (sensor_type, samples) in test_cases {
            let sensor = Sensor {
                uuid: Uuid::new_v4(),
                name: format!("test_{:?}_sensor", sensor_type),
                sensor_type,
                unit: None,
                labels: smallvec::SmallVec::new(),
            };

            let sensor_data = SensorData::new(sensor, samples);
            let result = ArrowConverter::to_record_batch(&sensor_data);
            assert!(
                result.is_ok(),
                "Failed to convert {:?} sensor data",
                sensor_type
            );

            let batch = result.unwrap();
            assert_eq!(batch.num_rows(), 1);
            assert!(batch.num_columns() >= 3); // At least timestamp, value, sensor_id
        }
    }

    #[test]
    fn test_arrow_file_format_validation() {
        let sensor = Sensor {
            uuid: Uuid::new_v4(),
            name: "test_sensor".to_string(),
            sensor_type: SensorType::Integer,
            unit: None,
            labels: smallvec::SmallVec::new(),
        };

        let samples = TypedSamples::Integer(smallvec![Sample {
            datetime: SensAppDateTime::now().unwrap(),
            value: 42
        }]);

        let sensor_data = SensorData::new(sensor, samples);
        let arrow_bytes = ArrowConverter::to_arrow_file(&sensor_data).unwrap();

        // Verify Arrow file format
        assert!(!arrow_bytes.is_empty());
        assert_eq!(&arrow_bytes[0..6], b"ARROW1"); // Arrow magic number
    }

    #[test]
    fn test_multiple_sensors_to_arrow() {
        let sensor1 = Sensor {
            uuid: Uuid::new_v4(),
            name: "sensor1".to_string(),
            sensor_type: SensorType::Integer,
            unit: None,
            labels: smallvec::SmallVec::new(),
        };

        let sensor2 = Sensor {
            uuid: Uuid::new_v4(),
            name: "sensor2".to_string(),
            sensor_type: SensorType::Float,
            unit: None,
            labels: smallvec::SmallVec::new(),
        };

        let samples1 = TypedSamples::Integer(smallvec![Sample {
            datetime: SensAppDateTime::now().unwrap(),
            value: 10
        }]);

        let samples2 = TypedSamples::Float(smallvec![Sample {
            datetime: SensAppDateTime::now().unwrap(),
            value: std::f64::consts::PI
        }]);

        let sensor_data1 = SensorData::new(sensor1, samples1);
        let sensor_data2 = SensorData::new(sensor2, samples2);

        let sensor_list = vec![sensor_data1, sensor_data2];
        let arrow_bytes = ArrowConverter::sensor_data_list_to_arrow_file(&sensor_list).unwrap();

        assert!(!arrow_bytes.is_empty());
        assert_eq!(&arrow_bytes[0..6], b"ARROW1");
    }
}
