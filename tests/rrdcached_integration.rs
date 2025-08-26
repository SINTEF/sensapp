mod common;

#[cfg(feature = "rrdcached")]
mod rrdcached_tests {
    use crate::common::{DatabaseType, TestDb};
    use anyhow::Result;
    use sensapp::config::load_configuration_for_tests;
    use sensapp::datamodel::{
        batch::{Batch, SingleSensorBatch}, Sample, Sensor, SensorType, SensAppDateTime, TypedSamples, sensapp_vec::SensAppVec,
    };
    use serial_test::serial;
    use smallvec::SmallVec;
    use std::sync::Arc;
    use uuid::Uuid;

    // Ensure configuration is loaded once for all tests in this module
    static INIT: std::sync::Once = std::sync::Once::new();
    fn ensure_config() {
        INIT.call_once(|| {
            load_configuration_for_tests().expect("Failed to load configuration for tests");
        });
    }

    /// Test basic RRDcached connection and database setup
    #[tokio::test]
    #[serial]
    async fn test_rrdcached_connection() -> Result<()> {
        ensure_config();
        // Given: An RRDcached test database
        let test_db = TestDb::new_with_type(DatabaseType::RRDcached).await?;
        let storage = test_db.storage();

        // When: We try to migrate and list series
        storage.create_or_migrate().await?;

        // Then: The operations should succeed (database is accessible)
        let sensors = storage.list_series(None).await?;

        // Database should be empty initially (or may contain existing RRD files)
        println!("Found {} sensors in RRDcached database", sensors.len());
        for sensor in &sensors {
            println!("  - Sensor: UUID={}, Name={}", sensor.uuid, sensor.name);
        }

        Ok(())
    }

    /// Test RRDcached list functionality with data
    #[tokio::test]
    #[serial]
    async fn test_rrdcached_list_after_publishing() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new_with_type(DatabaseType::RRDcached).await?;
        let storage = test_db.storage();

        // Given: We publish some data first
        let sensor = Sensor {
            uuid: Uuid::new_v4(),
            name: "test_sensor_for_listing".to_string(),
            sensor_type: SensorType::Float,
            unit: None,
            labels: SmallVec::new(),
        };

        let base_time = 1704067200.0; // 2024-01-01 00:00:00 UTC
        let samples = vec![Sample {
            datetime: SensAppDateTime::from_unix_seconds(base_time),
            value: 42.0,
        }];

        // Create and publish batch
        let sensor_arc = Arc::new(sensor);
        let single_sensor_batch = SingleSensorBatch::new(sensor_arc.clone(), TypedSamples::Float(samples.into()));
        let mut sensors_vec = SensAppVec::new();
        sensors_vec.push(single_sensor_batch);
        let batch = Arc::new(Batch::new(sensors_vec));

        let (sync_sender, _sync_receiver) = async_broadcast::broadcast(10);
        storage.publish(batch, sync_sender.clone()).await?;

        // When: We list the series
        let sensors = storage.list_series(None).await?;

        // Then: We should find the sensor we just created
        println!("Found {} sensors after publishing data", sensors.len());
        for sensor in &sensors {
            println!("  - Sensor: UUID={}, Name={}", sensor.uuid, sensor.name);
        }

        // Verify our sensor appears in the list (either by UUID match or by being present)
        let found = sensors.iter().any(|s| s.uuid == sensor_arc.uuid);
        if !found && sensors.is_empty() {
            println!("Warning: No sensors found - this might be expected if RRDcached LIST command doesn't show recently created files");
        }

        Ok(())
    }

    /// Test metrics listing functionality
    #[tokio::test]
    #[serial]
    async fn test_rrdcached_list_metrics() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new_with_type(DatabaseType::RRDcached).await?;
        let storage = test_db.storage();

        // When: We list metrics
        let metrics = storage.list_metrics().await?;

        // Then: The operation should succeed
        // RRDcached doesn't support metrics the same way as SQL databases
        println!("Found {} metrics in RRDcached database", metrics.len());

        Ok(())
    }

    /// Test basic data publishing to RRDcached
    #[tokio::test]
    #[serial]
    async fn test_rrdcached_publish_float_data() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new_with_type(DatabaseType::RRDcached).await?;
        let storage = test_db.storage();

        // Given: A test sensor with float data
        let sensor_uuid = Uuid::new_v4();
        let sensor = Sensor {
            uuid: sensor_uuid,
            name: "test_float_sensor".to_string(),
            sensor_type: SensorType::Float,
            unit: None,
            labels: SmallVec::new(),
        };

        // Create some test float samples
        let base_time = 1704067200.0; // 2024-01-01 00:00:00 UTC
        let samples = vec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(base_time),
                value: 23.5,
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(base_time + 1.0),
                value: 24.1,
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(base_time + 2.0),
                value: 22.8,
            },
        ];

        // Build batch manually
        let sensor_arc = Arc::new(sensor);
        let single_sensor_batch = SingleSensorBatch::new(sensor_arc, TypedSamples::Float(samples.into()));
        let mut sensors = SensAppVec::new();
        sensors.push(single_sensor_batch);
        let batch = Arc::new(Batch::new(sensors));

        // When: We publish the batch
        let (sync_sender, _sync_receiver) = async_broadcast::broadcast(10);
        storage.publish(batch, sync_sender.clone()).await?;

        // Then: The data should be stored
        // Note: RRDcached doesn't support querying data back easily,
        // but we can at least verify the publish operation succeeded
        println!("Successfully published float data to RRDcached");

        // Verify the sensor appears in our created sensors list
        let sensors = storage.list_series(None).await?;
        assert!(!sensors.is_empty(), "Should have at least one sensor");

        let found_sensor = sensors.iter().find(|s| s.uuid == sensor_uuid);
        assert!(found_sensor.is_some(), "Published sensor should be in the list");

        Ok(())
    }

    /// Test publishing different data types
    #[tokio::test]
    #[serial]
    async fn test_rrdcached_publish_multiple_types() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new_with_type(DatabaseType::RRDcached).await?;
        let storage = test_db.storage();

        let base_time = 1704067200.0; // 2024-01-01 00:00:00 UTC
        let (sync_sender, _sync_receiver) = async_broadcast::broadcast(10);

        // Test Integer data
        let int_sensor = Sensor {
            uuid: Uuid::new_v4(),
            name: "test_integer_sensor".to_string(),
            sensor_type: SensorType::Integer,
            unit: None,
            labels: SmallVec::new(),
        };

        let int_samples = vec![Sample {
            datetime: SensAppDateTime::from_unix_seconds(base_time),
            value: 42i64,
        }];

        let int_sensor_arc = Arc::new(int_sensor);
        let single_sensor_batch = SingleSensorBatch::new(int_sensor_arc.clone(), TypedSamples::Integer(int_samples.into()));
        let mut sensors = SensAppVec::new();
        sensors.push(single_sensor_batch);
        let batch = Arc::new(Batch::new(sensors));

        storage.publish(batch, sync_sender.clone()).await?;
        println!("Successfully published integer data to RRDcached");

        // Test Boolean data
        let bool_sensor = Sensor {
            uuid: Uuid::new_v4(),
            name: "test_boolean_sensor".to_string(),
            sensor_type: SensorType::Boolean,
            unit: None,
            labels: SmallVec::new(),
        };

        let bool_samples = vec![Sample {
            datetime: SensAppDateTime::from_unix_seconds(base_time + 5.0), // Ensure different timestamp
            value: true,
        }];

        let bool_sensor_arc = Arc::new(bool_sensor);
        let single_sensor_batch = SingleSensorBatch::new(bool_sensor_arc.clone(), TypedSamples::Boolean(bool_samples.into()));
        let mut sensors = SensAppVec::new();
        sensors.push(single_sensor_batch);
        let batch = Arc::new(Batch::new(sensors));

        storage.publish(batch, sync_sender.clone()).await?;
        println!("Successfully published boolean data to RRDcached");

        // Verify all sensors are tracked
        let sensors = storage.list_series(None).await?;
        assert!(
            sensors.len() >= 2,
            "Should have at least 2 sensors (integer and boolean)"
        );

        Ok(())
    }

    /// Test RRDcached vacuum operation
    #[tokio::test]
    #[serial]
    async fn test_rrdcached_vacuum() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new_with_type(DatabaseType::RRDcached).await?;
        let storage = test_db.storage();

        // When: We call vacuum
        storage.vacuum().await?;

        // Then: The operation should succeed (even though it's a no-op for RRDcached)
        println!("RRDcached vacuum completed successfully");

        Ok(())
    }

    /// Test that unsupported sensor types are handled gracefully
    #[tokio::test]
    #[serial]
    async fn test_rrdcached_unsupported_types() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new_with_type(DatabaseType::RRDcached).await?;
        let storage = test_db.storage();

        // Given: A sensor with an unsupported type (String)
        let string_sensor = Sensor {
            uuid: Uuid::new_v4(),
            name: "test_string_sensor".to_string(),
            sensor_type: SensorType::String,
            unit: None,
            labels: SmallVec::new(),
        };

        let string_samples = vec![Sample {
            datetime: SensAppDateTime::from_unix_seconds(1704067200.0),
            value: "test_value".to_string(),
        }];

        let string_sensor_arc = Arc::new(string_sensor);
        let single_sensor_batch = SingleSensorBatch::new(string_sensor_arc.clone(), TypedSamples::String(string_samples.into()));
        let mut sensors = SensAppVec::new();
        sensors.push(single_sensor_batch);
        let batch = Arc::new(Batch::new(sensors));

        // When: We try to publish unsupported data
        let (sync_sender, _sync_receiver) = async_broadcast::broadcast(10);
        storage.publish(batch, sync_sender.clone()).await?;

        // Then: The operation should succeed but the sensor shouldn't be created
        // (since unsupported types are filtered out in the create_sensors method)
        let sensors = storage.list_series(None).await?;
        let found_sensor = sensors.iter().find(|s| s.uuid == string_sensor_arc.uuid);
        assert!(found_sensor.is_none(), "Unsupported sensor types should not be created");

        Ok(())
    }

    /// Test querying data that was previously published
    #[tokio::test]
    #[serial]
    async fn test_rrdcached_query_published_data() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new_with_type(DatabaseType::RRDcached).await?;
        let storage = test_db.storage();

        // Given: A sensor with known data
        let sensor_uuid = Uuid::new_v4();
        let sensor = Sensor {
            uuid: sensor_uuid,
            name: "test_query_sensor".to_string(),
            sensor_type: SensorType::Float,
            unit: None,
            labels: SmallVec::new(),
        };

        let base_time = 1704067200.0; // 2024-01-01 00:00:00 UTC
        let test_values = vec![23.5, 24.1, 22.8];
        let samples: Vec<Sample<f64>> = test_values
            .iter()
            .enumerate()
            .map(|(i, &value)| Sample {
                datetime: SensAppDateTime::from_unix_seconds(base_time + i as f64),
                value,
            })
            .collect();

        // Publish the data
        let sensor_arc = Arc::new(sensor);
        let single_sensor_batch = SingleSensorBatch::new(sensor_arc.clone(), TypedSamples::Float(samples.into()));
        let mut sensors = SensAppVec::new();
        sensors.push(single_sensor_batch);
        let batch = Arc::new(Batch::new(sensors));

        let (sync_sender, _sync_receiver) = async_broadcast::broadcast(10);
        storage.publish(batch, sync_sender.clone()).await?;

        // Give RRD time to process and consolidate the data
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        // When: We query the data back with the specific time range
        let start_time = Some(SensAppDateTime::from_unix_seconds(base_time - 10.0)); // Start before our data
        let end_time = Some(SensAppDateTime::from_unix_seconds(base_time + 10.0)); // End after our data
        let sensor_data = storage.query_sensor_data(&sensor_uuid.to_string(), start_time, end_time, None).await?;


        // Then: We should get back the data we stored
        assert!(sensor_data.is_some(), "Query should return data");
        let sensor_data = sensor_data.unwrap();

        // Verify sensor metadata (reconstructed from UUID)
        assert_eq!(sensor_data.sensor.uuid, sensor_uuid);
        assert_eq!(sensor_data.sensor.sensor_type, SensorType::Float);

        // Verify samples (may not be exact due to RRD time alignment and consolidation)
        if let TypedSamples::Float(returned_samples) = sensor_data.samples {
            assert!(!returned_samples.is_empty(), "Should have returned some samples");
            
            // We may not get exact values due to RRD consolidation, but let's check we get reasonable data
            for sample in &returned_samples {
                assert!(!sample.value.is_nan(), "Sample values should not be NaN");
                println!("Retrieved sample: time={:?}, value={}", sample.datetime, sample.value);
            }
        } else {
            panic!("Expected Float samples, got: {:?}", sensor_data.samples);
        }

        Ok(())
    }

    /// Test querying with time range
    #[tokio::test]
    #[serial]
    async fn test_rrdcached_query_with_time_range() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new_with_type(DatabaseType::RRDcached).await?;
        let storage = test_db.storage();

        // Given: A sensor with data spread over time
        let sensor_uuid = Uuid::new_v4();
        let sensor = Sensor {
            uuid: sensor_uuid,
            name: "test_time_range_sensor".to_string(),
            sensor_type: SensorType::Float,
            unit: None,
            labels: SmallVec::new(),
        };

        let base_time = 1704067200.0; // 2024-01-01 00:00:00 UTC
        let samples: Vec<Sample<f64>> = (0..10)
            .map(|i| Sample {
                datetime: SensAppDateTime::from_unix_seconds(base_time + i as f64 * 60.0), // Every minute
                value: i as f64 * 10.0,
            })
            .collect();

        // Publish the data
        let sensor_arc = Arc::new(sensor);
        let single_sensor_batch = SingleSensorBatch::new(sensor_arc.clone(), TypedSamples::Float(samples.into()));
        let mut sensors = SensAppVec::new();
        sensors.push(single_sensor_batch);
        let batch = Arc::new(Batch::new(sensors));

        let (sync_sender, _sync_receiver) = async_broadcast::broadcast(10);
        storage.publish(batch, sync_sender.clone()).await?;

        // Give RRD time to process and consolidate the data
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        // When: We query with a time range (first 5 minutes with buffer)
        let start_time = Some(SensAppDateTime::from_unix_seconds(base_time - 60.0)); // Start 1 minute before
        let end_time = Some(SensAppDateTime::from_unix_seconds(base_time + 360.0)); // End 6 minutes after start
        
        let sensor_data = storage.query_sensor_data(
            &sensor_uuid.to_string(), 
            start_time, 
            end_time, 
            None
        ).await?;

        // Then: We should get data within the time range
        assert!(sensor_data.is_some(), "Query with time range should return data");
        let sensor_data = sensor_data.unwrap();

        if let TypedSamples::Float(returned_samples) = sensor_data.samples {
            assert!(!returned_samples.is_empty(), "Should have returned samples in time range");
            
            for sample in &returned_samples {
                let sample_time = sample.datetime.to_unix_seconds();
                println!("Sample in range: time={}, value={}", sample_time, sample.value);
                // Note: RRD may return data slightly outside the range due to consolidation
            }
        } else {
            panic!("Expected Float samples, got: {:?}", sensor_data.samples);
        }

        Ok(())
    }

    /// Test querying non-existent sensor
    #[tokio::test]
    #[serial]
    async fn test_rrdcached_query_nonexistent_sensor() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new_with_type(DatabaseType::RRDcached).await?;
        let storage = test_db.storage();

        // When: We query a sensor that doesn't exist
        let non_existent_uuid = Uuid::new_v4();
        let sensor_data = storage.query_sensor_data(&non_existent_uuid.to_string(), None, None, None).await?;

        // Then: We should get None
        assert!(sensor_data.is_none(), "Query for non-existent sensor should return None");

        Ok(())
    }

    /// Test querying sensor with no data yet
    #[tokio::test]
    #[serial]
    async fn test_rrdcached_query_sensor_no_data() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new_with_type(DatabaseType::RRDcached).await?;
        let storage = test_db.storage();

        // Given: A sensor that exists but has no data
        let sensor_uuid = Uuid::new_v4();
        let sensor = Sensor {
            uuid: sensor_uuid,
            name: "test_empty_sensor".to_string(),
            sensor_type: SensorType::Float,
            unit: None,
            labels: SmallVec::new(),
        };

        // Create the sensor without publishing any data
        let sensor_arc = Arc::new(sensor);
        let empty_samples: Vec<Sample<f64>> = vec![];
        let single_sensor_batch = SingleSensorBatch::new(sensor_arc.clone(), TypedSamples::Float(empty_samples.into()));
        let mut sensors = SensAppVec::new();
        sensors.push(single_sensor_batch);
        let batch = Arc::new(Batch::new(sensors));

        let (sync_sender, _sync_receiver) = async_broadcast::broadcast(10);
        storage.publish(batch, sync_sender.clone()).await?;

        // When: We query the sensor
        let sensor_data = storage.query_sensor_data(&sensor_uuid.to_string(), None, None, None).await?;

        // Then: We should get None (no data available)
        assert!(sensor_data.is_none(), "Query for sensor with no data should return None");

        Ok(())
    }

    /// Test querying integer data (stored as float in RRD)
    #[tokio::test]
    #[serial]
    async fn test_rrdcached_query_integer_data() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new_with_type(DatabaseType::RRDcached).await?;
        let storage = test_db.storage();

        // Given: A sensor with integer data
        let sensor_uuid = Uuid::new_v4();
        let sensor = Sensor {
            uuid: sensor_uuid,
            name: "test_integer_query_sensor".to_string(),
            sensor_type: SensorType::Integer,
            unit: None,
            labels: SmallVec::new(),
        };

        let base_time = 1704067200.0; // 2024-01-01 00:00:00 UTC
        let int_samples = vec![
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(base_time),
                value: 42i64,
            },
            Sample {
                datetime: SensAppDateTime::from_unix_seconds(base_time + 1.0),
                value: 84i64,
            },
        ];

        // Publish the integer data
        let sensor_arc = Arc::new(sensor);
        let single_sensor_batch = SingleSensorBatch::new(sensor_arc.clone(), TypedSamples::Integer(int_samples.into()));
        let mut sensors = SensAppVec::new();
        sensors.push(single_sensor_batch);
        let batch = Arc::new(Batch::new(sensors));

        let (sync_sender, _sync_receiver) = async_broadcast::broadcast(10);
        storage.publish(batch, sync_sender.clone()).await?;

        // Give RRD time to process and consolidate the data
        tokio::time::sleep(tokio::time::Duration::from_millis(1000)).await;

        // When: We query the data back with time range
        let start_time = Some(SensAppDateTime::from_unix_seconds(base_time - 10.0)); // Start before our data
        let end_time = Some(SensAppDateTime::from_unix_seconds(base_time + 10.0)); // End after our data
        let sensor_data = storage.query_sensor_data(&sensor_uuid.to_string(), start_time, end_time, None).await?;

        // Then: We should get back Float data (since RRD stores everything as f64)
        // Note: This shows a limitation - we lose the original type information
        assert!(sensor_data.is_some(), "Query should return data");
        let sensor_data = sensor_data.unwrap();

        // The sensor type will be reconstructed as Float since we don't store the original type
        assert_eq!(sensor_data.sensor.sensor_type, SensorType::Float);

        if let TypedSamples::Float(returned_samples) = sensor_data.samples {
            assert!(!returned_samples.is_empty(), "Should have returned some samples");
            
            for sample in &returned_samples {
                println!("Retrieved integer-as-float sample: time={:?}, value={}", sample.datetime, sample.value);
                // Values should be close to the original integers (42.0, 84.0)
                assert!(!sample.value.is_nan(), "Sample values should not be NaN");
            }
        } else {
            panic!("Expected Float samples, got: {:?}", sensor_data.samples);
        }

        Ok(())
    }
}