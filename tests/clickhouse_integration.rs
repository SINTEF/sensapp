mod common;

#[cfg(feature = "clickhouse")]
mod clickhouse_tests {
    use crate::common::{DatabaseType, TestDb};
    use anyhow::Result;
    use sensapp::config::load_configuration_for_tests;
    use sensapp::datamodel::batch_builder::BatchBuilder;
    use sensapp::datamodel::sensapp_vec::SensAppLabels;
    use sensapp::datamodel::{Sample, Sensor, SensorType, TypedSamples};
    use sensapp::storage::query::LabelMatcher;
    use serial_test::serial;
    use std::sync::Arc;
    use uuid::Uuid;

    // Ensure configuration is loaded once for all tests in this module
    static INIT: std::sync::Once = std::sync::Once::new();
    fn ensure_config() {
        INIT.call_once(|| {
            load_configuration_for_tests().expect("Failed to load configuration for tests");
        });
    }

    fn create_sensor_with_labels(
        name: &str,
        sensor_type: SensorType,
        labels: Vec<(String, String)>,
    ) -> Sensor {
        let labels: SensAppLabels = labels.into_iter().collect();
        Sensor::new(
            Uuid::new_v4(),
            name.to_string(),
            sensor_type,
            None,
            Some(labels),
        )
    }

    fn create_float_samples(count: usize) -> TypedSamples {
        let samples: smallvec::SmallVec<[Sample<f64>; 4]> = (0..count)
            .map(|index| Sample {
                datetime: hifitime::Epoch::from_unix_seconds((1704067200 + index * 60) as f64),
                value: 20.0 + index as f64,
            })
            .collect();
        TypedSamples::Float(samples)
    }

    async fn publish_test_sensors(
        storage: &Arc<dyn sensapp::storage::StorageInstance>,
        sensors_with_samples: Vec<(Sensor, TypedSamples)>,
    ) -> Result<()> {
        let mut batch_builder = BatchBuilder::new()?;

        for (sensor, samples) in sensors_with_samples {
            let sensor = Arc::new(sensor);
            batch_builder.add(sensor.clone(), samples).await?;
        }

        batch_builder.send_what_is_left(storage.clone()).await?;
        Ok(())
    }

    /// Test basic ClickHouse connection and database setup
    #[tokio::test]
    #[serial]
    async fn test_clickhouse_connection() -> Result<()> {
        ensure_config();
        // Given: A ClickHouse test database
        let test_db = TestDb::new_with_type(DatabaseType::ClickHouse).await?;
        let storage = test_db.storage();

        // When: We try to migrate and list series
        storage.create_or_migrate().await?;

        // Then: The operations should succeed (database is accessible)
        let result = storage.list_series(None, None, None).await?;

        // Database should be empty or contain existing sensors
        println!(
            "Found {} sensors in ClickHouse database",
            result.series.len()
        );

        Ok(())
    }

    /// Test repeated migrations are safe and idempotent
    #[tokio::test]
    #[serial]
    async fn test_clickhouse_create_or_migrate_is_idempotent() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new_with_type(DatabaseType::ClickHouse).await?;
        let storage = test_db.storage();

        storage.create_or_migrate().await?;
        storage.create_or_migrate().await?;

        let result = storage.list_series(None, None, None).await?;
        assert!(result.series.is_empty());

        Ok(())
    }

    /// Test ClickHouse storage health check against a real service
    #[tokio::test]
    #[serial]
    async fn test_clickhouse_health_check() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new_with_type(DatabaseType::ClickHouse).await?;
        let storage = test_db.storage();

        storage.create_or_migrate().await?;
        storage.health_check().await?;

        Ok(())
    }

    /// Test metrics listing functionality
    #[tokio::test]
    #[serial]
    async fn test_clickhouse_list_metrics() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new_with_type(DatabaseType::ClickHouse).await?;
        let storage = test_db.storage();

        // When: We list metrics
        let metrics = storage.list_metrics().await?;

        // Then: The operation should succeed
        println!("Found {} metrics in ClickHouse database", metrics.len());

        Ok(())
    }

    /// Test vacuum operation
    #[tokio::test]
    #[serial]
    async fn test_clickhouse_vacuum() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new_with_type(DatabaseType::ClickHouse).await?;
        let storage = test_db.storage();

        // When: We run vacuum
        let result = storage.vacuum().await;

        // Then: The operation should succeed
        assert!(result.is_ok(), "Vacuum operation should succeed");

        Ok(())
    }

    /// Test cleanup functionality
    #[tokio::test]
    #[serial]
    async fn test_clickhouse_cleanup() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new_with_type(DatabaseType::ClickHouse).await?;
        let storage = test_db.storage();

        // When: We run cleanup
        let result = storage.cleanup_test_data().await;

        // Then: The operation should succeed
        assert!(result.is_ok(), "Cleanup operation should succeed");

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_clickhouse_query_sensors_by_labels_exact_match() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new_with_type(DatabaseType::ClickHouse).await?;
        let storage = test_db.storage();

        let sensor1 = create_sensor_with_labels(
            "cpu_usage",
            SensorType::Float,
            vec![("environment".to_string(), "production".to_string())],
        );
        let sensor2 = create_sensor_with_labels(
            "cpu_usage",
            SensorType::Float,
            vec![("environment".to_string(), "staging".to_string())],
        );

        publish_test_sensors(
            &storage,
            vec![
                (sensor1, create_float_samples(3)),
                (sensor2, create_float_samples(2)),
            ],
        )
        .await?;

        let matchers = vec![
            LabelMatcher::eq("__name__", "cpu_usage"),
            LabelMatcher::eq("environment", "production"),
        ];
        let results = storage
            .query_sensors_by_labels(&matchers, None, None, None, false)
            .await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].sensor.name, "cpu_usage");
        assert_eq!(results[0].sensor.labels[0].0, "environment");
        assert_eq!(results[0].sensor.labels[0].1, "production");

        match &results[0].samples {
            TypedSamples::Float(samples) => assert_eq!(samples.len(), 3),
            other => panic!("Expected float samples, got {other:?}"),
        }

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_clickhouse_query_sensors_by_labels_numeric_only() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new_with_type(DatabaseType::ClickHouse).await?;
        let storage = test_db.storage();

        let numeric_sensor = create_sensor_with_labels(
            "room_value",
            SensorType::Float,
            vec![("site".to_string(), "lab".to_string())],
        );
        let string_sensor = create_sensor_with_labels(
            "room_state",
            SensorType::String,
            vec![("site".to_string(), "lab".to_string())],
        );

        publish_test_sensors(
            &storage,
            vec![
                (numeric_sensor, create_float_samples(2)),
                (
                    string_sensor,
                    TypedSamples::String(smallvec::smallvec![
                        Sample {
                            datetime: hifitime::Epoch::from_unix_seconds(1704067200.0),
                            value: "ok".to_string(),
                        },
                        Sample {
                            datetime: hifitime::Epoch::from_unix_seconds(1704067260.0),
                            value: "warn".to_string(),
                        }
                    ]),
                ),
            ],
        )
        .await?;

        let matchers = vec![LabelMatcher::eq("site", "lab")];
        let results = storage
            .query_sensors_by_labels(&matchers, None, None, None, true)
            .await?;

        assert_eq!(results.len(), 1);
        assert_eq!(results[0].sensor.name, "room_value");
        assert_eq!(results[0].sensor.sensor_type, SensorType::Float);

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_clickhouse_list_series_pagination() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new_with_type(DatabaseType::ClickHouse).await?;
        let storage = test_db.storage();

        let sensors = vec![
            create_sensor_with_labels("sensor_a", SensorType::Float, vec![]),
            create_sensor_with_labels("sensor_b", SensorType::Float, vec![]),
            create_sensor_with_labels("sensor_c", SensorType::Float, vec![]),
        ];

        publish_test_sensors(
            &storage,
            sensors
                .into_iter()
                .map(|sensor| (sensor, create_float_samples(1)))
                .collect(),
        )
        .await?;

        let first_page = storage.list_series(None, Some(2), None).await?;
        assert_eq!(first_page.series.len(), 2);
        assert!(first_page.bookmark.is_some());

        let first_page_ids: Vec<_> = first_page.series.iter().map(|sensor| sensor.uuid).collect();
        let second_page = storage
            .list_series(None, Some(2), first_page.bookmark.as_deref())
            .await?;

        assert_eq!(second_page.series.len(), 1);
        assert!(second_page.bookmark.is_none());
        for sensor in &second_page.series {
            assert!(!first_page_ids.contains(&sensor.uuid));
        }

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_clickhouse_list_series_exact_last_page_has_no_bookmark() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new_with_type(DatabaseType::ClickHouse).await?;
        let storage = test_db.storage();

        publish_test_sensors(
            &storage,
            vec![
                (
                    create_sensor_with_labels("sensor_x", SensorType::Float, vec![]),
                    create_float_samples(1),
                ),
                (
                    create_sensor_with_labels("sensor_y", SensorType::Float, vec![]),
                    create_float_samples(1),
                ),
            ],
        )
        .await?;

        let page = storage.list_series(None, Some(2), None).await?;
        assert_eq!(page.series.len(), 2);
        assert!(page.bookmark.is_none());

        Ok(())
    }

    #[tokio::test]
    #[serial]
    async fn test_clickhouse_list_series_invalid_bookmark() -> Result<()> {
        ensure_config();
        let test_db = TestDb::new_with_type(DatabaseType::ClickHouse).await?;
        let storage = test_db.storage();

        let error = storage.list_series(None, Some(2), Some("invalid")).await;
        assert!(error.is_err(), "invalid bookmark should be rejected");

        Ok(())
    }
}
