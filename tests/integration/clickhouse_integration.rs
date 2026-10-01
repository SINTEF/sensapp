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

    /// A client for administrative statements (creating throwaway databases, inspecting
    /// `system` tables), built from the test connection string.
    fn admin_client(url: &url::Url) -> clickhouse::Client {
        clickhouse::Client::default()
            .with_url(format!(
                "http://{}:{}",
                url.host_str().expect("host"),
                url.port().unwrap_or(8123)
            ))
            .with_user(url.username())
            .with_password(url.password().unwrap_or_default())
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
    /// Databases created before the metadata tables became `ReplacingMergeTree` cannot be read
    /// with `FINAL`: startup must say so instead of failing on the first query.
    #[tokio::test]
    #[serial]
    async fn test_clickhouse_rejects_databases_with_plain_merge_tree_metadata() -> Result<()> {
        ensure_config();
        let connection_string = DatabaseType::ClickHouse.default_connection_string();
        let mut url = url::Url::parse(&connection_string)?;
        let admin = admin_client(&url);

        admin
            .query("DROP DATABASE IF EXISTS sensapp_legacy_test")
            .execute()
            .await?;
        admin
            .query("CREATE DATABASE sensapp_legacy_test")
            .execute()
            .await?;
        admin
            .query(
                "CREATE TABLE sensapp_legacy_test.sensors (sensor_id UInt64) \
                 ENGINE = MergeTree() ORDER BY sensor_id",
            )
            .execute()
            .await?;

        url.set_path("/sensapp_legacy_test");
        let storage =
            sensapp::storage::storage_factory::create_storage_from_connection_string(url.as_str())
                .await?;
        let result = storage.create_or_migrate().await;

        admin
            .query("DROP DATABASE IF EXISTS sensapp_legacy_test")
            .execute()
            .await?;

        let error = result.expect_err("a legacy database must be rejected");
        assert!(
            format!("{error:#}").contains("older SensApp"),
            "unexpected error: {error:#}"
        );
        Ok(())
    }

    /// Database names such as `sensapp-prod` need quoting, and credentials are percent-decoded.
    #[tokio::test]
    #[serial]
    async fn test_clickhouse_hyphenated_database_and_percent_encoded_password() -> Result<()> {
        ensure_config();
        let connection_string = DatabaseType::ClickHouse.default_connection_string();
        let mut url = url::Url::parse(&connection_string)?;
        let password = url.password().unwrap_or_default().to_string();
        let admin = admin_client(&url);

        // Every byte of the password written as %XX
        let encoded: String = password
            .bytes()
            .map(|byte| format!("%{byte:02X}"))
            .collect();
        url.set_password(Some(&encoded)).expect("password");
        url.set_path("/sensapp-hyphen-test");

        admin
            .query("DROP DATABASE IF EXISTS `sensapp-hyphen-test`")
            .execute()
            .await?;
        let result = async {
            let storage = sensapp::storage::storage_factory::create_storage_from_connection_string(
                url.as_str(),
            )
            .await?;
            storage.create_or_migrate().await?;
            storage.health_check().await?;
            publish_test_sensors(
                &storage,
                vec![(
                    create_sensor_with_labels("hyphen", SensorType::Float, vec![]),
                    create_float_samples(3),
                )],
            )
            .await?;
            let listed = storage.list_series(Some("hyphen"), None, None).await?;
            assert_eq!(listed.series.len(), 1);
            Ok::<_, anyhow::Error>(())
        }
        .await;
        admin
            .query("DROP DATABASE IF EXISTS `sensapp-hyphen-test`")
            .execute()
            .await?;
        result
    }
    /// The backend reads the base tables directly: no materialized view should be kept up to
    /// date on every write for nothing.
    #[tokio::test]
    #[serial]
    async fn test_clickhouse_schema_has_no_unused_views() -> Result<()> {
        ensure_config();
        let _test_db = TestDb::new_with_type(DatabaseType::ClickHouse).await?;
        let url = url::Url::parse(&DatabaseType::ClickHouse.default_connection_string())?;
        let database = url.path().trim_start_matches('/');

        let views: u64 = admin_client(&url)
            .query(
                "SELECT count() FROM system.tables \
                 WHERE database = ? AND engine = 'MaterializedView'",
            )
            .bind(database)
            .fetch_one()
            .await?;
        assert_eq!(views, 0);
        Ok(())
    }
}
