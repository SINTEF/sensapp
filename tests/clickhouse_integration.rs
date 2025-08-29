#[cfg(feature = "clickhouse")]
mod clickhouse_tests {
    use anyhow::Result;
    use sensapp::test_utils::load_configuration_for_tests;
    use sensapp::test_utils::{DatabaseType, TestDb};
    use serial_test::serial;

    // Ensure configuration is loaded once for all tests in this module
    static INIT: std::sync::Once = std::sync::Once::new();
    fn ensure_config() {
        INIT.call_once(|| {
            load_configuration_for_tests().expect("Failed to load configuration for tests");
        });
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
        let sensors = storage.list_series(None).await?;

        // Database should be empty or contain existing sensors
        println!("Found {} sensors in ClickHouse database", sensors.len());

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
}
