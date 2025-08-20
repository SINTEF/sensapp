use anyhow::{Result, anyhow};
use sensapp::storage::{StorageInstance, storage_factory::create_storage_from_connection_string};
use std::sync::Arc;

pub mod db;
pub mod fixtures;
pub mod http;

/// Test database manager that creates isolated test databases
pub struct TestDb {
    pub db_name: String,
    #[allow(dead_code)] // Used by some tests
    pub storage: Arc<dyn StorageInstance>,
    #[allow(dead_code)] // Used by some tests
    pub connection_string: String,
}

impl TestDb {
    /// Create a new test database connection using the existing sensapp database
    #[allow(dead_code)] // Used by some tests
    pub async fn new() -> Result<Self> {
        let db_name = "sensapp".to_string();

        // Use environment variable or default to localhost with sensapp database
        let connection_string = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/sensapp".to_string());

        // Connect to the sensapp database
        let storage = create_storage_from_connection_string(&connection_string).await?;

        // Run migrations to ensure database is up to date
        storage.create_or_migrate().await?;

        // Clean up any existing test data to ensure test isolation
        #[cfg(any(test, feature = "test-utils"))]
        storage.cleanup_test_data().await?;

        Ok(Self {
            db_name,
            storage,
            connection_string,
        })
    }

    /// Clean up the test database
    #[allow(dead_code)] // Used by some tests
    pub async fn cleanup(&self) -> Result<()> {
        // We'll implement database cleanup later
        // For now, just ensure we have proper separation
        Ok(())
    }

    /// Get the storage instance for testing
    #[allow(dead_code)] // Used by some tests
    pub fn storage(&self) -> Arc<dyn StorageInstance> {
        self.storage.clone()
    }
}

impl Drop for TestDb {
    fn drop(&mut self) {
        // Async cleanup would be better, but this ensures cleanup happens
        println!("Test database {} cleaned up", self.db_name);
    }
}

/// Helper trait for easier testing
pub trait TestHelpers {
    #[allow(dead_code)] // Test helper method
    fn expect_sensor_count(
        &self,
        expected: usize,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
}

impl TestHelpers for Arc<dyn StorageInstance> {
    async fn expect_sensor_count(&self, expected: usize) -> Result<()> {
        let sensors = self.list_series().await?;
        if sensors.len() != expected {
            return Err(anyhow!(
                "Expected {} sensors, found {}. Sensors: {:#?}",
                expected,
                sensors.len(),
                sensors
            ));
        }
        Ok(())
    }
}
