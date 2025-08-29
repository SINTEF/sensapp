use anyhow::{Result, anyhow};
use crate::storage::{StorageInstance, storage_factory::create_storage_from_connection_string};
use crate::config::{SensAppConfig, SENSAPP_CONFIG};
use std::sync::{Arc, Mutex};

pub mod db;
pub mod fixtures;
pub mod http;

// Test configuration initialization
static TEST_CONFIG_INIT: Mutex<()> = Mutex::new(());

/// Test-only function to ensure configuration is loaded exactly once per test run
/// Available for both unit tests and integration tests
pub fn load_configuration_for_tests() -> Result<()> {
    let _guard = TEST_CONFIG_INIT.lock().unwrap();

    // If config is already loaded, return success
    if SENSAPP_CONFIG.get().is_some() {
        return Ok(());
    }

    // Load default configuration for tests
    let config = SensAppConfig::load()?;
    SENSAPP_CONFIG.get_or_init(|| Arc::new(config));

    Ok(())
}

/// Supported database types for testing
#[derive(Debug, Clone, PartialEq)]
pub enum DatabaseType {
    PostgreSQL,
    SQLite,
    ClickHouse,
    RRDcached,
}

impl DatabaseType {
    /// Get the appropriate connection string for the database type
    pub fn default_connection_string(&self) -> String {
        match self {
            DatabaseType::PostgreSQL => std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
                "postgres://postgres:postgres@localhost:5432/sensapp".to_string()
            }),
            DatabaseType::SQLite => std::env::var("TEST_DATABASE_URL")
                .unwrap_or_else(|_| "sqlite://test.db".to_string()),
            DatabaseType::ClickHouse => std::env::var("TEST_DATABASE_URL").unwrap_or_else(|_| {
                "clickhouse://default:password@localhost:9000/sensapp_test".to_string()
            }),
            DatabaseType::RRDcached => std::env::var("TEST_DATABASE_URL")
                .unwrap_or_else(|_| "rrdcached://127.0.0.1:42217?preset=hoarder".to_string()),
        }
    }

    /// Get the database type from the connection string prefix
    pub fn from_connection_string(connection_string: &str) -> Self {
        if connection_string.starts_with("sqlite://") {
            DatabaseType::SQLite
        } else if connection_string.starts_with("clickhouse://") {
            DatabaseType::ClickHouse
        } else if connection_string.starts_with("rrdcached://") {
            DatabaseType::RRDcached
        } else {
            // Default to PostgreSQL for postgres://, postgresql://, or any other prefix
            DatabaseType::PostgreSQL
        }
    }

    /// Get the database type from environment variables or use default
    pub fn from_env() -> Self {
        let connection_string = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432/sensapp".to_string());
        Self::from_connection_string(&connection_string)
    }
}

/// Test database manager that creates isolated test databases
pub struct TestDb {
    pub storage: Arc<dyn StorageInstance>,
    connection_string: String,
}

impl TestDb {
    /// Create a new test database connection using the database type from environment
    /// or defaulting to PostgreSQL
    pub async fn new() -> Result<Self> {
        let db_type = DatabaseType::from_env();
        Self::new_with_type(db_type).await
    }

    /// Create a new test database connection with a specific database type
    pub async fn new_with_type(db_type: DatabaseType) -> Result<Self> {
        let connection_string = db_type.default_connection_string();

        // Connect to the database
        let storage = create_storage_from_connection_string(&connection_string)
            .await
            .map_err(|e| anyhow!("Failed to create storage for {}: {}", connection_string, e))?;

        // Run migrations to ensure database is up to date
        storage
            .create_or_migrate()
            .await
            .map_err(|e| anyhow!("Failed to run migrations for {}: {}", connection_string, e))?;

        // Clean up any existing test data to ensure test isolation
        storage.cleanup_test_data().await.map_err(|e| {
            anyhow!(
                "Failed to cleanup test data for {}: {}",
                connection_string,
                e
            )
        })?;

        Ok(Self {
            storage,
            connection_string,
        })
    }

    /// Get the storage instance for testing
    pub fn storage(&self) -> Arc<dyn StorageInstance> {
        self.storage.clone()
    }

    /// Get the connection string for direct database access
    pub fn connection_string(&self) -> &str {
        &self.connection_string
    }

    /// Clean up the test database
    pub async fn cleanup(&self) -> Result<()> {
        self.storage.cleanup_test_data().await
    }
}

/// Helper trait for easier testing
pub trait TestHelpers {
    fn expect_sensor_count(
        &self,
        expected: usize,
    ) -> impl std::future::Future<Output = Result<()>> + Send;
}

impl TestHelpers for Arc<dyn StorageInstance> {
    async fn expect_sensor_count(&self, expected: usize) -> Result<()> {
        let sensors = self.list_series(None).await?;
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