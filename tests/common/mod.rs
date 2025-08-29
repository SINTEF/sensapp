use anyhow::{Result, anyhow};
use sensapp::storage::{StorageInstance, storage_factory::create_storage_from_connection_string};
use std::sync::Arc;

pub mod db;
pub mod fixtures;
pub mod http;

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
    pub db_name: String,
    pub db_type: DatabaseType,
    pub storage: Arc<dyn StorageInstance>,
    pub connection_string: String,
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
        let db_name = match &db_type {
            DatabaseType::PostgreSQL => "sensapp".to_string(),
            DatabaseType::SQLite => "test.db".to_string(),
            DatabaseType::ClickHouse => "sensapp_test".to_string(),
            DatabaseType::RRDcached => "rrdcached".to_string(),
        };

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
            db_name,
            db_type,
            storage,
            connection_string,
        })
    }

    /// Create test databases for both PostgreSQL and SQLite
    /// Returns (postgresql_db, sqlite_db) tuple
    pub async fn new_multi_database() -> Result<(TestDb, TestDb)> {
        let postgres_db = Self::new_with_type(DatabaseType::PostgreSQL).await?;
        let sqlite_db = Self::new_with_type(DatabaseType::SQLite).await?;
        Ok((postgres_db, sqlite_db))
    }

    /// Clean up the test database
    pub async fn cleanup(&self) -> Result<()> {
        // We'll implement database cleanup later
        // For now, just ensure we have proper separation
        Ok(())
    }

    /// Get the storage instance for testing
    pub fn storage(&self) -> Arc<dyn StorageInstance> {
        self.storage.clone()
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
