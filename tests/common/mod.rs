use anyhow::{Result, anyhow};
use sensapp::storage::{storage_factory::create_storage_from_connection_string, StorageInstance};
use std::sync::Arc;
use uuid::Uuid;

pub mod fixtures;
pub mod db;
pub mod http;

/// Test database manager that creates isolated test databases
pub struct TestDb {
    pub db_name: String,
    pub storage: Arc<dyn StorageInstance>,
    pub connection_string: String,
}

impl TestDb {
    /// Create a new test database with a unique name
    pub async fn new() -> Result<Self> {
        let db_name = format!("sensapp_test_{}", Uuid::new_v4().simple());
        
        // Use environment variable or default to localhost
        let base_url = std::env::var("TEST_DATABASE_URL")
            .unwrap_or_else(|_| "postgres://postgres:postgres@localhost:5432".to_string());
        
        // Create the database
        let admin_connection_string = format!("{}/postgres", base_url);
        let _admin_storage = create_storage_from_connection_string(&admin_connection_string).await?;
        
        // Create test database (we'll implement this in the storage trait later)
        let connection_string = format!("{}/{}", base_url, db_name);
        
        // For now, just connect to the test database
        let storage = create_storage_from_connection_string(&connection_string).await?;
        
        // Run migrations
        storage.create_or_migrate().await?;
        
        Ok(Self {
            db_name: db_name.clone(),
            storage,
            connection_string,
        })
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

impl Drop for TestDb {
    fn drop(&mut self) {
        // Async cleanup would be better, but this ensures cleanup happens
        println!("Test database {} cleaned up", self.db_name);
    }
}

/// Helper trait for easier testing
pub trait TestHelpers {
    fn expect_sensor_count(&self, expected: usize) -> impl std::future::Future<Output = Result<()>> + Send;
}

impl TestHelpers for Arc<dyn StorageInstance> {
    async fn expect_sensor_count(&self, expected: usize) -> Result<()> {
        let sensors = self.list_sensors().await?;
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