// Core storage traits and factory - always available
use anyhow::Result;
use async_trait::async_trait;
use std::fmt::Debug;
use crate::datamodel::SensAppDateTime;

pub mod error;
pub use error::StorageError;

pub mod common;

#[async_trait]
pub trait StorageInstance: Send + Sync + Debug {
    async fn create_or_migrate(&self) -> Result<()>;
    async fn publish(
        &self,
        batch: std::sync::Arc<crate::datamodel::batch::Batch>,
        sync_sender: async_broadcast::Sender<()>,
    ) -> Result<()>;
    async fn sync(&self, sync_sender: async_broadcast::Sender<()>) -> Result<()>;

    async fn vacuum(&self) -> Result<()>;

    async fn list_series(&self, metric_filter: Option<&str>) -> Result<Vec<crate::datamodel::Sensor>>;

    async fn list_metrics(&self) -> Result<Vec<crate::datamodel::Metric>>;

    /// Query sensor data by UUID with optional time range and limit
    async fn query_sensor_data(
        &self,
        sensor_uuid: &str,
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
        limit: Option<usize>,
    ) -> Result<Option<crate::datamodel::SensorData>>;

    /// Clean up all test data from the database
    /// This method is intended for testing purposes only
    #[cfg(any(test, feature = "test-utils"))]
    #[allow(dead_code)]
    async fn cleanup_test_data(&self) -> Result<()>;
}

pub mod storage_factory;

// Storage backends - conditionally compiled based on features
#[cfg(feature = "postgres")]
pub mod postgresql;

#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(feature = "timescaledb")]
pub mod timescaledb;

#[cfg(feature = "duckdb")]
pub mod duckdb;

#[cfg(feature = "bigquery")]
pub mod bigquery;

#[cfg(feature = "rrdcached")]
pub mod rrdcached;
