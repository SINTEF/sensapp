// Core storage traits and factory - always available
use crate::datamodel::{Sample, SensAppDateTime, Sensor};
use anyhow::Result;
use async_trait::async_trait;
use std::fmt::Debug;

pub mod error;
pub use error::StorageError;

pub mod common;

/// Default limit for timeseries queries when no limit is specified
/// Set to 10 million records - appropriate for timeseries data
pub const DEFAULT_QUERY_LIMIT: usize = 10_000_000;

#[async_trait]
pub trait StorageInstance: Send + Sync + Debug {
    async fn create_or_migrate(&self) -> Result<()>;
    async fn publish(&self, batch: std::sync::Arc<crate::datamodel::batch::Batch>) -> Result<()>;

    async fn vacuum(&self) -> Result<()>;

    async fn list_series(
        &self,
        metric_filter: Option<&str>,
    ) -> Result<Vec<crate::datamodel::Sensor>>;

    async fn list_metrics(&self) -> Result<Vec<crate::datamodel::Metric>>;

    /// Query sensor data by UUID with optional time range and limit
    async fn query_sensor_data(
        &self,
        sensor_uuid: &str,
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
        limit: Option<usize>,
    ) -> Result<Option<crate::datamodel::SensorData>>;

    /// Query Prometheus time series data using label matchers
    /// Returns a vector of (Sensor, samples) tuples for matching time series
    async fn query_prometheus_time_series(
        &self,
        matchers: &[crate::parsing::prometheus::remote_read_models::LabelMatcher],
        start_time_ms: i64,
        end_time_ms: i64,
    ) -> Result<Vec<(Sensor, Vec<Sample<f64>>)>>;

    /// Clean up all test data from the database
    /// This method is intended for testing purposes only
    #[cfg(feature = "test-utils")]
    #[allow(dead_code)] // False positive: Used by test_utils::TestDb through trait object
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

#[cfg(feature = "clickhouse")]
pub mod clickhouse;
