// Core storage traits and factory - always available
use crate::datamodel::{SensAppDateTime, Sensor};
use anyhow::Result;
use async_trait::async_trait;
use std::fmt::Debug;

pub mod error;
pub use error::StorageError;

pub mod common;
pub mod data_query;
pub mod query;

pub use data_query::{Aggregation, SensorDataQueryOptions, SimplifyOptions};
pub use query::{LabelMatcher, MatcherType};

/// Default limit for timeseries queries when no limit is specified
/// Set to 10 million records - appropriate for timeseries data
#[allow(dead_code)]
pub const DEFAULT_QUERY_LIMIT: usize = 10_000_000;

/// Default limit for list_series when no limit is specified
pub const DEFAULT_LIST_SERIES_LIMIT: usize = 256;

/// Maximum limit for list_series to prevent excessive memory usage
pub const MAX_LIST_SERIES_LIMIT: usize = 16384;

/// Result type for list_series with pagination support
#[derive(Debug)]
pub struct ListSeriesResult {
    /// The series matching the query
    pub series: Vec<crate::datamodel::Sensor>,
    /// Bookmark for the next page (sensor_id as string)
    /// None if this is the last page
    pub bookmark: Option<String>,
}

#[derive(Debug)]
pub struct SensorAvailabilitySummary {
    pub sensor: Sensor,
    pub sample_count: usize,
    pub first_sample_at: Option<SensAppDateTime>,
    pub last_sample_at: Option<SensAppDateTime>,
    pub covered_buckets: Option<usize>,
}

#[async_trait]
pub trait StorageInstance: Send + Sync + Debug {
    async fn create_or_migrate(&self) -> Result<()>;
    async fn publish(&self, batch: std::sync::Arc<crate::datamodel::batch::Batch>) -> Result<()>;

    async fn vacuum(&self) -> Result<()>;

    async fn list_series(
        &self,
        metric_filter: Option<&str>,
        limit: Option<usize>,
        bookmark: Option<&str>,
    ) -> Result<ListSeriesResult>;

    async fn list_metrics(&self) -> Result<Vec<crate::datamodel::Metric>>;

    /// Query sensor data by UUID with optional time range and limit
    async fn query_sensor_data(
        &self,
        sensor_uuid: &str,
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
        limit: Option<usize>,
    ) -> Result<Option<crate::datamodel::SensorData>>;

    async fn query_sensor_data_advanced(
        &self,
        sensor_uuid: &str,
        options: &crate::storage::SensorDataQueryOptions,
    ) -> Result<Option<crate::datamodel::SensorData>> {
        options.validate()?;

        let raw = self
            .query_sensor_data(
                sensor_uuid,
                options.start_time,
                options.end_time,
                options.limit,
            )
            .await?;

        raw.map(|sensor_data| crate::storage::common::apply_query_options(sensor_data, options))
            .transpose()
    }

    async fn query_sensor_data_latest(
        &self,
        sensor_uuid: &str,
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
    ) -> Result<Option<crate::datamodel::SensorData>> {
        let Some(sensor_data) = self
            .query_sensor_data(sensor_uuid, start_time, end_time, None)
            .await?
        else {
            return Ok(None);
        };

        Ok(crate::storage::common::keep_only_last_sample(sensor_data))
    }

    async fn query_sensor_data_availability(
        &self,
        sensor_uuid: &str,
        start_time: SensAppDateTime,
        end_time: SensAppDateTime,
        step_ms: Option<i64>,
    ) -> Result<Option<SensorAvailabilitySummary>> {
        let Some(sensor_data) = self
            .query_sensor_data(sensor_uuid, Some(start_time), Some(end_time), None)
            .await?
        else {
            return Ok(None);
        };

        crate::storage::common::summarize_sensor_data_availability(sensor_data, start_time, step_ms)
            .map(Some)
    }

    /// Query sensors and their data by label matchers.
    ///
    /// This method finds all sensors matching the given label matchers and returns
    /// their data within the specified time range. Multiple matchers are combined
    /// with AND logic.
    ///
    /// # Arguments
    ///
    /// * `matchers` - Label matchers to filter sensors. Use `__name__` to filter by metric name.
    /// * `start_time` - Optional start of time range (inclusive)
    /// * `end_time` - Optional end of time range (inclusive)
    /// * `limit` - Optional maximum number of samples per sensor
    /// * `numeric_only` - If true, only return sensors with numeric types (Integer, Numeric, Float).
    ///                    This is useful for Prometheus compatibility which only supports numeric data.
    ///
    /// # Returns
    ///
    /// A vector of `SensorData` for all matching sensors.
    async fn query_sensors_by_labels(
        &self,
        matchers: &[LabelMatcher],
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
        limit: Option<usize>,
        numeric_only: bool,
    ) -> Result<Vec<crate::datamodel::SensorData>>;

    /// Health check for the storage backend
    /// Returns Ok(()) if the storage is healthy and can accept connections
    /// Returns Err if the storage is unhealthy
    async fn health_check(&self) -> Result<()>;

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

#[cfg(feature = "clickhouse")]
pub mod clickhouse;
