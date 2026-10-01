use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
use crate::datamodel::{Metric, SensAppDateTime, SensorData};
use crate::storage::{
    LabelMatcher, ListSeriesResult, SelectorRead, SensorAvailabilitySummary,
    SensorDataQueryOptions, StorageInstance,
};
use anyhow::Result;
use async_trait::async_trait;
use std::sync::Arc;

use super::auth::AccessContext;

#[derive(Debug, thiserror::Error)]
#[error("Token cannot access sensor '{0}'")]
pub struct SensorAccessDenied(pub String);

/// Applies a token's sensor-name allow list at the storage boundary for HTTP requests.
#[derive(Debug)]
pub struct AuthorizedStorage {
    inner: Arc<dyn StorageInstance>,
    access: AccessContext,
}

impl AuthorizedStorage {
    pub fn new(inner: Arc<dyn StorageInstance>, access: AccessContext) -> Self {
        Self { inner, access }
    }

    fn allows(&self, name: &str) -> bool {
        self.access.can_access_sensor(name)
    }

    /// Whether the token may act on the sensor with this UUID.
    ///
    /// Sensors the token cannot access are reported like missing ones, as the read
    /// paths do. Unknown UUIDs pass through so the inner storage reports them.
    async fn can_see_sensor(&self, sensor_uuid: &str) -> Result<bool> {
        if self.access.sensor_allow_list.is_none() {
            return Ok(true);
        }
        // A one-instant window returns the sensor metadata without loading its samples
        let instant = SensAppDateTime::from_unix_microseconds_i64(0);
        let sensor_data = self
            .inner
            .query_sensor_data(sensor_uuid, Some(instant), Some(instant), Some(1))
            .await?;
        Ok(sensor_data.is_none_or(|data| self.allows(&data.sensor.name)))
    }
}

#[async_trait]
impl StorageInstance for AuthorizedStorage {
    async fn create_or_migrate(&self) -> Result<()> {
        self.inner.create_or_migrate().await
    }

    async fn publish(&self, batch: Arc<crate::datamodel::batch::Batch>) -> Result<()> {
        if let Some(sensor) = batch
            .sensors
            .iter()
            .find(|item| !self.allows(&item.sensor.name))
        {
            return Err(SensorAccessDenied(sensor.sensor.name.clone()).into());
        }
        self.inner.publish(batch).await
    }

    async fn vacuum(&self) -> Result<()> {
        self.inner.vacuum().await
    }

    async fn delete_series(&self, sensor_uuid: &str) -> Result<bool> {
        if !self.can_see_sensor(sensor_uuid).await? {
            return Ok(false);
        }
        self.inner.delete_series(sensor_uuid).await
    }

    async fn delete_series_samples(
        &self,
        sensor_uuid: &str,
        start_time: SensAppDateTime,
        end_time: SensAppDateTime,
    ) -> Result<Option<u64>> {
        if !self.can_see_sensor(sensor_uuid).await? {
            return Ok(None);
        }
        self.inner
            .delete_series_samples(sensor_uuid, start_time, end_time)
            .await
    }

    async fn list_series(
        &self,
        metric_filter: Option<&str>,
        limit: Option<usize>,
        bookmark: Option<&str>,
    ) -> Result<ListSeriesResult> {
        let mut result = self
            .inner
            .list_series(metric_filter, limit, bookmark)
            .await?;
        result.series.retain(|sensor| self.allows(&sensor.name));
        Ok(result)
    }

    async fn list_metrics(&self) -> Result<Vec<Metric>> {
        let mut metrics = self.inner.list_metrics().await?;
        metrics.retain(|metric| self.allows(&metric.name));
        Ok(metrics)
    }

    async fn query_sensor_data(
        &self,
        sensor_uuid: &str,
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
        limit: Option<usize>,
    ) -> Result<Option<SensorData>> {
        Ok(self
            .inner
            .query_sensor_data(sensor_uuid, start_time, end_time, limit)
            .await?
            .filter(|data| self.allows(&data.sensor.name)))
    }

    async fn query_sensor_data_advanced(
        &self,
        sensor_uuid: &str,
        options: &SensorDataQueryOptions,
    ) -> Result<Option<SensorData>> {
        Ok(self
            .inner
            .query_sensor_data_advanced(sensor_uuid, options)
            .await?
            .filter(|data| self.allows(&data.sensor.name)))
    }

    async fn query_sensor_data_latest(
        &self,
        sensor_uuid: &str,
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
    ) -> Result<Option<SensorData>> {
        Ok(self
            .inner
            .query_sensor_data_latest(sensor_uuid, start_time, end_time)
            .await?
            .filter(|data| self.allows(&data.sensor.name)))
    }

    async fn query_sensor_data_availability(
        &self,
        sensor_uuid: &str,
        start_time: SensAppDateTime,
        end_time: SensAppDateTime,
        step_ms: Option<i64>,
    ) -> Result<Option<SensorAvailabilitySummary>> {
        Ok(self
            .inner
            .query_sensor_data_availability(sensor_uuid, start_time, end_time, step_ms)
            .await?
            .filter(|summary| self.allows(&summary.sensor.name)))
    }

    async fn query_sensors_by_labels(
        &self,
        matchers: &[LabelMatcher],
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
        limit: Option<usize>,
        numeric_only: bool,
    ) -> Result<Vec<SensorData>> {
        let mut result = self
            .inner
            .query_sensors_by_labels(matchers, start_time, end_time, limit, numeric_only)
            .await?;
        result.retain(|data| self.allows(&data.sensor.name));
        Ok(result)
    }

    async fn query_selector(
        &self,
        matchers: &[LabelMatcher],
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
        numeric_only: bool,
        max_series: usize,
        max_samples: usize,
    ) -> Result<SelectorRead> {
        if self.access.sensor_allow_list.is_none() {
            // The token sees every sensor: the backend's own read applies as is
            return self
                .inner
                .query_selector(
                    matchers,
                    start_time,
                    end_time,
                    numeric_only,
                    max_series,
                    max_samples,
                )
                .await;
        }
        // Otherwise the limits only count the sensors the token sees: read through the
        // filtered methods of this wrapper
        crate::storage::selector::query_selector_sequential(
            self,
            matchers,
            start_time,
            end_time,
            numeric_only,
            max_series,
            max_samples,
        )
        .await
    }

    async fn health_check(&self) -> Result<()> {
        self.inner.health_check().await
    }

    #[cfg(any(test, feature = "test-utils"))]
    async fn cleanup_test_data(&self) -> Result<()> {
        self.inner.cleanup_test_data().await
    }
}
