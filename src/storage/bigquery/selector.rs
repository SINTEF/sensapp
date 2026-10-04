//! Reading the series of a selector with a few queries: one for the sensors and their labels, then
//! one per numeric type for all their samples.
//!
//! Aggregated selectors too: one aggregating statement per numeric type.

use super::BigQueryStorage;
use crate::datamodel::{SensAppDateTime, Sensor, SensorType, TypedSamples};
use crate::storage::LabelMatcher;
use crate::storage::selector::{AggregatedRead, BulkSelectorBackend};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;

#[async_trait]
impl BulkSelectorBackend for BigQueryStorage {
    type SensorKey = i64;

    async fn find_selector_sensors(
        &self,
        matchers: &[LabelMatcher],
        numeric_only: bool,
        limit: Option<usize>,
    ) -> Result<Vec<(i64, Sensor)>> {
        self.find_sensors_by_matchers(matchers, numeric_only, limit)
            .await
    }

    async fn read_numeric_samples(
        &self,
        sensor_type: SensorType,
        sensors: &[i64],
        start_us: Option<i64>,
        end_us: Option<i64>,
        limit: usize,
    ) -> Result<HashMap<i64, TypedSamples>> {
        self.query_numeric_samples_of_many(sensor_type, sensors, start_us, end_us, limit)
            .await
    }

    async fn read_aggregated_samples(
        &self,
        sensor_type: SensorType,
        sensors: &[i64],
        read: &AggregatedRead,
        limit: usize,
    ) -> Result<HashMap<i64, TypedSamples>> {
        self.query_aggregated_of_many(sensor_type, sensors, read, limit)
            .await
    }

    async fn read_other_samples(
        &self,
        sensor_key: i64,
        sensor: &Sensor,
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
        limit: usize,
    ) -> Result<TypedSamples> {
        self.query_samples_by_type(
            sensor_key,
            sensor.sensor_type,
            start_time,
            end_time,
            limit,
            false,
        )
        .await
    }
}
