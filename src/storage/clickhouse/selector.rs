//! Reading the series of a selector with a handful of queries: one for the sensors and their
//! labels, then one per numeric value type for all the samples, whatever the number of series.

use super::ClickHouseStorage;
use super::clickhouse_utilities::{
    decimal_from_clickhouse_raw, map_clickhouse_error, micros_to_datetime,
};
use crate::datamodel::{Sample, SensAppDateTime, Sensor, SensorType, TypedSamples};
use crate::storage::LabelMatcher;
use crate::storage::selector::{BulkSelectorBackend, empty_samples};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;

#[async_trait]
impl BulkSelectorBackend for ClickHouseStorage {
    type SensorKey = u64;

    async fn find_selector_sensors(
        &self,
        matchers: &[LabelMatcher],
        numeric_only: bool,
    ) -> Result<Vec<(u64, Sensor)>> {
        let (name_matchers, label_matchers): (Vec<_>, Vec<_>) = matchers
            .iter()
            .partition(|matcher| matcher.is_name_matcher());
        self.find_sensors_by_matchers(&name_matchers, &label_matchers, numeric_only)
            .await
    }

    async fn read_numeric_samples(
        &self,
        sensor_type: SensorType,
        sensor_ids: &[u64],
        start_us: Option<i64>,
        end_us: Option<i64>,
        limit: usize,
    ) -> Result<HashMap<u64, TypedSamples>> {
        let table = match sensor_type {
            SensorType::Integer => "integer_values",
            SensorType::Numeric => "numeric_values",
            SensorType::Float => "float_values",
            other => anyhow::bail!("{other} is not a numeric type"),
        };
        // The ids are numbers: written into the statement, they cannot inject anything
        let ids = sensor_ids
            .iter()
            .map(u64::to_string)
            .collect::<Vec<_>>()
            .join(",");
        let mut time_where = String::new();
        if start_us.is_some() {
            time_where.push_str(" AND timestamp_us >= ?");
        }
        if end_us.is_some() {
            time_where.push_str(" AND timestamp_us <= ?");
        }
        let sql = format!(
            "SELECT sensor_id, timestamp_us, value FROM {table} \
             WHERE sensor_id IN ({ids}){time_where} \
             ORDER BY sensor_id ASC, timestamp_us ASC LIMIT {limit}"
        );

        let mut query = self.client.query(&sql);
        if let Some(start_us) = start_us {
            query = query.bind(start_us);
        }
        if let Some(end_us) = end_us {
            query = query.bind(end_us);
        }

        let mut result: HashMap<u64, TypedSamples> = HashMap::new();
        match sensor_type {
            SensorType::Integer => {
                let mut cursor = query
                    .fetch::<(u64, i64, i64)>()
                    .map_err(|e| map_clickhouse_error(e, None, None))?;
                while let Some((sensor_id, timestamp_us, value)) = cursor.next().await? {
                    if let TypedSamples::Integer(samples) = result
                        .entry(sensor_id)
                        .or_insert_with(|| empty_samples(SensorType::Integer))
                    {
                        samples.push(Sample {
                            datetime: micros_to_datetime(timestamp_us),
                            value,
                        });
                    }
                }
            }
            SensorType::Numeric => {
                let mut cursor = query
                    .fetch::<(u64, i64, i128)>()
                    .map_err(|e| map_clickhouse_error(e, None, None))?;
                while let Some((sensor_id, timestamp_us, raw)) = cursor.next().await? {
                    if let TypedSamples::Numeric(samples) = result
                        .entry(sensor_id)
                        .or_insert_with(|| empty_samples(SensorType::Numeric))
                    {
                        samples.push(Sample {
                            datetime: micros_to_datetime(timestamp_us),
                            value: decimal_from_clickhouse_raw(raw),
                        });
                    }
                }
            }
            _ => {
                let mut cursor = query
                    .fetch::<(u64, i64, f64)>()
                    .map_err(|e| map_clickhouse_error(e, None, None))?;
                while let Some((sensor_id, timestamp_us, value)) = cursor.next().await? {
                    if let TypedSamples::Float(samples) = result
                        .entry(sensor_id)
                        .or_insert_with(|| empty_samples(SensorType::Float))
                    {
                        samples.push(Sample {
                            datetime: micros_to_datetime(timestamp_us),
                            value,
                        });
                    }
                }
            }
        }
        Ok(result)
    }

    async fn read_other_samples(
        &self,
        sensor_id: u64,
        sensor: &Sensor,
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
        limit: usize,
    ) -> Result<TypedSamples> {
        self.query_samples_by_type(sensor_id, &sensor.sensor_type, start_time, end_time, limit)
            .await
    }
}
