//! Reading the series of a selector with a handful of queries: one for the sensors and their
//! labels, then one per numeric value type for all the samples, whatever the number of series.

use super::ClickHouseStorage;
use super::clickhouse_utilities::{
    decimal_from_clickhouse_raw, map_clickhouse_error, micros_to_datetime,
};
use crate::datamodel::{Sample, SensAppDateTime, Sensor, SensorType, TypedSamples};
use crate::storage::LabelMatcher;
use crate::storage::selector::{AggregatedRead, BulkSelectorBackend, empty_samples};
use anyhow::{Context, Result};
use async_trait::async_trait;
use std::collections::HashMap;

#[async_trait]
impl BulkSelectorBackend for ClickHouseStorage {
    type SensorKey = u64;

    async fn find_selector_sensors(
        &self,
        matchers: &[LabelMatcher],
        numeric_only: bool,
        limit: Option<usize>,
    ) -> Result<Vec<(u64, Sensor)>> {
        let (name_matchers, label_matchers): (Vec<_>, Vec<_>) = matchers
            .iter()
            .partition(|matcher| matcher.is_name_matcher());
        self.find_sensors_by_matchers(&name_matchers, &label_matchers, numeric_only, limit)
            .await
    }

    async fn read_aggregated_samples(
        &self,
        sensor_type: SensorType,
        sensor_ids: &[u64],
        read: &AggregatedRead,
        limit: usize,
    ) -> Result<HashMap<u64, TypedSamples>> {
        self.read_aggregated_bulk(sensor_type, sensor_ids, read, limit)
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
        let mut time_where = String::new();
        if start_us.is_some() {
            time_where.push_str(" AND timestamp_us >= ?");
        }
        if end_us.is_some() {
            time_where.push_str(" AND timestamp_us <= ?");
        }
        let sql = format!(
            "SELECT sensor_id, timestamp_us, value FROM {table} \
             WHERE sensor_id IN {{ids:Array(UInt64)}}{time_where} \
             ORDER BY sensor_id ASC, timestamp_us ASC LIMIT {limit}"
        );

        // The ids are a server-side parameter: the statement is the same whatever the ids, and the
        // primary key still narrows the read (checked with EXPLAIN: `has(?, sensor_id)` does not)
        let mut query = self.client.query(&sql).param("ids", sensor_ids);
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

impl ClickHouseStorage {
    /// The aggregated buckets of many sensors of one numeric type, with one query: the SQL of the
    /// read of a single sensor, over many sensors, grouped by sensor and bucket.
    pub(super) async fn read_aggregated_bulk(
        &self,
        sensor_type: SensorType,
        sensor_ids: &[u64],
        read: &AggregatedRead,
        limit: usize,
    ) -> Result<HashMap<u64, TypedSamples>> {
        use super::{
            AggregatedSamplesQuery, clickhouse_float_expression, clickhouse_integer_expression,
            clickhouse_numeric_expression, clickhouse_time_where,
        };
        use crate::storage::Aggregation;
        use crate::storage::selector::{AggregatedKind, aggregated_kind};

        let table = match sensor_type {
            SensorType::Integer => "integer_values",
            SensorType::Numeric => "numeric_values",
            SensorType::Float => "float_values",
            other => anyhow::bail!("{other} is not a numeric type"),
        };
        let expression = match (sensor_type, read.aggregation) {
            (_, Aggregation::Count) => "toInt64(count())",
            (SensorType::Integer, Aggregation::Avg) => "avg(value)",
            (SensorType::Integer, aggregation) => clickhouse_integer_expression(aggregation),
            (SensorType::Float, aggregation) => clickhouse_float_expression(aggregation),
            (_, aggregation) => clickhouse_numeric_expression(aggregation),
        };
        let step_us = read
            .step_ms
            .checked_mul(1000)
            .context("step is too large")?;
        let bucket_expr = Self::aggregated_bucket_expr(&AggregatedSamplesQuery {
            sensor_id: 0,
            start_time_us: read.start_us,
            end_time_us: read.end_us,
            step_us,
            origin_us: read.start_us.unwrap_or(0),
            aggregation: read.aggregation,
            limit: None,
        });
        let where_clause = clickhouse_time_where(read.start_us, read.end_us);
        let sql = format!(
            "SELECT sensor_id, {bucket_expr} AS bucket_us, {expression} AS value \
             FROM {table} WHERE sensor_id IN {{ids:Array(UInt64)}}{where_clause} \
             GROUP BY sensor_id, bucket_us \
             ORDER BY sensor_id ASC, bucket_us ASC LIMIT {limit}"
        );

        macro_rules! fetch {
            ($value:ty, $variant:ident, $convert:expr) => {{
                let mut query = self.client.query(&sql).param("ids", sensor_ids);
                if let Some(start_us) = read.start_us {
                    query = query.bind(start_us);
                }
                if let Some(end_us) = read.end_us {
                    query = query.bind(end_us);
                }
                let mut cursor = query
                    .fetch::<(u64, i64, $value)>()
                    .map_err(|e| map_clickhouse_error(e, None, None))?;
                let mut result: HashMap<u64, TypedSamples> = HashMap::new();
                while let Some((sensor_id, timestamp_us, value)) = cursor.next().await? {
                    if let TypedSamples::$variant(samples) = result
                        .entry(sensor_id)
                        .or_insert_with(|| empty_samples(SensorType::$variant))
                    {
                        let convert: fn($value) -> _ = $convert;
                        samples.push(Sample {
                            datetime: micros_to_datetime(timestamp_us),
                            value: convert(value),
                        });
                    }
                }
                result
            }};
        }

        Ok(match aggregated_kind(sensor_type, read.aggregation) {
            AggregatedKind::Integer => fetch!(i64, Integer, |value| value),
            AggregatedKind::Float => fetch!(f64, Float, |value| value),
            AggregatedKind::Numeric => fetch!(i128, Numeric, decimal_from_clickhouse_raw),
        })
    }
}
