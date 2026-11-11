//! Reading the series of a selector with a handful of queries: one for the sensors and their
//! labels, then one per numeric value type for all the samples, whatever the number of series.

use super::SqliteStorage;
use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
use crate::datamodel::{Sample, SensAppDateTime, Sensor, SensorType, TypedSamples};
use crate::storage::LabelMatcher;
use crate::storage::selector::{AggregatedRead, BulkSelectorBackend, empty_samples};
use anyhow::{Context, Result};
use async_trait::async_trait;
use std::collections::HashMap;

#[async_trait]
impl BulkSelectorBackend for SqliteStorage {
    type SensorKey = i64;

    async fn find_selector_sensors(
        &self,
        matchers: &[LabelMatcher],
        numeric_only: bool,
        limit: Option<usize>,
    ) -> Result<Vec<(i64, Sensor)>> {
        let (name_matchers, label_matchers): (Vec<_>, Vec<_>) = matchers
            .iter()
            .partition(|matcher| matcher.is_name_matcher());
        self.find_sensors_by_matchers(&name_matchers, &label_matchers, numeric_only, limit)
            .await
    }

    async fn read_aggregated_samples(
        &self,
        sensor_type: SensorType,
        sensor_ids: &[i64],
        read: &AggregatedRead,
        limit: usize,
    ) -> Result<HashMap<i64, TypedSamples>> {
        self.read_aggregated_bulk(sensor_type, sensor_ids, read, limit)
            .await
    }

    async fn read_numeric_samples(
        &self,
        sensor_type: SensorType,
        sensor_ids: &[i64],
        start_us: Option<i64>,
        end_us: Option<i64>,
        limit: usize,
    ) -> Result<HashMap<i64, TypedSamples>> {
        let table = match sensor_type {
            SensorType::Integer => "integer_values",
            SensorType::Numeric => "numeric_values",
            SensorType::Float => "float_values",
            other => anyhow::bail!("{other} is not a numeric type"),
        };
        // The ids are one JSON array: no limit on the number of bound variables
        let ids = serde_json::to_string(sensor_ids)?;
        let sql = format!(
            "SELECT sensor_id, timestamp_us, value FROM {table} \
             WHERE sensor_id IN (SELECT value FROM json_each(?)) \
             AND timestamp_us >= COALESCE(?, -9223372036854775807) AND timestamp_us <= COALESCE(?, 9223372036854775807) \
             ORDER BY sensor_id, timestamp_us, rowid LIMIT ?"
        );

        macro_rules! read {
            ($value:ty, $variant:ident, $convert:expr) => {{
                #[derive(sqlx::FromRow)]
                struct Row {
                    sensor_id: i64,
                    timestamp_us: i64,
                    value: $value,
                }
                let rows = sqlx::query_as::<_, Row>(sqlx::AssertSqlSafe(sql.as_str()))
                    .bind(&ids)
                    .bind(start_us)
                    .bind(end_us)
                    .bind(i64::try_from(limit).unwrap_or(i64::MAX))
                    .fetch_all(&self.pool)
                    .await?;

                let mut result: HashMap<i64, TypedSamples> = HashMap::new();
                for row in rows {
                    if let TypedSamples::$variant(samples) = result
                        .entry(row.sensor_id)
                        .or_insert_with(|| empty_samples(SensorType::$variant))
                    {
                        let convert: fn($value) -> Result<_> = $convert;
                        samples.push(Sample {
                            datetime: SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us),
                            value: convert(row.value)?,
                        });
                    }
                }
                result
            }};
        }

        Ok(match sensor_type {
            SensorType::Integer => read!(i64, Integer, |value| Ok(value)),
            SensorType::Numeric => read!(String, Numeric, |value| {
                rust_decimal::Decimal::from_str_exact(&value)
                    .context("Failed to parse decimal value")
            }),
            _ => read!(f64, Float, |value| Ok(value)),
        })
    }
}

impl SqliteStorage {
    /// The aggregated buckets of many sensors of one numeric type, with one query: the SQL of the
    /// read of a single sensor, over many sensors, grouped by sensor and bucket. `?1` and `?2`
    /// bound the time, `?3` is the step and `?4` the origin of the buckets in microseconds, `?5`
    /// the limit, and the sensor ids follow.
    pub(super) async fn read_aggregated_bulk(
        &self,
        sensor_type: SensorType,
        sensor_ids: &[i64],
        read: &AggregatedRead,
        limit: usize,
    ) -> Result<HashMap<i64, TypedSamples>> {
        use super::storage_query_helpers::{sqlite_float_expression, sqlite_integer_expression};
        use crate::storage::Aggregation;
        use crate::storage::selector::{AggregatedKind, aggregated_kind};

        let table = match sensor_type {
            SensorType::Integer => "integer_values",
            SensorType::Numeric => "numeric_values",
            SensorType::Float => "float_values",
            other => anyhow::bail!("{other} is not a numeric type"),
        };
        let kind = aggregated_kind(sensor_type, read.aggregation);
        // Numeric values are stored as text and read back as text
        let numeric = sensor_type == SensorType::Numeric && kind == AggregatedKind::Numeric;
        let step_us = read
            .step_ms
            .checked_mul(1000)
            .context("step is too large")?;
        let origin_us = read.start_us.unwrap_or(0);
        if numeric
            && matches!(
                read.aggregation,
                Aggregation::Min | Aggregation::Max | Aggregation::Sum | Aggregation::Avg
            )
        {
            // Computed from the stored values, one series after the other, as for one series:
            // the values are text, which MIN and MAX compare as text
            let mut result = HashMap::new();
            for &sensor_id in sensor_ids {
                let samples = self
                    .query_numeric_samples_aggregated(
                        sensor_id,
                        read.start_us,
                        read.end_us,
                        step_us,
                        origin_us,
                        read.aggregation,
                        Some(limit),
                    )
                    .await?;
                if !samples.is_empty() {
                    result.insert(sensor_id, samples);
                }
            }
            return Ok(result);
        }
        // The ids are one JSON array: no limit on the number of bound variables
        let ids = serde_json::to_string(sensor_ids)?;
        let value = if numeric {
            "CAST(value AS TEXT)"
        } else {
            "value"
        };

        // The table and the fragments are static; the ids, the window, the step and the limit are
        // bound.
        let sql = match read.aggregation {
            Aggregation::First | Aggregation::Last | Aggregation::Latest => {
                let direction = if read.aggregation == Aggregation::First {
                    "ASC"
                } else {
                    "DESC"
                };
                // `Latest` is stamped with the timestamp of the sample, not the start of the bucket
                let stamp = if read.aggregation == Aggregation::Latest {
                    "sample_us"
                } else {
                    "bucket_us"
                };
                format!(
                    "WITH bucketed AS (
                       SELECT sensor_id, timestamp_us, rowid AS row_id, {value} AS value,
                              (timestamp_us - (((timestamp_us - ?4) % ?3) + ?3) % ?3) AS bucket_us
                       FROM {table}
                       WHERE sensor_id IN (SELECT value FROM json_each(?6))
                         AND timestamp_us >= COALESCE(?1, -9223372036854775807)
                         AND timestamp_us <= COALESCE(?2, 9223372036854775807)
                     ), ranked AS (
                       SELECT sensor_id, bucket_us, timestamp_us AS sample_us, value,
                              ROW_NUMBER() OVER (PARTITION BY sensor_id, bucket_us ORDER BY timestamp_us {direction}, row_id {direction}) AS row_num
                       FROM bucketed
                     )
                     SELECT sensor_id, {stamp} AS timestamp_us, value
                     FROM ranked WHERE row_num = 1
                     ORDER BY sensor_id, bucket_us ASC LIMIT ?5"
                )
            }
            aggregation => {
                let expression = match (sensor_type, aggregation) {
                    (_, Aggregation::Count) => "COUNT(*)".to_string(),
                    (SensorType::Integer, Aggregation::Avg) => "AVG(value)".to_string(),
                    (SensorType::Integer, aggregation) => {
                        sqlite_integer_expression(aggregation).to_string()
                    }
                    (SensorType::Float, aggregation) => {
                        sqlite_float_expression(aggregation).to_string()
                    }
                    (_, _) => unreachable!("Numeric buckets are computed in Rust, above"),
                };
                format!(
                    "WITH bucketed AS (
                       SELECT sensor_id, timestamp_us, value,
                              (timestamp_us - (((timestamp_us - ?4) % ?3) + ?3) % ?3) AS bucket_us
                       FROM {table}
                       WHERE sensor_id IN (SELECT value FROM json_each(?6))
                         AND timestamp_us >= COALESCE(?1, -9223372036854775807)
                         AND timestamp_us <= COALESCE(?2, 9223372036854775807)
                     )
                     SELECT sensor_id, bucket_us AS timestamp_us, {expression} AS value
                     FROM bucketed GROUP BY sensor_id, bucket_us
                     ORDER BY sensor_id, bucket_us ASC LIMIT ?5"
                )
            }
        };
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);

        macro_rules! fetch {
            ($value:ty, $variant:ident, $convert:expr) => {{
                #[derive(sqlx::FromRow)]
                struct Row {
                    sensor_id: i64,
                    timestamp_us: i64,
                    value: $value,
                }
                let query = sqlx::query_as::<_, Row>(sqlx::AssertSqlSafe(sql.as_str()))
                    .bind(read.start_us)
                    .bind(read.end_us)
                    .bind(step_us)
                    .bind(origin_us)
                    .bind(limit)
                    .bind(&ids);
                let rows = query
                    .fetch_all(&self.pool)
                    .await
                    .map_err(crate::storage::common::sql_sum_overflow)?;
                let mut result: HashMap<i64, TypedSamples> = HashMap::new();
                for row in rows {
                    if let TypedSamples::$variant(samples) = result
                        .entry(row.sensor_id)
                        .or_insert_with(|| empty_samples(SensorType::$variant))
                    {
                        let convert: fn($value) -> Result<_> = $convert;
                        samples.push(Sample {
                            datetime: SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us),
                            value: convert(row.value)?,
                        });
                    }
                }
                result
            }};
        }

        Ok(match kind {
            AggregatedKind::Integer => fetch!(i64, Integer, |value| Ok(value)),
            AggregatedKind::Float => fetch!(f64, Float, |value| Ok(value)),
            AggregatedKind::Numeric => fetch!(String, Numeric, |value| {
                rust_decimal::Decimal::from_str_exact(&value)
                    .context("Failed to parse aggregated SQLite numeric value")
            }),
        })
    }
}
