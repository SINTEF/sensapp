//! Reading the series of a selector with a handful of queries: one for the sensors and their
//! labels, then one per numeric value type for all the samples, whatever the number of series.

use super::{TimeScaleDBStorage, micros_to_offset_datetime, offset_datetime_to_sensapp};
use crate::datamodel::{Sample, Sensor, SensorType, TypedSamples};
use crate::storage::LabelMatcher;
use crate::storage::selector::{AggregatedRead, BulkSelectorBackend, empty_samples};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;

#[async_trait]
impl BulkSelectorBackend for TimeScaleDBStorage {
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
        let start = start_us.map(micros_to_offset_datetime);
        let end = end_us.map(micros_to_offset_datetime);

        // One statement per type, written out: the table is chosen here, never by the caller
        macro_rules! read {
            ($table:literal, $value:ty, $variant:ident) => {{
                #[derive(sqlx::FromRow)]
                struct Row {
                    sensor_id: i64,
                    time: sqlx::types::time::OffsetDateTime,
                    value: $value,
                }
                let rows: Vec<Row> = sqlx::query_as(concat!(
                    "SELECT sensor_id, time, value FROM ",
                    $table,
                    " WHERE sensor_id = ANY($1)",
                    " AND ($2::TIMESTAMPTZ IS NULL OR time >= $2)",
                    " AND ($3::TIMESTAMPTZ IS NULL OR time <= $3)",
                    " ORDER BY sensor_id, time LIMIT $4"
                ))
                .bind(sensor_ids)
                .bind(start)
                .bind(end)
                .bind(i64::try_from(limit).unwrap_or(i64::MAX))
                .fetch_all(&self.pool)
                .await?;

                let mut result: HashMap<i64, TypedSamples> = HashMap::new();
                for row in rows {
                    if let TypedSamples::$variant(samples) = result
                        .entry(row.sensor_id)
                        .or_insert_with(|| empty_samples(SensorType::$variant))
                    {
                        samples.push(Sample {
                            datetime: offset_datetime_to_sensapp(row.time),
                            value: row.value,
                        });
                    }
                }
                result
            }};
        }

        Ok(match sensor_type {
            SensorType::Integer => read!("integer_values", i64, Integer),
            SensorType::Numeric => read!("numeric_values", rust_decimal::Decimal, Numeric),
            SensorType::Float => read!("float_values", f64, Float),
            other => anyhow::bail!("{other} is not a numeric type"),
        })
    }
}

impl TimeScaleDBStorage {
    /// The aggregated buckets of many sensors of one numeric type, with one query: the SQL of the
    /// read of a single sensor, over many sensors, grouped by sensor and bucket.
    pub(super) async fn read_aggregated_bulk(
        &self,
        sensor_type: SensorType,
        sensor_ids: &[i64],
        read: &AggregatedRead,
        limit: usize,
    ) -> Result<HashMap<i64, TypedSamples>> {
        use super::{
            timescaledb_bucketed_cte_for, timescaledb_float_expression,
            timescaledb_group_by_clause_for, timescaledb_integer_expression,
            timescaledb_numeric_expression,
        };
        use crate::storage::Aggregation;
        use crate::storage::selector::{AggregatedKind, aggregated_kind};

        let (table, expression): (&'static str, String) = match (sensor_type, read.aggregation) {
            (SensorType::Integer, Aggregation::Count) => {
                ("integer_values", "COUNT(value)::bigint".into())
            }
            (SensorType::Float, Aggregation::Count) => {
                ("float_values", "COUNT(value)::bigint".into())
            }
            (SensorType::Numeric, Aggregation::Count) => {
                ("numeric_values", "COUNT(value)::bigint".into())
            }
            (SensorType::Integer, Aggregation::Avg) => {
                ("integer_values", "AVG(value)::double precision".into())
            }
            (SensorType::Integer, aggregation) => (
                "integer_values",
                timescaledb_integer_expression(aggregation).into(),
            ),
            (SensorType::Float, aggregation) => (
                "float_values",
                timescaledb_float_expression(aggregation).into(),
            ),
            (SensorType::Numeric, aggregation) => (
                "numeric_values",
                timescaledb_numeric_expression(aggregation).into(),
            ),
            (other, _) => anyhow::bail!("{other} is not a numeric type"),
        };
        // The fragments are static and the table comes from the match above; the ids, the window,
        // the step and the limit are bound.
        let sql = format!(
            "{} SELECT sensor_id, bucket_time, {expression} AS value {}",
            timescaledb_bucketed_cte_for(table, true),
            timescaledb_group_by_clause_for(true)
        );
        let start = read.start_us.map(micros_to_offset_datetime);
        let end = read.end_us.map(micros_to_offset_datetime);
        let origin_us = read.start_us.unwrap_or(0);
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);

        macro_rules! fetch {
            ($value:ty, $variant:ident) => {{
                #[derive(sqlx::FromRow)]
                struct Row {
                    sensor_id: i64,
                    bucket_time: sqlx::types::time::OffsetDateTime,
                    value: $value,
                }
                let rows: Vec<Row> = sqlx::query_as(sqlx::AssertSqlSafe(sql.as_str()))
                    .bind(sensor_ids)
                    .bind(start)
                    .bind(end)
                    .bind(read.step_ms)
                    .bind(origin_us as f64)
                    .bind(limit)
                    .fetch_all(&self.pool)
                    .await?;
                let mut result: HashMap<i64, TypedSamples> = HashMap::new();
                for row in rows {
                    if let TypedSamples::$variant(samples) = result
                        .entry(row.sensor_id)
                        .or_insert_with(|| empty_samples(SensorType::$variant))
                    {
                        samples.push(Sample {
                            datetime: offset_datetime_to_sensapp(row.bucket_time),
                            value: row.value,
                        });
                    }
                }
                result
            }};
        }

        Ok(match aggregated_kind(sensor_type, read.aggregation) {
            AggregatedKind::Integer => fetch!(i64, Integer),
            AggregatedKind::Float => fetch!(f64, Float),
            AggregatedKind::Numeric => fetch!(rust_decimal::Decimal, Numeric),
        })
    }
}
