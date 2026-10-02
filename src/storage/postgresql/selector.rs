//! Reading the series of a selector with a handful of queries: one for the sensors and their
//! labels, then one per numeric value type for all the samples, whatever the number of series.

use super::PostgresStorage;
use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
use crate::datamodel::{Sample, SensAppDateTime, Sensor, SensorType, TypedSamples};
use crate::storage::LabelMatcher;
use crate::storage::selector::{AggregatedRead, BulkSelectorBackend, empty_samples};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;

#[async_trait]
impl BulkSelectorBackend for PostgresStorage {
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

    async fn read_numeric_samples(
        &self,
        sensor_type: SensorType,
        sensor_ids: &[i64],
        start_us: Option<i64>,
        end_us: Option<i64>,
        limit: usize,
    ) -> Result<HashMap<i64, TypedSamples>> {
        // One statement per type, written out: the table is chosen here, never by the caller.
        // The ordered scan of (sensor_id, timestamp_us) stops after `limit` rows in total.
        macro_rules! read {
            ($table:literal, $value:ty, $variant:ident) => {{
                #[derive(sqlx::FromRow)]
                struct Row {
                    sensor_id: i64,
                    timestamp_us: i64,
                    value: $value,
                }
                let rows: Vec<Row> = sqlx::query_as(concat!(
                    "SELECT sensor_id, timestamp_us, value FROM ",
                    $table,
                    " WHERE sensor_id = ANY($1)",
                    " AND ($2::BIGINT IS NULL OR timestamp_us >= $2)",
                    " AND ($3::BIGINT IS NULL OR timestamp_us <= $3)",
                    " ORDER BY sensor_id, timestamp_us LIMIT $4"
                ))
                .bind(sensor_ids)
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
                        samples.push(Sample {
                            datetime: SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us),
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
}

impl PostgresStorage {
    /// The aggregated buckets of many sensors of one numeric type, with one query. The SQL is the
    /// one of the read of a single sensor, over many sensors, grouped by sensor and bucket.
    pub(super) async fn read_aggregated_bulk(
        &self,
        sensor_type: SensorType,
        sensor_ids: &[i64],
        read: &AggregatedRead,
        limit: usize,
    ) -> Result<HashMap<i64, TypedSamples>> {
        use super::queries::{
            bucketed_cte, float_aggregate_expression, group_by_clause,
            integer_aggregate_expression, numeric_aggregate_expression,
        };
        use crate::storage::Aggregation;
        use crate::storage::selector::{AggregatedKind, aggregated_kind};

        let (table, expression) = match (sensor_type, read.aggregation) {
            (_, Aggregation::Count) => (table_of(sensor_type)?, "COUNT(*)::bigint"),
            (SensorType::Integer, Aggregation::Avg) => {
                ("integer_values", "AVG(value)::double precision")
            }
            (SensorType::Integer, aggregation) => {
                ("integer_values", integer_aggregate_expression(aggregation))
            }
            (SensorType::Float, aggregation) => {
                ("float_values", float_aggregate_expression(aggregation))
            }
            (SensorType::Numeric, aggregation) => {
                ("numeric_values", numeric_aggregate_expression(aggregation))
            }
            (other, _) => anyhow::bail!("{other} is not a numeric type"),
        };
        // The fragments are static, the table comes from the match above; the ids, the window,
        // the step and the limit are bound.
        let sql = format!(
            "{} SELECT sensor_id, bucket_us AS timestamp_us, {expression} AS value {}",
            bucketed_cte(table, true),
            group_by_clause(true)
        );
        let origin_us = read.start_us.unwrap_or(0);
        let limit = i64::try_from(limit).unwrap_or(i64::MAX);

        macro_rules! fetch {
            ($value:ty, $variant:ident) => {{
                #[derive(sqlx::FromRow)]
                struct Row {
                    sensor_id: i64,
                    timestamp_us: i64,
                    value: $value,
                }
                let rows: Vec<Row> = sqlx::query_as(sqlx::AssertSqlSafe(sql.as_str()))
                    .bind(sensor_ids)
                    .bind(read.start_us)
                    .bind(read.end_us)
                    .bind(read.step_ms)
                    .bind(origin_us)
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
                            datetime: SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us),
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

fn table_of(sensor_type: SensorType) -> Result<&'static str> {
    Ok(match sensor_type {
        SensorType::Integer => "integer_values",
        SensorType::Numeric => "numeric_values",
        SensorType::Float => "float_values",
        other => anyhow::bail!("{other} is not a numeric type"),
    })
}
