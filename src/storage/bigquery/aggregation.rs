//! Aggregation in BigQuery: the buckets of `step` starting at the beginning of the window (at 0 without
//! one), as in the other backends and in `storage::common::apply_query_options`, which is the
//! reference the integration tests compare with.

use super::BigQueryStorage;
use super::client::{int_array_param, int_param};
use super::reads::{read_sample, sample_table, window};
use crate::datamodel::{SensorType, TypedSamples};
use crate::storage::Aggregation;
use crate::storage::selector::{AggregatedKind, AggregatedRead, aggregated_kind, empty_samples};
use anyhow::{Context, Result, bail};
use gcp_bigquery_client::model::query_parameter::QueryParameter;
use std::collections::HashMap;

/// The aggregate of a bucket. `First` and `Last` take the value of the oldest and the newest sample.
///
/// `AVG` is BigQuery's own: it is faster than a sum over a count, and floating point averages differ
/// from the exact one in the last bits (`-5.5e-17` for integers that average to 0).
fn value_expression(aggregation: Aggregation) -> &'static str {
    match aggregation {
        Aggregation::Avg => "AVG(value)",
        Aggregation::Min => "MIN(value)",
        Aggregation::Max => "MAX(value)",
        Aggregation::Sum => "SUM(value)",
        Aggregation::Count => "COUNT(*)",
        Aggregation::First => "ANY_VALUE(value HAVING MIN timestamp)",
        Aggregation::Last => "ANY_VALUE(value HAVING MAX timestamp)",
    }
}

/// The start of the bucket of a sample, in microseconds. A floor division, so that a sample before the
/// origin falls in the bucket before it, as `bucket_start` does (BigQuery's `DIV` truncates).
const BUCKET_EXPRESSION: &str = "@origin + (DIV(UNIX_MICROS(timestamp) - @origin, @step) \
     - IF(MOD(UNIX_MICROS(timestamp) - @origin, @step) < 0, 1, 0)) * @step";

pub fn kind_type(kind: AggregatedKind) -> SensorType {
    match kind {
        AggregatedKind::Integer => SensorType::Integer,
        AggregatedKind::Float => SensorType::Float,
        AggregatedKind::Numeric => SensorType::Numeric,
    }
}

/// The statement that aggregates the series of `@ids` (a numeric type), and its parameters other
/// than the ids: one row `sensor_id, bucket_us, value` per series and bucket, at most `limit`.
pub fn aggregated_sql(
    table: &str,
    sensor_type: SensorType,
    read: &AggregatedRead,
    limit: usize,
) -> Result<(String, Vec<QueryParameter>)> {
    if !matches!(
        sensor_type,
        SensorType::Integer | SensorType::Numeric | SensorType::Float
    ) {
        bail!("{sensor_type} is not a numeric type");
    }
    let step_us = read
        .step_ms
        .checked_mul(1000)
        .context("step is too large")?;
    let mut params = vec![
        int_param("origin", read.start_us.unwrap_or(0)),
        int_param("step", step_us),
    ];
    let conditions = window(read.start_us, read.end_us, &mut params);
    let sql = format!(
        "SELECT sensor_id, {BUCKET_EXPRESSION} AS bucket_us, {} AS value FROM {table} \
         WHERE sensor_id IN UNNEST(@ids){conditions} \
         GROUP BY sensor_id, bucket_us ORDER BY sensor_id, bucket_us LIMIT {}",
        value_expression(read.aggregation),
        limit.min(super::reads::MAX_LIMIT),
    );
    Ok((sql, params))
}

impl BigQueryStorage {
    /// The buckets of many series of one numeric type, with one statement, at most `limit` in total.
    pub(super) async fn query_aggregated_of_many(
        &self,
        sensor_type: SensorType,
        ids: &[i64],
        read: &AggregatedRead,
        limit: usize,
    ) -> Result<HashMap<i64, TypedSamples>> {
        let (sql, mut params) = aggregated_sql(
            &self.table(sample_table(sensor_type)),
            sensor_type,
            read,
            limit,
        )?;
        params.push(int_array_param("ids", ids));
        let kind = kind_type(aggregated_kind(sensor_type, read.aggregation));

        let rows = self
            .query_rows("aggregate samples", sql, params, |row| {
                let sensor_id = super::client::required(row.get_i64(0), "sensor_id")?;
                let mut bucket = empty_samples(kind);
                read_sample(&mut bucket, row)?;
                Ok((sensor_id, bucket))
            })
            .await?;
        let mut by_sensor: HashMap<i64, TypedSamples> = HashMap::new();
        for (sensor_id, bucket) in rows {
            super::reads::append_samples(
                by_sensor
                    .entry(sensor_id)
                    .or_insert_with(|| empty_samples(kind)),
                bucket,
            );
        }
        Ok(by_sensor)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn read(
        aggregation: Aggregation,
        start_us: Option<i64>,
        end_us: Option<i64>,
    ) -> AggregatedRead {
        AggregatedRead {
            start_us,
            end_us,
            step_ms: 21_600_000,
            aggregation,
        }
    }

    #[test]
    fn six_hour_buckets_are_computed_in_the_database_with_parameters() {
        let (sql, params) = aggregated_sql(
            "`p.d.float_values`",
            SensorType::Float,
            &read(Aggregation::Avg, Some(1_000), Some(2_000)),
            500,
        )
        .unwrap();
        assert!(
            sql.contains("AVG(value) AS value FROM `p.d.float_values`"),
            "{sql}"
        );
        assert!(sql.contains("GROUP BY sensor_id, bucket_us"), "{sql}");
        assert!(
            sql.contains("timestamp >= TIMESTAMP_MICROS(@start)"),
            "{sql}"
        );
        assert!(sql.contains("timestamp <= TIMESTAMP_MICROS(@end)"), "{sql}");
        assert!(
            sql.ends_with("ORDER BY sensor_id, bucket_us LIMIT 500"),
            "{sql}"
        );
        let values: Vec<_> = params
            .iter()
            .map(|p| {
                (
                    p.name.clone().unwrap(),
                    p.parameter_value.as_ref().unwrap().value.clone().unwrap(),
                )
            })
            .collect();
        assert_eq!(
            values,
            vec![
                ("origin".to_string(), "1000".to_string()),
                ("step".to_string(), "21600000000".to_string()),
                ("start".to_string(), "1000".to_string()),
                ("end".to_string(), "2000".to_string()),
            ]
        );
    }

    #[test]
    fn without_a_window_the_buckets_start_at_zero() {
        let (sql, params) = aggregated_sql(
            "`t`",
            SensorType::Integer,
            &read(Aggregation::Count, None, None),
            10,
        )
        .unwrap();
        assert!(!sql.contains("@start") && !sql.contains("@end"));
        assert!(sql.contains("COUNT(*) AS value"));
        assert_eq!(
            params[0].parameter_value.as_ref().unwrap().value.as_deref(),
            Some("0")
        );
    }

    #[test]
    fn every_aggregation_has_an_expression() {
        for (aggregation, expected) in [
            (Aggregation::Avg, "AVG(value)"),
            (Aggregation::Min, "MIN(value)"),
            (Aggregation::Max, "MAX(value)"),
            (Aggregation::Sum, "SUM(value)"),
            (Aggregation::Count, "COUNT(*)"),
            (Aggregation::First, "HAVING MIN timestamp"),
            (Aggregation::Last, "HAVING MAX timestamp"),
        ] {
            assert!(value_expression(aggregation).contains(expected));
        }
    }

    #[test]
    fn only_numeric_series_are_aggregated() {
        assert!(
            aggregated_sql(
                "`t`",
                SensorType::String,
                &read(Aggregation::Count, None, None),
                1
            )
            .is_err()
        );
        let huge = AggregatedRead {
            step_ms: i64::MAX,
            ..read(Aggregation::Avg, None, None)
        };
        assert!(aggregated_sql("`t`", SensorType::Float, &huge, 1).is_err());
    }

    #[test]
    fn the_type_of_the_buckets_follows_the_shared_rule() {
        assert_eq!(
            kind_type(aggregated_kind(SensorType::Integer, Aggregation::Avg)),
            SensorType::Float
        );
        assert_eq!(
            kind_type(aggregated_kind(SensorType::Float, Aggregation::Count)),
            SensorType::Integer
        );
        assert_eq!(
            kind_type(aggregated_kind(SensorType::Numeric, Aggregation::Sum)),
            SensorType::Numeric
        );
    }
}
