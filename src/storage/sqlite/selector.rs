//! Reading the series of a selector with a handful of queries: one for the sensors and their
//! labels, then one per numeric value type for all the samples, whatever the number of series.

use super::SqliteStorage;
use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
use crate::datamodel::{Sample, SensAppDateTime, Sensor, SensorType, TypedSamples};
use crate::storage::LabelMatcher;
use crate::storage::selector::{BulkSelectorBackend, empty_samples};
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
    ) -> Result<Vec<(i64, Sensor)>> {
        let (name_matchers, label_matchers): (Vec<_>, Vec<_>) = matchers
            .iter()
            .partition(|matcher| matcher.is_name_matcher());
        self.find_sensors_by_matchers(&name_matchers, &label_matchers, numeric_only)
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
        // Generated placeholders only: the ids, the time bounds and the limit are bound below.
        // A selector reads at most `max_series` sensors, far below SQLite's variable limit.
        let placeholders = vec!["?"; sensor_ids.len()].join(", ");
        let sql = format!(
            "SELECT sensor_id, timestamp_us, value FROM {table} \
             WHERE sensor_id IN ({placeholders}) \
             AND (? IS NULL OR timestamp_us >= ?) AND (? IS NULL OR timestamp_us <= ?) \
             ORDER BY sensor_id, timestamp_us LIMIT ?"
        );

        macro_rules! read {
            ($value:ty, $variant:ident, $convert:expr) => {{
                #[derive(sqlx::FromRow)]
                struct Row {
                    sensor_id: i64,
                    timestamp_us: i64,
                    value: $value,
                }
                let mut query = sqlx::query_as::<_, Row>(sqlx::AssertSqlSafe(sql.as_str()));
                for sensor_id in sensor_ids {
                    query = query.bind(sensor_id);
                }
                let rows = query
                    .bind(start_us)
                    .bind(start_us)
                    .bind(end_us)
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
