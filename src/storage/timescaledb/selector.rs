//! Reading the series of a selector with a handful of queries: one for the sensors and their
//! labels, then one per numeric value type for all the samples, whatever the number of series.

use super::{TimeScaleDBStorage, micros_to_offset_datetime, offset_datetime_to_sensapp};
use crate::datamodel::{Sample, Sensor, SensorType, TypedSamples};
use crate::storage::LabelMatcher;
use crate::storage::selector::{BulkSelectorBackend, empty_samples};
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
