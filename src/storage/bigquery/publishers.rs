//! Writing: the registration of new series and the samples of a batch, through the default stream of
//! the Storage Write API (rows are queryable at once, delivery is at least once).

use super::BigQueryStorage;
use super::client::{grpc_code_is_transient, int_array_param, map_error};
use super::rows::{self, *};
use crate::datamodel::{Sensor, TypedSamples, batch::Batch};
use crate::storage::StorageError;
use crate::storage::common::{datetime_to_micros, unit_name_to_id, uuid_to_sensor_id};
use anyhow::Result;
use futures::future::try_join_all;
use gcp_bigquery_client::google::cloud::bigquery::storage::v1::append_rows_response::Response;
use gcp_bigquery_client::storage::{StreamName, TableBatch, TableDescriptor};
use prost::Message;
use rust_decimal::{Decimal, RoundingStrategy};
use std::collections::HashMap;
use std::sync::Arc;

/// NUMERIC has 9 decimal digits.
const NUMERIC_SCALE: u32 = 9;

/// The id of a series in the tables: the bits of the 64-bit key shared with ClickHouse.
pub fn sensor_id(sensor: &Sensor) -> i64 {
    uuid_to_sensor_id(&sensor.uuid) as i64
}

pub fn unit_id(name: &str) -> i64 {
    unit_name_to_id(name) as i64
}

/// A decimal as BigQuery reads it in a NUMERIC column: rounded to 9 digits.
pub fn numeric_text(value: &Decimal) -> String {
    value
        .round_dp_with_strategy(NUMERIC_SCALE, RoundingStrategy::MidpointNearestEven)
        .to_string()
}

/// The rows of a batch, by table.
#[derive(Default)]
struct SampleRows {
    integer: Vec<IntegerValueRow>,
    numeric: Vec<NumericValueRow>,
    float: Vec<FloatValueRow>,
    string: Vec<StringValueRow>,
    boolean: Vec<BooleanValueRow>,
    location: Vec<LocationValueRow>,
    json: Vec<JsonValueRow>,
    blob: Vec<BlobValueRow>,
}

async fn sample_rows(batch: &Batch) -> SampleRows {
    let mut rows = SampleRows::default();
    for single in batch.sensors.iter() {
        let id = sensor_id(&single.sensor);
        let samples = single.samples.read().await;
        match &*samples {
            TypedSamples::Integer(samples) => {
                rows.integer.extend(samples.iter().map(|s| IntegerValueRow {
                    sensor_id: id,
                    timestamp: datetime_to_micros(&s.datetime),
                    value: s.value,
                }))
            }
            TypedSamples::Numeric(samples) => {
                rows.numeric.extend(samples.iter().map(|s| NumericValueRow {
                    sensor_id: id,
                    timestamp: datetime_to_micros(&s.datetime),
                    value: numeric_text(&s.value),
                }))
            }
            TypedSamples::Float(samples) => {
                rows.float.extend(samples.iter().map(|s| FloatValueRow {
                    sensor_id: id,
                    timestamp: datetime_to_micros(&s.datetime),
                    value: s.value,
                }))
            }
            TypedSamples::String(samples) => {
                rows.string.extend(samples.iter().map(|s| StringValueRow {
                    sensor_id: id,
                    timestamp: datetime_to_micros(&s.datetime),
                    value: s.value.clone(),
                }))
            }
            TypedSamples::Boolean(samples) => {
                rows.boolean.extend(samples.iter().map(|s| BooleanValueRow {
                    sensor_id: id,
                    timestamp: datetime_to_micros(&s.datetime),
                    value: s.value,
                }))
            }
            TypedSamples::Location(samples) => {
                rows.location
                    .extend(samples.iter().map(|s| LocationValueRow {
                        sensor_id: id,
                        timestamp: datetime_to_micros(&s.datetime),
                        latitude: s.value.y(),
                        longitude: s.value.x(),
                    }))
            }
            TypedSamples::Json(samples) => rows.json.extend(samples.iter().map(|s| JsonValueRow {
                sensor_id: id,
                timestamp: datetime_to_micros(&s.datetime),
                value: s.value.to_string(),
            })),
            TypedSamples::Blob(samples) => rows.blob.extend(samples.iter().map(|s| BlobValueRow {
                sensor_id: id,
                timestamp: datetime_to_micros(&s.datetime),
                value: s.value.clone(),
            })),
        }
    }
    rows
}

impl BigQueryStorage {
    /// Append rows to a table. A request over 10 MB is cut in several by the client; the answers are
    /// read all the way: a rejected append says so in the `error` or the `row_errors` of its
    /// answer, not only in the status of the call.
    pub(super) async fn append<M: Message + Send + 'static>(
        &self,
        table: &'static str,
        descriptor: &Arc<TableDescriptor>,
        rows: Vec<M>,
    ) -> Result<()> {
        if rows.is_empty() {
            return Ok(());
        }
        let stream = StreamName::new_default(
            self.dataset.project_id.clone(),
            self.dataset.dataset_id.clone(),
            table.to_string(),
        );
        let operation = format!("write to {table}");
        let trace_id = format!("sensapp-{table}-{}", uuid::Uuid::new_v4());
        let results = self
            .client
            .storage()
            .append_table_batches_concurrent(
                vec![TableBatch::new(stream, descriptor.clone(), rows)],
                1,
                &trace_id,
            )
            .await
            .map_err(|error| map_error(&operation, error))?;

        for result in results {
            for response in result.responses {
                let response = response.map_err(|status| {
                    map_error(
                        &operation,
                        gcp_bigquery_client::error::BQError::from(status),
                    )
                })?;
                if let Some(Response::Error(status)) = response.response {
                    return Err(append_failure(&operation, status.code, &status.message));
                }
                if let Some(row_error) = response.row_errors.first() {
                    return Err(StorageError::OperationFailed {
                        operation: format!("BigQuery {operation}"),
                        details: format!(
                            "row {} rejected: {} ({} rows rejected in this append)",
                            row_error.index,
                            row_error.message,
                            response.row_errors.len()
                        ),
                    }
                    .into());
                }
            }
        }
        Ok(())
    }

    /// Write the series that are not stored yet: their units, their labels, and last the sensors
    /// themselves, so a series is never listed with a part of its labels missing. Two writers can
    /// register the same series at the same time; the rows are identical and the reads collapse them.
    pub(super) async fn register_sensors(&self, sensors: &[&Sensor]) -> Result<()> {
        let mut by_id: HashMap<i64, &Sensor> = HashMap::with_capacity(sensors.len());
        for sensor in sensors {
            by_id.entry(sensor_id(sensor)).or_insert(sensor);
        }
        by_id.retain(|id, _| !self.is_registered(*id));
        if by_id.is_empty() {
            return Ok(());
        }

        let ids: Vec<i64> = by_id.keys().copied().collect();
        let stored = self.existing_ids("sensors", "sensor_id", &ids).await?;
        for id in &stored {
            self.remember_registered(*id);
            by_id.remove(id);
        }
        if by_id.is_empty() {
            return Ok(());
        }

        let mut units: HashMap<i64, &crate::datamodel::unit::Unit> = HashMap::new();
        for sensor in by_id.values() {
            if let Some(unit) = &sensor.unit {
                units.entry(unit_id(&unit.name)).or_insert(unit);
            }
        }
        let unit_ids: Vec<i64> = units.keys().copied().collect();
        let stored_units = self.existing_ids("units", "id", &unit_ids).await?;
        let unit_rows: Vec<UnitRow> = units
            .iter()
            .filter(|(id, _)| !stored_units.contains(id))
            .map(|(id, unit)| UnitRow {
                id: *id,
                name: unit.name.clone(),
                description: unit.description.clone(),
            })
            .collect();
        let label_rows: Vec<LabelRow> = by_id
            .iter()
            .flat_map(|(id, sensor)| {
                sensor.labels.iter().map(|(name, description)| LabelRow {
                    sensor_id: *id,
                    name: name.clone(),
                    description: description.clone(),
                })
            })
            .collect();
        tokio::try_join!(
            self.append("units", &rows::UNITS, unit_rows),
            self.append("labels", &rows::LABELS, label_rows),
        )?;

        let sensor_rows: Vec<SensorRow> = by_id
            .iter()
            .map(|(id, sensor)| SensorRow {
                sensor_id: *id,
                uuid: sensor.uuid.to_string(),
                name: sensor.name.clone(),
                r#type: sensor.sensor_type.to_string(),
                unit: sensor.unit.as_ref().map(|unit| unit_id(&unit.name)),
            })
            .collect();
        self.append("sensors", &rows::SENSORS, sensor_rows).await?;
        for id in by_id.keys() {
            self.remember_registered(*id);
        }
        Ok(())
    }

    /// Which of the `ids` are in the column `column` of `table`.
    async fn existing_ids(&self, table: &str, column: &str, ids: &[i64]) -> Result<Vec<i64>> {
        if ids.is_empty() {
            return Ok(Vec::new());
        }
        let sql = format!(
            "SELECT DISTINCT {column} FROM {} WHERE {column} IN UNNEST(@ids)",
            self.table(table)
        );
        self.query_rows(
            &format!("look up {table}"),
            sql,
            vec![int_array_param("ids", ids)],
            |row| super::client::required(row.get_i64(0), column),
        )
        .await
    }

    pub(super) async fn publish_batch(&self, batch: &Batch) -> Result<()> {
        let sensors: Vec<&Sensor> = batch.sensors.iter().map(|s| s.sensor.as_ref()).collect();
        self.register_sensors(&sensors).await?;

        let rows = sample_rows(batch).await;
        try_join_all([
            Box::pin(self.append("integer_values", &rows::INTEGER_VALUES, rows.integer))
                as std::pin::Pin<Box<dyn Future<Output = Result<()>> + Send + '_>>,
            Box::pin(self.append("numeric_values", &rows::NUMERIC_VALUES, rows.numeric)),
            Box::pin(self.append("float_values", &rows::FLOAT_VALUES, rows.float)),
            Box::pin(self.append("string_values", &rows::STRING_VALUES, rows.string)),
            Box::pin(self.append("boolean_values", &rows::BOOLEAN_VALUES, rows.boolean)),
            Box::pin(self.append("location_values", &rows::LOCATION_VALUES, rows.location)),
            Box::pin(self.append("json_values", &rows::JSON_VALUES, rows.json)),
            Box::pin(self.append("blob_values", &rows::BLOB_VALUES, rows.blob)),
        ])
        .await?;
        Ok(())
    }
}

/// An append rejected by the service: `code` is a `google.rpc.Code`.
fn append_failure(operation: &str, code: i32, message: &str) -> anyhow::Error {
    if grpc_code_is_transient(code) {
        StorageError::Unavailable(format!("{operation}: {message}")).into()
    } else {
        StorageError::OperationFailed {
            operation: format!("BigQuery {operation}"),
            details: format!("code {code}: {message}"),
        }
        .into()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datamodel::{Sample, SensAppDateTime, SensorType, batch::SingleSensorBatch};
    use hifitime::Epoch;
    use smallvec::smallvec;
    use std::str::FromStr;
    use tokio::sync::RwLock;
    use uuid::Uuid;

    fn single(sensor_type: SensorType, samples: TypedSamples) -> SingleSensorBatch {
        SingleSensorBatch {
            sensor: Arc::new(Sensor::new(
                Uuid::from_u128(0x0123_4567_89ab_cdef_fedc_ba98_7654_3210),
                "test".to_string(),
                sensor_type,
                None,
                None,
            )),
            samples: RwLock::new(samples),
        }
    }

    fn at(seconds: f64) -> SensAppDateTime {
        Epoch::from_unix_seconds(seconds)
    }

    #[test]
    fn numeric_values_are_rounded_to_the_scale_of_the_column() {
        let text = |value: &str| numeric_text(&Decimal::from_str(value).unwrap());
        assert_eq!(text("1.5"), "1.5");
        assert_eq!(text("-0.000000001"), "-0.000000001");
        assert_eq!(text("1.0000000014"), "1.000000001");
        assert_eq!(text("1.0000000016"), "1.000000002");
        assert_eq!(
            text("79228162514264337593543950335"),
            "79228162514264337593543950335"
        );
    }

    #[test]
    fn sensor_ids_are_the_clickhouse_ids_as_signed_integers() {
        let sensor = Sensor::new(
            Uuid::parse_str("9d87123d-9b47-466d-9eda-001c2ecf9c54").unwrap(),
            "s".to_string(),
            SensorType::Float,
            None,
            None,
        );
        assert_eq!(
            sensor_id(&sensor) as u64,
            0x9d87_123d_9b47_466d ^ 0x9eda_001c_2ecf_9c54
        );
        let negative = Sensor::new(
            Uuid::parse_str("00000000-0000-4000-8000-000000000001").unwrap(),
            "s".to_string(),
            SensorType::Float,
            None,
            None,
        );
        assert_eq!(
            sensor_id(&negative),
            i64::MIN + 0x4001,
            "the top bit is the sign"
        );
    }

    #[tokio::test]
    async fn every_type_goes_to_its_table_with_microseconds_and_full_precision() {
        let precise = 0.1 + 0.2; // not representable as an f32
        let batch = Batch {
            sensors: smallvec![
                single(
                    SensorType::Float,
                    TypedSamples::Float(smallvec![Sample {
                        datetime: at(1_700_000_000.5),
                        value: precise
                    }])
                ),
                single(
                    SensorType::Location,
                    TypedSamples::Location(smallvec![Sample {
                        datetime: at(1.0),
                        value: geo::Point::new(10.123_456_789_012, 59.987_654_321_098)
                    }])
                ),
                single(
                    SensorType::Json,
                    TypedSamples::Json(smallvec![Sample {
                        datetime: at(2.0),
                        value: serde_json::json!({"a": [1, 2], "b": null})
                    }])
                ),
                single(
                    SensorType::Numeric,
                    TypedSamples::Numeric(smallvec![Sample {
                        datetime: at(3.0),
                        value: Decimal::from_str("12.3456789012").unwrap()
                    }])
                ),
                single(
                    SensorType::Blob,
                    TypedSamples::Blob(smallvec![Sample {
                        datetime: at(4.0),
                        value: vec![0, 255, 7]
                    }])
                ),
            ],
        };
        let rows = sample_rows(&batch).await;
        assert_eq!(rows.float[0].value, precise);
        assert_eq!(rows.float[0].timestamp, 1_700_000_000_500_000);
        assert_eq!(rows.location[0].latitude, 59.987_654_321_098);
        assert_eq!(rows.location[0].longitude, 10.123_456_789_012);
        assert_eq!(rows.json[0].value, r#"{"a":[1,2],"b":null}"#);
        assert_eq!(rows.numeric[0].value, "12.345678901");
        assert_eq!(rows.blob[0].value, vec![0, 255, 7]);
        assert!(rows.integer.is_empty() && rows.string.is_empty() && rows.boolean.is_empty());
    }

    #[test]
    fn a_rejected_append_is_an_error_a_busy_service_is_unavailable() {
        let busy = append_failure("write to sensors", 14, "unavailable");
        assert!(matches!(
            busy.downcast_ref::<StorageError>(),
            Some(StorageError::Unavailable(_))
        ));
        let rejected = append_failure("write to sensors", 3, "schema mismatch");
        assert!(matches!(
            rejected.downcast_ref::<StorageError>(),
            Some(StorageError::OperationFailed { .. })
        ));
    }
}
