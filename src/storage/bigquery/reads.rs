//! Reading: sensors and their labels, and the samples of the eight types.

use super::BigQueryStorage;
use super::client::{int_array_param, int_param, required, string_param};
use super::publishers::uuid_id;
use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
use crate::datamodel::sensapp_vec::SensAppLabels;
use crate::datamodel::unit::Unit;
use crate::datamodel::{Sample, SensAppDateTime, Sensor, SensorType, TypedSamples};
use crate::storage::StorageError;
use crate::storage::selector::empty_samples;
use anyhow::Result;
use gcp_bigquery_client::model::{query_parameter::QueryParameter, query_response::ResultSet};
use rust_decimal::Decimal;
use std::collections::HashMap;
use std::str::FromStr;
use uuid::Uuid;

/// Sensor ids sent to BigQuery in one query for their labels
const LABEL_LOOKUP_CHUNK: usize = 2000;

/// `LIMIT` takes an INT64
pub(super) const MAX_LIMIT: usize = i64::MAX as usize;

/// BigQuery's TIMESTAMP goes from year 1 to year 9999
const MIN_MICROS: i64 = -62_135_596_800_000_000;
const MAX_MICROS: i64 = 253_402_300_799_999_999;

/// A bound of a time window, that `TIMESTAMP_MICROS` accepts.
pub fn clamp_micros(micros: i64) -> i64 {
    micros.clamp(MIN_MICROS, MAX_MICROS)
}

pub fn parse_uuid(sensor_uuid: &str) -> Result<Uuid> {
    Uuid::from_str(sensor_uuid).map_err(|error| {
        StorageError::invalid_data_format(
            &format!("Invalid UUID '{sensor_uuid}': {error}"),
            None,
            None,
        )
        .into()
    })
}

/// The table of the samples of a type.
pub fn sample_table(sensor_type: SensorType) -> &'static str {
    match sensor_type {
        SensorType::Integer => "integer_values",
        SensorType::Numeric => "numeric_values",
        SensorType::Float => "float_values",
        SensorType::String => "string_values",
        SensorType::Boolean => "boolean_values",
        SensorType::Location => "location_values",
        SensorType::Json => "json_values",
        SensorType::Blob => "blob_values",
    }
}

/// The value columns that `read_sample` expects after the sensor id and the timestamp.
fn sample_columns(sensor_type: SensorType) -> &'static str {
    match sensor_type {
        SensorType::Location => "latitude, longitude",
        SensorType::Json => "TO_JSON_STRING(value) AS value",
        SensorType::Blob => "TO_BASE64(value) AS value",
        _ => "value",
    }
}

fn parse_error(what: &str, error: impl std::fmt::Display) -> anyhow::Error {
    StorageError::invalid_data_format(&format!("Failed to parse {what}: {error}"), None, None)
        .into()
}

/// Add the sample of a row `sensor_id, timestamp_us, value...` to `samples`.
pub fn read_sample(samples: &mut TypedSamples, row: &ResultSet) -> Result<()> {
    let datetime =
        SensAppDateTime::from_unix_microseconds_i64(required(row.get_i64(1), "timestamp")?);
    match samples {
        TypedSamples::Integer(samples) => samples.push(Sample {
            datetime,
            value: required(row.get_i64(2), "value")?,
        }),
        TypedSamples::Numeric(samples) => {
            let text = required(row.get_string(2), "value")?;
            let value = Decimal::from_str(&text)
                .or_else(|_| Decimal::from_scientific(&text))
                .map_err(|error| parse_error("a NUMERIC value", error))?;
            samples.push(Sample { datetime, value });
        }
        TypedSamples::Float(samples) => samples.push(Sample {
            datetime,
            value: required(row.get_f64(2), "value")?,
        }),
        TypedSamples::String(samples) => samples.push(Sample {
            datetime,
            value: required(row.get_string(2), "value")?,
        }),
        TypedSamples::Boolean(samples) => samples.push(Sample {
            datetime,
            value: required(row.get_bool(2), "value")?,
        }),
        TypedSamples::Location(samples) => samples.push(Sample {
            datetime,
            value: geo::Point::new(
                required(row.get_f64(3), "longitude")?,
                required(row.get_f64(2), "latitude")?,
            ),
        }),
        TypedSamples::Json(samples) => {
            let text = required(row.get_string(2), "value")?;
            samples.push(Sample {
                datetime,
                value: serde_json::from_str(&text)
                    .map_err(|error| parse_error("a JSON value", error))?,
            });
        }
        TypedSamples::Blob(samples) => {
            use base64::Engine;
            let text = required(row.get_string(2), "value")?;
            samples.push(Sample {
                datetime,
                value: base64::engine::general_purpose::STANDARD
                    .decode(text)
                    .map_err(|error| parse_error("a BYTES value", error))?,
            });
        }
    }
    Ok(())
}

/// The `WHERE` conditions of a window on the timestamp, with the parameters `start` and `end`.
pub(super) fn window(
    start_us: Option<i64>,
    end_us: Option<i64>,
    params: &mut Vec<QueryParameter>,
) -> String {
    let mut conditions = String::new();
    if let Some(start_us) = start_us {
        conditions.push_str(" AND timestamp >= TIMESTAMP_MICROS(@start)");
        params.push(int_param("start", clamp_micros(start_us)));
    }
    if let Some(end_us) = end_us {
        conditions.push_str(" AND timestamp <= TIMESTAMP_MICROS(@end)");
        params.push(int_param("end", clamp_micros(end_us)));
    }
    conditions
}

/// Where the tables are, and the statements that name them.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Dataset {
    pub project_id: String,
    pub dataset_id: String,
}

impl Dataset {
    /// The fully qualified, quoted name of a table of the dataset.
    pub fn table(&self, name: &str) -> String {
        format!("`{}.{}.{}`", self.project_id, self.dataset_id, name)
    }

    /// The sensors table as a set: a sensor registered twice at once is one sensor.
    pub fn sensors_set(&self) -> String {
        format!(
            "(SELECT sensor_id, ANY_VALUE(uuid) AS uuid, ANY_VALUE(name) AS name, \
             ANY_VALUE(type) AS type, ANY_VALUE(unit) AS unit \
             FROM {} GROUP BY sensor_id)",
            self.table("sensors")
        )
    }

    pub fn units_set(&self) -> String {
        format!(
            "(SELECT id, ANY_VALUE(name) AS name, ANY_VALUE(description) AS description \
             FROM {} GROUP BY id)",
            self.table("units")
        )
    }

    /// `SELECT` the sensors (`s`) and their units, then `conditions` and `tail` (order, limit).
    pub fn sensors_sql(&self, conditions: &[String], tail: &str) -> String {
        let mut sql = format!(
            "SELECT s.sensor_id, s.uuid, s.name, s.type, \
             u.name AS unit_name, u.description AS unit_description \
             FROM {} s LEFT JOIN {} u ON s.unit = u.id",
            self.sensors_set(),
            self.units_set()
        );
        if !conditions.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&conditions.join(" AND "));
        }
        sql.push(' ');
        sql.push_str(tail);
        sql
    }
}

impl BigQueryStorage {
    /// Read sensors with a query shaped by `sensors_sql`, and their labels with one more query.
    pub(super) async fn read_sensors(
        &self,
        sql: String,
        params: Vec<QueryParameter>,
    ) -> Result<Vec<(i64, Sensor)>> {
        struct SensorRow {
            sensor_id: i64,
            uuid: Uuid,
            name: String,
            sensor_type: SensorType,
            unit: Option<Unit>,
        }
        let rows = self
            .query_rows("read sensors", sql, params, |row| {
                let uuid = required(row.get_string_by_name("uuid"), "uuid")?;
                let uuid =
                    Uuid::from_str(&uuid).map_err(|error| parse_error("a sensor uuid", error))?;
                let name = required(row.get_string_by_name("name"), "name")?;
                let sensor_type = required(row.get_string_by_name("type"), "type")?;
                let sensor_type = SensorType::from_str(&sensor_type).map_err(|error| {
                    StorageError::invalid_data_format(
                        &format!("Failed to parse sensor type '{sensor_type}': {error}"),
                        Some(uuid),
                        Some(&name),
                    )
                })?;
                Ok(SensorRow {
                    sensor_id: required(row.get_i64_by_name("sensor_id"), "sensor_id")?,
                    uuid,
                    name,
                    sensor_type,
                    unit: row
                        .get_string_by_name("unit_name")?
                        .map(|name| -> Result<Unit> {
                            Ok(Unit::new(name, row.get_string_by_name("unit_description")?))
                        })
                        .transpose()?,
                })
            })
            .await?;
        if rows.is_empty() {
            return Ok(Vec::new());
        }

        let ids: Vec<i64> = rows.iter().map(|row| row.sensor_id).collect();
        let mut labels = self.labels_of_sensors(&ids).await?;
        Ok(rows
            .into_iter()
            .map(|row| {
                let labels = labels.remove(&row.sensor_id).unwrap_or_default();
                (
                    row.sensor_id,
                    Sensor::new(row.uuid, row.name, row.sensor_type, row.unit, Some(labels)),
                )
            })
            .collect())
    }

    /// The labels of many sensors, one query per chunk of ids.
    pub(super) async fn labels_of_sensors(
        &self,
        ids: &[i64],
    ) -> Result<HashMap<i64, SensAppLabels>> {
        let sql = format!(
            "SELECT DISTINCT sensor_id, name, description FROM {} \
             WHERE sensor_id IN UNNEST(@ids) ORDER BY sensor_id, name, description",
            self.table("labels")
        );
        let mut labels: HashMap<i64, SensAppLabels> = HashMap::new();
        for chunk in ids.chunks(LABEL_LOOKUP_CHUNK) {
            let rows = self
                .query_rows(
                    "read labels",
                    sql.clone(),
                    vec![int_array_param("ids", chunk)],
                    |row| {
                        Ok((
                            required(row.get_i64(0), "sensor_id")?,
                            required(row.get_string(1), "label name")?,
                            required(row.get_string(2), "label description")?,
                        ))
                    },
                )
                .await?;
            for (sensor_id, name, description) in rows {
                labels
                    .entry(sensor_id)
                    .or_default()
                    .push((name, description));
            }
        }
        Ok(labels)
    }

    /// A series by its UUID, with its id in the tables.
    pub(super) async fn get_sensor_metadata(
        &self,
        sensor_uuid: &str,
    ) -> Result<Option<(i64, Sensor)>> {
        let uuid = parse_uuid(sensor_uuid)?;
        let id = uuid_id(&uuid);
        let sql = self.dataset.sensors_sql(
            &[
                "s.sensor_id = @id".to_string(),
                "s.uuid = @uuid".to_string(),
            ],
            "LIMIT 1",
        );
        let params = vec![int_param("id", id), string_param("uuid", &uuid.to_string())];
        Ok(self.read_sensors(sql, params).await?.into_iter().next())
    }

    /// The samples of one series, oldest first (newest first on request), at most `limit`.
    pub(super) async fn query_samples_by_type(
        &self,
        id: i64,
        sensor_type: SensorType,
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
        limit: usize,
        newest_first: bool,
    ) -> Result<TypedSamples> {
        let mut params = vec![int_param("id", id)];
        let conditions = window(
            start_time
                .as_ref()
                .map(crate::storage::common::datetime_to_micros),
            end_time
                .as_ref()
                .map(crate::storage::common::datetime_to_micros),
            &mut params,
        );
        let sql = format!(
            "SELECT sensor_id, UNIX_MICROS(timestamp) AS timestamp_us, {} FROM {} \
             WHERE sensor_id = @id{conditions} ORDER BY timestamp {order} LIMIT {limit}",
            sample_columns(sensor_type),
            self.table(sample_table(sensor_type)),
            limit = limit.min(MAX_LIMIT),
            order = if newest_first { "DESC" } else { "ASC" },
        );
        let mut samples = empty_samples(sensor_type);
        for row in self
            .query_rows("read samples", sql, params, |row| {
                let mut one = empty_samples(sensor_type);
                read_sample(&mut one, row)?;
                Ok(one)
            })
            .await?
        {
            append_samples(&mut samples, row);
        }
        Ok(samples)
    }

    /// The samples of many series of one numeric type, with one query, at most `limit` in total.
    pub(super) async fn query_numeric_samples_of_many(
        &self,
        sensor_type: SensorType,
        ids: &[i64],
        start_us: Option<i64>,
        end_us: Option<i64>,
        limit: usize,
    ) -> Result<HashMap<i64, TypedSamples>> {
        let mut params = vec![int_array_param("ids", ids)];
        let conditions = window(start_us, end_us, &mut params);
        let sql = format!(
            "SELECT sensor_id, UNIX_MICROS(timestamp) AS timestamp_us, {} FROM {} \
             WHERE sensor_id IN UNNEST(@ids){conditions} \
             ORDER BY sensor_id, timestamp LIMIT {limit}",
            sample_columns(sensor_type),
            self.table(sample_table(sensor_type)),
            limit = limit.min(MAX_LIMIT),
        );
        let mut by_sensor: HashMap<i64, TypedSamples> = HashMap::new();
        let rows = self
            .query_rows("read samples", sql, params, |row| {
                let sensor_id = required(row.get_i64(0), "sensor_id")?;
                let mut one = empty_samples(sensor_type);
                read_sample(&mut one, row)?;
                Ok((sensor_id, one))
            })
            .await?;
        for (sensor_id, one) in rows {
            append_samples(
                by_sensor
                    .entry(sensor_id)
                    .or_insert_with(|| empty_samples(sensor_type)),
                one,
            );
        }
        Ok(by_sensor)
    }
}

/// Move the samples of `from` to the end of `to`, of the same type.
pub(super) fn append_samples(to: &mut TypedSamples, from: TypedSamples) {
    match (to, from) {
        (TypedSamples::Integer(to), TypedSamples::Integer(from)) => to.extend(from),
        (TypedSamples::Numeric(to), TypedSamples::Numeric(from)) => to.extend(from),
        (TypedSamples::Float(to), TypedSamples::Float(from)) => to.extend(from),
        (TypedSamples::String(to), TypedSamples::String(from)) => to.extend(from),
        (TypedSamples::Boolean(to), TypedSamples::Boolean(from)) => to.extend(from),
        (TypedSamples::Location(to), TypedSamples::Location(from)) => to.extend(from),
        (TypedSamples::Json(to), TypedSamples::Json(from)) => to.extend(from),
        (TypedSamples::Blob(to), TypedSamples::Blob(from)) => to.extend(from),
        _ => unreachable!("the samples of a table have the type of the table"),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn window_bounds_stay_inside_the_timestamp_range() {
        assert_eq!(clamp_micros(0), 0);
        assert_eq!(clamp_micros(i64::MIN), MIN_MICROS);
        assert_eq!(clamp_micros(i64::MAX), MAX_MICROS);
        // 0001-01-01 and 9999-12-31 23:59:59.999999 UTC
        assert_eq!(MIN_MICROS / 1_000_000, -62_135_596_800);
        assert_eq!(MAX_MICROS, 253_402_300_800 * 1_000_000 - 1);
    }

    #[test]
    fn sensors_are_read_as_a_set_with_their_units() {
        let dataset = Dataset {
            project_id: "my-project".to_string(),
            dataset_id: "sensapp".to_string(),
        };
        assert_eq!(dataset.table("labels"), "`my-project.sensapp.labels`");
        let sql = dataset.sensors_sql(
            &["s.name = @m0".to_string()],
            "ORDER BY s.sensor_id LIMIT 5",
        );
        assert!(
            sql.contains("FROM `my-project.sensapp.sensors` GROUP BY sensor_id"),
            "{sql}"
        );
        assert!(
            sql.contains("FROM `my-project.sensapp.units` GROUP BY id"),
            "{sql}"
        );
        assert!(
            sql.ends_with("WHERE s.name = @m0 ORDER BY s.sensor_id LIMIT 5"),
            "{sql}"
        );
        assert!(!dataset.sensors_sql(&[], "").contains("WHERE"));
    }

    #[test]
    fn the_tables_are_the_shared_list() {
        use SensorType::*;
        for sensor_type in [
            Integer, Numeric, Float, String, Boolean, Location, Json, Blob,
        ] {
            assert!(crate::storage::common::VALUE_TABLES.contains(&sample_table(sensor_type)));
        }
    }

    #[test]
    fn windows_use_parameters() {
        let mut params = Vec::new();
        let sql = window(Some(10), None, &mut params);
        assert_eq!(sql, " AND timestamp >= TIMESTAMP_MICROS(@start)");
        assert_eq!(params.len(), 1);
        let sql = window(Some(10), Some(20), &mut Vec::new());
        assert!(sql.contains("@start") && sql.contains("@end"));
        assert_eq!(window(None, None, &mut Vec::new()), "");
    }

    #[test]
    fn an_invalid_uuid_is_a_bad_request() {
        let error = parse_uuid("not-a-uuid").unwrap_err();
        assert!(matches!(
            error.downcast_ref::<StorageError>(),
            Some(StorageError::InvalidDataFormat { .. })
        ));
    }
}
