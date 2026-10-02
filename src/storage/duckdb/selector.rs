//! Reading the series of a selector with a handful of queries: one for the sensors, one for their
//! labels, then one per numeric value type for all the samples, whatever the number of series.

use super::DuckDBStorage;
use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
use crate::datamodel::sensapp_vec::SensAppLabels;
use crate::datamodel::unit::Unit;
use crate::datamodel::{Sample, SensAppDateTime, Sensor, SensorType, TypedSamples};
use crate::storage::selector::{BulkSelectorBackend, empty_samples};
use crate::storage::{LabelMatcher, StorageError};
use anyhow::{Context, Result};
use async_trait::async_trait;
use duckdb::Connection;
use rust_decimal::Decimal;
use smallvec::smallvec;
use std::collections::HashMap;
use std::fmt::Write;
use std::str::FromStr;
use tokio::task::spawn_blocking;
use uuid::Uuid;

/// The labels of many sensors with one query.
pub(super) fn labels_of(
    connection: &Connection,
    sensor_ids: &[i64],
) -> Result<HashMap<i64, SensAppLabels>> {
    let mut labels: HashMap<i64, SensAppLabels> = HashMap::new();
    if sensor_ids.is_empty() {
        return Ok(labels);
    }
    // The ids are integers read from the database and printed here, nothing else is interpolated
    let ids = sensor_ids
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT l.sensor_id, lnd.name, ldd.description
         FROM labels l
         JOIN labels_name_dictionary lnd ON l.name = lnd.id
         JOIN labels_description_dictionary ldd ON l.description = ldd.id
         WHERE l.sensor_id IN ({ids})
         ORDER BY l.sensor_id, lnd.name"
    );
    let mut statement = connection.prepare(&sql)?;
    let mut rows = statement.query([])?;
    while let Some(row) = rows.next()? {
        let sensor_id: i64 = row.get(0)?;
        labels
            .entry(sensor_id)
            .or_default()
            .push((row.get(1)?, row.get(2)?));
    }
    Ok(labels)
}

/// The sensors matching the matchers, with their labels. A negated matcher matches the sensors
/// without the label, like on the other SQL backends. The regexes are RE2, anchored by the
/// matcher.
fn find_sensors(
    connection: &Connection,
    matchers: &[LabelMatcher],
    numeric_only: bool,
) -> Result<Vec<(i64, Sensor)>> {
    let mut conditions: Vec<String> = Vec::new();
    let mut params: Vec<String> = Vec::new();
    if numeric_only {
        conditions.push("s.type IN ('Integer', 'Numeric', 'Float')".to_string());
    }
    for matcher in matchers {
        let regex = matcher.matcher_type.is_regex();
        let negated = matcher.matcher_type.is_negated();
        let test = if regex {
            "regexp_matches({column}, ?)"
        } else {
            "{column} = ?"
        };
        if matcher.is_name_matcher() {
            let test = test.replace("{column}", "s.name");
            conditions.push(if negated {
                format!("NOT ({test})")
            } else {
                test
            });
            params.push(matcher.value.clone());
        } else {
            // The label name and value are bound, the SQL is made of fixed fragments
            let test = test.replace("{column}", "ldd.description");
            let operator = if negated { "NOT IN" } else { "IN" };
            conditions.push(format!(
                "s.sensor_id {operator} (
                    SELECT l.sensor_id FROM labels l
                    JOIN labels_name_dictionary lnd ON l.name = lnd.id
                    JOIN labels_description_dictionary ldd ON l.description = ldd.id
                    WHERE lnd.name = ? AND {test}
                 )"
            ));
            params.push(matcher.name.clone());
            params.push(matcher.value.clone());
        }
    }
    let mut sql = String::from(
        "SELECT s.sensor_id, CAST(s.uuid AS TEXT), s.name, s.type, u.name, u.description
         FROM sensors s LEFT JOIN units u ON s.unit = u.id",
    );
    if !conditions.is_empty() {
        write!(sql, " WHERE {}", conditions.join(" AND "))?;
    }
    sql.push_str(" ORDER BY s.sensor_id");

    let mut statement = connection.prepare(&sql)?;
    let mut rows = statement.query(duckdb::params_from_iter(params.iter()))?;
    let mut found = Vec::new();
    while let Some(row) = rows.next()? {
        let sensor_id: i64 = row.get(0)?;
        let uuid: String = row.get(1)?;
        let name: String = row.get(2)?;
        let sensor_type: String = row.get(3)?;
        let unit_name: Option<String> = row.get(4)?;
        let unit_description: Option<String> = row.get(5)?;
        let uuid = Uuid::parse_str(&uuid).map_err(|e| {
            anyhow::Error::from(StorageError::invalid_data_format(
                &format!("Failed to parse sensor UUID '{uuid}': {e}"),
                None,
                Some(&name),
            ))
        })?;
        let sensor_type = SensorType::from_str(&sensor_type).map_err(|e| {
            anyhow::Error::from(StorageError::invalid_data_format(
                &format!("Failed to parse sensor type '{sensor_type}': {e}"),
                Some(uuid),
                Some(&name),
            ))
        })?;
        let unit = unit_name.map(|unit_name| Unit::new(unit_name, unit_description));
        found.push((sensor_id, uuid, name, sensor_type, unit));
    }
    drop(rows);
    drop(statement);

    let ids: Vec<i64> = found.iter().map(|sensor| sensor.0).collect();
    let mut labels = labels_of(connection, &ids)?;
    Ok(found
        .into_iter()
        .map(|(sensor_id, uuid, name, sensor_type, unit)| {
            let labels = labels.remove(&sensor_id).unwrap_or_else(|| smallvec![]);
            (
                sensor_id,
                Sensor::new(uuid, name, sensor_type, unit, Some(labels)),
            )
        })
        .collect())
}

fn read_numeric(
    connection: &Connection,
    sensor_type: SensorType,
    sensor_ids: &[i64],
    start_us: Option<i64>,
    end_us: Option<i64>,
    limit: usize,
) -> Result<HashMap<i64, TypedSamples>> {
    let (table, value) = match sensor_type {
        SensorType::Integer => ("integer_values", "value"),
        SensorType::Numeric => ("numeric_values", "CAST(value AS VARCHAR)"),
        SensorType::Float => ("float_values", "value"),
        other => anyhow::bail!("{other} is not a numeric type"),
    };
    // Integers read from the database, printed here
    let ids = sensor_ids
        .iter()
        .map(i64::to_string)
        .collect::<Vec<_>>()
        .join(", ");
    let sql = format!(
        "SELECT sensor_id, epoch_us(timestamp_us), {value} FROM {table}
         WHERE sensor_id IN ({ids})
           AND (?1 IS NULL OR timestamp_us >= make_timestamp(?1))
           AND (?2 IS NULL OR timestamp_us <= make_timestamp(?2))
         ORDER BY sensor_id, timestamp_us ASC LIMIT ?3"
    );
    let limit = i64::try_from(limit).unwrap_or(i64::MAX);
    let mut statement = connection.prepare(&sql)?;
    let mut rows = statement.query(duckdb::params![start_us, end_us, limit])?;
    let mut result: HashMap<i64, TypedSamples> = HashMap::new();
    while let Some(row) = rows.next()? {
        let sensor_id: i64 = row.get(0)?;
        let datetime = SensAppDateTime::from_unix_microseconds_i64(row.get(1)?);
        let samples = result
            .entry(sensor_id)
            .or_insert_with(|| empty_samples(sensor_type));
        match samples {
            TypedSamples::Integer(samples) => samples.push(Sample {
                datetime,
                value: row.get(2)?,
            }),
            TypedSamples::Float(samples) => samples.push(Sample {
                datetime,
                value: row.get(2)?,
            }),
            TypedSamples::Numeric(samples) => {
                let value: String = row.get(2)?;
                samples.push(Sample {
                    datetime,
                    value: Decimal::from_str(&value).context("Failed to parse decimal value")?,
                });
            }
            _ => unreachable!("empty_samples follows the sensor type"),
        }
    }
    Ok(result)
}

#[async_trait]
impl BulkSelectorBackend for DuckDBStorage {
    type SensorKey = i64;

    async fn find_selector_sensors(
        &self,
        matchers: &[LabelMatcher],
        numeric_only: bool,
    ) -> Result<Vec<(i64, Sensor)>> {
        let connection = std::sync::Arc::clone(&self.connection);
        let matchers = matchers.to_vec();
        spawn_blocking(move || {
            let connection = connection.blocking_lock();
            find_sensors(&connection, &matchers, numeric_only)
        })
        .await?
    }

    async fn read_numeric_samples(
        &self,
        sensor_type: SensorType,
        sensor_ids: &[i64],
        start_us: Option<i64>,
        end_us: Option<i64>,
        limit: usize,
    ) -> Result<HashMap<i64, TypedSamples>> {
        let connection = std::sync::Arc::clone(&self.connection);
        let sensor_ids = sensor_ids.to_vec();
        spawn_blocking(move || {
            let connection = connection.blocking_lock();
            read_numeric(
                &connection,
                sensor_type,
                &sensor_ids,
                start_us,
                end_us,
                limit,
            )
        })
        .await?
    }
}
