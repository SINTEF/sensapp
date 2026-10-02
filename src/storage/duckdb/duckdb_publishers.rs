use super::duckdb_registration::{ensure_string_ids, register_sensors};
use crate::datamodel::batch::SingleSensorBatch;
use crate::datamodel::{Sample, TypedSamples};
use anyhow::{Context, Result};
use duckdb::{Appender, Connection, params};
use geo::Point;
use rust_decimal::Decimal;
use serde_json::Value;
use std::collections::{BTreeSet, HashMap};

/// Writes a batch: the sensors and the strings are registered with a handful of statements, then
/// the samples of every sensor go through one appender per value table.
pub fn publish_batch(connection: &Connection, sensors: &[SingleSensorBatch]) -> Result<()> {
    let sensor_refs: Vec<&crate::datamodel::Sensor> =
        sensors.iter().map(|batch| batch.sensor.as_ref()).collect();
    let ids = register_sensors(connection, &sensor_refs)?;

    let guards: Vec<_> = sensors
        .iter()
        .map(|batch| batch.samples.blocking_read())
        .collect();
    let mut strings: BTreeSet<&str> = BTreeSet::new();
    for guard in &guards {
        if let TypedSamples::String(samples) = &**guard {
            strings.extend(samples.iter().map(|sample| sample.value.as_str()));
        }
    }
    let string_ids = ensure_string_ids(connection, &strings)?;

    let mut appenders = Appenders::new(connection);
    for (batch, guard) in sensors.iter().zip(&guards) {
        let sensor_id = *ids
            .get(&batch.sensor.uuid)
            .context("a registered sensor has no id")?;
        match &**guard {
            TypedSamples::Integer(samples) => {
                publish_integer_values(appenders.get("integer_values")?, sensor_id, samples)?
            }
            TypedSamples::Numeric(samples) => {
                publish_numeric_values(appenders.get("numeric_values")?, sensor_id, samples)?
            }
            TypedSamples::Float(samples) => {
                publish_float_values(appenders.get("float_values")?, sensor_id, samples)?
            }
            TypedSamples::String(samples) => publish_string_values(
                appenders.get("string_values")?,
                sensor_id,
                samples,
                &string_ids,
            )?,
            TypedSamples::Boolean(samples) => {
                publish_boolean_values(appenders.get("boolean_values")?, sensor_id, samples)?
            }
            TypedSamples::Location(samples) => {
                publish_location_values(appenders.get("location_values")?, sensor_id, samples)?
            }
            TypedSamples::Blob(samples) => {
                publish_blob_values(appenders.get("blob_values")?, sensor_id, samples)?
            }
            TypedSamples::Json(samples) => {
                publish_json_values(appenders.get("json_values")?, sensor_id, samples)?
            }
        }
    }
    appenders.flush()
}

/// The appenders of a batch, opened when a sensor of that table shows up.
struct Appenders<'a> {
    connection: &'a Connection,
    appenders: HashMap<&'static str, Appender<'a>>,
}

impl<'a> Appenders<'a> {
    fn new(connection: &'a Connection) -> Self {
        Self {
            connection,
            appenders: HashMap::new(),
        }
    }

    fn get(&mut self, table: &'static str) -> Result<&mut Appender<'a>> {
        if !self.appenders.contains_key(table) {
            self.appenders
                .insert(table, self.connection.appender(table)?);
        }
        Ok(self.appenders.get_mut(table).expect("inserted above"))
    }

    fn flush(mut self) -> Result<()> {
        for appender in self.appenders.values_mut() {
            appender.flush()?;
        }
        Ok(())
    }
}

fn publish_integer_values(
    appender: &mut Appender,
    sensor_id: i64,
    values: &[Sample<i64>],
) -> Result<()> {
    for value in values {
        let timestamp_us = value.datetime.to_rfc3339();
        appender.append_row(params![sensor_id, timestamp_us, value.value])?;
    }
    Ok(())
}

fn publish_numeric_values(
    appender: &mut Appender,
    sensor_id: i64,
    values: &[Sample<Decimal>],
) -> Result<()> {
    for value in values {
        let timestamp_us = value.datetime.to_rfc3339();
        let string_value = value.value.to_string();
        appender.append_row(params![sensor_id, timestamp_us, string_value])?;
    }
    Ok(())
}

fn publish_float_values(
    appender: &mut Appender,
    sensor_id: i64,
    values: &[Sample<f64>],
) -> Result<()> {
    for value in values {
        let timestamp_us = value.datetime.to_rfc3339();
        appender.append_row(params![sensor_id, timestamp_us, value.value])?;
    }
    Ok(())
}

fn publish_string_values(
    appender: &mut Appender,
    sensor_id: i64,
    values: &[Sample<String>],
    string_ids: &HashMap<String, i64>,
) -> Result<()> {
    for value in values {
        let string_id = string_ids
            .get(&value.value)
            .context("a registered string has no id")?;
        let timestamp_us = value.datetime.to_rfc3339();
        appender.append_row(params![sensor_id, timestamp_us, string_id])?;
    }
    Ok(())
}

fn publish_boolean_values(
    appender: &mut Appender,
    sensor_id: i64,
    values: &[Sample<bool>],
) -> Result<()> {
    for value in values {
        let timestamp_us = value.datetime.to_rfc3339();
        appender.append_row(params![sensor_id, timestamp_us, value.value])?;
    }
    Ok(())
}

fn publish_location_values(
    appender: &mut Appender,
    sensor_id: i64,
    values: &[Sample<Point>],
) -> Result<()> {
    for value in values {
        let timestamp_us = value.datetime.to_rfc3339();
        let lat = value.value.y();
        let lon = value.value.x();
        appender.append_row(params![sensor_id, timestamp_us, lat, lon])?;
    }
    Ok(())
}

fn publish_blob_values(
    appender: &mut Appender,
    sensor_id: i64,
    values: &[Sample<Vec<u8>>],
) -> Result<()> {
    for value in values {
        let timestamp_us = value.datetime.to_rfc3339();
        appender.append_row(params![sensor_id, timestamp_us, &value.value])?;
    }
    Ok(())
}

fn publish_json_values(
    appender: &mut Appender,
    sensor_id: i64,
    values: &[Sample<Value>],
) -> Result<()> {
    for value in values {
        let timestamp_us = value.datetime.to_rfc3339();
        let string_value = value.value.to_string();
        appender.append_row(params![sensor_id, timestamp_us, string_value])?;
    }
    Ok(())
}
