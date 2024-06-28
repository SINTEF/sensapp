use super::duckdb_utilities::get_string_value_id_or_create;
use crate::datamodel::Sample;
use anyhow::Result;
use duckdb::{params, Transaction};
use geo::Point;
use rust_decimal::Decimal;
use serde_json::Value;

pub fn publish_integer_values(
    transaction: &Transaction,
    sensor_id: i64,
    values: &[Sample<i64>],
) -> Result<()> {
    let mut stmt = transaction.prepare_cached(
        "INSERT INTO integer_values (sensor_id, timestamp_ms, value) VALUES (?, ?, ?)",
    )?;

    for value in values {
        let timestamp_ms = value.datetime.to_rfc3339();
        stmt.execute(params![sensor_id, timestamp_ms, value.value])?;
    }
    Ok(())
}

pub fn publish_numeric_values(
    transaction: &Transaction,
    sensor_id: i64,
    values: &[Sample<Decimal>],
) -> Result<()> {
    let mut stmt = transaction.prepare_cached(
        "INSERT INTO numeric_values (sensor_id, timestamp_ms, value) VALUES (?, ?, ?)",
    )?;

    for value in values {
        let timestamp_ms = value.datetime.to_rfc3339();
        let string_value = value.value.to_string();
        stmt.execute(params![sensor_id, timestamp_ms, string_value])?;
    }
    Ok(())
}

pub fn publish_float_values(
    transaction: &Transaction,
    sensor_id: i64,
    values: &[Sample<f64>],
) -> Result<()> {
    let mut stmt = transaction.prepare_cached(
        "INSERT INTO float_values (sensor_id, timestamp_ms, value) VALUES (?, ?, ?)",
    )?;

    for value in values {
        let timestamp_ms = value.datetime.to_rfc3339();
        stmt.execute(params![sensor_id, timestamp_ms, value.value])?;
    }
    Ok(())
}

pub fn publish_string_values(
    transaction: &Transaction,
    sensor_id: i64,
    values: &[Sample<String>],
) -> Result<()> {
    let mut stmt = transaction.prepare_cached(
        "INSERT INTO string_values (sensor_id, timestamp_ms, value) VALUES (?, ?, ?)",
    )?;

    for value in values {
        let string_id = get_string_value_id_or_create(transaction, &value.value)?;
        let timestamp_ms = value.datetime.to_rfc3339();
        stmt.execute(params![sensor_id, timestamp_ms, string_id])?;
    }
    Ok(())
}

pub fn publish_boolean_values(
    transaction: &Transaction,
    sensor_id: i64,
    values: &[Sample<bool>],
) -> Result<()> {
    let mut stmt = transaction.prepare_cached(
        "INSERT INTO boolean_values (sensor_id, timestamp_ms, value) VALUES (?, ?, ?)",
    )?;

    for value in values {
        let timestamp_ms = value.datetime.to_rfc3339();
        stmt.execute(params![sensor_id, timestamp_ms, value.value])?;
    }
    Ok(())
}

pub fn publish_location_values(
    transaction: &Transaction,
    sensor_id: i64,
    values: &[Sample<Point>],
) -> Result<()> {
    let mut stmt = transaction.prepare_cached(
        "INSERT INTO location_values (sensor_id, timestamp_ms, latitude, longitude) VALUES (?, ?, ?, ?)",
    )?;

    for value in values {
        let timestamp_ms = value.datetime.to_rfc3339();
        let lat = value.value.y();
        let lon = value.value.x();
        stmt.execute(params![sensor_id, timestamp_ms, lat, lon])?;
    }
    Ok(())
}

pub fn publish_blob_values(
    transaction: &Transaction,
    sensor_id: i64,
    values: &[Sample<Vec<u8>>],
) -> Result<()> {
    let mut stmt = transaction.prepare_cached(
        "INSERT INTO blob_values (sensor_id, timestamp_ms, value) VALUES (?, ?, ?)",
    )?;

    for value in values {
        let timestamp_ms = value.datetime.to_rfc3339();
        stmt.execute(params![sensor_id, timestamp_ms, &value.value])?;
    }
    Ok(())
}

pub fn publish_json_values(
    transaction: &Transaction,
    sensor_id: i64,
    values: &[Sample<Value>],
) -> Result<()> {
    let mut stmt = transaction.prepare_cached(
        "INSERT INTO json_values (sensor_id, timestamp_ms, value) VALUES (?, ?, ?)",
    )?;

    for value in values {
        let timestamp_ms = value.datetime.to_rfc3339();
        let string_value = value.value.to_string();
        stmt.execute(params![sensor_id, timestamp_ms, string_value])?;
    }
    Ok(())
}
