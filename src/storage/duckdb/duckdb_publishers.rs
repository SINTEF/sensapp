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
    let mut appender = transaction.appender("integer_values")?;
    for value in values {
        let timestamp_ms = value.datetime.to_rfc3339();
        appender.append_row(params![sensor_id, timestamp_ms, value.value])?;
    }
    appender.flush()?;
    Ok(())
}

pub fn publish_numeric_values(
    transaction: &Transaction,
    sensor_id: i64,
    values: &[Sample<Decimal>],
) -> Result<()> {
    let mut appender = transaction.appender("numeric_values")?;
    for value in values {
        let timestamp_ms = value.datetime.to_rfc3339();
        let string_value = value.value.to_string();
        appender.append_row(params![sensor_id, timestamp_ms, string_value])?;
    }
    appender.flush()?;
    Ok(())
}

pub fn publish_float_values(
    transaction: &Transaction,
    sensor_id: i64,
    values: &[Sample<f64>],
) -> Result<()> {
    let mut appender = transaction.appender("float_values")?;
    for value in values {
        let timestamp_ms = value.datetime.to_rfc3339();
        appender.append_row(params![sensor_id, timestamp_ms, value.value])?;
    }
    appender.flush()?;
    Ok(())
}

pub fn publish_string_values(
    transaction: &Transaction,
    sensor_id: i64,
    values: &[Sample<String>],
) -> Result<()> {
    let mut appender = transaction.appender("string_values")?;
    for value in values {
        let string_id = get_string_value_id_or_create(transaction, &value.value)?;
        let timestamp_ms = value.datetime.to_rfc3339();
        appender.append_row(params![sensor_id, timestamp_ms, string_id])?;
    }
    appender.flush()?;
    Ok(())
}

pub fn publish_boolean_values(
    transaction: &Transaction,
    sensor_id: i64,
    values: &[Sample<bool>],
) -> Result<()> {
    let mut appender = transaction.appender("boolean_values")?;
    for value in values {
        let timestamp_ms = value.datetime.to_rfc3339();
        appender.append_row(params![sensor_id, timestamp_ms, value.value])?;
    }
    appender.flush()?;
    Ok(())
}

pub fn publish_location_values(
    transaction: &Transaction,
    sensor_id: i64,
    values: &[Sample<Point>],
) -> Result<()> {
    let mut appender = transaction.appender("location_values")?;
    for value in values {
        let timestamp_ms = value.datetime.to_rfc3339();
        let lat = value.value.y();
        let lon = value.value.x();
        appender.append_row(params![sensor_id, timestamp_ms, lat, lon])?;
    }
    appender.flush()?;
    Ok(())
}

pub fn publish_blob_values(
    transaction: &Transaction,
    sensor_id: i64,
    values: &[Sample<Vec<u8>>],
) -> Result<()> {
    let mut appender = transaction.appender("blob_values")?;
    for value in values {
        let timestamp_ms = value.datetime.to_rfc3339();
        appender.append_row(params![sensor_id, timestamp_ms, &value.value])?;
    }
    appender.flush()?;
    Ok(())
}

pub fn publish_json_values(
    transaction: &Transaction,
    sensor_id: i64,
    values: &[Sample<Value>],
) -> Result<()> {
    let mut appender = transaction.appender("json_values")?;
    for value in values {
        let timestamp_ms = value.datetime.to_rfc3339();
        let string_value = value.value.to_string();
        appender.append_row(params![sensor_id, timestamp_ms, string_value])?;
    }
    appender.flush()?;
    Ok(())
}
