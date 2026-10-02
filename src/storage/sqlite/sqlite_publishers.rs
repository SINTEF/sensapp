use super::sqlite_utilities::get_string_value_id_or_create;
use crate::datamodel::Sample;
use crate::storage::common::datetime_to_micros;
use anyhow::Result;
use sqlx::{QueryBuilder, Sqlite, Transaction, prelude::*};

/*
Each function inserts the samples with multi-row INSERT statements built by
QueryBuilder::push_values, in chunks. SQLite accepts at most 32 766 bound
variables per statement and the widest table (location) has 4 columns per
row, so 8 000 rows per statement always fits whatever SENSAPP_BATCH_SIZE is.

Obviously not the most beautiful code,
but I'm not sure whether making it generic
and keeping the sqlx validation is easy/worth it.
 */
const MAX_ROWS_PER_INSERT: usize = 8_000;

pub async fn publish_integer_values(
    transaction: &mut Transaction<'_, Sqlite>,
    sensor_id: i64,
    values: &[Sample<i64>],
) -> Result<()> {
    for chunk in values.chunks(MAX_ROWS_PER_INSERT) {
        let mut query = QueryBuilder::<Sqlite>::new(
            "INSERT INTO integer_values (sensor_id, timestamp_us, value) ",
        );
        query.push_values(chunk, |mut row, value| {
            row.push_bind(sensor_id)
                .push_bind(datetime_to_micros(&value.datetime))
                .push_bind(value.value);
        });
        transaction.execute(query.build()).await?;
    }
    Ok(())
}

pub async fn publish_numeric_values(
    transaction: &mut Transaction<'_, Sqlite>,
    sensor_id: i64,
    values: &[Sample<rust_decimal::Decimal>],
) -> Result<()> {
    for chunk in values.chunks(MAX_ROWS_PER_INSERT) {
        let mut query = QueryBuilder::<Sqlite>::new(
            "INSERT INTO numeric_values (sensor_id, timestamp_us, value) ",
        );
        query.push_values(chunk, |mut row, value| {
            row.push_bind(sensor_id)
                .push_bind(datetime_to_micros(&value.datetime))
                .push_bind(value.value.to_string());
        });
        transaction.execute(query.build()).await?;
    }
    Ok(())
}

pub async fn publish_float_values(
    transaction: &mut Transaction<'_, Sqlite>,
    sensor_id: i64,
    values: &[Sample<f64>],
) -> Result<()> {
    // SQLite's REAL type doesn't support NaN or Inf - they get converted to NULL
    // which violates the NOT NULL constraint. Skip these values.
    let finite_values: Vec<&Sample<f64>> = values
        .iter()
        .filter(|value| value.value.is_finite())
        .collect();
    for chunk in finite_values.chunks(MAX_ROWS_PER_INSERT) {
        let mut query = QueryBuilder::<Sqlite>::new(
            "INSERT INTO float_values (sensor_id, timestamp_us, value) ",
        );
        query.push_values(chunk, |mut row, value| {
            row.push_bind(sensor_id)
                .push_bind(datetime_to_micros(&value.datetime))
                .push_bind(value.value);
        });
        transaction.execute(query.build()).await?;
    }
    Ok(())
}

pub async fn publish_string_values(
    transaction: &mut Transaction<'_, Sqlite>,
    sensor_id: i64,
    values: &[Sample<String>],
) -> Result<()> {
    let mut rows = Vec::with_capacity(values.len());
    for value in values {
        let string_id = get_string_value_id_or_create(transaction, &value.value).await?;
        rows.push((datetime_to_micros(&value.datetime), string_id));
    }
    for chunk in rows.chunks(MAX_ROWS_PER_INSERT) {
        let mut query = QueryBuilder::<Sqlite>::new(
            "INSERT INTO string_values (sensor_id, timestamp_us, value) ",
        );
        query.push_values(chunk, |mut row, (timestamp_us, string_id)| {
            row.push_bind(sensor_id)
                .push_bind(*timestamp_us)
                .push_bind(*string_id);
        });
        transaction.execute(query.build()).await?;
    }
    Ok(())
}

pub async fn publish_boolean_values(
    transaction: &mut Transaction<'_, Sqlite>,
    sensor_id: i64,
    values: &[Sample<bool>],
) -> Result<()> {
    for chunk in values.chunks(MAX_ROWS_PER_INSERT) {
        let mut query = QueryBuilder::<Sqlite>::new(
            "INSERT INTO boolean_values (sensor_id, timestamp_us, value) ",
        );
        query.push_values(chunk, |mut row, value| {
            row.push_bind(sensor_id)
                .push_bind(datetime_to_micros(&value.datetime))
                .push_bind(value.value);
        });
        transaction.execute(query.build()).await?;
    }
    Ok(())
}

pub async fn publish_location_values(
    transaction: &mut Transaction<'_, Sqlite>,
    sensor_id: i64,
    values: &[Sample<geo::Point>],
) -> Result<()> {
    for chunk in values.chunks(MAX_ROWS_PER_INSERT) {
        let mut query = QueryBuilder::<Sqlite>::new(
            "INSERT INTO location_values (sensor_id, timestamp_us, latitude, longitude) ",
        );
        query.push_values(chunk, |mut row, value| {
            row.push_bind(sensor_id)
                .push_bind(datetime_to_micros(&value.datetime))
                .push_bind(value.value.y())
                .push_bind(value.value.x());
        });
        transaction.execute(query.build()).await?;
    }
    Ok(())
}

pub async fn publish_blob_values(
    transaction: &mut Transaction<'_, Sqlite>,
    sensor_id: i64,
    values: &[Sample<Vec<u8>>],
) -> Result<()> {
    for chunk in values.chunks(MAX_ROWS_PER_INSERT) {
        let mut query = QueryBuilder::<Sqlite>::new(
            "INSERT INTO blob_values (sensor_id, timestamp_us, value) ",
        );
        query.push_values(chunk, |mut row, value| {
            row.push_bind(sensor_id)
                .push_bind(datetime_to_micros(&value.datetime))
                .push_bind(&value.value);
        });
        transaction.execute(query.build()).await?;
    }
    Ok(())
}

pub async fn publish_json_values(
    transaction: &mut Transaction<'_, Sqlite>,
    sensor_id: i64,
    values: &[Sample<serde_json::Value>],
) -> Result<()> {
    for chunk in values.chunks(MAX_ROWS_PER_INSERT) {
        let mut query = QueryBuilder::<Sqlite>::new(
            "INSERT INTO json_values (sensor_id, timestamp_us, value) ",
        );
        query.push_values(chunk, |mut row, value| {
            row.push_bind(sensor_id)
                .push_bind(datetime_to_micros(&value.datetime))
                // The column is a BLOB (STRICT table), a TEXT bind is rejected.
                .push_bind(value.value.to_string().into_bytes());
        });
        transaction.execute(query.build()).await?;
    }
    Ok(())
}
