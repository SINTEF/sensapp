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

/*
With `deduplicate` the rows that are stored already are left out, and so are the repeated rows of
the statement: `WITH u (columns) AS (VALUES ...) INSERT INTO table SELECT DISTINCT .. FROM u WHERE
NOT EXISTS (..)`. The index on (sensor_id, timestamp_us) answers the probe of each row. SQLite has
one writer at a time (and one process), so nothing else can write between the check and the insert.
 */

/// The start of the statement: a plain `INSERT INTO table (columns) ` followed by the VALUES, or
/// the `WITH` that names them.
fn insert_start(table: &str, columns: &str, deduplicate: bool) -> QueryBuilder<Sqlite> {
    if deduplicate {
        QueryBuilder::new(format!("WITH u ({columns}) AS ("))
    } else {
        QueryBuilder::new(format!("INSERT INTO {table} ({columns}) "))
    }
}

/// The end of the statement, after the VALUES.
fn insert_end(query: &mut QueryBuilder<Sqlite>, table: &str, columns: &str, deduplicate: bool) {
    if !deduplicate {
        return;
    }
    let same_sample = columns
        .split(", ")
        .map(|column| format!("e.{column} = u.{column}"))
        .collect::<Vec<_>>()
        .join(" AND ");
    query.push(format!(
        ") INSERT INTO {table} ({columns}) SELECT DISTINCT {columns} FROM u \
         WHERE NOT EXISTS (SELECT 1 FROM {table} e WHERE {same_sample})"
    ));
}

pub async fn publish_integer_values(
    transaction: &mut Transaction<'_, Sqlite>,
    sensor_id: i64,
    values: &[Sample<i64>],
    deduplicate: bool,
) -> Result<()> {
    for chunk in values.chunks(MAX_ROWS_PER_INSERT) {
        let mut query = insert_start(
            "integer_values",
            "sensor_id, timestamp_us, value",
            deduplicate,
        );
        query.push_values(chunk, |mut row, value| {
            row.push_bind(sensor_id)
                .push_bind(datetime_to_micros(&value.datetime))
                .push_bind(value.value);
        });
        insert_end(
            &mut query,
            "integer_values",
            "sensor_id, timestamp_us, value",
            deduplicate,
        );
        transaction.execute(query.build()).await?;
    }
    Ok(())
}

pub async fn publish_numeric_values(
    transaction: &mut Transaction<'_, Sqlite>,
    sensor_id: i64,
    values: &[Sample<rust_decimal::Decimal>],
    deduplicate: bool,
) -> Result<()> {
    for chunk in values.chunks(MAX_ROWS_PER_INSERT) {
        let mut query = insert_start(
            "numeric_values",
            "sensor_id, timestamp_us, value",
            deduplicate,
        );
        query.push_values(chunk, |mut row, value| {
            row.push_bind(sensor_id)
                .push_bind(datetime_to_micros(&value.datetime))
                .push_bind(value.value.to_string());
        });
        insert_end(
            &mut query,
            "numeric_values",
            "sensor_id, timestamp_us, value",
            deduplicate,
        );
        transaction.execute(query.build()).await?;
    }
    Ok(())
}

pub async fn publish_float_values(
    transaction: &mut Transaction<'_, Sqlite>,
    sensor_id: i64,
    values: &[Sample<f64>],
    deduplicate: bool,
) -> Result<()> {
    // SQLite's REAL type doesn't support NaN or Inf - they get converted to NULL
    // which violates the NOT NULL constraint. Skip these values.
    let finite_values: Vec<&Sample<f64>> = values
        .iter()
        .filter(|value| value.value.is_finite())
        .collect();
    for chunk in finite_values.chunks(MAX_ROWS_PER_INSERT) {
        let mut query = insert_start(
            "float_values",
            "sensor_id, timestamp_us, value",
            deduplicate,
        );
        query.push_values(chunk, |mut row, value| {
            row.push_bind(sensor_id)
                .push_bind(datetime_to_micros(&value.datetime))
                .push_bind(value.value);
        });
        insert_end(
            &mut query,
            "float_values",
            "sensor_id, timestamp_us, value",
            deduplicate,
        );
        transaction.execute(query.build()).await?;
    }
    Ok(())
}

pub async fn publish_string_values(
    transaction: &mut Transaction<'_, Sqlite>,
    sensor_id: i64,
    values: &[Sample<String>],
    deduplicate: bool,
) -> Result<()> {
    let mut rows = Vec::with_capacity(values.len());
    for value in values {
        let string_id = get_string_value_id_or_create(transaction, &value.value).await?;
        rows.push((datetime_to_micros(&value.datetime), string_id));
    }
    for chunk in rows.chunks(MAX_ROWS_PER_INSERT) {
        let mut query = insert_start(
            "string_values",
            "sensor_id, timestamp_us, value",
            deduplicate,
        );
        query.push_values(chunk, |mut row, (timestamp_us, string_id)| {
            row.push_bind(sensor_id)
                .push_bind(*timestamp_us)
                .push_bind(*string_id);
        });
        insert_end(
            &mut query,
            "string_values",
            "sensor_id, timestamp_us, value",
            deduplicate,
        );
        transaction.execute(query.build()).await?;
    }
    Ok(())
}

pub async fn publish_boolean_values(
    transaction: &mut Transaction<'_, Sqlite>,
    sensor_id: i64,
    values: &[Sample<bool>],
    deduplicate: bool,
) -> Result<()> {
    for chunk in values.chunks(MAX_ROWS_PER_INSERT) {
        let mut query = insert_start(
            "boolean_values",
            "sensor_id, timestamp_us, value",
            deduplicate,
        );
        query.push_values(chunk, |mut row, value| {
            row.push_bind(sensor_id)
                .push_bind(datetime_to_micros(&value.datetime))
                .push_bind(value.value);
        });
        insert_end(
            &mut query,
            "boolean_values",
            "sensor_id, timestamp_us, value",
            deduplicate,
        );
        transaction.execute(query.build()).await?;
    }
    Ok(())
}

pub async fn publish_location_values(
    transaction: &mut Transaction<'_, Sqlite>,
    sensor_id: i64,
    values: &[Sample<geo::Point>],
    deduplicate: bool,
) -> Result<()> {
    for chunk in values.chunks(MAX_ROWS_PER_INSERT) {
        let mut query = insert_start(
            "location_values",
            "sensor_id, timestamp_us, latitude, longitude",
            deduplicate,
        );
        query.push_values(chunk, |mut row, value| {
            row.push_bind(sensor_id)
                .push_bind(datetime_to_micros(&value.datetime))
                .push_bind(value.value.y())
                .push_bind(value.value.x());
        });
        insert_end(
            &mut query,
            "location_values",
            "sensor_id, timestamp_us, latitude, longitude",
            deduplicate,
        );
        transaction.execute(query.build()).await?;
    }
    Ok(())
}

pub async fn publish_blob_values(
    transaction: &mut Transaction<'_, Sqlite>,
    sensor_id: i64,
    values: &[Sample<Vec<u8>>],
    deduplicate: bool,
) -> Result<()> {
    for chunk in values.chunks(MAX_ROWS_PER_INSERT) {
        let mut query = insert_start("blob_values", "sensor_id, timestamp_us, value", deduplicate);
        query.push_values(chunk, |mut row, value| {
            row.push_bind(sensor_id)
                .push_bind(datetime_to_micros(&value.datetime))
                .push_bind(&value.value);
        });
        insert_end(
            &mut query,
            "blob_values",
            "sensor_id, timestamp_us, value",
            deduplicate,
        );
        transaction.execute(query.build()).await?;
    }
    Ok(())
}

pub async fn publish_json_values(
    transaction: &mut Transaction<'_, Sqlite>,
    sensor_id: i64,
    values: &[Sample<serde_json::Value>],
    deduplicate: bool,
) -> Result<()> {
    for chunk in values.chunks(MAX_ROWS_PER_INSERT) {
        let mut query = insert_start("json_values", "sensor_id, timestamp_us, value", deduplicate);
        query.push_values(chunk, |mut row, value| {
            row.push_bind(sensor_id)
                .push_bind(datetime_to_micros(&value.datetime))
                // The column is a BLOB (STRICT table), a TEXT bind is rejected.
                .push_bind(value.value.to_string().into_bytes());
        });
        insert_end(
            &mut query,
            "json_values",
            "sensor_id, timestamp_us, value",
            deduplicate,
        );
        transaction.execute(query.build()).await?;
    }
    Ok(())
}
