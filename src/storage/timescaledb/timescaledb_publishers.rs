use super::timescaledb_utilities::get_string_value_id_or_create;
use crate::datamodel::{Sample, sensapp_datetime::sensapp_datetime_to_offset_datetime};
use anyhow::Result;
use sqlx::types::time::OffsetDateTime;
use sqlx::{Postgres, Transaction, prelude::*};

/*
Each function sends the whole slice of samples as one statement, by binding
one array per column and expanding them with unnest(). That is one round
trip per call instead of one per sample, and it uses a handful of bind
parameters whatever the number of samples.
 */

fn times<T>(values: &[Sample<T>]) -> Result<Vec<OffsetDateTime>> {
    values
        .iter()
        .map(|value| sensapp_datetime_to_offset_datetime(&value.datetime))
        .collect()
}

pub async fn publish_integer_values(
    transaction: &mut Transaction<'_, Postgres>,
    sensor_id: i64,
    values: &[Sample<i64>],
) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    let query = sqlx::query(
        r#"
        INSERT INTO integer_values (sensor_id, time, value)
        SELECT $1, t, v FROM unnest($2::TIMESTAMPTZ[], $3::BIGINT[]) AS u(t, v)
        "#,
    )
    .bind(sensor_id)
    .bind(times(values)?)
    .bind(values.iter().map(|value| value.value).collect::<Vec<_>>());
    transaction.execute(query).await?;
    Ok(())
}

pub async fn publish_numeric_values(
    transaction: &mut Transaction<'_, Postgres>,
    sensor_id: i64,
    values: &[Sample<rust_decimal::Decimal>],
) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    let query = sqlx::query(
        r#"
        INSERT INTO numeric_values (sensor_id, time, value)
        SELECT $1, t, v FROM unnest($2::TIMESTAMPTZ[], $3::NUMERIC[]) AS u(t, v)
        "#,
    )
    .bind(sensor_id)
    .bind(times(values)?)
    .bind(values.iter().map(|value| value.value).collect::<Vec<_>>());
    transaction.execute(query).await?;
    Ok(())
}

pub async fn publish_float_values(
    transaction: &mut Transaction<'_, Postgres>,
    sensor_id: i64,
    values: &[Sample<f64>],
) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    let query = sqlx::query(
        r#"
        INSERT INTO float_values (sensor_id, time, value)
        SELECT $1, t, v FROM unnest($2::TIMESTAMPTZ[], $3::FLOAT8[]) AS u(t, v)
        "#,
    )
    .bind(sensor_id)
    .bind(times(values)?)
    .bind(values.iter().map(|value| value.value).collect::<Vec<_>>());
    transaction.execute(query).await?;
    Ok(())
}

pub async fn publish_string_values(
    transaction: &mut Transaction<'_, Postgres>,
    sensor_id: i64,
    values: &[Sample<String>],
) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    let mut string_ids = Vec::with_capacity(values.len());
    for value in values {
        string_ids.push(get_string_value_id_or_create(transaction, &value.value).await?);
    }
    let query = sqlx::query(
        r#"
        INSERT INTO string_values (sensor_id, time, value)
        SELECT $1, t, v FROM unnest($2::TIMESTAMPTZ[], $3::BIGINT[]) AS u(t, v)
        "#,
    )
    .bind(sensor_id)
    .bind(times(values)?)
    .bind(string_ids);
    transaction.execute(query).await?;
    Ok(())
}

pub async fn publish_boolean_values(
    transaction: &mut Transaction<'_, Postgres>,
    sensor_id: i64,
    values: &[Sample<bool>],
) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    let query = sqlx::query(
        r#"
        INSERT INTO boolean_values (sensor_id, time, value)
        SELECT $1, t, v FROM unnest($2::TIMESTAMPTZ[], $3::BOOLEAN[]) AS u(t, v)
        "#,
    )
    .bind(sensor_id)
    .bind(times(values)?)
    .bind(values.iter().map(|value| value.value).collect::<Vec<_>>());
    transaction.execute(query).await?;
    Ok(())
}

pub async fn publish_location_values(
    transaction: &mut Transaction<'_, Postgres>,
    sensor_id: i64,
    values: &[Sample<geo::Point>],
) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    let query = sqlx::query(
        r#"
        INSERT INTO location_values (sensor_id, time, latitude, longitude)
        SELECT $1, t, lat, lon
        FROM unnest($2::TIMESTAMPTZ[], $3::FLOAT8[], $4::FLOAT8[]) AS u(t, lat, lon)
        "#,
    )
    .bind(sensor_id)
    .bind(times(values)?)
    .bind(
        values
            .iter()
            .map(|value| value.value.y())
            .collect::<Vec<_>>(),
    )
    .bind(
        values
            .iter()
            .map(|value| value.value.x())
            .collect::<Vec<_>>(),
    );
    transaction.execute(query).await?;
    Ok(())
}

pub async fn publish_blob_values(
    transaction: &mut Transaction<'_, Postgres>,
    sensor_id: i64,
    values: &[Sample<Vec<u8>>],
) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    let query = sqlx::query(
        r#"
        INSERT INTO blob_values (sensor_id, time, value)
        SELECT $1, t, v FROM unnest($2::TIMESTAMPTZ[], $3::BYTEA[]) AS u(t, v)
        "#,
    )
    .bind(sensor_id)
    .bind(times(values)?)
    .bind(
        values
            .iter()
            .map(|value| value.value.as_slice())
            .collect::<Vec<_>>(),
    );
    transaction.execute(query).await?;
    Ok(())
}

pub async fn publish_json_values(
    transaction: &mut Transaction<'_, Postgres>,
    sensor_id: i64,
    values: &[Sample<serde_json::Value>],
) -> Result<()> {
    if values.is_empty() {
        return Ok(());
    }
    // The column is JSONB, the values travel as text and are cast on the way in.
    let query = sqlx::query(
        r#"
        INSERT INTO json_values (sensor_id, time, value)
        SELECT $1, t, v::JSONB FROM unnest($2::TIMESTAMPTZ[], $3::TEXT[]) AS u(t, v)
        "#,
    )
    .bind(sensor_id)
    .bind(times(values)?)
    .bind(
        values
            .iter()
            .map(|value| value.value.to_string())
            .collect::<Vec<_>>(),
    );
    transaction.execute(query).await?;
    Ok(())
}
