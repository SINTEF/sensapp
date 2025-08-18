use super::postgresql_utilities::get_string_value_id_or_create;
use crate::datamodel::Sample;
use anyhow::Result;
use sqlx::{Postgres, Transaction, prelude::*};

pub async fn publish_integer_values(
    transaction: &mut Transaction<'_, Postgres>,
    sensor_id: i64,
    values: &[Sample<i64>],
) -> Result<()> {
    for value in values {
        let timestamp_ms = value.datetime.to_unix_milliseconds().floor() as i64;
        let query = sqlx::query(
            r#"
            INSERT INTO integer_values (sensor_id, timestamp_ms, value)
            VALUES ($1, $2, $3)
            "#,
        )
        .bind(sensor_id)
        .bind(timestamp_ms)
        .bind(value.value);
        transaction.execute(query).await?;
    }
    Ok(())
}

pub async fn publish_numeric_values(
    transaction: &mut Transaction<'_, Postgres>,
    sensor_id: i64,
    values: &[Sample<rust_decimal::Decimal>],
) -> Result<()> {
    for value in values {
        let timestamp_ms = value.datetime.to_unix_milliseconds().floor() as i64;
        let string_value = value.value.to_string();
        let query = sqlx::query(
            r#"
            INSERT INTO numeric_values (sensor_id, timestamp_ms, value)
            VALUES ($1, $2, $3)
            "#,
        )
        .bind(sensor_id)
        .bind(timestamp_ms)
        .bind(string_value);
        transaction.execute(query).await?;
    }
    Ok(())
}

pub async fn publish_float_values(
    transaction: &mut Transaction<'_, Postgres>,
    sensor_id: i64,
    values: &[Sample<f64>],
) -> Result<()> {
    for value in values {
        let timestamp_ms = value.datetime.to_unix_milliseconds().floor() as i64;
        let query = sqlx::query(
            r#"
            INSERT INTO float_values (sensor_id, timestamp_ms, value)
            VALUES ($1, $2, $3)
            "#,
        )
        .bind(sensor_id)
        .bind(timestamp_ms)
        .bind(value.value);
        transaction.execute(query).await?;
    }
    Ok(())
}

pub async fn publish_string_values(
    transaction: &mut Transaction<'_, Postgres>,
    sensor_id: i64,
    values: &[Sample<String>],
) -> Result<()> {
    for value in values {
        let string_id = get_string_value_id_or_create(transaction, &value.value).await?;
        let timestamp_ms = value.datetime.to_unix_milliseconds().floor() as i64;
        let query = sqlx::query(
            r#"
            INSERT INTO string_values (sensor_id, timestamp_ms, value)
            VALUES ($1, $2, $3)
            "#,
        )
        .bind(sensor_id)
        .bind(timestamp_ms)
        .bind(string_id);
        transaction.execute(query).await?;
    }
    Ok(())
}

pub async fn publish_boolean_values(
    transaction: &mut Transaction<'_, Postgres>,
    sensor_id: i64,
    values: &[Sample<bool>],
) -> Result<()> {
    for value in values {
        let timestamp_ms = value.datetime.to_unix_milliseconds().floor() as i64;
        let query = sqlx::query(
            r#"
            INSERT INTO boolean_values (sensor_id, timestamp_ms, value)
            VALUES ($1, $2, $3)
            "#,
        )
        .bind(sensor_id)
        .bind(timestamp_ms)
        .bind(value.value);
        transaction.execute(query).await?;
    }
    Ok(())
}

pub async fn publish_location_values(
    transaction: &mut Transaction<'_, Postgres>,
    sensor_id: i64,
    values: &[Sample<geo::Point>],
) -> Result<()> {
    for value in values {
        let timestamp_ms = value.datetime.to_unix_milliseconds().floor() as i64;
        let lat = value.value.y();
        let lon = value.value.x();
        let query = sqlx::query(
            r#"
            INSERT INTO location_values (sensor_id, timestamp_ms, latitude, longitude)
            VALUES ($1, $2, $3, $4)
            "#,
        )
        .bind(sensor_id)
        .bind(timestamp_ms)
        .bind(lat)
        .bind(lon);
        transaction.execute(query).await?;
    }
    Ok(())
}

pub async fn publish_blob_values(
    transaction: &mut Transaction<'_, Postgres>,
    sensor_id: i64,
    values: &[Sample<Vec<u8>>],
) -> Result<()> {
    for value in values {
        let timestamp_ms = value.datetime.to_unix_milliseconds().floor() as i64;
        let query = sqlx::query(
            r#"
            INSERT INTO blob_values (sensor_id, timestamp_ms, value)
            VALUES ($1, $2, $3)
            "#,
        )
        .bind(sensor_id)
        .bind(timestamp_ms)
        .bind(value.value.clone());
        transaction.execute(query).await?;
    }
    Ok(())
}

pub async fn publish_json_values(
    transaction: &mut Transaction<'_, Postgres>,
    sensor_id: i64,
    values: &[Sample<serde_json::Value>],
) -> Result<()> {
    for value in values {
        let timestamp_ms = value.datetime.to_unix_milliseconds().floor() as i64;
        let string_value = value.value.to_string();
        let query = sqlx::query(
            r#"
            INSERT INTO json_values (sensor_id, timestamp_ms, value)
            VALUES ($1, $2, $3)
            "#,
        )
        .bind(sensor_id)
        .bind(timestamp_ms)
        .bind(string_value);
        transaction.execute(query).await?;
    }
    Ok(())
}
