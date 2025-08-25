use crate::datamodel::Sensor;
use crate::datamodel::unit::Unit;
use anyhow::Result;
use cached::proc_macro::cached;
use sqlx::{Executor, Postgres, Row, Transaction};
use std::time::Duration;
use uuid::Uuid;

#[cached(
    time = 120,
    result = true,
    sync_writes = "default",
    key = "String",
    convert = r#"{ label_name.to_string() }"#
)]
pub async fn get_label_name_id_or_create(
    transaction: &mut Transaction<'_, Postgres>,
    label_name: &str,
) -> Result<i64> {
    let query = sqlx::query(
        r#"
        WITH inserted AS (
            INSERT INTO labels_name_dictionary (name)
            VALUES ($1)
            ON CONFLICT (name) DO NOTHING
            RETURNING id
        )
        SELECT id FROM inserted
        UNION ALL
        SELECT id FROM labels_name_dictionary WHERE name = $1
        LIMIT 1
    "#,
    )
    .bind(label_name);
    let label_name_id = transaction.fetch_one(query).await?.get("id");
    Ok(label_name_id)
}

#[cached(
    time = 120,
    result = true,
    sync_writes = "default",
    key = "String",
    convert = r#"{ label_description.to_string() }"#
)]
pub async fn get_label_description_id_or_create(
    transaction: &mut Transaction<'_, Postgres>,
    label_description: &str,
) -> Result<i64> {
    let query = sqlx::query(
        r#"
        WITH inserted AS (
            INSERT INTO labels_description_dictionary (description)
            VALUES ($1)
            ON CONFLICT (description) DO NOTHING
            RETURNING id
        )
        SELECT id FROM inserted
        UNION ALL
        SELECT id FROM labels_description_dictionary WHERE description = $1
        LIMIT 1
    "#,
    )
    .bind(label_description);

    let label_description_id = transaction.fetch_one(query).await?.get("id");
    Ok(label_description_id)
}

#[cached(
    time = 120,
    result = true,
    sync_writes = "default",
    key = "String",
    convert = r#"{ unit.name.clone() }"#
)]
pub async fn get_unit_id_or_create(
    transaction: &mut Transaction<'_, sqlx::Postgres>,
    unit: &Unit,
) -> Result<i64, sqlx::Error> {
    let query = sqlx::query(
        r#"
        WITH inserted AS (
            INSERT INTO units (name, description)
            VALUES ($1, $2)
            ON CONFLICT (name) DO NOTHING
            RETURNING id
        )
        SELECT id FROM inserted
        UNION ALL
        SELECT id FROM units WHERE name = $1
        LIMIT 1
    "#,
    )
    .bind(&unit.name)
    .bind(&unit.description);

    let unit_id = transaction.fetch_one(query).await?.get("id");
    Ok(unit_id)
}

#[cached(
    time = 120,
    result = true,
    sync_writes = "default",
    key = "Uuid",
    convert = r#"{ sensor.uuid }"#
)]
pub async fn get_sensor_id_or_create_sensor(
    transaction: &mut Transaction<'_, Postgres>,
    sensor: &Sensor,
) -> Result<i64> {
    let sqlx_uuid = sensor.uuid;
    let sensor_id_query = sqlx::query(
        r#"
            SELECT sensor_id FROM sensors WHERE uuid = $1
            "#,
    )
    .bind(sqlx_uuid);

    let sensor_id = transaction
        .fetch_optional(sensor_id_query)
        .await?
        .map(|row| row.get("sensor_id"));

    if let Some(Some(sensor_id)) = sensor_id {
        return Ok(sensor_id);
    }

    let sensor_type_string = sensor.sensor_type.to_string();

    let unit_id = match sensor.unit {
        Some(ref unit) => Some(get_unit_id_or_create(transaction, unit).await?),
        None => None,
    };

    let create_sensor_query = sqlx::query(
        r#"
            INSERT INTO sensors (uuid, name, type, unit)
            VALUES ($1, $2, $3, $4)
            RETURNING sensor_id
            "#,
    )
    .bind(sensor.uuid)
    .bind(sensor.name.to_string())
    .bind(sensor_type_string)
    .bind(unit_id);

    let sensor_id = transaction
        .fetch_one(create_sensor_query)
        .await?
        .get("sensor_id");

    // Add the labels
    for (key, value) in sensor.labels.iter() {
        let label_name_id = get_label_name_id_or_create(transaction, key).await?;
        let label_description_id = get_label_description_id_or_create(transaction, value).await?;
        let create_label_query = sqlx::query(
            r#"
                INSERT INTO labels (sensor_id, name, description)
                VALUES ($1, $2, $3)
                "#,
        )
        .bind(sensor_id)
        .bind(label_name_id)
        .bind(label_description_id);
        transaction.execute(create_label_query).await?;
    }

    Ok(sensor_id)
}

#[cached(
    time = 120,
    result = true,
    sync_writes = "default",
    key = "String",
    convert = r#"{ string_value.to_string() }"#
)]
pub async fn get_string_value_id_or_create(
    transaction: &mut Transaction<'_, Postgres>,
    string_value: &str,
) -> Result<i64> {
    let query = sqlx::query(
        r#"
        WITH inserted AS (
            INSERT INTO strings_values_dictionary (value)
            VALUES ($1)
            ON CONFLICT (value) DO NOTHING
            RETURNING id
        )
        SELECT id FROM inserted
        UNION ALL
        SELECT id FROM strings_values_dictionary WHERE value = $1
        LIMIT 1
    "#,
    )
    .bind(string_value);

    let string_value_id = transaction.fetch_one(query).await?.get("id");
    Ok(string_value_id)
}
