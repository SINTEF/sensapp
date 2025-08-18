use crate::datamodel::Sensor;
use crate::datamodel::unit::Unit;
use anyhow::Result;
use cached::proc_macro::cached;
use sqlx::{Sqlite, Transaction, prelude::*};
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
    transaction: &mut Transaction<'_, Sqlite>,
    label_name: &str,
) -> Result<i64> {
    let label_name_id_query = sqlx::query!(
        r#"
            SELECT id FROM labels_name_dictionary WHERE name = ?
            "#,
        label_name
    );

    let label_name_id = transaction
        .fetch_optional(label_name_id_query)
        .await?
        .map(|row| row.get("id"));

    // If the label name exists, it's returned
    if let Some(Some(label_name_id)) = label_name_id {
        return Ok(label_name_id);
    }

    let create_label_name_query = sqlx::query!(
        r#"
            INSERT INTO labels_name_dictionary (name)
            VALUES (?)
            "#,
        label_name
    );

    // Execute the query
    let label_name_id = transaction
        .execute(create_label_name_query)
        .await?
        .last_insert_rowid();

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
    transaction: &mut Transaction<'_, Sqlite>,
    label_description: &str,
) -> Result<i64> {
    let label_description_id_query = sqlx::query!(
        r#"
            SELECT id FROM labels_description_dictionary WHERE description = ?
            "#,
        label_description
    );

    let label_description_id = transaction
        .fetch_optional(label_description_id_query)
        .await?
        .map(|row| row.get("id"));

    // If the label description exists, it's returned
    if let Some(Some(label_description_id)) = label_description_id {
        return Ok(label_description_id);
    }

    let create_label_description_query = sqlx::query!(
        r#"
            INSERT INTO labels_description_dictionary (description)
            VALUES (?)
            "#,
        label_description
    );

    // Execute the query
    let label_description_id = transaction
        .execute(create_label_description_query)
        .await?
        .last_insert_rowid();

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
    transaction: &mut Transaction<'_, Sqlite>,
    unit: &Unit,
) -> Result<i64> {
    let unit_id_query = sqlx::query!(
        r#"
            SELECT id FROM units WHERE name = ?
            "#,
        unit.name,
    );

    let unit_id = transaction
        .fetch_optional(unit_id_query)
        .await?
        .map(|row| row.get("id"));

    // If the unit exists, it's returned
    if let Some(Some(unit_id)) = unit_id {
        return Ok(unit_id);
    }

    let create_unit_query = sqlx::query!(
        r#"
            INSERT INTO units (name, description)
            VALUES (?, ?)
            "#,
        unit.name,
        unit.description,
    );

    // Execute the query
    let unit_id = transaction
        .execute(create_unit_query)
        .await?
        .last_insert_rowid();

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
    transaction: &mut Transaction<'_, Sqlite>,
    sensor: &Sensor,
) -> Result<i64> {
    println!("aaah");
    let uuid_string = sensor.uuid.to_string();
    let sensor_id_query = sqlx::query!(
        r#"
            SELECT sensor_id FROM sensors WHERE uuid = ?
            "#,
        uuid_string
    );

    let sensor_id = transaction
        .fetch_optional(sensor_id_query)
        .await?
        .map(|row| row.get("sensor_id"));

    // If the sensor exists, it's returned
    if let Some(Some(sensor_id)) = sensor_id {
        return Ok(sensor_id);
    }

    let sensor_type_string = sensor.sensor_type.to_string();

    let unit_id = match sensor.unit {
        Some(ref unit) => Some(get_unit_id_or_create(transaction, unit).await?),
        None => None,
    };

    let create_sensor_query = sqlx::query!(
        r#"
            INSERT INTO sensors (uuid, name, type, unit)
            VALUES (?, ?, ?, ?)
            "#,
        uuid_string,
        sensor.name,
        sensor_type_string,
        unit_id
    );

    // Execute the query
    let sensor_id = transaction
        .execute(create_sensor_query)
        .await?
        .last_insert_rowid();

    // Add the labels
    for (key, value) in sensor.labels.iter() {
        let label_name_id = get_label_name_id_or_create(transaction, key).await?;
        let label_description_id = get_label_description_id_or_create(transaction, value).await?;
        let label_query = sqlx::query!(
            r#"
                INSERT INTO labels (sensor_id, name, description)
                VALUES (?, ?, ?)
                "#,
            sensor_id,
            label_name_id,
            label_description_id,
        );
        transaction.execute(label_query).await?;
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
    transaction: &mut Transaction<'_, Sqlite>,
    string_value: &str,
) -> Result<i64> {
    let get_query = sqlx::query!(
        r#"
        SELECT id FROM strings_values_dictionary WHERE value = ?
        "#,
        string_value
    );
    let string_id = transaction
        .fetch_optional(get_query)
        .await?
        .map(|row| row.get("id"));
    if let Some(Some(string_id)) = string_id {
        return Ok(string_id);
    }
    let create_query = sqlx::query!(
        r#"
        INSERT INTO strings_values_dictionary (value) VALUES (?)
        "#,
        string_value
    );
    let string_id = transaction.execute(create_query).await?.last_insert_rowid();
    Ok(string_id)
}
