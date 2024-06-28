use crate::datamodel::unit::Unit;
use crate::datamodel::Sensor;
use anyhow::Result;
use cached::proc_macro::cached;
use duckdb::{params, CachedStatement, Transaction};
use uuid::Uuid;

#[cached(
    time = 120,
    result = true,
    sync_writes = true,
    key = "String",
    convert = r#"{ label_name.to_string() }"#
)]
pub fn get_label_name_id_or_create(transaction: &Transaction, label_name: &str) -> Result<i64> {
    let mut select_stmt: CachedStatement =
        transaction.prepare_cached("SELECT id FROM labels_name_dictionary WHERE name = ?")?;

    let label_name_id = select_stmt.query_row(params![label_name], |row| row.get(0))?;

    if let Some(id) = label_name_id {
        Ok(id)
    } else {
        let mut insert_stmt: CachedStatement = transaction
            .prepare_cached("INSERT INTO labels_name_dictionary (name) VALUES (?) RETURNING id")?;
        let label_name_id: i64 = insert_stmt.query_row(params![label_name], |row| row.get(0))?;
        Ok(label_name_id)
    }
}

#[cached(
    time = 120,
    result = true,
    sync_writes = true,
    key = "String",
    convert = r#"{ label_description.to_string() }"#
)]
pub fn get_label_description_id_or_create(
    transaction: &Transaction,
    label_description: &str,
) -> Result<i64> {
    let mut select_stmt: CachedStatement = transaction
        .prepare_cached("SELECT id FROM labels_description_dictionary WHERE description = ?")?;

    let label_description_id =
        select_stmt.query_row(params![label_description], |row| row.get(0))?;

    if let Some(id) = label_description_id {
        Ok(id)
    } else {
        let mut insert_stmt: CachedStatement = transaction.prepare_cached(
            "INSERT INTO labels_description_dictionary (description) VALUES (?) RETURNING id",
        )?;
        let label_description_id: i64 =
            insert_stmt.query_row(params![label_description], |row| row.get(0))?;
        Ok(label_description_id)
    }
}

#[cached(
    time = 120,
    result = true,
    sync_writes = true,
    key = "String",
    convert = r#"{ unit.name.clone() }"#
)]
pub fn get_unit_id_or_create(transaction: &Transaction, unit: &Unit) -> Result<i64> {
    let mut select_stmt: CachedStatement =
        transaction.prepare_cached("SELECT id FROM units WHERE name = ?")?;

    let unit_id = select_stmt.query_row(params![unit.name], |row| row.get(0))?;

    if let Some(id) = unit_id {
        Ok(id)
    } else {
        let mut insert_stmt: CachedStatement = transaction
            .prepare_cached("INSERT INTO units (name, description) VALUES (?, ?) RETURNING id")?;
        let unit_id: i64 =
            insert_stmt.query_row(params![unit.name, unit.description], |row| row.get(0))?;
        Ok(unit_id)
    }
}

#[cached(
    time = 120,
    result = true,
    sync_writes = true,
    key = "Uuid",
    convert = r#"{ sensor.uuid }"#
)]
pub fn get_sensor_id_or_create_sensor(transaction: &Transaction, sensor: &Sensor) -> Result<i64> {
    let uuid_string = sensor.uuid.to_string();

    let mut select_stmt: CachedStatement =
        transaction.prepare_cached("SELECT sensor_id FROM sensors WHERE uuid = ?")?;

    let mut binding = select_stmt.query(params![uuid_string])?;
    let existing_sensor_id = binding.next()?;

    if let Some(existing_sensor_id) = existing_sensor_id {
        Ok(existing_sensor_id.get(0)?)
    } else {
        let sensor_type_string = sensor.sensor_type.to_string();

        let unit_id = match sensor.unit {
            Some(ref unit) => Some(get_unit_id_or_create(transaction, unit)?),
            None => None,
        };

        let mut insert_stmt: CachedStatement = transaction.prepare_cached(
            "INSERT INTO sensors (uuid, name, type, unit) VALUES (?, ?, ?, ?) RETURNING sensor_id",
        )?;
        let sensor_id: i64 = insert_stmt.query_row(
            params![uuid_string, sensor.name, sensor_type_string, unit_id],
            |row| row.get(0),
        )?;

        // Add the labels
        let mut label_insert_stmt: CachedStatement = transaction
            .prepare_cached("INSERT INTO labels (sensor_id, name, description) VALUES (?, ?, ?)")?;
        for (key, value) in sensor.labels.iter() {
            let label_name_id = get_label_name_id_or_create(transaction, key)?;
            let label_description_id = get_label_description_id_or_create(transaction, value)?;
            label_insert_stmt.execute(params![sensor_id, label_name_id, label_description_id])?;
        }

        Ok(sensor_id)
    }
}

#[cached(
    time = 120,
    result = true,
    sync_writes = true,
    key = "String",
    convert = r#"{ string_value.to_string() }"#
)]
pub fn get_string_value_id_or_create(transaction: &Transaction, string_value: &str) -> Result<i64> {
    let mut select_stmt: CachedStatement =
        transaction.prepare_cached("SELECT id FROM strings_values_dictionary WHERE value = ?")?;

    let string_id = select_stmt.query_row(params![string_value], |row| row.get(0))?;

    if let Some(id) = string_id {
        Ok(id)
    } else {
        let mut insert_stmt: CachedStatement = transaction.prepare_cached(
            "INSERT INTO strings_values_dictionary (value) VALUES (?) RETURNING id",
        )?;
        let string_id: i64 = insert_stmt.query_row(params![string_value], |row| row.get(0))?;
        Ok(string_id)
    }
}
