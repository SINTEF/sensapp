//! Registering the sensors and the strings of a batch on SQLite with a handful of statements,
//! whatever the number of sensors and strings. The lists go to SQLite as one JSON array each,
//! read with `json_each`, so there is no limit on the number of bound variables.
//!
//! Everything runs in the `BEGIN IMMEDIATE` transaction of the batch: a failed batch leaves nothing
//! behind, and nothing is cached, so there is no stale id after a rollback or a deletion. The
//! transaction holds the write lock from its start, so nothing can insert between a lookup and the
//! insert that follows it.

use crate::datamodel::Sensor;
use crate::storage::common::{check_sensor_identity, unique_sensors};
use anyhow::{Context, Result};
use serde_json::json;
use sqlx::{Sqlite, Transaction};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use uuid::Uuid;

/// The sensor ids of the given sensors, creating the ones that do not exist with their unit and
/// labels. A sensor that exists must have the stored type, see `check_sensor_identity`.
pub async fn register_sensors(
    transaction: &mut Transaction<'_, Sqlite>,
    sensors: &[&Sensor],
) -> Result<HashMap<Uuid, i64>> {
    let unique = unique_sensors(sensors)?;
    let uuids: Vec<String> = unique.keys().map(Uuid::to_string).collect();
    let mut ids = HashMap::with_capacity(unique.len());
    for (uuid, sensor_id, sensor_type) in fetch_sensors(transaction, &uuids).await? {
        let sensor = unique[&uuid];
        check_sensor_identity(sensor, &sensor_type)?;
        ids.insert(uuid, sensor_id);
    }
    let missing: Vec<&Sensor> = unique
        .values()
        .filter(|sensor| !ids.contains_key(&sensor.uuid))
        .copied()
        .collect();
    if missing.is_empty() {
        return Ok(ids);
    }

    let unit_ids = ensure_units(transaction, &missing).await?;
    let label_names = ensure_label_names(
        transaction,
        missing
            .iter()
            .flat_map(|sensor| sensor.labels.iter().map(|(name, _)| name.as_str()))
            .collect(),
    )
    .await?;
    let label_descriptions = ensure_label_descriptions(
        transaction,
        missing
            .iter()
            .flat_map(|sensor| {
                sensor
                    .labels
                    .iter()
                    .map(|(_, description)| description.as_str())
            })
            .collect(),
    )
    .await?;

    let rows: Vec<_> = missing
        .iter()
        .map(|sensor| {
            let unit_id = sensor
                .unit
                .as_ref()
                .map(|unit| unit_ids[unit.name.as_str()]);
            json!([
                sensor.uuid.to_string(),
                sensor.name,
                sensor.sensor_type.to_string(),
                unit_id
            ])
        })
        .collect();
    let created: Vec<(String, i64)> = sqlx::query_as(
        r#"
        INSERT INTO sensors (uuid, name, type, unit)
        SELECT value ->> 0, value ->> 1, value ->> 2, value ->> 3 FROM json_each(?1)
        RETURNING uuid, sensor_id
        "#,
    )
    .bind(serde_json::to_string(&rows)?)
    .fetch_all(&mut **transaction)
    .await?;
    for (uuid, sensor_id) in created {
        ids.insert(Uuid::parse_str(&uuid)?, sensor_id);
    }

    let mut labels = Vec::new();
    for sensor in &missing {
        let sensor_id = ids[&sensor.uuid];
        for (name, description) in sensor.labels.iter() {
            labels.push(json!([
                sensor_id,
                label_names[name.as_str()],
                label_descriptions[description.as_str()]
            ]));
        }
    }
    if !labels.is_empty() {
        sqlx::query(
            r#"
            INSERT OR IGNORE INTO labels (sensor_id, name, description)
            SELECT value ->> 0, value ->> 1, value ->> 2 FROM json_each(?1)
            "#,
        )
        .bind(serde_json::to_string(&labels)?)
        .execute(&mut **transaction)
        .await?;
    }

    Ok(ids)
}

/// The UUID, id, name and type of the stored sensors among `uuids`.
async fn fetch_sensors(
    transaction: &mut Transaction<'_, Sqlite>,
    uuids: &[String],
) -> Result<Vec<(Uuid, i64, String)>> {
    let rows: Vec<(String, i64, String)> = sqlx::query_as(
        "SELECT uuid, sensor_id, type FROM sensors \
         WHERE uuid IN (SELECT value FROM json_each(?1))",
    )
    .bind(serde_json::to_string(uuids)?)
    .fetch_all(&mut **transaction)
    .await?;
    rows.into_iter()
        .map(|(uuid, sensor_id, sensor_type)| Ok((Uuid::parse_str(&uuid)?, sensor_id, sensor_type)))
        .collect()
}

/// The ids of the units of the given sensors, by name, creating the missing ones. A unit that
/// exists keeps its description.
async fn ensure_units<'a>(
    transaction: &mut Transaction<'_, Sqlite>,
    sensors: &[&'a Sensor],
) -> Result<HashMap<&'a str, i64>> {
    let mut units: BTreeMap<&str, Option<&str>> = BTreeMap::new();
    for sensor in sensors {
        if let Some(unit) = &sensor.unit {
            units
                .entry(unit.name.as_str())
                .or_insert(unit.description.as_deref());
        }
    }
    if units.is_empty() {
        return Ok(HashMap::new());
    }
    let rows: Vec<_> = units
        .iter()
        .map(|(name, description)| json!([name, description]))
        .collect();
    sqlx::query(
        r#"
        INSERT OR IGNORE INTO units (name, description)
        SELECT value ->> 0, value ->> 1 FROM json_each(?1)
        "#,
    )
    .bind(serde_json::to_string(&rows)?)
    .execute(&mut **transaction)
    .await?;
    let names: Vec<&str> = units.into_keys().collect();
    let ids: HashMap<String, i64> = sqlx::query_as::<_, (i64, String)>(
        "SELECT id, name FROM units WHERE name IN (SELECT value FROM json_each(?1))",
    )
    .bind(serde_json::to_string(&names)?)
    .fetch_all(&mut **transaction)
    .await?
    .into_iter()
    .map(|(id, name)| (name, id))
    .collect();
    names
        .into_iter()
        .map(|name| {
            let id = ids
                .get(name)
                .with_context(|| format!("The unit {name:?} was not stored"))?;
            Ok((name, *id))
        })
        .collect()
}

/// One function per dictionary table, written out so that the table and the column are fixed
/// text, never something a caller chooses. Each returns the ids of the given values, creating the
/// missing ones.
macro_rules! dictionary {
    ($function:ident, $table:literal, $column:literal) => {
        pub async fn $function<'a>(
            transaction: &mut Transaction<'_, Sqlite>,
            values: BTreeSet<&'a str>,
        ) -> Result<HashMap<&'a str, i64>> {
            if values.is_empty() {
                return Ok(HashMap::new());
            }
            let values_json = serde_json::to_string(&values)?;
            sqlx::query(concat!(
                "INSERT OR IGNORE INTO ",
                $table,
                " (",
                $column,
                ") SELECT value FROM json_each(?1)"
            ))
            .bind(&values_json)
            .execute(&mut **transaction)
            .await?;
            let ids: HashMap<String, i64> = sqlx::query_as::<_, (i64, String)>(concat!(
                "SELECT id, ",
                $column,
                " FROM ",
                $table,
                " WHERE ",
                $column,
                " IN (SELECT value FROM json_each(?1))"
            ))
            .bind(&values_json)
            .fetch_all(&mut **transaction)
            .await?
            .into_iter()
            .map(|(id, value)| (value, id))
            .collect();
            // A value that does not come back (SQLite changed it on the way) is an error rather
            // than a sample stored under no string.
            values
                .into_iter()
                .map(|value| {
                    let id = ids.get(value).with_context(|| {
                        format!(concat!("The value {:?} was not stored in ", $table), value)
                    })?;
                    Ok((value, *id))
                })
                .collect()
        }
    };
}

dictionary!(ensure_label_names, "labels_name_dictionary", "name");
dictionary!(
    ensure_label_descriptions,
    "labels_description_dictionary",
    "description"
);
dictionary!(ensure_string_values, "strings_values_dictionary", "value");
