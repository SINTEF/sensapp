//! Registering the sensors of a batch on the PostgreSQL family of backends (PostgreSQL and
//! TimescaleDB share these tables) with a handful of statements, whatever the number of sensors.
//!
//! A Prometheus request carries thousands of series. Registering them one by one (a lookup, a
//! unit, an insert, then two dictionary lookups and an insert per label) took 3.9 ms per new
//! series, so about 7 700 new series passed the 30 s request timeout.
//!
//! Everything runs in the transaction of the batch: a failed batch leaves nothing behind, and
//! nothing is cached, so there is no stale id after a rollback or a deletion by another instance.

use crate::datamodel::Sensor;
use anyhow::Result;
use sqlx::PgConnection;
use std::collections::{BTreeSet, HashMap};
use uuid::Uuid;

/// The sensor ids of the given sensors, creating the ones that do not exist.
///
/// Concurrent writers registering the same new sensor are safe: every insert is
/// `ON CONFLICT DO NOTHING`, and the ids are read again afterwards. Values are sorted before they
/// are inserted so that concurrent transactions take their locks in the same order.
pub async fn register_sensors(
    connection: &mut PgConnection,
    sensors: &[&Sensor],
) -> Result<HashMap<Uuid, i64>> {
    let mut unique: HashMap<Uuid, &Sensor> = HashMap::with_capacity(sensors.len());
    for sensor in sensors {
        unique.entry(sensor.uuid).or_insert(sensor);
    }

    let mut ids = fetch_sensor_ids(connection, unique.keys().copied().collect()).await?;
    let mut missing: Vec<&Sensor> = unique
        .values()
        .filter(|sensor| !ids.contains_key(&sensor.uuid))
        .copied()
        .collect();
    if missing.is_empty() {
        return Ok(ids);
    }
    missing.sort_by_key(|sensor| sensor.uuid);

    // Units and the dictionaries of label names and descriptions
    let unit_ids = ensure_units(connection, &missing).await?;
    let names: Vec<String> = missing
        .iter()
        .flat_map(|sensor| sensor.labels.iter().map(|(name, _)| name.clone()))
        .collect();
    let label_names = ensure_label_names(connection, names).await?;
    let descriptions: Vec<String> = missing
        .iter()
        .flat_map(|sensor| {
            sensor
                .labels
                .iter()
                .map(|(_, description)| description.clone())
        })
        .collect();
    let label_descriptions = ensure_label_descriptions(connection, descriptions).await?;

    // The sensors. The rows that come back are the ones this statement created; the others were
    // created by a concurrent transaction, which also writes their labels.
    let uuids: Vec<Uuid> = missing.iter().map(|sensor| sensor.uuid).collect();
    let names: Vec<&str> = missing.iter().map(|sensor| sensor.name.as_str()).collect();
    let types: Vec<String> = missing
        .iter()
        .map(|sensor| sensor.sensor_type.to_string())
        .collect();
    let units: Vec<Option<i64>> = missing
        .iter()
        .map(|sensor| {
            sensor
                .unit
                .as_ref()
                .and_then(|unit| unit_ids.get(&unit.name).copied())
        })
        .collect();
    let created: Vec<(Uuid, i64)> = sqlx::query_as(
        r#"
        INSERT INTO sensors (uuid, name, type, unit)
        SELECT * FROM unnest($1::uuid[], $2::text[], $3::text[], $4::bigint[])
        ON CONFLICT (uuid) DO NOTHING
        RETURNING uuid, sensor_id
        "#,
    )
    .bind(&uuids)
    .bind(&names)
    .bind(&types)
    .bind(&units)
    .fetch_all(&mut *connection)
    .await?;
    let created: HashMap<Uuid, i64> = created.into_iter().collect();
    ids.extend(created.iter().map(|(uuid, id)| (*uuid, *id)));

    let others: Vec<Uuid> = uuids
        .into_iter()
        .filter(|uuid| !created.contains_key(uuid))
        .collect();
    if !others.is_empty() {
        ids.extend(fetch_sensor_ids(connection, others).await?);
    }

    // The labels of the sensors this call created
    let mut label_sensor_ids: Vec<i64> = Vec::new();
    let mut label_name_ids: Vec<i64> = Vec::new();
    let mut label_description_ids: Vec<i64> = Vec::new();
    for sensor in &missing {
        let Some(sensor_id) = created.get(&sensor.uuid) else {
            continue;
        };
        for (name, description) in sensor.labels.iter() {
            label_sensor_ids.push(*sensor_id);
            label_name_ids.push(label_names[name]);
            label_description_ids.push(label_descriptions[description]);
        }
    }
    if !label_sensor_ids.is_empty() {
        sqlx::query(
            r#"
            INSERT INTO labels (sensor_id, name, description)
            SELECT * FROM unnest($1::bigint[], $2::bigint[], $3::bigint[])
            ON CONFLICT (sensor_id, name) DO NOTHING
            "#,
        )
        .bind(&label_sensor_ids)
        .bind(&label_name_ids)
        .bind(&label_description_ids)
        .execute(&mut *connection)
        .await?;
    }

    Ok(ids)
}

async fn fetch_sensor_ids(
    connection: &mut PgConnection,
    uuids: Vec<Uuid>,
) -> Result<HashMap<Uuid, i64>> {
    let rows: Vec<(Uuid, i64)> =
        sqlx::query_as("SELECT uuid, sensor_id FROM sensors WHERE uuid = ANY($1)")
            .bind(&uuids)
            .fetch_all(connection)
            .await?;
    Ok(rows.into_iter().collect())
}

async fn ensure_units(
    connection: &mut PgConnection,
    sensors: &[&Sensor],
) -> Result<HashMap<String, i64>> {
    let mut units: HashMap<&str, Option<&str>> = HashMap::new();
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
    let mut units: Vec<(&str, Option<&str>)> = units.into_iter().collect();
    units.sort();
    let names: Vec<&str> = units.iter().map(|(name, _)| *name).collect();
    let descriptions: Vec<Option<&str>> =
        units.iter().map(|(_, description)| *description).collect();

    sqlx::query(
        r#"
        INSERT INTO units (name, description)
        SELECT * FROM unnest($1::text[], $2::text[])
        ON CONFLICT (name) DO NOTHING
        "#,
    )
    .bind(&names)
    .bind(&descriptions)
    .execute(&mut *connection)
    .await?;
    let rows: Vec<(i64, String)> =
        sqlx::query_as("SELECT id, name FROM units WHERE name = ANY($1)")
            .bind(&names)
            .fetch_all(connection)
            .await?;
    Ok(rows.into_iter().map(|(id, name)| (name, id)).collect())
}

/// One statement pair per dictionary table, written out so that the table and the column are
/// fixed text, never something a caller chooses.
macro_rules! dictionary {
    ($function:ident, $table:literal, $column:literal) => {
        async fn $function(
            connection: &mut PgConnection,
            values: Vec<String>,
        ) -> Result<HashMap<String, i64>> {
            let values: Vec<String> = values
                .into_iter()
                .collect::<BTreeSet<_>>()
                .into_iter()
                .collect();
            if values.is_empty() {
                return Ok(HashMap::new());
            }
            sqlx::query(concat!(
                "INSERT INTO ",
                $table,
                " (",
                $column,
                ") SELECT unnest($1::text[]) ON CONFLICT (",
                $column,
                ") DO NOTHING"
            ))
            .bind(&values)
            .execute(&mut *connection)
            .await?;
            let rows: Vec<(i64, String)> = sqlx::query_as(concat!(
                "SELECT id, ",
                $column,
                " FROM ",
                $table,
                " WHERE ",
                $column,
                " = ANY($1)"
            ))
            .bind(&values)
            .fetch_all(connection)
            .await?;
            Ok(rows.into_iter().map(|(id, value)| (value, id)).collect())
        }
    };
}

dictionary!(ensure_label_names, "labels_name_dictionary", "name");
dictionary!(
    ensure_label_descriptions,
    "labels_description_dictionary",
    "description"
);
