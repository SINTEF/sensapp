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
use crate::storage::common::{check_sensor_identity, unique_sensors};
use anyhow::Result;
use sqlx::PgConnection;
use std::collections::{BTreeMap, BTreeSet, HashMap};
use uuid::Uuid;

/// The sensor ids of the given sensors, creating the ones that do not exist. A sensor that exists
/// must have the stored name and type, see `check_sensor_identity`.
///
/// Concurrent writers registering the same new sensor are safe: every insert is
/// `ON CONFLICT DO NOTHING`, and the ids are read again afterwards. Values are sorted before they
/// are inserted so that concurrent transactions take their locks in the same order.
///
/// `lock_first` are tables (the hypertables the batch writes to, sorted) that are locked in
/// `SHARE UPDATE EXCLUSIVE` mode before the first sensor is inserted, when there is one to insert.
/// On TimescaleDB creating a chunk takes that lock on the hypertable, then a `SHARE ROW EXCLUSIVE`
/// lock on `sensors` (it copies the foreign key onto the chunk), which waits for every transaction
/// that inserted a sensor and did not commit. A transaction that inserted a sensor and then needs a
/// chunk would take the two locks in the other order: a deadlock. Taking the table first makes the
/// order the same for everybody.
pub async fn register_sensors(
    connection: &mut PgConnection,
    sensors: &[&Sensor],
    lock_first: &[&'static str],
) -> Result<HashMap<Uuid, i64>> {
    let unique = unique_sensors(sensors)?;

    let mut ids = fetch_sensor_ids(connection, &unique, unique.keys().copied().collect()).await?;
    // In UUID order, from the BTreeMap
    let missing: Vec<&Sensor> = unique
        .values()
        .filter(|sensor| !ids.contains_key(&sensor.uuid))
        .copied()
        .collect();
    if missing.is_empty() {
        return Ok(ids);
    }

    if !lock_first.is_empty() {
        // `ONLY`: without it the lock is taken on every chunk too. The names are static.
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "LOCK TABLE ONLY {} IN SHARE UPDATE EXCLUSIVE MODE",
            lock_first.join(", ")
        )))
        .execute(&mut *connection)
        .await?;
    }

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
        ids.extend(fetch_sensor_ids(connection, &unique, others).await?);
    }
    // A sensor another transaction created, then a third one deleted before the read above, has
    // no id. Trying again creates it.
    if let Some(uuid) = unique.keys().find(|uuid| !ids.contains_key(*uuid)) {
        return Err(SensorDeletedDuringWrite(*uuid).into());
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

/// A sensor of the batch was deleted by another transaction while the batch was written. The
/// transaction is rolled back; the write can be tried again.
#[derive(Debug, thiserror::Error)]
#[error("sensor {0} was deleted while it was written to")]
pub struct SensorDeletedDuringWrite(pub Uuid);

/// The ids of the stored sensors among `uuids`, after checking that each has the name and the type
/// of the sensor of the batch (`sensors`) with its UUID.
async fn fetch_sensor_ids(
    connection: &mut PgConnection,
    sensors: &BTreeMap<Uuid, &Sensor>,
    uuids: Vec<Uuid>,
) -> Result<HashMap<Uuid, i64>> {
    let rows: Vec<(Uuid, i64, String, String)> =
        sqlx::query_as("SELECT uuid, sensor_id, name, type FROM sensors WHERE uuid = ANY($1)")
            .bind(&uuids)
            .fetch_all(connection)
            .await?;
    let mut ids = HashMap::with_capacity(rows.len());
    for (uuid, sensor_id, name, sensor_type) in rows {
        check_sensor_identity(sensors[&uuid], &name, &sensor_type)?;
        ids.insert(uuid, sensor_id);
    }
    Ok(ids)
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
        ON CONFLICT ((md5(name))) DO NOTHING
        "#,
    )
    .bind(&names)
    .bind(&descriptions)
    .execute(&mut *connection)
    .await?;
    let rows: Vec<(i64, String)> = sqlx::query_as(
        r#"
        SELECT id, name FROM units
        WHERE md5(name) = ANY(ARRAY(SELECT md5(v) FROM unnest($1::text[]) AS v))
          AND name = ANY($1)
        "#,
    )
    .bind(&names)
    .fetch_all(connection)
    .await?;
    crate::storage::pg_strings::all_found(names.len(), rows)
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
                ") SELECT unnest($1::text[]) ON CONFLICT ((md5(",
                $column,
                "))) DO NOTHING"
            ))
            .bind(&values)
            .execute(&mut *connection)
            .await?;
            let rows: Vec<(i64, String)> = sqlx::query_as(concat!(
                "SELECT id, ",
                $column,
                " FROM ",
                $table,
                " WHERE md5(",
                $column,
                ") = ANY(ARRAY(SELECT md5(v) FROM unnest($1::text[]) AS v)) AND ",
                $column,
                " = ANY($1)"
            ))
            .bind(&values)
            .fetch_all(connection)
            .await?;
            crate::storage::pg_strings::all_found(values.len(), rows)
        }
    };
}

dictionary!(ensure_label_names, "labels_name_dictionary", "name");
dictionary!(
    ensure_label_descriptions,
    "labels_description_dictionary",
    "description"
);

/// Delete a series, its samples, its labels and its sensor row, in one transaction. False when
/// it does not exist.
///
/// The sensor row is locked first (`FOR UPDATE`). A writer that inserts a sample of the series
/// holds a `FOR KEY SHARE` lock on that row through the foreign key: either the delete waits for
/// the writer to commit and deletes its samples too, or the writer waits for the delete, then
/// fails on the foreign key and registers the sensor again, as `publish` tries again. Without
/// the lock, a sample written between the delete of the samples and the delete of the sensor made
/// the delete fail on the foreign key.
///
/// On TimescaleDB (`timescaledb`), a writer that creates a chunk can be in a deadlock with the
/// delete. PostgreSQL aborts one of them, and the delete tries again after a random pause. The
/// samples of compressed chunks are deleted whatever their number, see
/// [`unlimited_decompression`].
pub async fn delete_series(pool: &sqlx::PgPool, uuid: Uuid, timescaledb: bool) -> Result<bool> {
    const ATTEMPTS: usize = 5;
    let mut attempt = 1;
    loop {
        match delete_series_once(pool, uuid, timescaledb).await {
            Err(error) if attempt < ATTEMPTS && crate::storage::common::is_deadlock(&error) => {
                tokio::time::sleep(std::time::Duration::from_millis(fastrand::u64(10..60))).await;
            }
            result => return result,
        }
        attempt += 1;
    }
}

/// TimescaleDB refuses a statement that decompresses more than
/// `timescaledb.max_tuples_decompressed_per_dml_transaction` rows (100,000 by default), so a
/// delete over the compressed chunks of a large series failed. A delete is asked for and is
/// expected to happen: no limit for the transaction.
pub async fn unlimited_decompression(connection: &mut PgConnection) -> Result<()> {
    sqlx::query("SET LOCAL timescaledb.max_tuples_decompressed_per_dml_transaction = 0")
        .execute(connection)
        .await?;
    Ok(())
}

async fn delete_series_once(pool: &sqlx::PgPool, uuid: Uuid, timescaledb: bool) -> Result<bool> {
    let mut transaction = pool.begin().await?;
    if timescaledb {
        unlimited_decompression(&mut transaction).await?;
    }
    let sensor_id: Option<i64> =
        sqlx::query_scalar("SELECT sensor_id FROM sensors WHERE uuid = $1 FOR UPDATE")
            .bind(uuid)
            .fetch_optional(&mut *transaction)
            .await?;
    let Some(sensor_id) = sensor_id else {
        return Ok(false);
    };

    for table in crate::storage::common::VALUE_TABLES {
        // `table` comes from the static VALUE_TABLES list
        sqlx::query(sqlx::AssertSqlSafe(format!(
            "DELETE FROM {table} WHERE sensor_id = $1"
        )))
        .bind(sensor_id)
        .execute(&mut *transaction)
        .await?;
    }
    sqlx::query("DELETE FROM labels WHERE sensor_id = $1")
        .bind(sensor_id)
        .execute(&mut *transaction)
        .await?;
    sqlx::query("DELETE FROM sensors WHERE sensor_id = $1")
        .bind(sensor_id)
        .execute(&mut *transaction)
        .await?;

    transaction.commit().await?;
    Ok(true)
}
