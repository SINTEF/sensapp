//! Registering the sensors and the strings of a batch on DuckDB with a handful of statements,
//! whatever the number of sensors.
//!
//! A Prometheus request carries thousands of series. Registering them one by one (a lookup, an
//! insert, then a lookup and an insert per label) took about 0.9 ms per new series. The ids are
//! looked up with chunked `IN` lists, and the missing rows go in through appenders. Appenders take
//! the ids of the dictionaries and of the sensors from the sequences of the tables, so only the
//! columns that matter are given.
//!
//! Everything runs in the transaction of the batch, and nothing is cached: a failed batch leaves
//! nothing behind, and a deleted sensor cannot leave a stale id. The connection is behind a mutex,
//! so no other writer of this process runs at the same time.

use crate::datamodel::Sensor;
use anyhow::{Result, bail};
use duckdb::{Connection, params, params_from_iter};
use std::collections::{BTreeSet, HashMap};
use uuid::Uuid;

/// How many keys one `IN (...)` lookup carries.
const LOOKUP_CHUNK: usize = 500;

/// The sensor ids of the given sensors, creating the ones that do not exist.
///
/// The labels of an existing sensor are left as they are: they are written when the sensor is
/// created.
pub fn register_sensors(
    connection: &Connection,
    sensors: &[&Sensor],
) -> Result<HashMap<Uuid, i64>> {
    let mut unique: HashMap<Uuid, &Sensor> = HashMap::with_capacity(sensors.len());
    for sensor in sensors {
        unique.entry(sensor.uuid).or_insert(sensor);
    }

    let uuids: Vec<String> = unique.keys().map(Uuid::to_string).collect();
    let mut ids: HashMap<Uuid, i64> =
        fetch_ids(connection, "sensors", "uuid", "sensor_id", &uuids)?
            .into_iter()
            .map(|(uuid, id)| Ok((Uuid::parse_str(&uuid)?, id)))
            .collect::<Result<_>>()?;

    let mut missing: Vec<&Sensor> = unique
        .values()
        .filter(|sensor| !ids.contains_key(&sensor.uuid))
        .copied()
        .collect();
    if missing.is_empty() {
        return Ok(ids);
    }
    missing.sort_by_key(|sensor| sensor.uuid);

    let unit_ids = ensure_units(connection, &missing)?;
    let label_names = ensure_dictionary(
        connection,
        "labels_name_dictionary",
        "name",
        &missing
            .iter()
            .flat_map(|sensor| sensor.labels.iter().map(|(name, _)| name.as_str()))
            .collect(),
    )?;
    let label_descriptions = ensure_dictionary(
        connection,
        "labels_description_dictionary",
        "description",
        &missing
            .iter()
            .flat_map(|sensor| {
                sensor
                    .labels
                    .iter()
                    .map(|(_, description)| description.as_str())
            })
            .collect(),
    )?;

    {
        let mut appender = connection.appender("sensors")?;
        for column in ["uuid", "name", "type", "unit"] {
            appender.add_column(column)?;
        }
        for sensor in &missing {
            let unit_id: Option<i64> = sensor
                .unit
                .as_ref()
                .and_then(|unit| unit_ids.get(&unit.name).copied());
            appender.append_row(params![
                sensor.uuid.to_string(),
                sensor.name,
                sensor.sensor_type.to_string(),
                unit_id
            ])?;
        }
        appender.flush()?;
    }

    let new_uuids: Vec<String> = missing
        .iter()
        .map(|sensor| sensor.uuid.to_string())
        .collect();
    for (uuid, id) in fetch_ids(connection, "sensors", "uuid", "sensor_id", &new_uuids)? {
        ids.insert(Uuid::parse_str(&uuid)?, id);
    }

    {
        let mut appender = connection.appender("labels")?;
        for sensor in &missing {
            let Some(sensor_id) = ids.get(&sensor.uuid) else {
                bail!("sensor {} was not created", sensor.uuid);
            };
            for (name, description) in sensor.labels.iter() {
                appender.append_row(params![
                    sensor_id,
                    label_names[name.as_str()],
                    label_descriptions[description.as_str()]
                ])?;
            }
        }
        appender.flush()?;
    }

    Ok(ids)
}

/// The dictionary ids of the given strings, adding the ones that are not in it yet.
pub fn ensure_string_ids(
    connection: &Connection,
    strings: &BTreeSet<&str>,
) -> Result<HashMap<String, i64>> {
    ensure_dictionary(connection, "strings_values_dictionary", "value", strings)
}

/// The ids of the units of the given sensors, adding the ones that do not exist. The description
/// of a unit that exists already is kept.
fn ensure_units(connection: &Connection, sensors: &[&Sensor]) -> Result<HashMap<String, i64>> {
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

    let names: Vec<String> = units.keys().map(|name| name.to_string()).collect();
    let mut ids = fetch_ids(connection, "units", "name", "id", &names)?;
    let mut missing: Vec<(&str, Option<&str>)> = units
        .into_iter()
        .filter(|(name, _)| !ids.contains_key(*name))
        .collect();
    if missing.is_empty() {
        return Ok(ids);
    }
    missing.sort();

    {
        let mut appender = connection.appender("units")?;
        appender.add_column("name")?;
        appender.add_column("description")?;
        for (name, description) in &missing {
            appender.append_row(params![name, description])?;
        }
        appender.flush()?;
    }
    let new_names: Vec<String> = missing.iter().map(|(name, _)| name.to_string()).collect();
    ids.extend(fetch_ids(connection, "units", "name", "id", &new_names)?);
    Ok(ids)
}

/// The ids of the given values of a dictionary table with one text column, adding the missing
/// ones. The table and the column are static names, never something a caller chooses.
fn ensure_dictionary(
    connection: &Connection,
    table: &'static str,
    column: &'static str,
    values: &BTreeSet<&str>,
) -> Result<HashMap<String, i64>> {
    if values.is_empty() {
        return Ok(HashMap::new());
    }
    let values: Vec<String> = values.iter().map(|value| value.to_string()).collect();
    let mut ids = fetch_ids(connection, table, column, "id", &values)?;
    let missing: Vec<&String> = values
        .iter()
        .filter(|value| !ids.contains_key(*value))
        .collect();
    if missing.is_empty() {
        return Ok(ids);
    }

    {
        let mut appender = connection.appender(table)?;
        appender.add_column(column)?;
        for value in &missing {
            appender.append_row(params![value])?;
        }
        appender.flush()?;
    }
    let new_values: Vec<String> = missing.into_iter().cloned().collect();
    ids.extend(fetch_ids(connection, table, column, "id", &new_values)?);
    Ok(ids)
}

/// `key -> id` for the rows of `table` whose `key_column` is one of the keys. The table and the
/// columns are static names; the keys are bound as parameters, a chunk at a time.
fn fetch_ids(
    connection: &Connection,
    table: &'static str,
    key_column: &'static str,
    id_column: &'static str,
    keys: &[String],
) -> Result<HashMap<String, i64>> {
    let mut ids = HashMap::with_capacity(keys.len());
    for chunk in keys.chunks(LOOKUP_CHUNK) {
        let placeholders = vec!["?"; chunk.len()].join(", ");
        let sql = format!(
            "SELECT CAST({key_column} AS VARCHAR), {id_column} FROM {table} \
             WHERE {key_column} IN ({placeholders})"
        );
        let mut statement = connection.prepare(&sql)?;
        let rows = statement.query_map(params_from_iter(chunk.iter()), |row| {
            Ok((row.get::<_, String>(0)?, row.get::<_, i64>(1)?))
        })?;
        for row in rows {
            let (key, id) = row?;
            ids.insert(key, id);
        }
    }
    Ok(ids)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datamodel::SensorType;
    use crate::datamodel::sensapp_vec::SensAppLabels;
    use crate::datamodel::unit::Unit;

    fn connection() -> Connection {
        let connection = Connection::open_in_memory().unwrap();
        connection.execute_batch(super::super::INIT_SQL).unwrap();
        connection
    }

    fn sensor(name: &str, unit: Option<Unit>, labels: &[(&str, &str)]) -> Sensor {
        let labels: SensAppLabels = labels
            .iter()
            .map(|(name, value)| (name.to_string(), value.to_string()))
            .collect();
        Sensor::new(
            Uuid::new_v4(),
            name.to_string(),
            SensorType::Float,
            unit,
            Some(labels),
        )
    }

    fn count(connection: &Connection, table: &str) -> i64 {
        connection
            .query_row(&format!("SELECT COUNT(*) FROM {table}"), [], |row| {
                row.get(0)
            })
            .unwrap()
    }

    #[test]
    fn registers_sensors_with_their_units_and_labels_once() {
        let connection = connection();
        let celsius = Some(Unit::new("Cel".to_string(), Some("degrees".to_string())));
        let first = sensor("a", celsius.clone(), &[("room", "kitchen"), ("floor", "1")]);
        let second = sensor("b", celsius, &[("room", "kitchen")]);

        // The same sensor twice in the input is one sensor
        let ids = register_sensors(&connection, &[&first, &second, &first]).unwrap();
        assert_eq!(ids.len(), 2);
        assert_ne!(ids[&first.uuid], ids[&second.uuid]);
        assert_eq!(count(&connection, "sensors"), 2);
        assert_eq!(count(&connection, "units"), 1);
        assert_eq!(count(&connection, "labels"), 3);
        assert_eq!(count(&connection, "labels_name_dictionary"), 2);
        assert_eq!(count(&connection, "labels_description_dictionary"), 2);

        // Registering again finds the same ids and writes nothing
        let again = register_sensors(&connection, &[&second, &first]).unwrap();
        assert_eq!(again, ids);
        assert_eq!(count(&connection, "sensors"), 2);
        assert_eq!(count(&connection, "labels"), 3);

        // A unit that exists keeps its description
        let renamed = sensor(
            "c",
            Some(Unit::new("Cel".to_string(), Some("other".to_string()))),
            &[],
        );
        register_sensors(&connection, &[&renamed]).unwrap();
        let description: String = connection
            .query_row(
                "SELECT description FROM units WHERE name = 'Cel'",
                [],
                |row| row.get(0),
            )
            .unwrap();
        assert_eq!(description, "degrees");
    }

    #[test]
    fn a_rolled_back_batch_leaves_nothing_and_can_be_written_again() {
        let mut connection = connection();
        let kelvin = Some(Unit::new("K".to_string(), None));
        let sensors: Vec<Sensor> = (0..3)
            .map(|index| sensor(&format!("s{index}"), kelvin.clone(), &[("room", "lab")]))
            .collect();
        let refs: Vec<&Sensor> = sensors.iter().collect();

        let transaction = connection.transaction().unwrap();
        register_sensors(&transaction, &refs).unwrap();
        ensure_string_ids(&transaction, &BTreeSet::from(["on", "off"])).unwrap();
        transaction.rollback().unwrap();
        for table in [
            "sensors",
            "units",
            "labels",
            "labels_name_dictionary",
            "labels_description_dictionary",
            "strings_values_dictionary",
        ] {
            assert_eq!(count(&connection, table), 0, "{table}");
        }

        let transaction = connection.transaction().unwrap();
        let ids = register_sensors(&transaction, &refs).unwrap();
        transaction.commit().unwrap();
        assert_eq!(ids.len(), 3);
        assert_eq!(count(&connection, "sensors"), 3);
        assert_eq!(count(&connection, "labels"), 3);
        assert_eq!(count(&connection, "units"), 1);
    }

    #[test]
    fn lookups_cross_the_chunk_size() {
        let connection = connection();
        let total = LOOKUP_CHUNK * 2 + 17;
        let sensors: Vec<Sensor> = (0..total)
            .map(|index| sensor(&format!("s{index}"), None, &[("n", &format!("v{index}"))]))
            .collect();
        let refs: Vec<&Sensor> = sensors.iter().collect();

        let ids = register_sensors(&connection, &refs).unwrap();
        assert_eq!(ids.len(), total);
        assert_eq!(count(&connection, "labels"), total as i64);
        assert_eq!(register_sensors(&connection, &refs).unwrap(), ids);
        assert_eq!(count(&connection, "sensors"), total as i64);

        let owned: Vec<String> = (0..total)
            .map(|index| format!("string {index} é 日本"))
            .collect();
        let strings: BTreeSet<&str> = owned.iter().map(String::as_str).collect();
        let string_ids = ensure_string_ids(&connection, &strings).unwrap();
        assert_eq!(string_ids.len(), total);
        assert_eq!(
            ensure_string_ids(&connection, &strings).unwrap(),
            string_ids
        );
        assert_eq!(
            count(&connection, "strings_values_dictionary"),
            total as i64
        );
    }

    #[test]
    fn empty_input_registers_nothing() {
        let connection = connection();
        assert!(register_sensors(&connection, &[]).unwrap().is_empty());
        assert!(
            ensure_string_ids(&connection, &BTreeSet::new())
                .unwrap()
                .is_empty()
        );
        let unlabelled = sensor("a", None, &[]);
        register_sensors(&connection, &[&unlabelled]).unwrap();
        assert_eq!(count(&connection, "labels"), 0);
    }
}
