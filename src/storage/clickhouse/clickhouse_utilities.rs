use crate::datamodel::SensAppDateTime;
use crate::datamodel::{SensorType, sensapp_datetime::SensAppDateTimeExt, unit::Unit};
use crate::storage::StorageError;
use anyhow::Result;
use clickhouse::Row;
use serde::Serialize;
use std::collections::hash_map::DefaultHasher;
use std::hash::{Hash, Hasher};
use uuid::Uuid;

/// Convert UUID to UInt64 using a deterministic hash function
/// We use the standard library's DefaultHasher which is typically xxHash64
pub fn uuid_to_sensor_id(uuid: &Uuid) -> u64 {
    let mut hasher = DefaultHasher::new();
    uuid.hash(&mut hasher);
    hasher.finish()
}

/// Convert SensAppDateTime to microseconds timestamp - using common implementation
pub use crate::storage::common::datetime_to_micros;

/// Convert microseconds timestamp to SensAppDateTime
pub fn micros_to_datetime(micros: i64) -> SensAppDateTime {
    SensAppDateTime::from_unix_microseconds_i64(micros)
}

/// Get sensor_id for a given UUID, creating the sensor if it doesn't exist
pub async fn get_sensor_id_or_create_sensor(
    client: &clickhouse::Client,
    uuid: &Uuid,
    name: &str,
    sensor_type: &SensorType,
    unit: Option<&Unit>,
) -> Result<u64> {
    let sensor_id = uuid_to_sensor_id(uuid);

    // First, try to find existing sensor
    let existing_query = "SELECT sensor_id FROM sensors WHERE sensor_id = ? LIMIT 1";
    let mut cursor = client
        .query(existing_query)
        .bind(sensor_id)
        .fetch::<u64>()
        .map_err(|e| StorageError::invalid_data_format(&e.to_string(), Some(*uuid), Some(name)))?;

    if cursor.next().await?.is_some() {
        return Ok(sensor_id);
    }

    // Sensor doesn't exist, create it
    let unit_id = if let Some(unit) = unit {
        Some(get_or_create_unit(client, unit).await?)
    } else {
        None
    };

    // Define Row struct for sensor insertion
    #[derive(Row, Serialize)]
    struct SensorRow {
        sensor_id: u64,
        #[serde(with = "clickhouse::serde::uuid")]
        uuid: Uuid,
        name: String,
        r#type: String,
        unit: Option<u64>,
    }

    let type_str = sensor_type.to_string();
    let sensor_row = SensorRow {
        sensor_id,
        uuid: *uuid,
        name: name.to_string(),
        r#type: type_str,
        unit: unit_id,
    };

    let mut insert = client
        .insert::<SensorRow>("sensors")
        .await
        .map_err(|e| StorageError::invalid_data_format(&e.to_string(), Some(*uuid), Some(name)))?;

    insert
        .write(&sensor_row)
        .await
        .map_err(|e| StorageError::invalid_data_format(&e.to_string(), Some(*uuid), Some(name)))?;

    insert
        .end()
        .await
        .map_err(|e| StorageError::invalid_data_format(&e.to_string(), Some(*uuid), Some(name)))?;

    Ok(sensor_id)
}

/// Get or create a unit in the units table
async fn get_or_create_unit(client: &clickhouse::Client, unit: &Unit) -> Result<u64> {
    // Use hash of unit name as ID for consistency
    let unit_id = {
        let mut hasher = DefaultHasher::new();
        unit.name.hash(&mut hasher);
        hasher.finish()
    };

    // Check if unit exists
    let existing_query = "SELECT id FROM units WHERE id = ? LIMIT 1";
    let mut cursor = client
        .query(existing_query)
        .bind(unit_id)
        .fetch::<u64>()
        .map_err(|e| StorageError::invalid_data_format(&e.to_string(), None, None))?;

    if cursor.next().await?.is_some() {
        return Ok(unit_id);
    }

    // Define Row struct for unit insertion
    #[derive(Row, Serialize)]
    struct UnitRow {
        id: u64,
        name: String,
        description: Option<String>,
    }

    let unit_row = UnitRow {
        id: unit_id,
        name: unit.name.clone(),
        description: unit.description.clone(),
    };

    // Unit doesn't exist, create it
    let mut insert = client
        .insert::<UnitRow>("units")
        .await
        .map_err(|e| StorageError::invalid_data_format(&e.to_string(), None, None))?;

    insert
        .write(&unit_row)
        .await
        .map_err(|e| StorageError::invalid_data_format(&e.to_string(), None, None))?;

    insert
        .end()
        .await
        .map_err(|e| StorageError::invalid_data_format(&e.to_string(), None, None))?;

    Ok(unit_id)
}

/// Convert ClickHouse error to StorageError with context
pub fn map_clickhouse_error(
    error: clickhouse::error::Error,
    uuid: Option<Uuid>,
    name: Option<&str>,
) -> anyhow::Error {
    StorageError::invalid_data_format(&error.to_string(), uuid, name).into()
}

#[cfg(any(test, feature = "test-utils"))]
pub mod test_utils {
    use super::*;
    use anyhow::Context;

    /// Clean up test data from all tables
    pub async fn cleanup_test_data(client: &clickhouse::Client) -> Result<()> {
        let tables = vec![
            "integer_values",
            "numeric_values",
            "float_values",
            "string_values",
            "boolean_values",
            "location_values",
            "json_values",
            "blob_values",
            "labels",
            "sensors",
            "units",
        ];

        for table in tables {
            let query = format!("TRUNCATE TABLE {}", table);
            client
                .query(&query)
                .execute()
                .await
                .with_context(|| format!("Failed to truncate table {}", table))?;
        }

        Ok(())
    }
}
