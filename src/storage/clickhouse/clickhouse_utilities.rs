use crate::datamodel::SensAppDateTime;
use crate::datamodel::{Sensor, sensapp_datetime::SensAppDateTimeExt, unit::Unit};
use crate::storage::StorageError;
use anyhow::Result;
use clickhouse::Row;
use rust_decimal::{Decimal, RoundingStrategy};
use serde::Serialize;
use std::collections::{HashMap, HashSet};
use uuid::Uuid;

pub const CLICKHOUSE_NUMERIC_SCALE: u32 = 8;

pub use crate::storage::common::{unit_name_to_id, uuid_to_sensor_id};

/// Convert SensAppDateTime to microseconds timestamp - using common implementation
pub use crate::storage::common::datetime_to_micros;

/// Convert microseconds timestamp to SensAppDateTime
pub fn micros_to_datetime(micros: i64) -> SensAppDateTime {
    SensAppDateTime::from_unix_microseconds_i64(micros)
}

pub fn decimal_to_clickhouse_raw(value: &Decimal) -> Result<i128> {
    let mut normalized = if value.scale() > CLICKHOUSE_NUMERIC_SCALE {
        value.round_dp_with_strategy(
            CLICKHOUSE_NUMERIC_SCALE,
            RoundingStrategy::MidpointNearestEven,
        )
    } else {
        *value
    };
    normalized.rescale(CLICKHOUSE_NUMERIC_SCALE);
    Ok(normalized.mantissa())
}

pub fn decimal_from_clickhouse_raw(raw: i128) -> Decimal {
    Decimal::from_i128_with_scale(raw, CLICKHOUSE_NUMERIC_SCALE)
}

/// Ids sent to ClickHouse in one query: a few thousand keeps the statement small.
const ID_LOOKUP_CHUNK: usize = 2000;

/// The ids among `ids` that exist in `table`, with one query per chunk of ids.
async fn existing_ids(
    client: &clickhouse::Client,
    table: &str,
    id_column: &str,
    ids: &[u64],
) -> Result<HashSet<u64>> {
    let mut existing = HashSet::new();
    for chunk in ids.chunks(ID_LOOKUP_CHUNK) {
        let mut cursor = client
            .query(&format!(
                "SELECT {id_column} FROM {table} WHERE has(?, {id_column})"
            ))
            .bind(chunk)
            .fetch::<u64>()
            .map_err(|e| map_clickhouse_error(e, None, None))?;
        while let Some(id) = cursor.next().await? {
            existing.insert(id);
        }
    }
    Ok(existing)
}

/// Write `rows` to `table` with a single INSERT. Nothing is sent for an empty slice.
async fn insert_rows<R>(client: &clickhouse::Client, table: &str, rows: &[R]) -> Result<()>
where
    R: clickhouse::RowOwned + clickhouse::RowWrite,
{
    if rows.is_empty() {
        return Ok(());
    }
    let mut insert = client
        .insert::<R>(table)
        .await
        .map_err(|e| map_clickhouse_error(e, None, None))?;
    for row in rows {
        insert
            .write(row)
            .await
            .map_err(|e| map_clickhouse_error(e, None, None))?;
    }
    insert
        .end()
        .await
        .map_err(|e| map_clickhouse_error(e, None, None))?;
    Ok(())
}

/// Make sure every sensor of a batch exists, with a handful of statements whatever the number
/// of sensors: one lookup, then one INSERT for the new units, one for their labels and one for
/// the new sensors. A Prometheus request carries thousands of series: doing this sensor by
/// sensor took 4 ms per series for series that already existed, and 14 ms for new ones.
///
/// The sensor rows go last: a sensor row is what makes a sensor visible, so a failure in
/// between leaves no sensor without its labels, and the retry starts again from here. Writers
/// that register the same new sensor at the same time both insert it, which the
/// `ReplacingMergeTree` tables collapse.
///
/// Labels are written only for new sensors: a sensor UUID is derived from its name, type, unit
/// and labels, so they cannot change afterwards.
pub async fn register_sensors(client: &clickhouse::Client, sensors: &[&Sensor]) -> Result<()> {
    let mut by_id: HashMap<u64, &Sensor> = HashMap::with_capacity(sensors.len());
    for sensor in sensors {
        by_id
            .entry(uuid_to_sensor_id(&sensor.uuid))
            .or_insert(sensor);
    }
    let ids: Vec<u64> = by_id.keys().copied().collect();
    let existing = existing_ids(client, "sensors", "sensor_id", &ids).await?;
    let new_sensors: Vec<(u64, &Sensor)> = by_id
        .into_iter()
        .filter(|(sensor_id, _)| !existing.contains(sensor_id))
        .collect();
    if new_sensors.is_empty() {
        return Ok(());
    }

    // Units
    let mut units: HashMap<u64, &Unit> = HashMap::new();
    for (_, sensor) in &new_sensors {
        if let Some(unit) = &sensor.unit {
            units.entry(unit_name_to_id(&unit.name)).or_insert(unit);
        }
    }
    let unit_ids: Vec<u64> = units.keys().copied().collect();
    let existing_units = existing_ids(client, "units", "id", &unit_ids).await?;

    #[derive(Row, Serialize)]
    struct UnitRow {
        id: u64,
        name: String,
        description: Option<String>,
    }
    let unit_rows: Vec<UnitRow> = units
        .iter()
        .filter(|(id, _)| !existing_units.contains(id))
        .map(|(id, unit)| UnitRow {
            id: *id,
            name: unit.name.clone(),
            description: unit.description.clone(),
        })
        .collect();
    insert_rows(client, "units", &unit_rows).await?;

    // Labels
    #[derive(Row, Serialize)]
    struct LabelRow {
        sensor_id: u64,
        name: String,
        description: Option<String>,
    }
    let label_rows: Vec<LabelRow> = new_sensors
        .iter()
        .flat_map(|(sensor_id, sensor)| {
            sensor.labels.iter().map(|(name, description)| LabelRow {
                sensor_id: *sensor_id,
                name: name.clone(),
                description: Some(description.clone()),
            })
        })
        .collect();
    insert_rows(client, "labels", &label_rows).await?;

    // Sensors, last
    #[derive(Row, Serialize)]
    struct SensorRow {
        sensor_id: u64,
        #[serde(with = "clickhouse::serde::uuid")]
        uuid: Uuid,
        name: String,
        r#type: String,
        unit: Option<u64>,
    }
    let sensor_rows: Vec<SensorRow> = new_sensors
        .iter()
        .map(|(sensor_id, sensor)| SensorRow {
            sensor_id: *sensor_id,
            uuid: sensor.uuid,
            name: sensor.name.clone(),
            r#type: sensor.sensor_type.to_string(),
            unit: sensor.unit.as_ref().map(|unit| unit_name_to_id(&unit.name)),
        })
        .collect();
    insert_rows(client, "sensors", &sensor_rows).await
}

/// ClickHouse server error codes that mean "try again later" rather than "this request is
/// wrong": timeouts, network failures, overload and read-only or unavailable replicas.
const TRANSIENT_SERVER_ERROR_CODES: [u32; 12] = [
    159, // TIMEOUT_EXCEEDED
    164, // READONLY
    202, // TOO_MANY_SIMULTANEOUS_QUERIES
    203, // NO_FREE_CONNECTION
    209, // SOCKET_TIMEOUT
    210, // NETWORK_ERROR
    225, // NO_ZOOKEEPER
    241, // MEMORY_LIMIT_EXCEEDED
    242, // TABLE_IS_READ_ONLY
    252, // TOO_MANY_PARTS: merges cannot keep up with the inserts
    285, // TOO_FEW_LIVE_REPLICAS
    319, // UNKNOWN_STATUS_OF_INSERT
];

/// The code of a ClickHouse exception message: `Code: 252. DB::Exception: ...`.
fn server_error_code(message: &str) -> Option<u32> {
    let digits = message.trim_start().strip_prefix("Code: ")?;
    let end = digits.find(|c: char| !c.is_ascii_digit())?;
    digits[..end].parse().ok()
}

/// Sort an error of the ClickHouse client into the categories SensApp reports over HTTP.
///
/// Network failures, timeouts and overload are `Unavailable` (503, the client may retry).
/// Everything else the server rejects is a failure on our side (500): SensApp builds its
/// own statements and rows, so ClickHouse refusing them is never the caller's data format.
pub fn classify_clickhouse_error(error: &clickhouse::error::Error) -> StorageError {
    use clickhouse::error::Error;

    match error {
        Error::Network(_) | Error::TimedOut | Error::Other(_) => {
            StorageError::Unavailable(error.to_string())
        }
        Error::BadResponse(message)
            if server_error_code(message)
                .is_some_and(|code| TRANSIENT_SERVER_ERROR_CODES.contains(&code)) =>
        {
            StorageError::Unavailable(message.clone())
        }
        other => StorageError::OperationFailed {
            operation: "ClickHouse request".to_string(),
            details: other.to_string(),
        },
    }
}

/// Convert ClickHouse error to StorageError with context
pub fn map_clickhouse_error(
    error: clickhouse::error::Error,
    uuid: Option<Uuid>,
    name: Option<&str>,
) -> anyhow::Error {
    match classify_clickhouse_error(&error) {
        StorageError::OperationFailed { details, .. } => StorageError::OperationFailed {
            operation: format!(
                "ClickHouse request for sensor {}",
                name.map_or_else(
                    || uuid.map_or_else(|| "unknown".to_string(), |u| u.to_string()),
                    str::to_string
                )
            ),
            details,
        }
        .into(),
        classified => classified.into(),
    }
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

#[cfg(test)]
mod tests {
    use super::*;
    use clickhouse::error::Error;

    fn bad_response(code: u32) -> Error {
        Error::BadResponse(format!(
            "Code: {code}. DB::Exception: something (SOME_NAME)"
        ))
    }

    #[test]
    fn server_error_codes_are_read_from_the_message() {
        assert_eq!(server_error_code("Code: 252. DB::Exception: x"), Some(252));
        assert_eq!(server_error_code("  Code: 62. DB::Exception"), Some(62));
        assert_eq!(server_error_code("<html>Bad Gateway</html>"), None);
        assert_eq!(server_error_code("Code: abc."), None);
    }

    #[test]
    fn outages_timeouts_and_overload_are_unavailable() {
        assert!(matches!(
            classify_clickhouse_error(&Error::TimedOut),
            StorageError::Unavailable(_)
        ));
        assert!(matches!(
            classify_clickhouse_error(&Error::Network("connection refused".into())),
            StorageError::Unavailable(_)
        ));
        for code in TRANSIENT_SERVER_ERROR_CODES {
            assert!(
                matches!(
                    classify_clickhouse_error(&bad_response(code)),
                    StorageError::Unavailable(_)
                ),
                "code {code}"
            );
        }
    }

    #[test]
    fn other_server_errors_are_server_side_failures_not_bad_requests() {
        // 62 SYNTAX_ERROR, 60 UNKNOWN_TABLE, 516 AUTHENTICATION_FAILED
        for code in [62, 60, 516] {
            assert!(
                matches!(
                    classify_clickhouse_error(&bad_response(code)),
                    StorageError::OperationFailed { .. }
                ),
                "code {code}"
            );
        }
        assert!(matches!(
            classify_clickhouse_error(&Error::NotEnoughData),
            StorageError::OperationFailed { .. }
        ));
    }

    #[test]
    fn sensor_context_is_kept_for_failures() {
        let error = map_clickhouse_error(bad_response(62), None, Some("temperature"));
        assert!(format!("{error}").contains("temperature"), "{error}");
    }
}
