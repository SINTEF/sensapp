use crate::datamodel::SensAppDateTime;
use crate::datamodel::{SensorType, sensapp_datetime::SensAppDateTimeExt, unit::Unit};
use crate::storage::StorageError;
use anyhow::Result;
use clickhouse::Row;
use rust_decimal::{Decimal, RoundingStrategy};
use serde::Serialize;
use uuid::Uuid;

pub const CLICKHOUSE_NUMERIC_SCALE: u32 = 8;

/// Convert a UUID to the `UInt64` key used by every ClickHouse table.
///
/// The ids are stored, so this function is part of the on-disk format and must never change:
/// the XOR of the two big-endian 64-bit halves of the UUID. Sensor UUIDs are hashes, so the
/// result is uniformly distributed. Do not use `std`'s `DefaultHasher` here, its algorithm is
/// explicitly allowed to change between Rust releases, which would orphan every stored sample.
pub fn uuid_to_sensor_id(uuid: &Uuid) -> u64 {
    let value = uuid.as_u128();
    ((value >> 64) as u64) ^ (value as u64)
}

/// Id of a unit in the `units` table: the first 8 bytes of the BLAKE3 hash of its name,
/// read as a little-endian integer. Part of the on-disk format, like [`uuid_to_sensor_id`].
pub fn unit_name_to_id(name: &str) -> u64 {
    let hash = blake3::hash(name.as_bytes());
    let mut bytes = [0u8; 8];
    bytes.copy_from_slice(&hash.as_bytes()[..8]);
    u64::from_le_bytes(bytes)
}

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

/// Get sensor_id for a given UUID, creating the sensor if it doesn't exist
pub async fn get_sensor_id_or_create_sensor(
    client: &clickhouse::Client,
    uuid: &Uuid,
    name: &str,
    sensor_type: &SensorType,
    unit: Option<&Unit>,
    labels: &[(String, String)],
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

    // The labels go first and the sensor row last: the sensor row is what makes the sensor
    // visible, so a failure in between leaves no sensor without its labels, and a retry
    // starts again from here.
    insert_labels(client, sensor_id, uuid, name, labels).await?;

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

/// Write the labels of a newly created sensor.
async fn insert_labels(
    client: &clickhouse::Client,
    sensor_id: u64,
    uuid: &Uuid,
    name: &str,
    labels: &[(String, String)],
) -> Result<()> {
    if labels.is_empty() {
        return Ok(());
    }

    #[derive(Row, Serialize)]
    struct LabelRow<'a> {
        sensor_id: u64,
        name: &'a str,
        description: Option<&'a str>,
    }

    let to_error = |e: clickhouse::error::Error| map_clickhouse_error(e, Some(*uuid), Some(name));

    let mut insert = client
        .insert::<LabelRow>("labels")
        .await
        .map_err(to_error)?;
    for (label_name, label_description) in labels {
        insert
            .write(&LabelRow {
                sensor_id,
                name: label_name,
                description: Some(label_description),
            })
            .await
            .map_err(to_error)?;
    }
    insert.end().await.map_err(to_error)?;
    Ok(())
}

/// Get or create a unit in the units table
async fn get_or_create_unit(client: &clickhouse::Client, unit: &Unit) -> Result<u64> {
    let unit_id = unit_name_to_id(&unit.name);

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

    // These values are stored in ClickHouse. If one of these tests fails, the on-disk format
    // changed: existing data would no longer be found.
    #[test]
    fn sensor_ids_are_pinned() {
        let id = |uuid: &str| uuid_to_sensor_id(&Uuid::parse_str(uuid).unwrap());
        assert_eq!(
            id("00000000-0000-4000-8000-000000000001"),
            0x8000_0000_0000_4001
        );
        assert_eq!(id("00000000-0000-0000-0000-000000000000"), 0);
        assert_eq!(id("ffffffff-ffff-ffff-ffff-ffffffffffff"), 0);
        assert_eq!(
            id("0123456789abcdeffedcba9876543210"),
            0xffff_ffff_ffff_ffff
        );
        assert_eq!(
            id("9d87123d-9b47-466d-9eda-001c2ecf9c54"),
            0x9d87_123d_9b47_466d ^ 0x9eda_001c_2ecf_9c54
        );
    }

    #[test]
    fn unit_ids_are_pinned() {
        assert_eq!(unit_name_to_id("°C"), UNIT_CELSIUS_ID);
        assert_eq!(unit_name_to_id(""), UNIT_EMPTY_ID);
        assert_ne!(unit_name_to_id("m"), unit_name_to_id("s"));
    }

    const UNIT_CELSIUS_ID: u64 = 14_058_937_354_730_164_320;
    // First 8 bytes of the published BLAKE3 test vector for empty input
    // (af1349b9f5f9a1a6...), read as little-endian.
    const UNIT_EMPTY_ID: u64 = 0xa6a1_f9f5_b949_13af;
}
