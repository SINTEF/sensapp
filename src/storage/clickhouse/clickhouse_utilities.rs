use crate::datamodel::SensAppDateTime;
use crate::datamodel::{Sensor, sensapp_datetime::SensAppDateTimeExt, unit::Unit};
use crate::storage::StorageError;
use crate::storage::common::{check_sensor_identity, unique_sensors};
use anyhow::Result;
use clickhouse::Row;
use rust_decimal::{Decimal, RoundingStrategy};
use serde::{Deserialize, Serialize};
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

/// The raw `Decimal(38, 8)` of a value: its mantissa at scale 8. A `Decimal` has 96 bits of
/// mantissa, and 2^96 × 10^8 is under 10^38, so every value fits; more than 8 digits after the
/// point are rounded.
pub fn decimal_to_clickhouse_raw(value: &Decimal) -> i128 {
    let value = value.round_dp_with_strategy(
        CLICKHOUSE_NUMERIC_SCALE,
        RoundingStrategy::MidpointNearestEven,
    );
    value.mantissa() * 10_i128.pow(CLICKHOUSE_NUMERIC_SCALE - value.scale())
}

/// The `Decimal` of a raw `Decimal(38, 8)`. A stored value always comes back exactly, without its
/// trailing zeros. An aggregate may have more digits than the 96 bits of a `Decimal`: its last
/// decimals are then rounded away, as `Decimal` arithmetic does, and one over the range of a
/// `Decimal` is an error.
pub fn decimal_from_clickhouse_raw(raw: i128) -> Result<Decimal, StorageError> {
    let (mut mantissa, mut scale) = (raw, CLICKHOUSE_NUMERIC_SCALE);
    while scale > 0 && mantissa % 10 == 0 {
        mantissa /= 10;
        scale -= 1;
    }
    // Drop as few digits as it takes to fit, rounding half away from zero once from the whole
    // value: rounding a digit at a time rounds a rounded value again
    let mut result = Decimal::try_from_i128_with_scale(mantissa, scale);
    for dropped in 1..=scale {
        if result.is_ok() {
            break;
        }
        let divisor = 10_i128.pow(dropped);
        let rounded = (mantissa + mantissa.signum() * (divisor / 2)) / divisor;
        result = Decimal::try_from_i128_with_scale(rounded, scale - dropped);
    }
    result.map_err(|_| StorageError::InvalidDataFormat {
        message: format!(
            "the value {raw}e-{CLICKHOUSE_NUMERIC_SCALE} is outside the Numeric range"
        ),
        sensor_context: "numeric".to_string(),
    })
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
                "SELECT {id_column} FROM {table} WHERE {id_column} IN {{ids:Array(UInt64)}}"
            ))
            .param("ids", chunk)
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

/// The ids among the keys of `sensors` that are stored, after checking that each stored sensor
/// is the sensor of the batch (and not another UUID with the same key) and has its name and type. ClickHouse has no transaction: two writers
/// that register one new UUID with two identities at the same moment are not caught here.
async fn existing_sensors(
    client: &clickhouse::Client,
    sensors: &HashMap<u64, &Sensor>,
) -> Result<HashSet<u64>> {
    #[derive(Row, Deserialize)]
    struct StoredSensor {
        sensor_id: u64,
        #[serde(with = "clickhouse::serde::uuid")]
        uuid: Uuid,
        r#type: String,
    }
    let ids: Vec<u64> = sensors.keys().copied().collect();
    let mut existing = HashSet::new();
    for chunk in ids.chunks(ID_LOOKUP_CHUNK) {
        let mut cursor = client
            .query(
                "SELECT sensor_id, uuid, type FROM sensors \
                 WHERE sensor_id IN {ids:Array(UInt64)}",
            )
            .param("ids", chunk)
            .fetch::<StoredSensor>()
            .map_err(|e| map_clickhouse_error(e, None, None))?;
        while let Some(stored) = cursor.next().await? {
            let sensor = sensors[&stored.sensor_id];
            if stored.uuid != sensor.uuid {
                return Err(StorageError::SensorKeyCollision { uuid: sensor.uuid }.into());
            }
            check_sensor_identity(sensor, &stored.r#type)?;
            existing.insert(stored.sensor_id);
        }
    }
    Ok(existing)
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
/// and labels, so they cannot change afterwards. A sensor that exists must have the stored name
/// and type, see `check_sensor_identity`.
pub async fn register_sensors(client: &clickhouse::Client, sensors: &[&Sensor]) -> Result<()> {
    let mut by_id: HashMap<u64, &Sensor> = HashMap::new();
    for sensor in unique_sensors(sensors)?.into_values() {
        if by_id
            .insert(uuid_to_sensor_id(&sensor.uuid)?, sensor)
            .is_some()
        {
            return Err(StorageError::SensorKeyCollision { uuid: sensor.uuid }.into());
        }
    }
    let existing = existing_sensors(client, &by_id).await?;
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
        // The `accurateCast` and the `throwIf` of an aggregation whose sum leaves the type of
        // the series (CANNOT_CONVERT_TYPE, FUNCTION_THROW_IF_VALUE_IS_NON_ZERO)
        Error::BadResponse(message)
            if server_error_code(message).is_some_and(|code| code == 70 || code == 395) =>
        {
            StorageError::invalid_data_format(
                "the sum of a bucket does not fit in the type of the series: use a shorter step",
                None,
                None,
            )
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

    /// `Decimal(38, 8)` holds every `Decimal` at scale 8 (96 bits × 10^8 is under 10^38): large
    /// values were stored divided by a power of ten, then refused.
    /// An aggregate over the 96 bits of a `Decimal` loses its last digits once, rounded from the
    /// whole value: digit by digit, `…49` became `…5` and then rounded up again.
    #[test]
    fn an_aggregate_over_96_bits_is_rounded_once() {
        let max = Decimal::MAX.mantissa();
        // `max` with two more digits that are 49: rounded to the two digits it must drop, it is
        // `max` at scale 6. Two roundings of one digit each gave `max + 1`, which does not fit,
        // and a third dropped a digit more.
        let raw = max * 100 + 49;
        let value = decimal_from_clickhouse_raw(raw).unwrap();
        assert_eq!(value, Decimal::from_i128_with_scale(max, 6));
        // Half away from zero, on both signs
        let raw = max * 100 + 50;
        assert!(decimal_from_clickhouse_raw(raw).is_ok());
        assert_eq!(
            decimal_from_clickhouse_raw(-(max * 100 + 49)).unwrap(),
            Decimal::from_i128_with_scale(-max, 6)
        );
    }

    #[test]
    fn decimals_round_trip_through_clickhouse() {
        let round_trip = |value: Decimal| {
            decimal_from_clickhouse_raw(decimal_to_clickhouse_raw(&value)).unwrap()
        };
        assert_eq!(
            decimal_to_clickhouse_raw(&Decimal::new(1234, 2)),
            1_234_000_000
        );
        for value in [
            Decimal::new(1234, 2),
            Decimal::new(-5, 8),
            Decimal::from_i128_with_scale(10_i128.pow(21), 0),
            Decimal::MAX,
            Decimal::MIN,
            Decimal::from_i128_with_scale(123_456_789_012_345_678_901_234_567, 8),
        ] {
            assert_eq!(round_trip(value), value, "{value}");
        }
        // More than 8 digits after the point are rounded to 8
        assert_eq!(
            round_trip(Decimal::from_i128_with_scale(1_234_567_896, 9)),
            Decimal::new(123_456_790, 8)
        );

        // An aggregate with more digits than a `Decimal` holds is rounded, as `Decimal` arithmetic
        // rounds: 10^21 + 1/3 at scale 8 is 30 digits, over 96 bits
        let raw = 10_i128.pow(29) + 33_333_333;
        assert_eq!(
            decimal_from_clickhouse_raw(raw).unwrap(),
            Decimal::from_i128_with_scale(10_i128.pow(28) + 3_333_333, 7)
        );
        // An aggregate over the range of a `Decimal`
        assert!(decimal_from_clickhouse_raw(10_i128.pow(37)).is_err());
        assert!(decimal_from_clickhouse_raw(-(10_i128.pow(37))).is_err());
    }

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
