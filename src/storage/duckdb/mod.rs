use crate::datamodel::batch::{Batch, SingleSensorBatch};
use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
use crate::datamodel::sensapp_vec::SensAppLabels;
use crate::datamodel::unit::Unit;
use crate::datamodel::{Metric, Sample, Sensor, SensorData, SensorType, TypedSamples};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use duckdb::Connection;
use duckdb_publishers::*;
use duckdb_utilities::get_sensor_id_or_create_sensor;
use geo::Point;
use regex::Regex;
use rust_decimal::Decimal;
use serde_json::Value as JsonValue;
use smallvec::smallvec;
use std::str::FromStr;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::spawn_blocking;
use uuid::Uuid;

use super::{StorageError, StorageInstance};

mod duckdb_publishers;
mod duckdb_utilities;

#[derive(Debug)]
pub struct DuckDBStorage {
    connection: Arc<Mutex<Connection>>,
}

const INIT_SQL: &str = include_str!("./migrations/20240223133248_init.sql");

impl DuckDBStorage {
    pub async fn connect(connection_string: &str) -> Result<Self> {
        const PREFIX: &str = "duckdb://";

        if !connection_string.starts_with(PREFIX) {
            bail!("Invalid connection string, must start with {}", PREFIX);
        }

        let connection = Connection::open(&connection_string[PREFIX.len()..])
            .context("Failed to open DuckDB connection")?;
        let connection = Arc::new(Mutex::new(connection));
        Ok(Self { connection })
    }
}

#[async_trait]
impl StorageInstance for DuckDBStorage {
    async fn create_or_migrate(&self) -> Result<()> {
        let connection = self.connection.lock().await;
        connection
            .execute_batch(INIT_SQL)
            .context("Failed to initialise database")?;
        Ok(())
    }
    async fn publish(&self, batch: Arc<Batch>) -> Result<()> {
        let connection = Arc::clone(&self.connection);
        let bbatch = batch.clone();
        spawn_blocking(move || -> Result<()> {
            let mut connection = connection.blocking_lock();
            let transaction = connection.transaction()?;
            for single_sensor_batch in bbatch.sensors.as_ref() {
                publish_single_sensor_batch(&transaction, single_sensor_batch)?;
            }
            transaction.commit()?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    async fn vacuum(&self) -> Result<()> {
        let connection = self.connection.lock().await;
        /*let transaction = connection.transaction()?;

        transaction.execute(
            r#"
            DELETE FROM integer_values WHERE rowid NOT IN (
                SELECT MIN(rowid) FROM integer_values GROUP BY sensor_id, timestamp_ms, value
            )
            "#,
            [],
        )?;
        transaction.commit()?;*/

        connection.execute("VACUUM ANALYZE", [])?;
        Ok(())
    }

    async fn list_series(
        &self,
        metric_filter: Option<&str>,
        limit: Option<usize>,
        bookmark: Option<&str>,
    ) -> Result<crate::storage::ListSeriesResult> {
        let connection = Arc::clone(&self.connection);
        let metric_filter = metric_filter.map(str::to_string);
        let bookmark_id = if let Some(bookmark) = bookmark {
            Some(bookmark.parse::<i64>().map_err(|e| {
                anyhow::Error::from(StorageError::invalid_data_format(
                    &format!("Invalid bookmark format: {}", e),
                    None,
                    None,
                ))
            })?)
        } else {
            None
        };
        let effective_limit = limit
            .unwrap_or(crate::storage::DEFAULT_LIST_SERIES_LIMIT)
            .min(crate::storage::MAX_LIST_SERIES_LIMIT);

        spawn_blocking(move || -> Result<crate::storage::ListSeriesResult> {
            let connection = connection.blocking_lock();
            let sql = if metric_filter.is_some() {
                r#"
                SELECT s.sensor_id, CAST(s.uuid AS TEXT) AS uuid, s.name, s.type, u.name, u.description
                FROM sensors s
                LEFT JOIN units u ON s.unit = u.id
                WHERE s.name = ?
                ORDER BY s.sensor_id ASC
                "#
            } else {
                r#"
                SELECT s.sensor_id, CAST(s.uuid AS TEXT) AS uuid, s.name, s.type, u.name, u.description
                FROM sensors s
                LEFT JOIN units u ON s.unit = u.id
                ORDER BY s.sensor_id ASC
                "#
            };

            let mut statement = connection.prepare(sql)?;
            let mut rows = if let Some(filter) = metric_filter.as_deref() {
                statement.query([filter])?
            } else {
                statement.query([])?
            };

            let mut label_statement = connection.prepare(
                r#"
                SELECT lnd.name, ldd.description
                FROM labels l
                JOIN labels_name_dictionary lnd ON l.name = lnd.id
                JOIN labels_description_dictionary ldd ON l.description = ldd.id
                WHERE l.sensor_id = ?
                "#,
            )?;

            let mut sensors = Vec::new();
            let mut last_sensor_id = None;

            while let Some(row) = rows.next()? {
                let sensor_id: i64 = row.get(0)?;
                if bookmark_id.is_some_and(|bookmark| sensor_id <= bookmark) {
                    continue;
                }

                let sensor_uuid: String = row.get(1)?;
                let sensor_name: String = row.get(2)?;
                let sensor_type_str: String = row.get(3)?;
                let unit_name: Option<String> = row.get(4)?;
                let unit_description: Option<String> = row.get(5)?;

                let sensor_uuid = Uuid::parse_str(&sensor_uuid).map_err(|e| {
                    anyhow::Error::from(StorageError::invalid_data_format(
                        &format!("Failed to parse sensor UUID '{}': {}", sensor_uuid, e),
                        None,
                        Some(&sensor_name),
                    ))
                })?;

                let sensor_type = SensorType::from_str(&sensor_type_str).map_err(|e| {
                    anyhow::Error::from(StorageError::invalid_data_format(
                        &format!("Failed to parse sensor type '{}': {}", sensor_type_str, e),
                        Some(sensor_uuid),
                        Some(&sensor_name),
                    ))
                })?;

                let unit = unit_name.map(|unit_name| Unit::new(unit_name, unit_description.clone()));

                let mut label_rows = label_statement.query([sensor_id])?;
                let mut labels: SensAppLabels = smallvec![];
                while let Some(label_row) = label_rows.next()? {
                    let label_name: String = label_row.get(0)?;
                    let label_value: String = label_row.get(1)?;
                    labels.push((label_name, label_value));
                }

                sensors.push(Sensor::new(
                    sensor_uuid,
                    sensor_name,
                    sensor_type,
                    unit,
                    Some(labels),
                ));

                last_sensor_id = Some(sensor_id);

                if sensors.len() >= effective_limit {
                    break;
                }
            }

            let next_bookmark = if sensors.len() == effective_limit {
                last_sensor_id.map(|value| value.to_string())
            } else {
                None
            };

            Ok(crate::storage::ListSeriesResult {
                series: sensors,
                bookmark: next_bookmark,
            })
        })
        .await?
    }

    async fn list_metrics(&self) -> Result<Vec<crate::datamodel::Metric>> {
        let connection = Arc::clone(&self.connection);

        spawn_blocking(move || -> Result<Vec<Metric>> {
            let connection = connection.blocking_lock();
            let mut statement = connection.prepare(
                r#"
                SELECT s.name, s.type, u.name, u.description, COUNT(*)
                FROM sensors s
                LEFT JOIN units u ON s.unit = u.id
                GROUP BY s.name, s.type, u.name, u.description
                ORDER BY s.name ASC
                "#,
            )?;

            let mut rows = statement.query([])?;
            let mut metrics = Vec::new();

            while let Some(row) = rows.next()? {
                let metric_name: String = row.get(0)?;
                let sensor_type_str: String = row.get(1)?;
                let unit_name: Option<String> = row.get(2)?;
                let unit_description: Option<String> = row.get(3)?;
                let series_count: i64 = row.get(4)?;

                let sensor_type = SensorType::from_str(&sensor_type_str).map_err(|e| {
                    anyhow::Error::from(StorageError::invalid_data_format(
                        &format!("Failed to parse sensor type '{}': {}", sensor_type_str, e),
                        None,
                        Some(&metric_name),
                    ))
                })?;

                let unit =
                    unit_name.map(|unit_name| Unit::new(unit_name, unit_description.clone()));

                metrics.push(Metric::new(
                    metric_name,
                    sensor_type,
                    unit,
                    series_count,
                    Vec::new(),
                ));
            }

            Ok(metrics)
        })
        .await?
    }

    async fn query_sensor_data(
        &self,
        sensor_uuid: &str,
        start_time: Option<crate::datamodel::SensAppDateTime>,
        end_time: Option<crate::datamodel::SensAppDateTime>,
        limit: Option<usize>,
    ) -> Result<Option<crate::datamodel::SensorData>> {
        let connection = Arc::clone(&self.connection);
        let sensor_uuid = sensor_uuid.to_string();
        let start_time_ms = start_time.map(|time| time.to_unix_milliseconds().floor() as i64);
        let end_time_ms = end_time.map(|time| time.to_unix_milliseconds().floor() as i64);

        spawn_blocking(move || -> Result<Option<SensorData>> {
            let connection = connection.blocking_lock();

            let mut sensor_statement = connection.prepare(
                r#"
                SELECT s.sensor_id, CAST(s.uuid AS TEXT) AS uuid, s.name, s.type, u.name, u.description
                FROM sensors s
                LEFT JOIN units u ON s.unit = u.id
                WHERE CAST(s.uuid AS TEXT) = ?
                "#,
            )?;

            let mut sensor_rows = sensor_statement.query([sensor_uuid.as_str()])?;
            let Some(sensor_row) = sensor_rows.next()? else {
                return Ok(None);
            };

            let sensor_id: i64 = sensor_row.get(0)?;
            let sensor_uuid_str: String = sensor_row.get(1)?;
            let sensor_name: String = sensor_row.get(2)?;
            let sensor_type_str: String = sensor_row.get(3)?;
            let unit_name: Option<String> = sensor_row.get(4)?;
            let unit_description: Option<String> = sensor_row.get(5)?;

            let sensor_uuid = Uuid::parse_str(&sensor_uuid_str).map_err(|e| {
                anyhow::Error::from(StorageError::invalid_data_format(
                    &format!("Failed to parse sensor UUID '{}': {}", sensor_uuid_str, e),
                    None,
                    Some(&sensor_name),
                ))
            })?;

            let sensor_type = SensorType::from_str(&sensor_type_str).map_err(|e| {
                anyhow::Error::from(StorageError::invalid_data_format(
                    &format!("Failed to parse sensor type '{}': {}", sensor_type_str, e),
                    Some(sensor_uuid),
                    Some(&sensor_name),
                ))
            })?;

            let unit = unit_name.map(|name| Unit::new(name, unit_description.clone()));

            let mut label_statement = connection.prepare(
                r#"
                SELECT lnd.name, ldd.description
                FROM labels l
                JOIN labels_name_dictionary lnd ON l.name = lnd.id
                JOIN labels_description_dictionary ldd ON l.description = ldd.id
                WHERE l.sensor_id = ?
                "#,
            )?;
            let mut label_rows = label_statement.query([sensor_id])?;
            let mut labels: SensAppLabels = smallvec![];
            while let Some(label_row) = label_rows.next()? {
                let label_name: String = label_row.get(0)?;
                let label_value: String = label_row.get(1)?;
                labels.push((label_name, label_value));
            }

            let sensor = Sensor::new(sensor_uuid, sensor_name, sensor_type, unit, Some(labels));

            fn is_in_range(timestamp_ms: i64, start: Option<i64>, end: Option<i64>) -> bool {
                start.is_none_or(|value| timestamp_ms >= value)
                    && end.is_none_or(|value| timestamp_ms <= value)
            }

            let samples = match sensor.sensor_type {
                SensorType::Integer => {
                    let mut statement = connection.prepare(
                        r#"
                        SELECT epoch_ms(timestamp_ms), value
                        FROM integer_values
                        WHERE sensor_id = ?
                        ORDER BY timestamp_ms ASC
                        "#,
                    )?;
                    let mut rows = statement.query([sensor_id])?;
                    let mut samples = smallvec![];
                    while let Some(row) = rows.next()? {
                        let timestamp_ms: i64 = row.get(0)?;
                        if !is_in_range(timestamp_ms, start_time_ms, end_time_ms) {
                            continue;
                        }
                        let value: i64 = row.get(1)?;
                        samples.push(Sample {
                            datetime: crate::datamodel::SensAppDateTime::from_unix_milliseconds_i64(timestamp_ms),
                            value,
                        });
                        if limit.is_some_and(|max| samples.len() >= max) {
                            break;
                        }
                    }
                    TypedSamples::Integer(samples)
                }
                SensorType::Numeric => {
                    let mut statement = connection.prepare(
                        r#"
                        SELECT epoch_ms(timestamp_ms), CAST(value AS VARCHAR)
                        FROM numeric_values
                        WHERE sensor_id = ?
                        ORDER BY timestamp_ms ASC
                        "#,
                    )?;
                    let mut rows = statement.query([sensor_id])?;
                    let mut samples = smallvec![];
                    while let Some(row) = rows.next()? {
                        let timestamp_ms: i64 = row.get(0)?;
                        if !is_in_range(timestamp_ms, start_time_ms, end_time_ms) {
                            continue;
                        }
                        let value: String = row.get(1)?;
                        samples.push(Sample {
                            datetime: crate::datamodel::SensAppDateTime::from_unix_milliseconds_i64(timestamp_ms),
                            value: Decimal::from_str(&value)
                                .context("Failed to parse DuckDB numeric value")?,
                        });
                        if limit.is_some_and(|max| samples.len() >= max) {
                            break;
                        }
                    }
                    TypedSamples::Numeric(samples)
                }
                SensorType::Float => {
                    let mut statement = connection.prepare(
                        r#"
                        SELECT epoch_ms(timestamp_ms), value
                        FROM float_values
                        WHERE sensor_id = ?
                        ORDER BY timestamp_ms ASC
                        "#,
                    )?;
                    let mut rows = statement.query([sensor_id])?;
                    let mut samples = smallvec![];
                    while let Some(row) = rows.next()? {
                        let timestamp_ms: i64 = row.get(0)?;
                        if !is_in_range(timestamp_ms, start_time_ms, end_time_ms) {
                            continue;
                        }
                        let value: f64 = row.get(1)?;
                        samples.push(Sample {
                            datetime: crate::datamodel::SensAppDateTime::from_unix_milliseconds_i64(timestamp_ms),
                            value,
                        });
                        if limit.is_some_and(|max| samples.len() >= max) {
                            break;
                        }
                    }
                    TypedSamples::Float(samples)
                }
                SensorType::String => {
                    let mut statement = connection.prepare(
                        r#"
                        SELECT epoch_ms(sv.timestamp_ms), svd.value
                        FROM string_values sv
                        JOIN strings_values_dictionary svd ON sv.value = svd.id
                        WHERE sv.sensor_id = ?
                        ORDER BY sv.timestamp_ms ASC
                        "#,
                    )?;
                    let mut rows = statement.query([sensor_id])?;
                    let mut samples = smallvec![];
                    while let Some(row) = rows.next()? {
                        let timestamp_ms: i64 = row.get(0)?;
                        if !is_in_range(timestamp_ms, start_time_ms, end_time_ms) {
                            continue;
                        }
                        let value: String = row.get(1)?;
                        samples.push(Sample {
                            datetime: crate::datamodel::SensAppDateTime::from_unix_milliseconds_i64(timestamp_ms),
                            value,
                        });
                        if limit.is_some_and(|max| samples.len() >= max) {
                            break;
                        }
                    }
                    TypedSamples::String(samples)
                }
                SensorType::Boolean => {
                    let mut statement = connection.prepare(
                        r#"
                        SELECT epoch_ms(timestamp_ms), value
                        FROM boolean_values
                        WHERE sensor_id = ?
                        ORDER BY timestamp_ms ASC
                        "#,
                    )?;
                    let mut rows = statement.query([sensor_id])?;
                    let mut samples = smallvec![];
                    while let Some(row) = rows.next()? {
                        let timestamp_ms: i64 = row.get(0)?;
                        if !is_in_range(timestamp_ms, start_time_ms, end_time_ms) {
                            continue;
                        }
                        let value: bool = row.get(1)?;
                        samples.push(Sample {
                            datetime: crate::datamodel::SensAppDateTime::from_unix_milliseconds_i64(timestamp_ms),
                            value,
                        });
                        if limit.is_some_and(|max| samples.len() >= max) {
                            break;
                        }
                    }
                    TypedSamples::Boolean(samples)
                }
                SensorType::Location => {
                    let mut statement = connection.prepare(
                        r#"
                        SELECT epoch_ms(timestamp_ms), latitude, longitude
                        FROM location_values
                        WHERE sensor_id = ?
                        ORDER BY timestamp_ms ASC
                        "#,
                    )?;
                    let mut rows = statement.query([sensor_id])?;
                    let mut samples = smallvec![];
                    while let Some(row) = rows.next()? {
                        let timestamp_ms: i64 = row.get(0)?;
                        if !is_in_range(timestamp_ms, start_time_ms, end_time_ms) {
                            continue;
                        }
                        let latitude: f64 = row.get(1)?;
                        let longitude: f64 = row.get(2)?;
                        samples.push(Sample {
                            datetime: crate::datamodel::SensAppDateTime::from_unix_milliseconds_i64(timestamp_ms),
                            value: Point::new(longitude, latitude),
                        });
                        if limit.is_some_and(|max| samples.len() >= max) {
                            break;
                        }
                    }
                    TypedSamples::Location(samples)
                }
                SensorType::Json => {
                    let mut statement = connection.prepare(
                        r#"
                        SELECT epoch_ms(timestamp_ms), CAST(value AS VARCHAR)
                        FROM json_values
                        WHERE sensor_id = ?
                        ORDER BY timestamp_ms ASC
                        "#,
                    )?;
                    let mut rows = statement.query([sensor_id])?;
                    let mut samples = smallvec![];
                    while let Some(row) = rows.next()? {
                        let timestamp_ms: i64 = row.get(0)?;
                        if !is_in_range(timestamp_ms, start_time_ms, end_time_ms) {
                            continue;
                        }
                        let value: String = row.get(1)?;
                        samples.push(Sample {
                            datetime: crate::datamodel::SensAppDateTime::from_unix_milliseconds_i64(timestamp_ms),
                            value: serde_json::from_str::<JsonValue>(&value)
                                .context("Failed to parse DuckDB JSON value")?,
                        });
                        if limit.is_some_and(|max| samples.len() >= max) {
                            break;
                        }
                    }
                    TypedSamples::Json(samples)
                }
                SensorType::Blob => {
                    let mut statement = connection.prepare(
                        r#"
                        SELECT epoch_ms(timestamp_ms), value
                        FROM blob_values
                        WHERE sensor_id = ?
                        ORDER BY timestamp_ms ASC
                        "#,
                    )?;
                    let mut rows = statement.query([sensor_id])?;
                    let mut samples = smallvec![];
                    while let Some(row) = rows.next()? {
                        let timestamp_ms: i64 = row.get(0)?;
                        if !is_in_range(timestamp_ms, start_time_ms, end_time_ms) {
                            continue;
                        }
                        let value: Vec<u8> = row.get(1)?;
                        samples.push(Sample {
                            datetime: crate::datamodel::SensAppDateTime::from_unix_milliseconds_i64(timestamp_ms),
                            value,
                        });
                        if limit.is_some_and(|max| samples.len() >= max) {
                            break;
                        }
                    }
                    TypedSamples::Blob(samples)
                }
            };

            Ok(Some(SensorData::new(sensor, samples)))
        })
        .await?
    }

    async fn query_sensors_by_labels(
        &self,
        matchers: &[super::LabelMatcher],
        start_time: Option<crate::datamodel::SensAppDateTime>,
        end_time: Option<crate::datamodel::SensAppDateTime>,
        limit: Option<usize>,
        numeric_only: bool,
    ) -> Result<Vec<crate::datamodel::SensorData>> {
        use crate::storage::MatcherType;

        if matchers.is_empty() {
            return Ok(Vec::new());
        }

        fn matches_value(
            actual: Option<&str>,
            expected: &str,
            matcher_type: MatcherType,
        ) -> Result<bool> {
            Ok(match matcher_type {
                MatcherType::Equal => actual == Some(expected),
                MatcherType::NotEqual => actual != Some(expected),
                MatcherType::RegexMatch => {
                    if let Some(value) = actual {
                        Regex::new(expected)?.is_match(value)
                    } else {
                        false
                    }
                }
                MatcherType::RegexNotMatch => {
                    if let Some(value) = actual {
                        !Regex::new(expected)?.is_match(value)
                    } else {
                        true
                    }
                }
            })
        }

        fn sensor_matches(sensor: &Sensor, matchers: &[super::LabelMatcher]) -> Result<bool> {
            for matcher in matchers {
                if matcher.is_name_matcher() {
                    if !matches_value(Some(&sensor.name), &matcher.value, matcher.matcher_type)? {
                        return Ok(false);
                    }
                    continue;
                }

                let actual = sensor
                    .labels
                    .iter()
                    .find(|(name, _)| name == &matcher.name)
                    .map(|(_, value)| value.as_str());

                if !matches_value(actual, &matcher.value, matcher.matcher_type)? {
                    return Ok(false);
                }
            }

            Ok(true)
        }

        fn is_numeric_sensor_type(sensor_type: SensorType) -> bool {
            matches!(
                sensor_type,
                SensorType::Integer | SensorType::Numeric | SensorType::Float
            )
        }

        let mut matching_sensors = Vec::new();
        let mut bookmark = None;

        loop {
            let page = self
                .list_series(
                    None,
                    Some(crate::storage::MAX_LIST_SERIES_LIMIT),
                    bookmark.as_deref(),
                )
                .await?;

            for sensor in page.series {
                if numeric_only && !is_numeric_sensor_type(sensor.sensor_type) {
                    continue;
                }

                if sensor_matches(&sensor, matchers)? {
                    matching_sensors.push(sensor);
                }
            }

            if page.bookmark.is_none() {
                break;
            }

            bookmark = page.bookmark;
        }

        let mut results = Vec::new();
        for sensor in matching_sensors {
            if let Some(sensor_data) = self
                .query_sensor_data(&sensor.uuid.to_string(), start_time, end_time, limit)
                .await?
            {
                results.push(sensor_data);
            }
        }

        Ok(results)
    }

    /// Health check for DuckDB storage
    /// Executes a simple SELECT 1 query to verify database connectivity
    async fn health_check(&self) -> Result<()> {
        let connection = self.connection.lock().await;
        connection
            .execute("SELECT 1", [])
            .context("DuckDB health check failed")?;
        Ok(())
    }

    /// Clean up all test data from the database (DuckDB implementation)
    #[cfg(any(test, feature = "test-utils"))]
    async fn cleanup_test_data(&self) -> Result<()> {
        // DuckDB implementation - delete all data from tables
        let connection = self.connection.lock().await;

        // Delete all value tables
        connection.execute("DELETE FROM blob_values", []).ok();
        connection.execute("DELETE FROM json_values", []).ok();
        connection.execute("DELETE FROM location_values", []).ok();
        connection.execute("DELETE FROM boolean_values", []).ok();
        connection.execute("DELETE FROM string_values", []).ok();
        connection.execute("DELETE FROM float_values", []).ok();
        connection.execute("DELETE FROM numeric_values", []).ok();
        connection.execute("DELETE FROM integer_values", []).ok();

        // Delete metadata tables
        connection.execute("DELETE FROM labels", []).ok();
        connection.execute("DELETE FROM sensors", []).ok();
        connection
            .execute("DELETE FROM strings_values_dictionary", [])
            .ok();
        connection
            .execute("DELETE FROM labels_description_dictionary", [])
            .ok();
        connection
            .execute("DELETE FROM labels_name_dictionary", [])
            .ok();
        connection.execute("DELETE FROM units", []).ok();

        // Step 2: Clear all cached function caches
        // The cached macro generates cache variables named after the function in uppercase
        use cached::Cached;
        duckdb_utilities::GET_LABEL_NAME_ID_OR_CREATE
            .lock()
            .expect("duckdb label-name cache poisoned")
            .cache_clear();
        duckdb_utilities::GET_LABEL_DESCRIPTION_ID_OR_CREATE
            .lock()
            .expect("duckdb label-description cache poisoned")
            .cache_clear();
        duckdb_utilities::GET_UNIT_ID_OR_CREATE
            .lock()
            .expect("duckdb unit cache poisoned")
            .cache_clear();
        duckdb_utilities::GET_SENSOR_ID_OR_CREATE_SENSOR
            .lock()
            .expect("duckdb sensor cache poisoned")
            .cache_clear();
        duckdb_utilities::GET_STRING_VALUE_ID_OR_CREATE
            .lock()
            .expect("duckdb string-value cache poisoned")
            .cache_clear();

        Ok(())
    }
}

fn publish_single_sensor_batch(
    transaction: &duckdb::Transaction,
    single_sensor_batch: &SingleSensorBatch,
) -> Result<()> {
    let sensor_id = get_sensor_id_or_create_sensor(transaction, &single_sensor_batch.sensor)?;
    {
        let samples_guard = single_sensor_batch.samples.blocking_read();
        match &*samples_guard {
            TypedSamples::Integer(samples) => {
                publish_integer_values(transaction, sensor_id, samples)?;
            }
            TypedSamples::Numeric(samples) => {
                publish_numeric_values(transaction, sensor_id, samples)?;
            }
            TypedSamples::Float(samples) => {
                publish_float_values(transaction, sensor_id, samples)?;
            }
            TypedSamples::String(samples) => {
                publish_string_values(transaction, sensor_id, samples)?;
            }
            TypedSamples::Boolean(samples) => {
                publish_boolean_values(transaction, sensor_id, samples)?;
            }
            TypedSamples::Location(samples) => {
                publish_location_values(transaction, sensor_id, samples)?;
            }
            TypedSamples::Blob(samples) => {
                publish_blob_values(transaction, sensor_id, samples)?;
            }
            TypedSamples::Json(samples) => {
                publish_json_values(transaction, sensor_id, samples)?;
            }
        }
    }
    Ok(())
}
