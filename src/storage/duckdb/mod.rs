use crate::datamodel::batch::Batch;
use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
use crate::datamodel::sensapp_vec::SensAppLabels;
use crate::datamodel::unit::Unit;
use crate::datamodel::{
    Metric, Sample, SensAppDateTime, Sensor, SensorData, SensorType, TypedSamples,
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use duckdb::{Connection, OptionalExt};
use duckdb_publishers::publish_batch;
use geo::Point;
use rust_decimal::Decimal;
use serde_json::Value as JsonValue;
use smallvec::smallvec;
use std::str::FromStr;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use tokio::sync::Mutex;
use tokio::task::spawn_blocking;
use uuid::Uuid;

use super::{
    Aggregation, SensorAvailabilitySummary, SensorDataQueryOptions, StorageError, StorageInstance,
    common::VALUE_TABLES,
};

mod duckdb_publishers;
mod duckdb_registration;
mod selector;

#[derive(Debug)]
pub struct DuckDBStorage {
    connection: Arc<Mutex<Connection>>,
    /// Drop the samples that are stored already when writing, see `duckdb_publishers`
    deduplicate_on_ingest: Arc<AtomicBool>,
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
        Ok(Self {
            connection,
            deduplicate_on_ingest: Arc::new(AtomicBool::new(false)),
        })
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
        let deduplicate = self.deduplicate_on_ingest.load(Ordering::Relaxed);
        spawn_blocking(move || -> Result<()> {
            let mut connection = connection.blocking_lock();
            let transaction = connection.transaction()?;
            publish_batch(&transaction, bbatch.sensors.as_ref(), deduplicate)?;
            transaction.commit()?;
            Ok(())
        })
        .await??;
        Ok(())
    }

    async fn vacuum(&self) -> Result<()> {
        let connection = self.connection.lock().await;
        connection.execute("VACUUM ANALYZE", [])?;
        Ok(())
    }

    async fn set_deduplicate_on_ingest(&self, enabled: bool) -> Result<()> {
        self.deduplicate_on_ingest.store(enabled, Ordering::Relaxed);
        Ok(())
    }

    async fn deduplicate_samples(&self) -> Result<u64> {
        let connection = Arc::clone(&self.connection);
        spawn_blocking(move || -> Result<u64> {
            let connection = connection.blocking_lock();
            // One statement per table. The first row written (the smallest rowid) of each group of
            // equal samples is kept. The table names and the columns come from static lists.
            let mut removed = 0;
            for table in VALUE_TABLES {
                let columns = crate::storage::common::duplicate_key_columns(table, "timestamp_us");
                let sql = format!(
                    "DELETE FROM {table} WHERE rowid NOT IN \
                       (SELECT MIN(rowid) FROM {table} GROUP BY {columns})"
                );
                removed += connection
                    .execute(&sql, [])
                    .with_context(|| format!("Failed to remove the duplicate samples of {table}"))?
                    as u64;
            }
            Ok(removed)
        })
        .await?
    }

    async fn delete_series(&self, sensor_uuid: &str) -> Result<bool> {
        let connection = Arc::clone(&self.connection);
        let parsed_uuid = Uuid::from_str(sensor_uuid).context("Failed to parse sensor UUID")?;
        let sensor_uuid = parsed_uuid.to_string();

        spawn_blocking(move || -> Result<bool> {
            let mut connection = connection.blocking_lock();

            let sensor_id: Option<i64> = connection
                .prepare("SELECT sensor_id FROM sensors WHERE CAST(uuid AS TEXT) = ?")?
                .query_row([sensor_uuid.as_str()], |row| row.get(0))
                .optional()?;
            let Some(sensor_id) = sensor_id else {
                return Ok(false);
            };

            // DuckDB checks foreign keys against committed data, so the labels must be
            // gone in a committed transaction before the sensor row can be deleted.
            let transaction = connection.transaction()?;
            for table in VALUE_TABLES {
                transaction.execute(
                    &format!("DELETE FROM {table} WHERE sensor_id = ?"),
                    [sensor_id],
                )?;
            }
            transaction.execute("DELETE FROM labels WHERE sensor_id = ?", [sensor_id])?;
            transaction.commit()?;

            connection.execute("DELETE FROM sensors WHERE sensor_id = ?", [sensor_id])?;
            Ok(true)
        })
        .await?
    }

    async fn delete_series_samples(
        &self,
        sensor_uuid: &str,
        start_time: SensAppDateTime,
        end_time: SensAppDateTime,
    ) -> Result<Option<u64>> {
        let connection = Arc::clone(&self.connection);
        let sensor_uuid = Uuid::from_str(sensor_uuid)
            .context("Failed to parse sensor UUID")?
            .to_string();
        // DuckDB stores microsecond timestamps
        let start_time_us = start_time.to_unix(hifitime::Unit::Microsecond).floor() as i64;
        let end_time_us = end_time.to_unix(hifitime::Unit::Microsecond).floor() as i64;

        spawn_blocking(move || -> Result<Option<u64>> {
            let connection = connection.blocking_lock();

            let sensor: Option<(i64, String)> = connection
                .prepare("SELECT sensor_id, type FROM sensors WHERE CAST(uuid AS TEXT) = ?")?
                .query_row([sensor_uuid.as_str()], |row| Ok((row.get(0)?, row.get(1)?)))
                .optional()?;
            let Some((sensor_id, sensor_type_str)) = sensor else {
                return Ok(None);
            };
            let sensor_type = SensorType::from_str(&sensor_type_str).map_err(|e| {
                anyhow::Error::from(StorageError::invalid_data_format(
                    &format!("Failed to parse sensor type '{}': {}", sensor_type_str, e),
                    None,
                    None,
                ))
            })?;

            let table = match sensor_type {
                SensorType::Integer => "integer_values",
                SensorType::Numeric => "numeric_values",
                SensorType::Float => "float_values",
                SensorType::String => "string_values",
                SensorType::Boolean => "boolean_values",
                SensorType::Location => "location_values",
                SensorType::Json => "json_values",
                SensorType::Blob => "blob_values",
            };
            let deleted = connection.execute(
                &format!(
                    "DELETE FROM {table} WHERE sensor_id = ? AND epoch_us(timestamp_us) BETWEEN ? AND ?"
                ),
                duckdb::params![sensor_id, start_time_us, end_time_us],
            )?;
            Ok(Some(deleted as u64))
        })
        .await?
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
        let fetch_limit = effective_limit.saturating_add(1) as i64;

        spawn_blocking(move || -> Result<crate::storage::ListSeriesResult> {
            let connection = connection.blocking_lock();
            let (sql, use_filter, use_bookmark) = match (metric_filter.is_some(), bookmark_id.is_some()) {
                (true, true) => (
                    r#"
                    SELECT s.sensor_id, CAST(s.uuid AS TEXT) AS uuid, s.name, s.type, u.name, u.description
                    FROM sensors s
                    LEFT JOIN units u ON s.unit = u.id
                    WHERE s.name = ? AND s.sensor_id > ?
                    ORDER BY s.sensor_id ASC
                    LIMIT ?
                    "#,
                    true,
                    true,
                ),
                (true, false) => (
                    r#"
                    SELECT s.sensor_id, CAST(s.uuid AS TEXT) AS uuid, s.name, s.type, u.name, u.description
                    FROM sensors s
                    LEFT JOIN units u ON s.unit = u.id
                    WHERE s.name = ?
                    ORDER BY s.sensor_id ASC
                    LIMIT ?
                    "#,
                    true,
                    false,
                ),
                (false, true) => (
                    r#"
                    SELECT s.sensor_id, CAST(s.uuid AS TEXT) AS uuid, s.name, s.type, u.name, u.description
                    FROM sensors s
                    LEFT JOIN units u ON s.unit = u.id
                    WHERE s.sensor_id > ?
                    ORDER BY s.sensor_id ASC
                    LIMIT ?
                    "#,
                    false,
                    true,
                ),
                (false, false) => (
                    r#"
                    SELECT s.sensor_id, CAST(s.uuid AS TEXT) AS uuid, s.name, s.type, u.name, u.description
                    FROM sensors s
                    LEFT JOIN units u ON s.unit = u.id
                    ORDER BY s.sensor_id ASC
                    LIMIT ?
                    "#,
                    false,
                    false,
                ),
            };

            let mut statement = connection.prepare(sql)?;
            let mut rows = match (use_filter, use_bookmark) {
                (true, true) => statement.query(duckdb::params![
                    metric_filter.as_deref().unwrap(),
                    bookmark_id.unwrap(),
                    fetch_limit,
                ])?,
                (true, false) => statement.query(duckdb::params![
                    metric_filter.as_deref().unwrap(),
                    fetch_limit,
                ])?,
                (false, true) => {
                    statement.query(duckdb::params![bookmark_id.unwrap(), fetch_limit])?
                }
                (false, false) => statement.query(duckdb::params![fetch_limit])?,
            };

            let mut found = Vec::new();
            let mut has_more = false;

            while let Some(row) = rows.next()? {
                if found.len() == effective_limit {
                    has_more = true;
                    break;
                }

                let sensor_id: i64 = row.get(0)?;

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
                found.push((sensor_id, sensor_uuid, sensor_name, sensor_type, unit));
            }
            drop(rows);
            drop(statement);

            // One query for the labels of the whole page
            let ids: Vec<i64> = found.iter().map(|sensor| sensor.0).collect();
            let mut labels = selector::labels_of(&connection, &ids)?;
            let last_sensor_id = ids.last().copied();
            let sensors: Vec<Sensor> = found
                .into_iter()
                .map(|(sensor_id, uuid, name, sensor_type, unit)| {
                    let labels = labels.remove(&sensor_id).unwrap_or_else(|| smallvec![]);
                    Sensor::new(uuid, name, sensor_type, unit, Some(labels))
                })
                .collect();

            let next_bookmark = if has_more {
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
        let start_time_us =
            start_time.map(|time| time.to_unix(hifitime::Unit::Microsecond).floor() as i64);
        let end_time_us =
            end_time.map(|time| time.to_unix(hifitime::Unit::Microsecond).floor() as i64);

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

            // The window and the limit are applied by the query: a narrow window of a long series
            // does not read the whole series
            let limit_value = limit.map_or(i64::MAX, |limit| i64::try_from(limit).unwrap_or(i64::MAX));

            let samples = match sensor.sensor_type {
                SensorType::Integer => {
                    let mut statement = connection.prepare(
                        r#"
                        SELECT epoch_us(timestamp_us), value
                        FROM integer_values
                        WHERE sensor_id = ?1
                        AND (?2 IS NULL OR timestamp_us >= make_timestamp(?2))
                        AND (?3 IS NULL OR timestamp_us <= make_timestamp(?3))
                        ORDER BY timestamp_us ASC
                        LIMIT ?4
                        "#,
                    )?;
                    let mut rows = statement.query(duckdb::params![sensor_id, start_time_us, end_time_us, limit_value])?;
                    let mut samples = smallvec![];
                    while let Some(row) = rows.next()? {
                        let timestamp_us: i64 = row.get(0)?;
                        let value: i64 = row.get(1)?;
                        samples.push(Sample {
                            datetime: crate::datamodel::SensAppDateTime::from_unix_microseconds_i64(timestamp_us),
                            value,
                        });
                    }
                    TypedSamples::Integer(samples)
                }
                SensorType::Numeric => {
                    let mut statement = connection.prepare(
                        r#"
                        SELECT epoch_us(timestamp_us), CAST(value AS VARCHAR)
                        FROM numeric_values
                        WHERE sensor_id = ?1
                        AND (?2 IS NULL OR timestamp_us >= make_timestamp(?2))
                        AND (?3 IS NULL OR timestamp_us <= make_timestamp(?3))
                        ORDER BY timestamp_us ASC
                        LIMIT ?4
                        "#,
                    )?;
                    let mut rows = statement.query(duckdb::params![sensor_id, start_time_us, end_time_us, limit_value])?;
                    let mut samples = smallvec![];
                    while let Some(row) = rows.next()? {
                        let timestamp_us: i64 = row.get(0)?;
                        let value: String = row.get(1)?;
                        samples.push(Sample {
                            datetime: crate::datamodel::SensAppDateTime::from_unix_microseconds_i64(timestamp_us),
                            value: Decimal::from_str(&value)
                                .context("Failed to parse DuckDB numeric value")?,
                        });
                    }
                    TypedSamples::Numeric(samples)
                }
                SensorType::Float => {
                    let mut statement = connection.prepare(
                        r#"
                        SELECT epoch_us(timestamp_us), value
                        FROM float_values
                        WHERE sensor_id = ?1
                        AND (?2 IS NULL OR timestamp_us >= make_timestamp(?2))
                        AND (?3 IS NULL OR timestamp_us <= make_timestamp(?3))
                        ORDER BY timestamp_us ASC
                        LIMIT ?4
                        "#,
                    )?;
                    let mut rows = statement.query(duckdb::params![sensor_id, start_time_us, end_time_us, limit_value])?;
                    let mut samples = smallvec![];
                    while let Some(row) = rows.next()? {
                        let timestamp_us: i64 = row.get(0)?;
                        let value: f64 = row.get(1)?;
                        samples.push(Sample {
                            datetime: crate::datamodel::SensAppDateTime::from_unix_microseconds_i64(timestamp_us),
                            value,
                        });
                    }
                    TypedSamples::Float(samples)
                }
                SensorType::String => {
                    let mut statement = connection.prepare(
                        r#"
                        SELECT epoch_us(sv.timestamp_us), svd.value
                        FROM string_values sv
                        JOIN strings_values_dictionary svd ON sv.value = svd.id
                        WHERE sv.sensor_id = ?1
                        AND (?2 IS NULL OR sv.timestamp_us >= make_timestamp(?2))
                        AND (?3 IS NULL OR sv.timestamp_us <= make_timestamp(?3))
                        ORDER BY sv.timestamp_us ASC
                        LIMIT ?4
                        "#,
                    )?;
                    let mut rows = statement.query(duckdb::params![
                        sensor_id,
                        start_time_us,
                        end_time_us,
                        limit_value
                    ])?;
                    let mut samples = smallvec![];
                    while let Some(row) = rows.next()? {
                        let timestamp_us: i64 = row.get(0)?;
                        let value: String = row.get(1)?;
                        samples.push(Sample {
                            datetime: crate::datamodel::SensAppDateTime::from_unix_microseconds_i64(timestamp_us),
                            value,
                        });
                    }
                    TypedSamples::String(samples)
                }
                SensorType::Boolean => {
                    let mut statement = connection.prepare(
                        r#"
                        SELECT epoch_us(timestamp_us), value
                        FROM boolean_values
                        WHERE sensor_id = ?1
                        AND (?2 IS NULL OR timestamp_us >= make_timestamp(?2))
                        AND (?3 IS NULL OR timestamp_us <= make_timestamp(?3))
                        ORDER BY timestamp_us ASC
                        LIMIT ?4
                        "#,
                    )?;
                    let mut rows = statement.query(duckdb::params![sensor_id, start_time_us, end_time_us, limit_value])?;
                    let mut samples = smallvec![];
                    while let Some(row) = rows.next()? {
                        let timestamp_us: i64 = row.get(0)?;
                        let value: bool = row.get(1)?;
                        samples.push(Sample {
                            datetime: crate::datamodel::SensAppDateTime::from_unix_microseconds_i64(timestamp_us),
                            value,
                        });
                    }
                    TypedSamples::Boolean(samples)
                }
                SensorType::Location => {
                    let mut statement = connection.prepare(
                        r#"
                        SELECT epoch_us(timestamp_us), latitude, longitude
                        FROM location_values
                        WHERE sensor_id = ?1
                        AND (?2 IS NULL OR timestamp_us >= make_timestamp(?2))
                        AND (?3 IS NULL OR timestamp_us <= make_timestamp(?3))
                        ORDER BY timestamp_us ASC
                        LIMIT ?4
                        "#,
                    )?;
                    let mut rows = statement.query(duckdb::params![sensor_id, start_time_us, end_time_us, limit_value])?;
                    let mut samples = smallvec![];
                    while let Some(row) = rows.next()? {
                        let timestamp_us: i64 = row.get(0)?;
                        let latitude: f64 = row.get(1)?;
                        let longitude: f64 = row.get(2)?;
                        samples.push(Sample {
                            datetime: crate::datamodel::SensAppDateTime::from_unix_microseconds_i64(timestamp_us),
                            value: Point::new(longitude, latitude),
                        });
                    }
                    TypedSamples::Location(samples)
                }
                SensorType::Json => {
                    let mut statement = connection.prepare(
                        r#"
                        SELECT epoch_us(timestamp_us), CAST(value AS VARCHAR)
                        FROM json_values
                        WHERE sensor_id = ?1
                        AND (?2 IS NULL OR timestamp_us >= make_timestamp(?2))
                        AND (?3 IS NULL OR timestamp_us <= make_timestamp(?3))
                        ORDER BY timestamp_us ASC
                        LIMIT ?4
                        "#,
                    )?;
                    let mut rows = statement.query(duckdb::params![sensor_id, start_time_us, end_time_us, limit_value])?;
                    let mut samples = smallvec![];
                    while let Some(row) = rows.next()? {
                        let timestamp_us: i64 = row.get(0)?;
                        let value: String = row.get(1)?;
                        samples.push(Sample {
                            datetime: crate::datamodel::SensAppDateTime::from_unix_microseconds_i64(timestamp_us),
                            value: serde_json::from_str::<JsonValue>(&value)
                                .context("Failed to parse DuckDB JSON value")?,
                        });
                    }
                    TypedSamples::Json(samples)
                }
                SensorType::Blob => {
                    let mut statement = connection.prepare(
                        r#"
                        SELECT epoch_us(timestamp_us), value
                        FROM blob_values
                        WHERE sensor_id = ?1
                        AND (?2 IS NULL OR timestamp_us >= make_timestamp(?2))
                        AND (?3 IS NULL OR timestamp_us <= make_timestamp(?3))
                        ORDER BY timestamp_us ASC
                        LIMIT ?4
                        "#,
                    )?;
                    let mut rows = statement.query(duckdb::params![sensor_id, start_time_us, end_time_us, limit_value])?;
                    let mut samples = smallvec![];
                    while let Some(row) = rows.next()? {
                        let timestamp_us: i64 = row.get(0)?;
                        let value: Vec<u8> = row.get(1)?;
                        samples.push(Sample {
                            datetime: crate::datamodel::SensAppDateTime::from_unix_microseconds_i64(timestamp_us),
                            value,
                        });
                    }
                    TypedSamples::Blob(samples)
                }
            };

            Ok(Some(SensorData::new(sensor, samples)))
        })
        .await?
    }

    async fn query_sensor_data_advanced(
        &self,
        sensor_uuid: &str,
        options: &SensorDataQueryOptions,
    ) -> Result<Option<SensorData>> {
        options.validate()?;

        if let (Some(step_ms), Some(aggregation)) = (options.step_ms, options.aggregation) {
            let connection = Arc::clone(&self.connection);
            let sensor_uuid = sensor_uuid.to_string();
            let options = options.clone();

            let aggregated = spawn_blocking(move || -> Result<Option<SensorData>> {
                let connection = connection.blocking_lock();

                let Some((sensor_id, mut sensor)) =
                    duckdb_get_sensor_metadata(&connection, &sensor_uuid)?
                else {
                    return Ok(None);
                };

                let start_time_us = options
                    .start_time
                    .map(|time| time.to_unix(hifitime::Unit::Microsecond).floor() as i64);
                let end_time_us = options
                    .end_time
                    .map(|time| time.to_unix(hifitime::Unit::Microsecond).floor() as i64);
                let origin_us = start_time_us.unwrap_or(0);

                let samples = match sensor.sensor_type {
                    SensorType::Integer => duckdb_query_integer_samples_aggregated(
                        &connection,
                        sensor_id,
                        start_time_us,
                        end_time_us,
                        step_ms,
                        origin_us,
                        aggregation,
                        options.limit,
                    )?,
                    SensorType::Numeric => duckdb_query_numeric_samples_aggregated(
                        &connection,
                        sensor_id,
                        start_time_us,
                        end_time_us,
                        step_ms,
                        origin_us,
                        aggregation,
                        options.limit,
                    )?,
                    SensorType::Float => duckdb_query_float_samples_aggregated(
                        &connection,
                        sensor_id,
                        start_time_us,
                        end_time_us,
                        step_ms,
                        origin_us,
                        aggregation,
                        options.limit,
                    )?,
                    _ => return Ok(None),
                };

                sensor.sensor_type = match &samples {
                    TypedSamples::Integer(_) => SensorType::Integer,
                    TypedSamples::Numeric(_) => SensorType::Numeric,
                    TypedSamples::Float(_) => SensorType::Float,
                    _ => sensor.sensor_type,
                };

                if aggregation.output_is_count() {
                    sensor.unit = None;
                }

                Ok(Some(SensorData::new(sensor, samples)))
            })
            .await??;

            if let Some(sensor_data) = aggregated {
                let post_query_options = SensorDataQueryOptions {
                    start_time: options.start_time,
                    end_time: options.end_time,
                    limit: options.limit,
                    step_ms: None,
                    aggregation: None,
                    simplify: options.simplify,
                };

                return crate::storage::common::apply_query_options(
                    sensor_data,
                    &post_query_options,
                )
                .map(Some);
            }
        }

        let raw = self
            .query_sensor_data(
                sensor_uuid,
                options.start_time,
                options.end_time,
                options.limit,
            )
            .await?;

        raw.map(|sensor_data| crate::storage::common::apply_query_options(sensor_data, options))
            .transpose()
    }

    async fn query_sensor_data_latest(
        &self,
        sensor_uuid: &str,
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
    ) -> Result<Option<SensorData>> {
        let connection = Arc::clone(&self.connection);
        let sensor_uuid_owned = sensor_uuid.to_string();
        let start_time_us =
            start_time.map(|time| time.to_unix(hifitime::Unit::Microsecond).floor() as i64);
        let end_time_us =
            end_time.map(|time| time.to_unix(hifitime::Unit::Microsecond).floor() as i64);

        let latest_timestamp_us = spawn_blocking(move || -> Result<Option<i64>> {
            let connection = connection.blocking_lock();
            let Some((sensor_id, sensor)) =
                duckdb_get_sensor_metadata(&connection, &sensor_uuid_owned)?
            else {
                return Ok(None);
            };

            duckdb_query_latest_timestamp_us(
                &connection,
                duckdb_sensor_table_name(sensor.sensor_type),
                sensor_id,
                start_time_us,
                end_time_us,
            )
        })
        .await??;

        let Some(latest_timestamp_us) = latest_timestamp_us else {
            return Ok(None);
        };

        self.query_sensor_data(
            sensor_uuid,
            Some(SensAppDateTime::from_unix_microseconds_i64(
                latest_timestamp_us,
            )),
            Some(SensAppDateTime::from_unix_microseconds_i64(
                latest_timestamp_us,
            )),
            Some(1),
        )
        .await
    }

    async fn query_sensor_data_availability(
        &self,
        sensor_uuid: &str,
        start_time: SensAppDateTime,
        end_time: SensAppDateTime,
        step_ms: Option<i64>,
    ) -> Result<Option<SensorAvailabilitySummary>> {
        let connection = Arc::clone(&self.connection);
        let sensor_uuid_owned = sensor_uuid.to_string();
        let start_time_us = start_time.to_unix(hifitime::Unit::Microsecond).floor() as i64;
        let end_time_us = end_time.to_unix(hifitime::Unit::Microsecond).floor() as i64;

        spawn_blocking(move || -> Result<Option<SensorAvailabilitySummary>> {
            let connection = connection.blocking_lock();
            let Some((sensor_id, sensor)) =
                duckdb_get_sensor_metadata(&connection, &sensor_uuid_owned)?
            else {
                return Ok(None);
            };

            let summary = duckdb_query_availability_summary(
                &connection,
                duckdb_sensor_table_name(sensor.sensor_type),
                sensor_id,
                sensor,
                start_time_us,
                end_time_us,
                step_ms,
            )?;

            Ok(Some(summary))
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
        if matchers.is_empty() {
            return Ok(Vec::new());
        }

        let matching_sensors: Vec<Sensor> =
            crate::storage::selector::BulkSelectorBackend::find_selector_sensors(
                self,
                matchers,
                numeric_only,
                None,
            )
            .await?
            .into_iter()
            .map(|(_, sensor)| sensor)
            .collect();

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
    async fn query_selector(
        &self,
        matchers: &[super::LabelMatcher],
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
        numeric_only: bool,
        max_series: usize,
        max_samples: usize,
    ) -> Result<super::SelectorRead> {
        super::selector::read_selector_in_bulk(
            self,
            matchers,
            start_time,
            end_time,
            numeric_only,
            max_series,
            max_samples,
        )
        .await
    }

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

        Ok(())
    }
}

fn duckdb_get_sensor_metadata(
    connection: &Connection,
    sensor_uuid: &str,
) -> Result<Option<(i64, Sensor)>> {
    let mut sensor_statement = connection.prepare(
        r#"
        SELECT s.sensor_id, CAST(s.uuid AS TEXT) AS uuid, s.name, s.type, u.name, u.description
        FROM sensors s
        LEFT JOIN units u ON s.unit = u.id
        WHERE CAST(s.uuid AS TEXT) = ?
        "#,
    )?;

    let mut sensor_rows = sensor_statement.query([sensor_uuid])?;
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

    Ok(Some((
        sensor_id,
        Sensor::new(sensor_uuid, sensor_name, sensor_type, unit, Some(labels)),
    )))
}

fn duckdb_sensor_table_name(sensor_type: SensorType) -> &'static str {
    match sensor_type {
        SensorType::Integer => "integer_values",
        SensorType::Numeric => "numeric_values",
        SensorType::Float => "float_values",
        SensorType::String => "string_values",
        SensorType::Boolean => "boolean_values",
        SensorType::Location => "location_values",
        SensorType::Json => "json_values",
        SensorType::Blob => "blob_values",
    }
}

fn duckdb_query_latest_timestamp_us(
    connection: &Connection,
    table_name: &str,
    sensor_id: i64,
    start_time_us: Option<i64>,
    end_time_us: Option<i64>,
) -> Result<Option<i64>> {
    let sql = format!(
        r#"
        SELECT epoch_us(MAX(timestamp_us))
        FROM {table_name}
        WHERE sensor_id = ?1
          AND (?2 IS NULL OR timestamp_us >= make_timestamp(?2))
          AND (?3 IS NULL OR timestamp_us <= make_timestamp(?3))
        "#
    );

    let mut statement = connection.prepare(&sql)?;
    let value: Option<i64> = statement.query_row(
        duckdb::params![sensor_id, start_time_us, end_time_us],
        |row| row.get(0),
    )?;

    Ok(value)
}

fn duckdb_query_availability_summary(
    connection: &Connection,
    table_name: &str,
    sensor_id: i64,
    sensor: Sensor,
    start_time_us: i64,
    end_time_us: i64,
    step_ms: Option<i64>,
) -> Result<SensorAvailabilitySummary> {
    let (sql, params): (String, Vec<i64>) = if let Some(step_ms) = step_ms {
        (
            format!(
                r#"
                SELECT
                    COUNT(*) AS sample_count,
                    epoch_us(MIN(timestamp_us)) AS first_sample_at,
                    epoch_us(MAX(timestamp_us)) AS last_sample_at,
                                        COUNT(DISTINCT time_bucket(INTERVAL '{step_ms} milliseconds', timestamp_us, make_timestamp(?2))) AS covered_buckets
                FROM {table_name}
                WHERE sensor_id = ?1
                  AND timestamp_us >= make_timestamp(?2)
                                    AND timestamp_us <= make_timestamp(?3)
                "#
            ),
            vec![sensor_id, start_time_us, end_time_us],
        )
    } else {
        (
            format!(
                r#"
                SELECT
                    COUNT(*) AS sample_count,
                    epoch_us(MIN(timestamp_us)) AS first_sample_at,
                    epoch_us(MAX(timestamp_us)) AS last_sample_at,
                    NULL AS covered_buckets
                FROM {table_name}
                WHERE sensor_id = ?1
                  AND timestamp_us >= make_timestamp(?2)
                  AND timestamp_us <= make_timestamp(?3)
                "#
            ),
            vec![sensor_id, start_time_us, end_time_us],
        )
    };

    let mut statement = connection.prepare(&sql)?;
    let row = statement.query_row(duckdb::params_from_iter(params.iter()), |row| {
        Ok((
            row.get::<_, i64>(0)?,
            row.get::<_, Option<i64>>(1)?,
            row.get::<_, Option<i64>>(2)?,
            row.get::<_, Option<i64>>(3)?,
        ))
    })?;

    Ok(SensorAvailabilitySummary {
        sensor,
        sample_count: row.0.max(0) as usize,
        first_sample_at: row.1.map(SensAppDateTime::from_unix_microseconds_i64),
        last_sample_at: row.2.map(SensAppDateTime::from_unix_microseconds_i64),
        covered_buckets: row.3.map(|value| value.max(0) as usize),
    })
}

fn duckdb_bucketed_cte(table_name: &str, step_ms: i64) -> String {
    format!(
        r#"
        WITH bucketed AS (
            SELECT
                time_bucket(INTERVAL '{step_ms} milliseconds', timestamp_us, make_timestamp(?4)) AS bucket_ts,
                timestamp_us,
                value
            FROM {table_name}
            WHERE sensor_id = ?1
              AND (?2 IS NULL OR timestamp_us >= make_timestamp(?2))
              AND (?3 IS NULL OR timestamp_us <= make_timestamp(?3))
        )
        "#
    )
}

fn duckdb_group_by_clause() -> &'static str {
    "FROM bucketed GROUP BY 1 ORDER BY 1 ASC LIMIT ?5"
}

fn duckdb_first_last_query(
    table_name: &str,
    step_ms: i64,
    aggregation: Aggregation,
    value_expression: &str,
) -> String {
    let (direction, stamp) = match aggregation {
        Aggregation::First => ("ASC", "bucket_ts"),
        Aggregation::Last => ("DESC", "bucket_ts"),
        // The timestamp of the sample, not the start of the bucket
        Aggregation::Latest => ("DESC", "sample_ts"),
        _ => unreachable!("only first/last/latest use duckdb_first_last_query"),
    };

    format!(
        r#"
        WITH bucketed AS (
            SELECT
                time_bucket(INTERVAL '{step_ms} milliseconds', timestamp_us, make_timestamp(?4)) AS bucket_ts,
                timestamp_us,
                {value_expression} AS value
            FROM {table_name}
            WHERE sensor_id = ?1
              AND (?2 IS NULL OR timestamp_us >= make_timestamp(?2))
              AND (?3 IS NULL OR timestamp_us <= make_timestamp(?3))
        ),
        ranked AS (
            SELECT
                bucket_ts,
                timestamp_us AS sample_ts,
                value,
                ROW_NUMBER() OVER (PARTITION BY bucket_ts ORDER BY timestamp_us {direction}) AS row_num
            FROM bucketed
        )
        SELECT epoch_us({stamp}) AS timestamp_us, value
        FROM ranked
        WHERE row_num = 1
        ORDER BY bucket_ts ASC
        LIMIT ?5
        "#
    )
}

#[allow(clippy::too_many_arguments)]
fn duckdb_query_integer_samples_aggregated(
    connection: &Connection,
    sensor_id: i64,
    start_time_us: Option<i64>,
    end_time_us: Option<i64>,
    step_ms: i64,
    origin_us: i64,
    aggregation: Aggregation,
    limit: Option<usize>,
) -> Result<TypedSamples> {
    let limit = limit.unwrap_or(super::DEFAULT_QUERY_LIMIT) as i64;

    match aggregation {
        Aggregation::Avg => {
            let sql = format!(
                "{} SELECT epoch_us(bucket_ts) AS timestamp_us, AVG(value) AS value {}",
                duckdb_bucketed_cte("integer_values", step_ms),
                duckdb_group_by_clause()
            );
            let mut statement = connection.prepare(&sql)?;
            let mut rows = statement.query(duckdb::params![
                sensor_id,
                start_time_us,
                end_time_us,
                origin_us,
                limit
            ])?;
            let mut samples = smallvec![];
            while let Some(row) = rows.next()? {
                let timestamp_us: i64 = row.get(0)?;
                let value: f64 = row.get(1)?;
                samples.push(Sample {
                    datetime: crate::datamodel::SensAppDateTime::from_unix_microseconds_i64(
                        timestamp_us,
                    ),
                    value,
                });
            }
            Ok(TypedSamples::Float(samples))
        }
        Aggregation::Count => {
            let sql = format!(
                "{} SELECT epoch_us(bucket_ts) AS timestamp_us, COUNT(*) AS value {}",
                duckdb_bucketed_cte("integer_values", step_ms),
                duckdb_group_by_clause()
            );
            let mut statement = connection.prepare(&sql)?;
            let mut rows = statement.query(duckdb::params![
                sensor_id,
                start_time_us,
                end_time_us,
                origin_us,
                limit
            ])?;
            let mut samples = smallvec![];
            while let Some(row) = rows.next()? {
                let timestamp_us: i64 = row.get(0)?;
                let value: i64 = row.get(1)?;
                samples.push(Sample {
                    datetime: crate::datamodel::SensAppDateTime::from_unix_microseconds_i64(
                        timestamp_us,
                    ),
                    value,
                });
            }
            Ok(TypedSamples::Integer(samples))
        }
        Aggregation::First | Aggregation::Last | Aggregation::Latest => {
            let sql = duckdb_first_last_query("integer_values", step_ms, aggregation, "value");
            let mut statement = connection.prepare(&sql)?;
            let mut rows = statement.query(duckdb::params![
                sensor_id,
                start_time_us,
                end_time_us,
                origin_us,
                limit
            ])?;
            let mut samples = smallvec![];
            while let Some(row) = rows.next()? {
                let timestamp_us: i64 = row.get(0)?;
                let value: i64 = row.get(1)?;
                samples.push(Sample {
                    datetime: crate::datamodel::SensAppDateTime::from_unix_microseconds_i64(
                        timestamp_us,
                    ),
                    value,
                });
            }
            Ok(TypedSamples::Integer(samples))
        }
        _ => {
            let expression = match aggregation {
                Aggregation::Min => "MIN(value)",
                Aggregation::Max => "MAX(value)",
                Aggregation::Sum => "SUM(value)",
                _ => unreachable!("handled separately"),
            };
            let sql = format!(
                "{} SELECT epoch_us(bucket_ts) AS timestamp_us, {} AS value {}",
                duckdb_bucketed_cte("integer_values", step_ms),
                expression,
                duckdb_group_by_clause()
            );
            let mut statement = connection.prepare(&sql)?;
            let mut rows = statement.query(duckdb::params![
                sensor_id,
                start_time_us,
                end_time_us,
                origin_us,
                limit
            ])?;
            let mut samples = smallvec![];
            while let Some(row) = rows.next()? {
                let timestamp_us: i64 = row.get(0)?;
                let value: i64 = row.get(1)?;
                samples.push(Sample {
                    datetime: crate::datamodel::SensAppDateTime::from_unix_microseconds_i64(
                        timestamp_us,
                    ),
                    value,
                });
            }
            Ok(TypedSamples::Integer(samples))
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn duckdb_query_float_samples_aggregated(
    connection: &Connection,
    sensor_id: i64,
    start_time_us: Option<i64>,
    end_time_us: Option<i64>,
    step_ms: i64,
    origin_us: i64,
    aggregation: Aggregation,
    limit: Option<usize>,
) -> Result<TypedSamples> {
    let limit = limit.unwrap_or(super::DEFAULT_QUERY_LIMIT) as i64;

    match aggregation {
        Aggregation::Count => {
            let sql = format!(
                "{} SELECT epoch_us(bucket_ts) AS timestamp_us, COUNT(*) AS value {}",
                duckdb_bucketed_cte("float_values", step_ms),
                duckdb_group_by_clause()
            );
            let mut statement = connection.prepare(&sql)?;
            let mut rows = statement.query(duckdb::params![
                sensor_id,
                start_time_us,
                end_time_us,
                origin_us,
                limit
            ])?;
            let mut samples = smallvec![];
            while let Some(row) = rows.next()? {
                let timestamp_us: i64 = row.get(0)?;
                let value: i64 = row.get(1)?;
                samples.push(Sample {
                    datetime: crate::datamodel::SensAppDateTime::from_unix_microseconds_i64(
                        timestamp_us,
                    ),
                    value,
                });
            }
            Ok(TypedSamples::Integer(samples))
        }
        Aggregation::First | Aggregation::Last | Aggregation::Latest => {
            let sql = duckdb_first_last_query("float_values", step_ms, aggregation, "value");
            let mut statement = connection.prepare(&sql)?;
            let mut rows = statement.query(duckdb::params![
                sensor_id,
                start_time_us,
                end_time_us,
                origin_us,
                limit
            ])?;
            let mut samples = smallvec![];
            while let Some(row) = rows.next()? {
                let timestamp_us: i64 = row.get(0)?;
                let value: f64 = row.get(1)?;
                samples.push(Sample {
                    datetime: crate::datamodel::SensAppDateTime::from_unix_microseconds_i64(
                        timestamp_us,
                    ),
                    value,
                });
            }
            Ok(TypedSamples::Float(samples))
        }
        _ => {
            let expression = match aggregation {
                Aggregation::Avg => "AVG(value)",
                Aggregation::Min => "MIN(value)",
                Aggregation::Max => "MAX(value)",
                Aggregation::Sum => "SUM(value)",
                _ => unreachable!("handled separately"),
            };
            let sql = format!(
                "{} SELECT epoch_us(bucket_ts) AS timestamp_us, {} AS value {}",
                duckdb_bucketed_cte("float_values", step_ms),
                expression,
                duckdb_group_by_clause()
            );
            let mut statement = connection.prepare(&sql)?;
            let mut rows = statement.query(duckdb::params![
                sensor_id,
                start_time_us,
                end_time_us,
                origin_us,
                limit
            ])?;
            let mut samples = smallvec![];
            while let Some(row) = rows.next()? {
                let timestamp_us: i64 = row.get(0)?;
                let value: f64 = row.get(1)?;
                samples.push(Sample {
                    datetime: crate::datamodel::SensAppDateTime::from_unix_microseconds_i64(
                        timestamp_us,
                    ),
                    value,
                });
            }
            Ok(TypedSamples::Float(samples))
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn duckdb_query_numeric_samples_aggregated(
    connection: &Connection,
    sensor_id: i64,
    start_time_us: Option<i64>,
    end_time_us: Option<i64>,
    step_ms: i64,
    origin_us: i64,
    aggregation: Aggregation,
    limit: Option<usize>,
) -> Result<TypedSamples> {
    let limit = limit.unwrap_or(super::DEFAULT_QUERY_LIMIT) as i64;

    match aggregation {
        Aggregation::Count => {
            let sql = format!(
                "{} SELECT epoch_us(bucket_ts) AS timestamp_us, COUNT(*) AS value {}",
                duckdb_bucketed_cte("numeric_values", step_ms),
                duckdb_group_by_clause()
            );
            let mut statement = connection.prepare(&sql)?;
            let mut rows = statement.query(duckdb::params![
                sensor_id,
                start_time_us,
                end_time_us,
                origin_us,
                limit
            ])?;
            let mut samples = smallvec![];
            while let Some(row) = rows.next()? {
                let timestamp_us: i64 = row.get(0)?;
                let value: i64 = row.get(1)?;
                samples.push(Sample {
                    datetime: crate::datamodel::SensAppDateTime::from_unix_microseconds_i64(
                        timestamp_us,
                    ),
                    value,
                });
            }
            Ok(TypedSamples::Integer(samples))
        }
        Aggregation::First | Aggregation::Last | Aggregation::Latest => {
            let sql = duckdb_first_last_query(
                "numeric_values",
                step_ms,
                aggregation,
                "CAST(value AS VARCHAR)",
            );
            let mut statement = connection.prepare(&sql)?;
            let mut rows = statement.query(duckdb::params![
                sensor_id,
                start_time_us,
                end_time_us,
                origin_us,
                limit
            ])?;
            let mut samples = smallvec![];
            while let Some(row) = rows.next()? {
                let timestamp_us: i64 = row.get(0)?;
                let value: String = row.get(1)?;
                samples.push(Sample {
                    datetime: crate::datamodel::SensAppDateTime::from_unix_microseconds_i64(
                        timestamp_us,
                    ),
                    value: Decimal::from_str(&value)
                        .context("Failed to parse DuckDB aggregated numeric value")?,
                });
            }
            Ok(TypedSamples::Numeric(samples))
        }
        _ => {
            let expression = match aggregation {
                Aggregation::Avg => "CAST(AVG(value) AS VARCHAR)",
                Aggregation::Min => "CAST(MIN(value) AS VARCHAR)",
                Aggregation::Max => "CAST(MAX(value) AS VARCHAR)",
                Aggregation::Sum => "CAST(SUM(value) AS VARCHAR)",
                _ => unreachable!("handled separately"),
            };
            let sql = format!(
                "{} SELECT epoch_us(bucket_ts) AS timestamp_us, {} AS value {}",
                duckdb_bucketed_cte("numeric_values", step_ms),
                expression,
                duckdb_group_by_clause()
            );
            let mut statement = connection.prepare(&sql)?;
            let mut rows = statement.query(duckdb::params![
                sensor_id,
                start_time_us,
                end_time_us,
                origin_us,
                limit
            ])?;
            let mut samples = smallvec![];
            while let Some(row) = rows.next()? {
                let timestamp_us: i64 = row.get(0)?;
                let value: String = row.get(1)?;
                samples.push(Sample {
                    datetime: crate::datamodel::SensAppDateTime::from_unix_microseconds_i64(
                        timestamp_us,
                    ),
                    value: Decimal::from_str(&value)
                        .context("Failed to parse DuckDB aggregated numeric value")?,
                });
            }
            Ok(TypedSamples::Numeric(samples))
        }
    }
}
