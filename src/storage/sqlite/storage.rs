use super::sqlite_publishers::*;
use super::sqlite_utilities::get_sensor_id_or_create_sensor;
use crate::datamodel::batch::{Batch, SingleSensorBatch};
use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
use crate::datamodel::unit::Unit;
use crate::datamodel::{
    Metric, Sample, SensAppDateTime, Sensor, SensorData, SensorType, TypedSamples,
    sensapp_vec::SensAppLabels,
};
use crate::storage::{
    DEFAULT_QUERY_LIMIT, StorageError, StorageInstance, common::datetime_to_micros,
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use geo::Point;
use rust_decimal::Decimal;
use serde_json::Value as JsonValue;
use smallvec::smallvec;
use sqlx::{Sqlite, Transaction, prelude::*};
use sqlx::{SqlitePool, sqlite::SqliteConnectOptions};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

// SQLite implementation
#[derive(Debug)]
pub struct SqliteStorage {
    pool: SqlitePool,
}

impl SqliteStorage {
    pub async fn connect(connection_string: &str) -> Result<Self> {
        let connect_options = SqliteConnectOptions::from_str(connection_string)
            .context("Failed to create sqlite connection options")?
            // Create the database file if it doesn't exist
            .create_if_missing(true)
            // The Wall mode should perform better for SensApp
            // It is the default in sqlx, but we want to make sure it stays that way
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            // Foreign keys have a performance impact, they are disabled by default
            // in SQLite, but we want to make sure they stay disabled.
            .foreign_keys(false)
            // Set a busy timeout of 5 seconds
            .busy_timeout(Duration::from_secs(5));

        let pool = sqlx::SqlitePool::connect_with(connect_options)
            .await
            .context("Failed to create sqlite pool")?;

        Ok(Self { pool })
    }
}

#[async_trait]
impl StorageInstance for SqliteStorage {
    async fn create_or_migrate(&self) -> Result<()> {
        // Implement schema creation or migration logic here
        sqlx::migrate!("src/storage/sqlite/migrations")
            .run(&self.pool)
            .await
            .context("Failed to migrate database")?;

        Ok(())
    }
    async fn publish(&self, batch: Arc<Batch>) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        for single_sensor_batch in batch.sensors.as_ref() {
            self.publish_single_sensor_batch(&mut transaction, single_sensor_batch)
                .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn vacuum(&self) -> Result<()> {
        // Vacuum the SQLite database to reclaim space and optimize performance
        sqlx::query("VACUUM")
            .execute(&self.pool)
            .await
            .context("Failed to vacuum database")?;
        Ok(())
    }

    async fn list_series(
        &self,
        metric_filter: Option<&str>,
    ) -> Result<Vec<crate::datamodel::Sensor>> {
        #[derive(sqlx::FromRow)]
        struct SensorRow {
            sensor_id: Option<i64>,
            uuid: String,
            name: String,
            r#type: String,
            unit_name: Option<String>,
            unit_description: Option<String>,
        }

        // Query sensors with their metadata using the catalog view, optionally filtered by metric name
        let sensor_rows: Vec<SensorRow> = sqlx::query_as(
            r#"
            SELECT sensor_id, uuid, name, type, unit_name, unit_description
            FROM sensor_catalog_view
            WHERE (?1 IS NULL OR name = ?1)
            ORDER BY uuid ASC
            "#,
        )
        .bind(metric_filter)
        .fetch_all(&self.pool)
        .await?;

        let mut sensors = Vec::new();

        for sensor_row in sensor_rows {
            // Parse sensor metadata with improved error handling
            let sensor_uuid = Uuid::parse_str(&sensor_row.uuid).map_err(|e| {
                anyhow::Error::from(StorageError::invalid_data_format(
                    &format!("Failed to parse sensor UUID '{}': {}", sensor_row.uuid, e),
                    None,
                    Some(&sensor_row.name),
                ))
            })?;

            let sensor_name = &sensor_row.name;
            let sensor_type_str = &sensor_row.r#type;

            let sensor_type = SensorType::from_str(sensor_type_str).map_err(|e| {
                anyhow::Error::from(StorageError::invalid_data_format(
                    &format!("Failed to parse sensor type '{}': {}", sensor_type_str, e),
                    Some(sensor_uuid),
                    Some(sensor_name),
                ))
            })?;

            let unit = match sensor_row.unit_name {
                Some(name) if !name.is_empty() => {
                    Some(Unit::new(name, sensor_row.unit_description))
                }
                _ => None,
            };

            // Query labels for this sensor with proper error context
            let sensor_id = sensor_row.sensor_id.ok_or_else(|| {
                anyhow::Error::from(StorageError::missing_field(
                    "sensor_id",
                    Some(sensor_uuid),
                    Some(sensor_name),
                ))
            })?;

            #[derive(sqlx::FromRow)]
            struct LabelRow {
                label_name: String,
                label_value: String,
            }

            let labels_rows: Vec<LabelRow> = sqlx::query_as(
                r#"
                SELECT lnd.name as label_name, ldd.description as label_value
                FROM labels l
                JOIN labels_name_dictionary lnd ON l.name = lnd.id
                JOIN labels_description_dictionary ldd ON l.description = ldd.id
                WHERE l.sensor_id = ?
                "#,
            )
            .bind(sensor_id)
            .fetch_all(&self.pool)
            .await
            .with_context(|| {
                format!(
                    "Failed to query labels for sensor UUID={} name='{}'",
                    sensor_uuid, sensor_name
                )
            })?;

            let mut labels: SensAppLabels = smallvec![];
            for label_row in labels_rows {
                labels.push((label_row.label_name, label_row.label_value));
            }

            let sensor = Sensor::new(
                sensor_uuid,
                sensor_name.to_string(),
                sensor_type,
                unit,
                Some(labels),
            );

            sensors.push(sensor);
        }

        Ok(sensors)
    }

    async fn list_metrics(&self) -> Result<Vec<crate::datamodel::Metric>> {
        #[derive(sqlx::FromRow)]
        struct MetricsRow {
            metric_name: Option<String>,
            r#type: Option<String>,
            unit_name: String,
            unit_description: Option<String>,
            series_count: i64,
            label_keys: Option<String>,
        }

        // Query metrics summary using the view
        let metrics_rows: Vec<MetricsRow> = sqlx::query_as(
            r#"
            SELECT metric_name, type, unit_name, unit_description, series_count, label_keys
            FROM metrics_summary
            ORDER BY metric_name ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut metrics = Vec::new();

        for metrics_row in metrics_rows {
            let metric_name = metrics_row.metric_name.ok_or_else(|| {
                anyhow::Error::from(StorageError::missing_field("metric_name", None, None))
            })?;

            let sensor_type_str = metrics_row.r#type.ok_or_else(|| {
                anyhow::Error::from(StorageError::missing_field(
                    "type",
                    None,
                    Some(&metric_name),
                ))
            })?;

            let sensor_type = SensorType::from_str(&sensor_type_str).map_err(|e| {
                anyhow::Error::from(StorageError::invalid_data_format(
                    &format!("Failed to parse sensor type '{}': {}", sensor_type_str, e),
                    None,
                    Some(&metric_name),
                ))
            })?;

            // Handle unit - SQLite returns concrete String or NULL
            let unit = if !metrics_row.unit_name.is_empty() {
                Some(Unit::new(
                    metrics_row.unit_name,
                    metrics_row.unit_description,
                ))
            } else {
                None
            };

            // Handle series_count - it's already an i64
            let series_count = metrics_row.series_count;

            // Handle label_keys - SQLite uses GROUP_CONCAT which returns a string
            let label_keys = metrics_row
                .label_keys
                .map(|keys| {
                    keys.as_str()
                        .split(',')
                        .map(|s| s.trim().to_string())
                        .collect()
                })
                .unwrap_or_default();

            let metric = Metric::new(metric_name, sensor_type, unit, series_count, label_keys);

            metrics.push(metric);
        }

        Ok(metrics)
    }

    async fn query_sensor_data(
        &self,
        sensor_uuid: &str,
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
        limit: Option<usize>,
    ) -> Result<Option<SensorData>> {
        // Parse UUID
        let parsed_uuid = Uuid::from_str(sensor_uuid).context("Failed to parse sensor UUID")?;

        #[derive(sqlx::FromRow)]
        struct SensorMetadataRow {
            sensor_id: Option<i64>,
            uuid: String,
            name: String,
            r#type: String,
            unit_name: Option<String>,
            unit_description: Option<String>,
        }

        // Query sensor metadata by UUID
        let uuid_string = parsed_uuid.to_string();
        let sensor_row: Option<SensorMetadataRow> = sqlx::query_as(
            r#"
            SELECT s.sensor_id, s.uuid, s.name, s.type, u.name as unit_name, u.description as unit_description
            FROM sensors s
            LEFT JOIN units u ON s.unit = u.id
            WHERE s.uuid = ?
            "#
        )
        .bind(&uuid_string)
        .fetch_optional(&self.pool)
        .await?;

        let sensor_row = match sensor_row {
            Some(row) => row,
            None => return Ok(None),
        };

        // Parse sensor metadata with improved error handling
        let sensor_uuid = Uuid::parse_str(&sensor_row.uuid).map_err(|e| {
            anyhow::Error::from(StorageError::invalid_data_format(
                &format!("Failed to parse sensor UUID '{}': {}", sensor_row.uuid, e),
                None,
                Some(&sensor_row.name),
            ))
        })?;

        let sensor_name = &sensor_row.name;
        let sensor_type_str = &sensor_row.r#type;

        let sensor_type = SensorType::from_str(sensor_type_str).map_err(|e| {
            anyhow::Error::from(StorageError::invalid_data_format(
                &format!("Failed to parse sensor type '{}': {}", sensor_type_str, e),
                Some(sensor_uuid),
                Some(sensor_name),
            ))
        })?;

        let unit = match (sensor_row.unit_name, sensor_row.unit_description) {
            (Some(name), description) if !name.is_empty() => Some(Unit::new(name, description)),
            _ => None,
        };

        // Query labels for this sensor with proper error context
        let sensor_id = sensor_row.sensor_id.ok_or_else(|| {
            anyhow::Error::from(StorageError::missing_field(
                "sensor_id",
                Some(sensor_uuid),
                Some(sensor_name),
            ))
        })?;

        #[derive(sqlx::FromRow)]
        struct LabelRow {
            label_name: String,
            label_value: String,
        }

        let labels_rows: Vec<LabelRow> = sqlx::query_as(
            r#"
            SELECT lnd.name as label_name, ldd.description as label_value
            FROM labels l
            JOIN labels_name_dictionary lnd ON l.name = lnd.id
            JOIN labels_description_dictionary ldd ON l.description = ldd.id
            WHERE l.sensor_id = ?
            "#,
        )
        .bind(sensor_id)
        .fetch_all(&self.pool)
        .await
        .with_context(|| {
            format!(
                "Failed to query labels for sensor UUID={} name='{}'",
                sensor_uuid, sensor_name
            )
        })?;

        let mut labels: SensAppLabels = smallvec![];
        for label_row in labels_rows {
            labels.push((label_row.label_name, label_row.label_value));
        }

        let sensor = Sensor::new(
            sensor_uuid,
            sensor_name.to_string(),
            sensor_type,
            unit,
            Some(labels),
        );

        // Convert datetime parameters to microseconds for database queries
        let start_time_micros = start_time.as_ref().map(datetime_to_micros);
        let end_time_micros = end_time.as_ref().map(datetime_to_micros);

        // Query samples based on sensor type
        let samples = match sensor_type {
            SensorType::Integer => {
                self.query_integer_samples(sensor_id, start_time_micros, end_time_micros, limit)
                    .await?
            }
            SensorType::Numeric => {
                self.query_numeric_samples(sensor_id, start_time_micros, end_time_micros, limit)
                    .await?
            }
            SensorType::Float => {
                self.query_float_samples(sensor_id, start_time_micros, end_time_micros, limit)
                    .await?
            }
            SensorType::String => {
                self.query_string_samples(sensor_id, start_time_micros, end_time_micros, limit)
                    .await?
            }
            SensorType::Boolean => {
                self.query_boolean_samples(sensor_id, start_time_micros, end_time_micros, limit)
                    .await?
            }
            SensorType::Location => {
                self.query_location_samples(sensor_id, start_time_micros, end_time_micros, limit)
                    .await?
            }
            SensorType::Json => {
                self.query_json_samples(sensor_id, start_time_micros, end_time_micros, limit)
                    .await?
            }
            SensorType::Blob => {
                self.query_blob_samples(sensor_id, start_time_micros, end_time_micros, limit)
                    .await?
            }
        };

        Ok(Some(SensorData::new(sensor, samples)))
    }

    /// Health check for SQLite storage
    /// Executes a simple SELECT 1 query to verify database connectivity
    async fn health_check(&self) -> Result<()> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .context("SQLite health check failed")?;
        Ok(())
    }

    /// Clean up all test data from the database
    /// This removes all sensor data but keeps the schema intact
    /// Uses DELETE statements in dependency order to avoid foreign key conflicts
    #[cfg(any(test, feature = "test-utils"))]
    async fn cleanup_test_data(&self) -> Result<()> {
        // Use a transaction to ensure atomicity
        let mut tx = self.pool.begin().await?;

        // Step 1: Delete all value tables (they reference sensors but nothing references them)
        sqlx::query("DELETE FROM blob_values")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM json_values")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM location_values")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM boolean_values")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM string_values")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM float_values")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM numeric_values")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM integer_values")
            .execute(&mut *tx)
            .await?;

        // Step 2: Delete labels (references sensors and dictionaries)
        sqlx::query("DELETE FROM labels").execute(&mut *tx).await?;

        // Step 3: Delete sensors (references units, but we'll preserve units for tests)
        sqlx::query("DELETE FROM sensors").execute(&mut *tx).await?;

        // Step 4: Delete dictionary tables (but preserve units for test data)
        sqlx::query("DELETE FROM strings_values_dictionary")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM labels_description_dictionary")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM labels_name_dictionary")
            .execute(&mut *tx)
            .await?;

        // Note: We preserve the units table to avoid foreign key violations in tests
        // But we need to ensure common test units exist

        // Insert common test units if they don't exist (using INSERT OR IGNORE for idempotency)
        sqlx::query("INSERT OR IGNORE INTO units (name, description) VALUES ('°C', 'Celsius')")
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT OR IGNORE INTO units (name, description) VALUES ('%', 'Percentage')")
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT OR IGNORE INTO units (name, description) VALUES ('m', 'Meters')")
            .execute(&mut *tx)
            .await?;
        sqlx::query("INSERT OR IGNORE INTO units (name, description) VALUES ('kg', 'Kilograms')")
            .execute(&mut *tx)
            .await?;

        // Reset SQLite sequences for clean test data
        sqlx::query("DELETE FROM sqlite_sequence WHERE name IN ('sensors', 'strings_values_dictionary', 'labels_description_dictionary', 'labels_name_dictionary')")
            .execute(&mut *tx).await.ok(); // Ignore errors if table doesn't exist

        tx.commit()
            .await
            .context("Failed to commit test data cleanup transaction")?;

        Ok(())
    }
}

impl SqliteStorage {
    async fn publish_single_sensor_batch(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        single_sensor_batch: &SingleSensorBatch,
    ) -> Result<()> {
        let sensor_id =
            get_sensor_id_or_create_sensor(transaction, &single_sensor_batch.sensor).await?;
        {
            let samples_guard = single_sensor_batch.samples.read().await;
            match &*samples_guard {
                TypedSamples::Integer(samples) => {
                    publish_integer_values(transaction, sensor_id, samples).await?;
                }
                TypedSamples::Numeric(samples) => {
                    publish_numeric_values(transaction, sensor_id, samples).await?;
                }
                TypedSamples::Float(samples) => {
                    publish_float_values(transaction, sensor_id, samples).await?;
                }
                TypedSamples::String(samples) => {
                    publish_string_values(transaction, sensor_id, samples).await?;
                }
                TypedSamples::Boolean(samples) => {
                    publish_boolean_values(transaction, sensor_id, samples).await?;
                }
                TypedSamples::Location(samples) => {
                    publish_location_values(transaction, sensor_id, samples).await?;
                }
                TypedSamples::Blob(samples) => {
                    publish_blob_values(transaction, sensor_id, samples).await?;
                }
                TypedSamples::Json(samples) => {
                    publish_json_values(transaction, sensor_id, samples).await?;
                }
            }
        }
        Ok(())
    }

    #[allow(dead_code)] // May be used for maintenance operations in the future
    async fn deduplicate(&self) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        transaction
            .execute(sqlx::query(
                r#"
            DELETE FROM integer_values WHERE rowid NOT IN (
                SELECT MIN(rowid) FROM integer_values GROUP BY sensor_id, timestamp_us, value
            )
            "#,
            ))
            .await?;

        transaction
            .execute(sqlx::query(
                r#"
            DELETE FROM float_values WHERE rowid NOT IN (
                SELECT MIN(rowid) FROM float_values GROUP BY sensor_id, timestamp_us, value
            )
            "#,
            ))
            .await?;

        transaction.commit().await?;

        let vacuum = sqlx::query("VACUUM");
        vacuum.execute(&self.pool).await?;

        Ok(())
    }

    async fn query_integer_samples(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        #[derive(sqlx::FromRow)]
        struct IntegerValueRow {
            timestamp_us: i64,
            value: i64,
        }

        let limit_value = limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64;
        let rows: Vec<IntegerValueRow> = sqlx::query_as(
            r#"
            SELECT timestamp_us, value FROM integer_values
            WHERE sensor_id = ?
            AND (? IS NULL OR timestamp_us >= ?)
            AND (? IS NULL OR timestamp_us <= ?)
            ORDER BY timestamp_us ASC
            LIMIT ?
            "#,
        )
        .bind(sensor_id)
        .bind(start_time)
        .bind(start_time)
        .bind(end_time)
        .bind(end_time)
        .bind(limit_value)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us);
            let value = row.value;
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::Integer(samples))
    }

    async fn query_numeric_samples(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        #[derive(sqlx::FromRow)]
        struct NumericValueRow {
            timestamp_us: i64,
            value: String,
        }

        let limit_value = limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64;
        let rows: Vec<NumericValueRow> = sqlx::query_as(
            r#"
            SELECT timestamp_us, value FROM numeric_values
            WHERE sensor_id = ?
            AND (? IS NULL OR timestamp_us >= ?)
            AND (? IS NULL OR timestamp_us <= ?)
            ORDER BY timestamp_us ASC
            LIMIT ?
            "#,
        )
        .bind(sensor_id)
        .bind(start_time)
        .bind(start_time)
        .bind(end_time)
        .bind(end_time)
        .bind(limit_value)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us);
            let value = Decimal::from_str(&row.value).context("Failed to parse decimal value")?;
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::Numeric(samples))
    }

    async fn query_float_samples(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        #[derive(sqlx::FromRow)]
        struct FloatValueRow {
            timestamp_us: i64,
            value: f64,
        }

        let limit_value = limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64;
        let rows: Vec<FloatValueRow> = sqlx::query_as(
            r#"
            SELECT timestamp_us, value FROM float_values
            WHERE sensor_id = ?
            AND (? IS NULL OR timestamp_us >= ?)
            AND (? IS NULL OR timestamp_us <= ?)
            ORDER BY timestamp_us ASC
            LIMIT ?
            "#,
        )
        .bind(sensor_id)
        .bind(start_time)
        .bind(start_time)
        .bind(end_time)
        .bind(end_time)
        .bind(limit_value)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us);
            let value = row.value;
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::Float(samples))
    }

    async fn query_string_samples(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        #[derive(sqlx::FromRow)]
        struct StringValueRow {
            timestamp_us: i64,
            string_value: String,
        }

        let limit_value = limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64;
        let rows: Vec<StringValueRow> = sqlx::query_as(
            r#"
            SELECT sv.timestamp_us, svd.value as string_value
            FROM string_values sv
            JOIN strings_values_dictionary svd ON sv.value = svd.id
            WHERE sv.sensor_id = ?
            AND (? IS NULL OR sv.timestamp_us >= ?)
            AND (? IS NULL OR sv.timestamp_us <= ?)
            ORDER BY sv.timestamp_us ASC
            LIMIT ?
            "#,
        )
        .bind(sensor_id)
        .bind(start_time)
        .bind(start_time)
        .bind(end_time)
        .bind(end_time)
        .bind(limit_value)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us);
            let value = row.string_value;
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::String(samples))
    }

    async fn query_boolean_samples(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        #[derive(sqlx::FromRow)]
        struct BooleanValueRow {
            timestamp_us: i64,
            value: i64,
        }

        let limit_value = limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64;
        let rows: Vec<BooleanValueRow> = sqlx::query_as(
            r#"
            SELECT timestamp_us, value FROM boolean_values
            WHERE sensor_id = ?
            AND (? IS NULL OR timestamp_us >= ?)
            AND (? IS NULL OR timestamp_us <= ?)
            ORDER BY timestamp_us ASC
            LIMIT ?
            "#,
        )
        .bind(sensor_id)
        .bind(start_time)
        .bind(start_time)
        .bind(end_time)
        .bind(end_time)
        .bind(limit_value)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us);
            let value = row.value != 0;
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::Boolean(samples))
    }

    async fn query_location_samples(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        #[derive(sqlx::FromRow)]
        struct LocationValueRow {
            timestamp_us: i64,
            latitude: f64,
            longitude: f64,
        }

        let limit_value = limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64;
        let rows: Vec<LocationValueRow> = sqlx::query_as(
            r#"
            SELECT timestamp_us, latitude, longitude FROM location_values
            WHERE sensor_id = ?
            AND (? IS NULL OR timestamp_us >= ?)
            AND (? IS NULL OR timestamp_us <= ?)
            ORDER BY timestamp_us ASC
            LIMIT ?
            "#,
        )
        .bind(sensor_id)
        .bind(start_time)
        .bind(start_time)
        .bind(end_time)
        .bind(end_time)
        .bind(limit_value)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us);
            let value = Point::new(row.longitude, row.latitude);
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::Location(samples))
    }

    async fn query_json_samples(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        #[derive(sqlx::FromRow)]
        struct JsonValueRow {
            timestamp_us: i64,
            value: Vec<u8>,
        }

        let limit_value = limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64;
        let rows: Vec<JsonValueRow> = sqlx::query_as(
            r#"
            SELECT timestamp_us, value FROM json_values
            WHERE sensor_id = ?
            AND (? IS NULL OR timestamp_us >= ?)
            AND (? IS NULL OR timestamp_us <= ?)
            ORDER BY timestamp_us ASC
            LIMIT ?
            "#,
        )
        .bind(sensor_id)
        .bind(start_time)
        .bind(start_time)
        .bind(end_time)
        .bind(end_time)
        .bind(limit_value)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us);
            let value: JsonValue =
                serde_json::from_slice(&row.value).context("Failed to parse JSON value")?;
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::Json(samples))
    }

    async fn query_blob_samples(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        #[derive(sqlx::FromRow)]
        struct BlobValueRow {
            timestamp_us: i64,
            value: Vec<u8>,
        }

        let limit_value = limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64;
        let rows: Vec<BlobValueRow> = sqlx::query_as(
            r#"
            SELECT timestamp_us, value FROM blob_values
            WHERE sensor_id = ?
            AND (? IS NULL OR timestamp_us >= ?)
            AND (? IS NULL OR timestamp_us <= ?)
            ORDER BY timestamp_us ASC
            LIMIT ?
            "#,
        )
        .bind(sensor_id)
        .bind(start_time)
        .bind(start_time)
        .bind(end_time)
        .bind(end_time)
        .bind(limit_value)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us);
            let value = row.value;
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::Blob(samples))
    }
}
