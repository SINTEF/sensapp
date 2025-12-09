pub mod timescaledb_publishers;
pub mod timescaledb_utilities;

use self::timescaledb_publishers::*;
use self::timescaledb_utilities::get_sensor_id_or_create_sensor;
use super::{DEFAULT_QUERY_LIMIT, StorageError, StorageInstance};
use crate::datamodel::{
    SensAppDateTime, Sensor, SensorData, SensorType, TypedSamples, batch::Batch,
};
use crate::datamodel::{sensapp_vec::SensAppLabels, unit::Unit};
use anyhow::{Context, Result};
use async_trait::async_trait;
use smallvec::smallvec;
use sqlx::{PgPool, postgres::PgConnectOptions};
use std::{str::FromStr, sync::Arc};
use uuid::Uuid;

#[derive(Debug)]
pub struct TimeScaleDBStorage {
    pool: PgPool,
}

impl TimeScaleDBStorage {
    pub async fn connect(connection_string: &str) -> Result<Self> {
        // Convert timescaledb:// to postgres:// for sqlx compatibility
        let postgres_connection_string = if connection_string.starts_with("timescaledb://") {
            connection_string.replace("timescaledb://", "postgres://")
        } else {
            connection_string.to_string()
        };

        let connect_options = PgConnectOptions::from_str(&postgres_connection_string)
            .context("Failed to create timescaledb connection options")?;

        let pool = PgPool::connect_with(connect_options)
            .await
            .context("Failed to create timescaledb pool")?;

        Ok(Self { pool })
    }
}

#[async_trait]
impl StorageInstance for TimeScaleDBStorage {
    async fn create_or_migrate(&self) -> Result<()> {
        sqlx::migrate!("src/storage/timescaledb/migrations")
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
        self.vacuum().await?;
        Ok(())
    }

    async fn list_series(
        &self,
        metric_filter: Option<&str>,
    ) -> Result<Vec<crate::datamodel::Sensor>> {
        #[derive(sqlx::FromRow)]
        struct SensorRow {
            sensor_id: Option<i64>,
            uuid: Option<uuid::Uuid>,
            name: Option<String>,
            r#type: Option<String>,
            unit_name: Option<String>,
            unit_description: Option<String>,
        }

        // Query sensors with their metadata using the catalog view, optionally filtered by metric name
        let sensor_rows: Vec<SensorRow> = sqlx::query_as(
            r#"
            SELECT sensor_id, uuid, name, type, unit_name, unit_description
            FROM sensor_catalog_view
            WHERE ($1::TEXT IS NULL OR name = $1)
            ORDER BY uuid ASC
            "#,
        )
        .bind(metric_filter)
        .fetch_all(&self.pool)
        .await?;

        let mut sensors = Vec::new();

        for sensor_row in sensor_rows {
            // Parse sensor metadata with improved error handling
            let sensor_uuid = sensor_row
                .uuid
                .ok_or_else(|| {
                    crate::storage::StorageError::missing_field(
                        "UUID",
                        None,
                        sensor_row.name.as_deref(),
                    )
                })
                .map_err(anyhow::Error::from)?;

            let sensor_name = sensor_row
                .name
                .ok_or_else(|| {
                    crate::storage::StorageError::missing_field("name", Some(sensor_uuid), None)
                })
                .map_err(anyhow::Error::from)?;

            let sensor_type_str = sensor_row
                .r#type
                .ok_or_else(|| {
                    crate::storage::StorageError::missing_field(
                        "type",
                        Some(sensor_uuid),
                        Some(&sensor_name),
                    )
                })
                .map_err(anyhow::Error::from)?;

            let sensor_type =
                crate::datamodel::SensorType::from_str(&sensor_type_str).map_err(|e| {
                    anyhow::Error::from(crate::storage::StorageError::invalid_data_format(
                        &format!("Failed to parse sensor type '{}': {}", sensor_type_str, e),
                        Some(sensor_uuid),
                        Some(&sensor_name),
                    ))
                })?;

            let unit = match (sensor_row.unit_name, sensor_row.unit_description) {
                (Some(name), description) => {
                    Some(crate::datamodel::unit::Unit::new(name, description))
                }
                _ => None,
            };

            // Query labels for this sensor with proper error context
            let sensor_id = sensor_row.sensor_id.ok_or_else(|| {
                anyhow::Error::from(crate::storage::StorageError::missing_field(
                    "sensor_id",
                    Some(sensor_uuid),
                    Some(&sensor_name),
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
                WHERE l.sensor_id = $1
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

            let mut labels: crate::datamodel::sensapp_vec::SensAppLabels = smallvec::smallvec![];
            for label_row in labels_rows {
                labels.push((label_row.label_name, label_row.label_value));
            }

            let sensor = crate::datamodel::Sensor::new(
                sensor_uuid,
                sensor_name,
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
            unit_name: Option<String>,
            unit_description: Option<String>,
            series_count: Option<i64>,
            label_keys: Option<Vec<String>>,
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
                anyhow::Error::from(crate::storage::StorageError::missing_field(
                    "metric_name",
                    None,
                    None,
                ))
            })?;

            let sensor_type_str = metrics_row.r#type.ok_or_else(|| {
                anyhow::Error::from(crate::storage::StorageError::missing_field(
                    "type",
                    None,
                    Some(&metric_name),
                ))
            })?;

            let sensor_type =
                crate::datamodel::SensorType::from_str(&sensor_type_str).map_err(|e| {
                    anyhow::Error::from(crate::storage::StorageError::invalid_data_format(
                        &format!("Failed to parse sensor type '{}': {}", sensor_type_str, e),
                        None,
                        Some(&metric_name),
                    ))
                })?;

            let unit = match (metrics_row.unit_name, metrics_row.unit_description) {
                (Some(name), description) => {
                    Some(crate::datamodel::unit::Unit::new(name, description))
                }
                _ => None,
            };

            let series_count = metrics_row.series_count.ok_or_else(|| {
                anyhow::Error::from(crate::storage::StorageError::missing_field(
                    "series_count",
                    None,
                    Some(&metric_name),
                ))
            })?;

            // Handle optional label_keys array
            let label_keys = metrics_row.label_keys.unwrap_or_default();

            let metric = crate::datamodel::Metric::new(
                metric_name,
                sensor_type,
                unit,
                series_count,
                label_keys,
            );

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
            uuid: Option<Uuid>,
            name: Option<String>,
            r#type: Option<String>,
            unit_name: Option<String>,
            unit_description: Option<String>,
        }

        // Query sensor metadata by UUID using the catalog view
        let sensor_row: Option<SensorMetadataRow> = sqlx::query_as(
            r#"
            SELECT sensor_id, uuid, name, type, unit_name, unit_description
            FROM sensor_catalog_view
            WHERE uuid = $1
            "#,
        )
        .bind(parsed_uuid)
        .fetch_optional(&self.pool)
        .await?;

        let sensor_row = match sensor_row {
            Some(row) => row,
            None => return Ok(None),
        };

        // Parse sensor metadata with improved error handling
        let sensor_uuid = sensor_row.uuid.ok_or_else(|| {
            anyhow::Error::from(StorageError::missing_field(
                "UUID",
                None,
                sensor_row.name.as_deref(),
            ))
        })?;

        let sensor_name = sensor_row.name.ok_or_else(|| {
            anyhow::Error::from(StorageError::missing_field("name", Some(sensor_uuid), None))
        })?;

        let sensor_type_str = sensor_row.r#type.ok_or_else(|| {
            anyhow::Error::from(StorageError::missing_field(
                "type",
                Some(sensor_uuid),
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

        let unit = match (sensor_row.unit_name, sensor_row.unit_description) {
            (Some(name), description) => Some(Unit::new(name, description)),
            _ => None,
        };

        // Query labels for this sensor with proper context
        let sensor_id = sensor_row.sensor_id.ok_or_else(|| {
            anyhow::Error::from(StorageError::missing_field(
                "sensor_id",
                Some(sensor_uuid),
                Some(&sensor_name),
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
            WHERE l.sensor_id = $1
            "#,
        )
        .bind(sensor_id)
        .fetch_all(&self.pool)
        .await?;

        let mut labels: SensAppLabels = smallvec![];
        for label_row in labels_rows {
            labels.push((label_row.label_name, label_row.label_value));
        }

        let sensor = Sensor::new(sensor_uuid, sensor_name, sensor_type, unit, Some(labels));

        // Convert SensAppDateTime to microseconds using a custom function for TimescaleDB
        let start_time_us = start_time.as_ref().map(|dt| {
            let unix_seconds = dt.to_unix_seconds();
            let subsec_nanos = dt.to_et_duration().total_nanoseconds() % 1_000_000_000;
            (unix_seconds as i64) * 1_000_000 + (subsec_nanos / 1000) as i64
        });
        let end_time_us = end_time.as_ref().map(|dt| {
            let unix_seconds = dt.to_unix_seconds();
            let subsec_nanos = dt.to_et_duration().total_nanoseconds() % 1_000_000_000;
            (unix_seconds as i64) * 1_000_000 + (subsec_nanos / 1000) as i64
        });

        // Query samples based on sensor type
        let samples = match sensor.sensor_type {
            SensorType::Integer => {
                self.query_integer_samples(sensor_id, start_time_us, end_time_us, limit)
                    .await?
            }
            SensorType::Numeric => {
                self.query_numeric_samples(sensor_id, start_time_us, end_time_us, limit)
                    .await?
            }
            SensorType::Float => {
                self.query_float_samples(sensor_id, start_time_us, end_time_us, limit)
                    .await?
            }
            SensorType::String => {
                self.query_string_samples(sensor_id, start_time_us, end_time_us, limit)
                    .await?
            }
            SensorType::Boolean => {
                self.query_boolean_samples(sensor_id, start_time_us, end_time_us, limit)
                    .await?
            }
            SensorType::Location => {
                self.query_location_samples(sensor_id, start_time_us, end_time_us, limit)
                    .await?
            }
            SensorType::Json => {
                self.query_json_samples(sensor_id, start_time_us, end_time_us, limit)
                    .await?
            }
            SensorType::Blob => {
                self.query_blob_samples(sensor_id, start_time_us, end_time_us, limit)
                    .await?
            }
        };

        Ok(Some(SensorData::new(sensor, samples)))
    }

    async fn query_sensors_by_labels(
        &self,
        _matchers: &[super::LabelMatcher],
        _start_time: Option<SensAppDateTime>,
        _end_time: Option<SensAppDateTime>,
        _limit: Option<usize>,
        _numeric_only: bool,
    ) -> Result<Vec<SensorData>> {
        // TODO: Implement label-based query for TimescaleDB
        anyhow::bail!("query_sensors_by_labels not yet implemented for TimescaleDB")
    }

    /// Health check for TimescaleDB storage
    /// Executes a simple SELECT 1 query to verify database connectivity
    async fn health_check(&self) -> Result<()> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .context("TimescaleDB health check failed")?;
        Ok(())
    }

    /// Clean up all test data from the database (TimescaleDB implementation)
    #[cfg(any(test, feature = "test-utils"))]
    async fn cleanup_test_data(&self) -> Result<()> {
        // TimescaleDB is PostgreSQL-based, so we use similar approach as PostgreSQL
        let mut tx = self.pool.begin().await?;

        // Delete all value tables in dependency order
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

        // Delete metadata tables
        sqlx::query("DELETE FROM labels").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM sensors").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM strings_values_dictionary")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM labels_description_dictionary")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM labels_name_dictionary")
            .execute(&mut *tx)
            .await?;

        // Preserve units and ensure common test units exist
        sqlx::query("INSERT INTO units (name, description) VALUES ('°C', 'Celsius') ON CONFLICT (name) DO NOTHING")
            .execute(&mut *tx).await?;
        sqlx::query("INSERT INTO units (name, description) VALUES ('%', 'Percentage') ON CONFLICT (name) DO NOTHING")
            .execute(&mut *tx).await?;

        tx.commit()
            .await
            .context("Failed to commit test data cleanup transaction")?;

        // Step 5: Clear all cached function caches
        // The cached macro generates cache variables named after the function in uppercase
        use cached::Cached;
        timescaledb_utilities::GET_LABEL_NAME_ID_OR_CREATE
            .lock()
            .await
            .cache_clear();
        timescaledb_utilities::GET_LABEL_DESCRIPTION_ID_OR_CREATE
            .lock()
            .await
            .cache_clear();
        timescaledb_utilities::GET_UNIT_ID_OR_CREATE
            .lock()
            .await
            .cache_clear();
        timescaledb_utilities::GET_SENSOR_ID_OR_CREATE_SENSOR
            .lock()
            .await
            .cache_clear();
        timescaledb_utilities::GET_STRING_VALUE_ID_OR_CREATE
            .lock()
            .await
            .cache_clear();

        Ok(())
    }
}

impl TimeScaleDBStorage {
    async fn publish_single_sensor_batch(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        single_sensor_batch: &crate::datamodel::batch::SingleSensorBatch,
    ) -> Result<()> {
        let sensor_id =
            get_sensor_id_or_create_sensor(transaction, &single_sensor_batch.sensor).await?;

        let samples_guard = single_sensor_batch.samples.read().await;
        match &*samples_guard {
            TypedSamples::Integer(values) => {
                publish_integer_values(transaction, sensor_id, values).await?;
            }
            TypedSamples::Numeric(values) => {
                publish_numeric_values(transaction, sensor_id, values).await?;
            }
            TypedSamples::Float(values) => {
                publish_float_values(transaction, sensor_id, values).await?;
            }
            TypedSamples::String(values) => {
                publish_string_values(transaction, sensor_id, values).await?;
            }
            TypedSamples::Boolean(values) => {
                publish_boolean_values(transaction, sensor_id, values).await?;
            }
            TypedSamples::Location(values) => {
                publish_location_values(transaction, sensor_id, values).await?;
            }
            TypedSamples::Blob(values) => {
                publish_blob_values(transaction, sensor_id, values).await?;
            }
            TypedSamples::Json(values) => {
                publish_json_values(transaction, sensor_id, values).await?;
            }
        }

        Ok(())
    }

    async fn vacuum(&self) -> Result<()> {
        sqlx::query("VACUUM")
            .execute(&self.pool)
            .await
            .context("Failed to vacuum database")?;

        Ok(())
    }

    async fn query_integer_samples(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        use crate::datamodel::{Sample, SensAppDateTime};
        use smallvec::smallvec;

        #[derive(sqlx::FromRow)]
        struct IntegerValueRow {
            time: sqlx::types::time::OffsetDateTime,
            value: i64,
        }

        // Convert microsecond timestamps to OffsetDateTime for TimescaleDB queries
        let start_time_ts = start_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });
        let end_time_ts = end_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });

        let rows: Vec<IntegerValueRow> = sqlx::query_as(
            r#"
            SELECT time, value FROM integer_values
            WHERE sensor_id = $1
            AND ($2::TIMESTAMPTZ IS NULL OR time >= $2)
            AND ($3::TIMESTAMPTZ IS NULL OR time <= $3)
            ORDER BY time ASC
            LIMIT $4
            "#,
        )
        .bind(sensor_id)
        .bind(start_time_ts)
        .bind(end_time_ts)
        .bind(limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            // Convert OffsetDateTime back to SensAppDateTime
            let unix_timestamp =
                row.time.unix_timestamp() as f64 + (row.time.nanosecond() as f64 / 1_000_000_000.0);
            let datetime = SensAppDateTime::from_unix_seconds(unix_timestamp);
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
        use crate::datamodel::{Sample, SensAppDateTime};
        use smallvec::smallvec;

        #[derive(sqlx::FromRow)]
        struct NumericValueRow {
            time: sqlx::types::time::OffsetDateTime,
            value: rust_decimal::Decimal,
        }

        // Convert microsecond timestamps to OffsetDateTime for TimescaleDB queries
        let start_time_ts = start_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });
        let end_time_ts = end_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });

        let rows: Vec<NumericValueRow> = sqlx::query_as(
            r#"
            SELECT time, value FROM numeric_values
            WHERE sensor_id = $1
            AND ($2::TIMESTAMPTZ IS NULL OR time >= $2)
            AND ($3::TIMESTAMPTZ IS NULL OR time <= $3)
            ORDER BY time ASC
            LIMIT $4
            "#,
        )
        .bind(sensor_id)
        .bind(start_time_ts)
        .bind(end_time_ts)
        .bind(limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            // Convert OffsetDateTime back to SensAppDateTime
            let unix_timestamp =
                row.time.unix_timestamp() as f64 + (row.time.nanosecond() as f64 / 1_000_000_000.0);
            let datetime = SensAppDateTime::from_unix_seconds(unix_timestamp);
            let value = row.value;
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
        use crate::datamodel::{Sample, SensAppDateTime};
        use smallvec::smallvec;

        #[derive(sqlx::FromRow)]
        struct FloatValueRow {
            time: sqlx::types::time::OffsetDateTime,
            value: f64,
        }

        // Convert microsecond timestamps to OffsetDateTime for TimescaleDB queries
        let start_time_ts = start_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });
        let end_time_ts = end_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });

        let rows: Vec<FloatValueRow> = sqlx::query_as(
            r#"
            SELECT time, value FROM float_values
            WHERE sensor_id = $1
            AND ($2::TIMESTAMPTZ IS NULL OR time >= $2)
            AND ($3::TIMESTAMPTZ IS NULL OR time <= $3)
            ORDER BY time ASC
            LIMIT $4
            "#,
        )
        .bind(sensor_id)
        .bind(start_time_ts)
        .bind(end_time_ts)
        .bind(limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            // Convert OffsetDateTime back to SensAppDateTime
            let unix_timestamp =
                row.time.unix_timestamp() as f64 + (row.time.nanosecond() as f64 / 1_000_000_000.0);
            let datetime = SensAppDateTime::from_unix_seconds(unix_timestamp);
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
        use crate::datamodel::{Sample, SensAppDateTime};
        use smallvec::smallvec;

        #[derive(sqlx::FromRow)]
        struct StringValueRow {
            time: sqlx::types::time::OffsetDateTime,
            string_value: String,
        }

        // Convert microsecond timestamps to OffsetDateTime for TimescaleDB queries
        let start_time_ts = start_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });
        let end_time_ts = end_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });

        let rows: Vec<StringValueRow> = sqlx::query_as(
            r#"
            SELECT sv.time, svd.value as string_value
            FROM string_values sv
            JOIN strings_values_dictionary svd ON sv.value = svd.id
            WHERE sv.sensor_id = $1
            AND ($2::TIMESTAMPTZ IS NULL OR sv.time >= $2)
            AND ($3::TIMESTAMPTZ IS NULL OR sv.time <= $3)
            ORDER BY sv.time ASC
            LIMIT $4
            "#,
        )
        .bind(sensor_id)
        .bind(start_time_ts)
        .bind(end_time_ts)
        .bind(limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            // Convert OffsetDateTime back to SensAppDateTime
            let unix_timestamp =
                row.time.unix_timestamp() as f64 + (row.time.nanosecond() as f64 / 1_000_000_000.0);
            let datetime = SensAppDateTime::from_unix_seconds(unix_timestamp);
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
        use crate::datamodel::{Sample, SensAppDateTime};
        use smallvec::smallvec;

        #[derive(sqlx::FromRow)]
        struct BooleanValueRow {
            time: sqlx::types::time::OffsetDateTime,
            value: bool,
        }

        // Convert microsecond timestamps to OffsetDateTime for TimescaleDB queries
        let start_time_ts = start_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });
        let end_time_ts = end_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });

        let rows: Vec<BooleanValueRow> = sqlx::query_as(
            r#"
            SELECT time, value FROM boolean_values
            WHERE sensor_id = $1
            AND ($2::TIMESTAMPTZ IS NULL OR time >= $2)
            AND ($3::TIMESTAMPTZ IS NULL OR time <= $3)
            ORDER BY time ASC
            LIMIT $4
            "#,
        )
        .bind(sensor_id)
        .bind(start_time_ts)
        .bind(end_time_ts)
        .bind(limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            // Convert OffsetDateTime back to SensAppDateTime
            let unix_timestamp =
                row.time.unix_timestamp() as f64 + (row.time.nanosecond() as f64 / 1_000_000_000.0);
            let datetime = SensAppDateTime::from_unix_seconds(unix_timestamp);
            let value = row.value;
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
        use crate::datamodel::{Sample, SensAppDateTime};
        use smallvec::smallvec;

        #[derive(sqlx::FromRow)]
        struct LocationValueRow {
            time: sqlx::types::time::OffsetDateTime,
            latitude: f64,
            longitude: f64,
        }

        // Convert microsecond timestamps to OffsetDateTime for TimescaleDB queries
        let start_time_ts = start_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });
        let end_time_ts = end_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });

        let rows: Vec<LocationValueRow> = sqlx::query_as(
            r#"
            SELECT time, latitude, longitude FROM location_values
            WHERE sensor_id = $1
            AND ($2::TIMESTAMPTZ IS NULL OR time >= $2)
            AND ($3::TIMESTAMPTZ IS NULL OR time <= $3)
            ORDER BY time ASC
            LIMIT $4
            "#,
        )
        .bind(sensor_id)
        .bind(start_time_ts)
        .bind(end_time_ts)
        .bind(limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            // Convert OffsetDateTime back to SensAppDateTime
            let unix_timestamp =
                row.time.unix_timestamp() as f64 + (row.time.nanosecond() as f64 / 1_000_000_000.0);
            let datetime = SensAppDateTime::from_unix_seconds(unix_timestamp);
            let value = geo::Point::new(row.longitude, row.latitude);
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
        use crate::datamodel::{Sample, SensAppDateTime};
        use smallvec::smallvec;

        #[derive(sqlx::FromRow)]
        struct JsonValueRow {
            time: sqlx::types::time::OffsetDateTime,
            value: serde_json::Value,
        }

        // Convert microsecond timestamps to OffsetDateTime for TimescaleDB queries
        let start_time_ts = start_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });
        let end_time_ts = end_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });

        let rows: Vec<JsonValueRow> = sqlx::query_as(
            r#"
            SELECT time, value FROM json_values
            WHERE sensor_id = $1
            AND ($2::TIMESTAMPTZ IS NULL OR time >= $2)
            AND ($3::TIMESTAMPTZ IS NULL OR time <= $3)
            ORDER BY time ASC
            LIMIT $4
            "#,
        )
        .bind(sensor_id)
        .bind(start_time_ts)
        .bind(end_time_ts)
        .bind(limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            // Convert OffsetDateTime back to SensAppDateTime
            let unix_timestamp =
                row.time.unix_timestamp() as f64 + (row.time.nanosecond() as f64 / 1_000_000_000.0);
            let datetime = SensAppDateTime::from_unix_seconds(unix_timestamp);
            let value: serde_json::Value = row.value;
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
        use crate::datamodel::{Sample, SensAppDateTime};
        use smallvec::smallvec;

        #[derive(sqlx::FromRow)]
        struct BlobValueRow {
            time: sqlx::types::time::OffsetDateTime,
            value: Vec<u8>,
        }

        // Convert microsecond timestamps to OffsetDateTime for TimescaleDB queries
        let start_time_ts = start_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });
        let end_time_ts = end_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });

        let rows: Vec<BlobValueRow> = sqlx::query_as(
            r#"
            SELECT time, value FROM blob_values
            WHERE sensor_id = $1
            AND ($2::TIMESTAMPTZ IS NULL OR time >= $2)
            AND ($3::TIMESTAMPTZ IS NULL OR time <= $3)
            ORDER BY time ASC
            LIMIT $4
            "#,
        )
        .bind(sensor_id)
        .bind(start_time_ts)
        .bind(end_time_ts)
        .bind(limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            // Convert OffsetDateTime back to SensAppDateTime
            let unix_timestamp =
                row.time.unix_timestamp() as f64 + (row.time.nanosecond() as f64 / 1_000_000_000.0);
            let datetime = SensAppDateTime::from_unix_seconds(unix_timestamp);
            let value = row.value;
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::Blob(samples))
    }
}
