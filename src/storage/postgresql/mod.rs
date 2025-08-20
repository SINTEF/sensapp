use super::{StorageInstance, StorageError, common::sync_with_timeout};
use crate::datamodel::{TypedSamples, batch::Batch, Sensor, SensorData, SensorType, Sample, SensAppDateTime, Metric};
use crate::datamodel::{sensapp_vec::SensAppLabels, unit::Unit};
use crate::config;
use anyhow::{Context, Result};
use async_broadcast::Sender;
use async_trait::async_trait;
use geo::Point;
use rust_decimal::Decimal;
use serde_json::Value as JsonValue;
use smallvec::smallvec;
use sqlx::{PgPool, postgres::PgConnectOptions};
use std::{str::FromStr, sync::Arc};
use uuid::Uuid;

pub mod postgresql_publishers;
pub mod postgresql_utilities;

use postgresql_publishers::*;
use postgresql_utilities::get_sensor_id_or_create_sensor;

#[derive(Debug)]
pub struct PostgresStorage {
    pool: PgPool,
}

impl PostgresStorage {
    pub async fn connect(connection_string: &str) -> Result<Self> {
        let connect_options = PgConnectOptions::from_str(connection_string)
            .context("Failed to create postgres connection options")?;

        let pool = PgPool::connect_with(connect_options)
            .await
            .context("Failed to create postgres pool")?;

        Ok(Self { pool })
    }
}

#[async_trait]
impl StorageInstance for PostgresStorage {
    async fn create_or_migrate(&self) -> Result<()> {
        sqlx::migrate!("src/storage/postgresql/migrations")
            .run(&self.pool)
            .await
            .context("Failed to migrate database")?;

        Ok(())
    }
    async fn publish(&self, batch: Arc<Batch>, sync_sender: Sender<()>) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        for single_sensor_batch in batch.sensors.as_ref() {
            self.publish_single_sensor_batch(&mut transaction, single_sensor_batch)
                .await?;
        }
        transaction.commit().await?;
        self.sync(sync_sender).await?;
        Ok(())
    }

    async fn sync(&self, sync_sender: Sender<()>) -> Result<()> {
        // PostgreSQL doesn't need to do anything special for sync
        // as we use transaction
        let config = config::get().context("Failed to get configuration")?;
        sync_with_timeout(&sync_sender, config.storage_sync_timeout_seconds).await
    }

    async fn vacuum(&self) -> Result<()> {
        self.vacuum().await?;
        Ok(())
    }

    async fn list_series(&self) -> Result<Vec<crate::datamodel::Sensor>> {

        // Query all sensors with their metadata using the catalog view
        let sensor_rows = sqlx::query!(
            r#"
            SELECT sensor_id, uuid, name, type, unit_name, unit_description
            FROM sensor_catalog_view
            ORDER BY uuid ASC
            "#
        )
        .fetch_all(&self.pool)
        .await?;

        let mut sensors = Vec::new();

        for sensor_row in sensor_rows {
            // Parse sensor metadata with improved error handling
            let sensor_uuid = sensor_row.uuid.ok_or_else(|| {
                StorageError::missing_field("UUID", None, sensor_row.name.as_deref())
            }).map_err(anyhow::Error::from)?;
            
            let sensor_name = sensor_row.name.ok_or_else(|| {
                StorageError::missing_field("name", Some(sensor_uuid), None)
            }).map_err(anyhow::Error::from)?;
            
            let sensor_type_str = sensor_row.r#type.ok_or_else(|| {
                StorageError::missing_field("type", Some(sensor_uuid), Some(&sensor_name))
            }).map_err(anyhow::Error::from)?;
            
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

            // Query labels for this sensor with proper error context
            let sensor_id = sensor_row.sensor_id.ok_or_else(|| {
                anyhow::Error::from(StorageError::missing_field(
                    "sensor_id", 
                    Some(sensor_uuid), 
                    Some(&sensor_name)
                ))
            })?;
            
            let labels_rows = sqlx::query!(
                r#"
                SELECT lnd.name as label_name, ldd.description as label_value
                FROM labels l
                JOIN labels_name_dictionary lnd ON l.name = lnd.id
                JOIN labels_description_dictionary ldd ON l.description = ldd.id
                WHERE l.sensor_id = $1
                "#,
                sensor_id
            )
            .fetch_all(&self.pool)
            .await
            .with_context(|| format!("Failed to query labels for sensor UUID={} name='{}'", sensor_uuid, sensor_name))?;

            let mut labels: SensAppLabels = smallvec![];
            for label_row in labels_rows {
                labels.push((label_row.label_name, label_row.label_value));
            }

            let sensor = Sensor::new(
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
        // Query metrics summary using the view
        let metrics_rows = sqlx::query!(
            r#"
            SELECT metric_name, type, unit_name, unit_description, series_count, label_keys
            FROM metrics_summary
            ORDER BY metric_name ASC
            "#
        )
        .fetch_all(&self.pool)
        .await?;

        let mut metrics = Vec::new();

        for metrics_row in metrics_rows {
            let metric_name = metrics_row.metric_name.ok_or_else(|| {
                anyhow::Error::from(StorageError::missing_field("metric_name", None, None))
            })?;
            
            let sensor_type_str = metrics_row.r#type.ok_or_else(|| {
                anyhow::Error::from(StorageError::missing_field("type", None, Some(&metric_name)))
            })?;
            
            let sensor_type = SensorType::from_str(&sensor_type_str).map_err(|e| {
                anyhow::Error::from(StorageError::invalid_data_format(
                    &format!("Failed to parse sensor type '{}': {}", sensor_type_str, e),
                    None,
                    Some(&metric_name),
                ))
            })?;
            
            let unit = match (metrics_row.unit_name, metrics_row.unit_description) {
                (Some(name), description) => Some(Unit::new(name, description)),
                _ => None,
            };

            let series_count = metrics_row.series_count.ok_or_else(|| {
                anyhow::Error::from(StorageError::missing_field(
                    "series_count", 
                    None, 
                    Some(&metric_name)
                ))
            })?;
            
            // Handle optional label_keys array
            let label_keys = metrics_row.label_keys.unwrap_or_default();

            let metric = Metric::new(
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
        sensor_name: &str,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<Option<crate::datamodel::SensorData>> {

        // Query sensor metadata by name using the catalog view
        let sensor_row = sqlx::query!(
            r#"
            SELECT sensor_id, uuid, name, type, unit_name, unit_description
            FROM sensor_catalog_view
            WHERE name = $1
            "#,
            sensor_name
        )
        .fetch_optional(&self.pool)
        .await?;

        let sensor_row = match sensor_row {
            Some(row) => row,
            None => return Ok(None),
        };

        // Parse sensor metadata with improved error handling
        let sensor_uuid = sensor_row.uuid.ok_or_else(|| {
            anyhow::Error::from(StorageError::missing_field("UUID", None, sensor_row.name.as_deref()))
        })?;
        
        let sensor_name = sensor_row.name.ok_or_else(|| {
            anyhow::Error::from(StorageError::missing_field("name", Some(sensor_uuid), None))
        })?;
        
        let sensor_type_str = sensor_row.r#type.ok_or_else(|| {
            anyhow::Error::from(StorageError::missing_field("type", Some(sensor_uuid), Some(&sensor_name)))
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
                Some(&sensor_name)
            ))
        })?;
        let labels_rows = sqlx::query!(
            r#"
            SELECT lnd.name as label_name, ldd.description as label_value
            FROM labels l
            JOIN labels_name_dictionary lnd ON l.name = lnd.id
            JOIN labels_description_dictionary ldd ON l.description = ldd.id
            WHERE l.sensor_id = $1
            "#,
            sensor_id
        )
        .fetch_all(&self.pool)
        .await?;

        let mut labels: SensAppLabels = smallvec![];
        for label_row in labels_rows {
            labels.push((label_row.label_name, label_row.label_value));
        }

        let sensor = Sensor::new(
            sensor_uuid,
            sensor_name,
            sensor_type,
            unit,
            Some(labels),
        );

        // Query samples based on sensor type
        let samples = match sensor_type {
            SensorType::Integer => {
                self.query_integer_samples(sensor_id, start_time, end_time, limit)
                    .await?
            }
            SensorType::Numeric => {
                self.query_numeric_samples(sensor_id, start_time, end_time, limit)
                    .await?
            }
            SensorType::Float => {
                self.query_float_samples(sensor_id, start_time, end_time, limit)
                    .await?
            }
            SensorType::String => {
                self.query_string_samples(sensor_id, start_time, end_time, limit)
                    .await?
            }
            SensorType::Boolean => {
                self.query_boolean_samples(sensor_id, start_time, end_time, limit)
                    .await?
            }
            SensorType::Location => {
                self.query_location_samples(sensor_id, start_time, end_time, limit)
                    .await?
            }
            SensorType::Json => {
                self.query_json_samples(sensor_id, start_time, end_time, limit)
                    .await?
            }
            SensorType::Blob => {
                self.query_blob_samples(sensor_id, start_time, end_time, limit)
                    .await?
            }
        };

        Ok(Some(SensorData::new(sensor, samples)))
    }

    async fn query_sensor_data_by_uuid(
        &self,
        sensor_uuid: &str,
        _start_time: Option<i64>,
        _end_time: Option<i64>,
        _limit: Option<usize>,
    ) -> Result<Option<crate::datamodel::SensorData>> {

        // Parse UUID
        let parsed_uuid = Uuid::from_str(sensor_uuid).context("Failed to parse sensor UUID")?;

        // Query sensor metadata by UUID using the catalog view
        let sensor_row = sqlx::query!(
            r#"
            SELECT sensor_id, uuid, name, type, unit_name, unit_description
            FROM sensor_catalog_view
            WHERE uuid = $1
            "#,
            parsed_uuid
        )
        .fetch_optional(&self.pool)
        .await?;

        let sensor_row = match sensor_row {
            Some(row) => row,
            None => return Ok(None),
        };

        // Parse sensor metadata with improved error handling
        let sensor_uuid = sensor_row.uuid.ok_or_else(|| {
            anyhow::Error::from(StorageError::missing_field("UUID", None, sensor_row.name.as_deref()))
        })?;
        
        let sensor_name = sensor_row.name.ok_or_else(|| {
            anyhow::Error::from(StorageError::missing_field("name", Some(sensor_uuid), None))
        })?;
        
        let sensor_type_str = sensor_row.r#type.ok_or_else(|| {
            anyhow::Error::from(StorageError::missing_field("type", Some(sensor_uuid), Some(&sensor_name)))
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
                Some(&sensor_name)
            ))
        })?;
        let labels_rows = sqlx::query!(
            r#"
            SELECT lnd.name as label_name, ldd.description as label_value
            FROM labels l
            JOIN labels_name_dictionary lnd ON l.name = lnd.id
            JOIN labels_description_dictionary ldd ON l.description = ldd.id
            WHERE l.sensor_id = $1
            "#,
            sensor_id
        )
        .fetch_all(&self.pool)
        .await?;

        let mut labels: SensAppLabels = smallvec![];
        for label_row in labels_rows {
            labels.push((label_row.label_name, label_row.label_value));
        }

        let sensor = Sensor::new(
            sensor_uuid,
            sensor_name,
            sensor_type,
            unit,
            Some(labels),
        );

        // Query samples based on sensor type
        let samples = match sensor.sensor_type {
            SensorType::Integer => {
                self.query_integer_samples(sensor_id, _start_time, _end_time, _limit)
                    .await?
            }
            SensorType::Numeric => {
                self.query_numeric_samples(sensor_id, _start_time, _end_time, _limit)
                    .await?
            }
            SensorType::Float => {
                self.query_float_samples(sensor_id, _start_time, _end_time, _limit)
                    .await?
            }
            SensorType::String => {
                self.query_string_samples(sensor_id, _start_time, _end_time, _limit)
                    .await?
            }
            SensorType::Boolean => {
                self.query_boolean_samples(sensor_id, _start_time, _end_time, _limit)
                    .await?
            }
            SensorType::Location => {
                self.query_location_samples(sensor_id, _start_time, _end_time, _limit)
                    .await?
            }
            SensorType::Json => {
                self.query_json_samples(sensor_id, _start_time, _end_time, _limit)
                    .await?
            }
            SensorType::Blob => {
                self.query_blob_samples(sensor_id, _start_time, _end_time, _limit)
                    .await?
            }
        };

        Ok(Some(SensorData::new(sensor, samples)))
    }

    /// Clean up all test data from the database
    /// This removes all sensor data but keeps the schema intact
    /// Uses DELETE statements in dependency order to avoid foreign key conflicts
    #[cfg(any(test, feature = "test-utils"))]
    async fn cleanup_test_data(&self) -> Result<()> {
        // Use a transaction to ensure atomicity
        let mut tx = self.pool.begin().await?;

        // Step 1: Delete all value tables (they reference sensors but nothing references them)
        sqlx::query("DELETE FROM blob_values").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM json_values").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM location_values").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM boolean_values").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM string_values").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM float_values").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM numeric_values").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM integer_values").execute(&mut *tx).await?;

        // Step 2: Delete labels (references sensors and dictionaries)
        sqlx::query("DELETE FROM labels").execute(&mut *tx).await?;

        // Step 3: Delete sensors (references units, but we'll preserve units for tests)
        sqlx::query("DELETE FROM sensors").execute(&mut *tx).await?;

        // Step 4: Delete dictionary tables (but preserve units for test data)
        sqlx::query("DELETE FROM strings_values_dictionary").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM labels_description_dictionary").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM labels_name_dictionary").execute(&mut *tx).await?;

        // Note: We preserve the units table to avoid foreign key violations in tests
        // But we need to ensure common test units exist
        
        // Insert common test units if they don't exist (using ON CONFLICT DO NOTHING for idempotency)
        sqlx::query("INSERT INTO units (name, description) VALUES ('°C', 'Celsius') ON CONFLICT (name) DO NOTHING")
            .execute(&mut *tx).await?;
        sqlx::query("INSERT INTO units (name, description) VALUES ('%', 'Percentage') ON CONFLICT (name) DO NOTHING")
            .execute(&mut *tx).await?;
        sqlx::query("INSERT INTO units (name, description) VALUES ('m', 'Meters') ON CONFLICT (name) DO NOTHING")
            .execute(&mut *tx).await?;
        sqlx::query("INSERT INTO units (name, description) VALUES ('kg', 'Kilograms') ON CONFLICT (name) DO NOTHING")
            .execute(&mut *tx).await?;

        // Reset sequences for clean test data
        sqlx::query("ALTER SEQUENCE sensors_sensor_id_seq RESTART WITH 1").execute(&mut *tx).await?;
        sqlx::query("ALTER SEQUENCE strings_values_dictionary_id_seq RESTART WITH 1").execute(&mut *tx).await?;
        sqlx::query("ALTER SEQUENCE labels_description_dictionary_id_seq RESTART WITH 1").execute(&mut *tx).await?;
        sqlx::query("ALTER SEQUENCE labels_name_dictionary_id_seq RESTART WITH 1").execute(&mut *tx).await?;

        tx.commit().await.context("Failed to commit test data cleanup transaction")?;

        Ok(())
    }
}

impl PostgresStorage {
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

        let rows = sqlx::query!(
            r#"
            SELECT timestamp_ms, value FROM integer_values 
            WHERE sensor_id = $1 
            AND ($2::BIGINT IS NULL OR timestamp_ms >= $2)
            AND ($3::BIGINT IS NULL OR timestamp_ms <= $3)
            ORDER BY timestamp_ms ASC
            LIMIT $4
            "#,
            sensor_id,
            start_time,
            end_time,
            limit.unwrap_or(1000) as i64
        )
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_milliseconds(row.timestamp_ms as f64);
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

        let rows = sqlx::query!(
            r#"
            SELECT timestamp_ms, value FROM numeric_values 
            WHERE sensor_id = $1 
            AND ($2::BIGINT IS NULL OR timestamp_ms >= $2)
            AND ($3::BIGINT IS NULL OR timestamp_ms <= $3)
            ORDER BY timestamp_ms ASC
            LIMIT $4
            "#,
            sensor_id,
            start_time,
            end_time,
            limit.unwrap_or(1000) as i64
        )
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_milliseconds(row.timestamp_ms as f64);
            let value = Decimal::from_str(&row.value.to_string())
                .context("Failed to parse decimal value")?;
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

        let rows = sqlx::query!(
            r#"
            SELECT timestamp_ms, value FROM float_values 
            WHERE sensor_id = $1 
            AND ($2::BIGINT IS NULL OR timestamp_ms >= $2)
            AND ($3::BIGINT IS NULL OR timestamp_ms <= $3)
            ORDER BY timestamp_ms ASC
            LIMIT $4
            "#,
            sensor_id,
            start_time,
            end_time,
            limit.unwrap_or(1000) as i64
        )
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_milliseconds(row.timestamp_ms as f64);
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

        let rows = sqlx::query!(
            r#"
            SELECT sv.timestamp_ms, svd.value as string_value
            FROM string_values sv
            JOIN strings_values_dictionary svd ON sv.value = svd.id
            WHERE sv.sensor_id = $1 
            AND ($2::BIGINT IS NULL OR sv.timestamp_ms >= $2)
            AND ($3::BIGINT IS NULL OR sv.timestamp_ms <= $3)
            ORDER BY sv.timestamp_ms ASC
            LIMIT $4
            "#,
            sensor_id,
            start_time,
            end_time,
            limit.unwrap_or(1000) as i64
        )
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_milliseconds(row.timestamp_ms as f64);
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

        let rows = sqlx::query!(
            r#"
            SELECT timestamp_ms, value FROM boolean_values 
            WHERE sensor_id = $1 
            AND ($2::BIGINT IS NULL OR timestamp_ms >= $2)
            AND ($3::BIGINT IS NULL OR timestamp_ms <= $3)
            ORDER BY timestamp_ms ASC
            LIMIT $4
            "#,
            sensor_id,
            start_time,
            end_time,
            limit.unwrap_or(1000) as i64
        )
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_milliseconds(row.timestamp_ms as f64);
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

        let rows = sqlx::query!(
            r#"
            SELECT timestamp_ms, latitude, longitude FROM location_values 
            WHERE sensor_id = $1 
            AND ($2::BIGINT IS NULL OR timestamp_ms >= $2)
            AND ($3::BIGINT IS NULL OR timestamp_ms <= $3)
            ORDER BY timestamp_ms ASC
            LIMIT $4
            "#,
            sensor_id,
            start_time,
            end_time,
            limit.unwrap_or(1000) as i64
        )
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_milliseconds(row.timestamp_ms as f64);
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

        let rows = sqlx::query!(
            r#"
            SELECT timestamp_ms, value FROM json_values 
            WHERE sensor_id = $1 
            AND ($2::BIGINT IS NULL OR timestamp_ms >= $2)
            AND ($3::BIGINT IS NULL OR timestamp_ms <= $3)
            ORDER BY timestamp_ms ASC
            LIMIT $4
            "#,
            sensor_id,
            start_time,
            end_time,
            limit.unwrap_or(1000) as i64
        )
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_milliseconds(row.timestamp_ms as f64);
            let value: JsonValue = row.value;
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

        let rows = sqlx::query!(
            r#"
            SELECT timestamp_ms, value FROM blob_values 
            WHERE sensor_id = $1 
            AND ($2::BIGINT IS NULL OR timestamp_ms >= $2)
            AND ($3::BIGINT IS NULL OR timestamp_ms <= $3)
            ORDER BY timestamp_ms ASC
            LIMIT $4
            "#,
            sensor_id,
            start_time,
            end_time,
            limit.unwrap_or(1000) as i64
        )
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_milliseconds(row.timestamp_ms as f64);
            let value = row.value;
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::Blob(samples))
    }

}
