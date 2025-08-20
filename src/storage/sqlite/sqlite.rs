use super::sqlite_publishers::*;
use super::sqlite_utilities::get_sensor_id_or_create_sensor;
use crate::datamodel::batch::{Batch, SingleSensorBatch};
use crate::datamodel::unit::Unit;
use crate::datamodel::{
    Sample, SensAppDateTime, Sensor, SensorData, SensorType, TypedSamples, Metric,
    sensapp_vec::SensAppLabels,
};
use crate::storage::{StorageInstance, common::sync_with_timeout};
use crate::config;
use anyhow::{Context, Result};
use async_broadcast::Sender;
use async_trait::async_trait;
use rust_decimal::Decimal;
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
        // SQLite doesn't need to do anything special for sync
        // As we use transactions and the WAL mode.
        let config = config::get().context("Failed to get configuration")?;
        sync_with_timeout(&sync_sender, config.storage_sync_timeout_seconds).await
    }

    async fn vacuum(&self) -> Result<()> {
        self.vacuum().await?;
        Ok(())
    }

    async fn list_series(&self, _metric_filter: Option<&str>) -> Result<Vec<crate::datamodel::Sensor>> {
        // Query all sensors with their metadata
        let sensor_rows = sqlx::query!(
            r#"
            SELECT s.sensor_id, s.uuid, s.name, s.type, u.name as unit_name, u.description as unit_description
            FROM sensors s
            LEFT JOIN units u ON s.unit = u.id
            ORDER BY s.created_at ASC, s.uuid ASC
            "#
        )
        .fetch_all(&self.pool)
        .await?;

        let mut sensors = Vec::new();

        for sensor_row in sensor_rows {
            // Parse sensor metadata
            let sensor_uuid =
                Uuid::parse_str(&sensor_row.uuid).context("Failed to parse sensor UUID")?;
            let sensor_type =
                SensorType::from_str(&sensor_row.r#type).context("Failed to parse sensor type")?;
            let unit = sensor_row
                .unit_name
                .map(|name| Unit::new(name, sensor_row.unit_description));

            // Query labels for this sensor
            let labels_rows = sqlx::query!(
                r#"
                SELECT lnd.name as label_name, ldd.description as label_value
                FROM labels l
                JOIN labels_name_dictionary lnd ON l.name = lnd.id
                JOIN labels_description_dictionary ldd ON l.description = ldd.id
                WHERE l.sensor_id = ?
                "#,
                sensor_row.sensor_id
            )
            .fetch_all(&self.pool)
            .await?;

            let mut labels: SensAppLabels = smallvec![];
            for label_row in labels_rows {
                labels.push((label_row.label_name, label_row.label_value));
            }

            let sensor = Sensor::new(
                sensor_uuid,
                sensor_row.name,
                sensor_type,
                unit,
                Some(labels),
            );

            sensors.push(sensor);
        }

        Ok(sensors)
    }

    async fn list_metrics(&self) -> Result<Vec<crate::datamodel::Metric>> {
        // Note: Metrics aggregation not yet implemented for SQLite
        // Current focus is on PostgreSQL backend
        Ok(vec![])
    }

    async fn query_sensor_data(
        &self,
        sensor_name: &str,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<Option<SensorData>> {
        // Query sensor metadata
        let sensor_row = sqlx::query!(
            r#"
            SELECT s.uuid, s.name, s.type, u.name as unit_name, u.description as unit_description
            FROM sensors s
            LEFT JOIN units u ON s.unit = u.id
            WHERE s.name = ?
            "#,
            sensor_name
        )
        .fetch_optional(&self.pool)
        .await?;

        let sensor_row = match sensor_row {
            Some(row) => row,
            None => return Ok(None),
        };

        // Parse sensor metadata
        let sensor_uuid =
            Uuid::parse_str(&sensor_row.uuid).context("Failed to parse sensor UUID")?;
        let sensor_type =
            SensorType::from_str(&sensor_row.r#type).context("Failed to parse sensor type")?;
        let unit = sensor_row
            .unit_name
            .map(|name| Unit::new(name, sensor_row.unit_description));

        // Query labels for this sensor
        let sensor_id_row = sqlx::query!(
            r#"
            SELECT sensor_id FROM sensors WHERE uuid = ?
            "#,
            sensor_row.uuid
        )
        .fetch_one(&self.pool)
        .await?;
        let sensor_id = sensor_id_row.sensor_id;

        let labels_rows = sqlx::query!(
            r#"
            SELECT lnd.name as label_name, ldd.description as label_value
            FROM labels l
            JOIN labels_name_dictionary lnd ON l.name = lnd.id
            JOIN labels_description_dictionary ldd ON l.description = ldd.id
            WHERE l.sensor_id = ?
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
            sensor_row.name,
            sensor_type.clone(),
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
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<Option<SensorData>> {
        // Query sensor metadata by UUID
        let sensor_row = sqlx::query!(
            r#"
            SELECT s.sensor_id, s.uuid, s.name, s.type, u.name as unit_name, u.description as unit_description
            FROM sensors s
            LEFT JOIN units u ON s.unit = u.id
            WHERE s.uuid = ?
            "#,
            sensor_uuid
        )
        .fetch_optional(&self.pool)
        .await?;

        let sensor_row = match sensor_row {
            Some(row) => row,
            None => return Ok(None),
        };

        // Parse sensor metadata
        let parsed_sensor_uuid =
            Uuid::parse_str(&sensor_row.uuid).context("Failed to parse sensor UUID")?;
        let sensor_type =
            SensorType::from_str(&sensor_row.r#type).context("Failed to parse sensor type")?;
        let unit = sensor_row
            .unit_name
            .map(|name| Unit::new(name, sensor_row.unit_description));

        // Query labels for this sensor
        let sensor_id = sensor_row.sensor_id;
        let labels_rows = sqlx::query!(
            r#"
            SELECT lnd.name as label_name, ldd.description as label_value
            FROM labels l
            JOIN labels_name_dictionary lnd ON l.name = lnd.id
            JOIN labels_description_dictionary ldd ON l.description = ldd.id
            WHERE l.sensor_id = ?
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
            parsed_sensor_uuid,
            sensor_row.name,
            sensor_type.clone(),
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

    /// Clean up all test data from the database (SQLite implementation)
    #[cfg(any(test, feature = "test-utils"))]
    async fn cleanup_test_data(&self) -> Result<()> {
        // Simple implementation for SQLite - just delete all data
        // SQLite doesn't have the same foreign key complexity as PostgreSQL when foreign keys are disabled
        let mut tx = self.pool.begin().await?;

        // Delete all data tables
        sqlx::query("DELETE FROM blob_values").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM json_values").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM location_values").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM boolean_values").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM string_values").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM float_values").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM numeric_values").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM integer_values").execute(&mut *tx).await?;

        // Delete metadata tables
        sqlx::query("DELETE FROM labels").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM sensors").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM strings_values_dictionary").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM labels_description_dictionary").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM labels_name_dictionary").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM units").execute(&mut *tx).await?;

        // Reset SQLite sequences (if any)
        sqlx::query("DELETE FROM sqlite_sequence").execute(&mut *tx).await.ok(); // Ignore errors if table doesn't exist

        tx.commit().await.context("Failed to commit test data cleanup transaction")?;

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

    async fn vacuum(&self) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        transaction
            .execute(sqlx::query!(
                r#"
            DELETE FROM integer_values WHERE rowid NOT IN (
                SELECT MIN(rowid) FROM integer_values GROUP BY sensor_id, timestamp_ms, value
            )
            "#
            ))
            .await?;

        transaction
            .execute(sqlx::query!(
                r#"
            DELETE FROM float_values WHERE rowid NOT IN (
                SELECT MIN(rowid) FROM float_values GROUP BY sensor_id, timestamp_ms, value
            )
            "#
            ))
            .await?;

        transaction.commit().await?;

        let vacuum = sqlx::query!("VACUUM");
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
        let rows = sqlx::query!(
            r#"
            SELECT timestamp_ms, value FROM integer_values
            WHERE sensor_id = ?
            AND (? IS NULL OR timestamp_ms >= ?)
            AND (? IS NULL OR timestamp_ms <= ?)
            ORDER BY timestamp_ms ASC
            LIMIT ?
            "#,
            sensor_id,
            start_time,
            start_time,
            end_time,
            end_time,
            limit.unwrap_or(1000) as i64
        )
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_milliseconds(row.timestamp_ms);
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
            WHERE sensor_id = ?
            AND (? IS NULL OR timestamp_ms >= ?)
            AND (? IS NULL OR timestamp_ms <= ?)
            ORDER BY timestamp_ms ASC
            LIMIT ?
            "#,
            sensor_id,
            start_time,
            start_time,
            end_time,
            end_time,
            limit.unwrap_or(1000) as i64
        )
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_milliseconds(row.timestamp_ms);
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
        let rows = sqlx::query!(
            r#"
            SELECT timestamp_ms, value FROM float_values
            WHERE sensor_id = ?
            AND (? IS NULL OR timestamp_ms >= ?)
            AND (? IS NULL OR timestamp_ms <= ?)
            ORDER BY timestamp_ms ASC
            LIMIT ?
            "#,
            sensor_id,
            start_time,
            start_time,
            end_time,
            end_time,
            limit.unwrap_or(1000) as i64
        )
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_milliseconds(row.timestamp_ms);
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
            WHERE sv.sensor_id = ?
            AND (? IS NULL OR sv.timestamp_ms >= ?)
            AND (? IS NULL OR sv.timestamp_ms <= ?)
            ORDER BY sv.timestamp_ms ASC
            LIMIT ?
            "#,
            sensor_id,
            start_time,
            start_time,
            end_time,
            end_time,
            limit.unwrap_or(1000) as i64
        )
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_milliseconds(row.timestamp_ms);
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
            WHERE sensor_id = ?
            AND (? IS NULL OR timestamp_ms >= ?)
            AND (? IS NULL OR timestamp_ms <= ?)
            ORDER BY timestamp_ms ASC
            LIMIT ?
            "#,
            sensor_id,
            start_time,
            start_time,
            end_time,
            end_time,
            limit.unwrap_or(1000) as i64
        )
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_milliseconds(row.timestamp_ms);
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
        let rows = sqlx::query!(
            r#"
            SELECT timestamp_ms, latitude, longitude FROM location_values
            WHERE sensor_id = ?
            AND (? IS NULL OR timestamp_ms >= ?)
            AND (? IS NULL OR timestamp_ms <= ?)
            ORDER BY timestamp_ms ASC
            LIMIT ?
            "#,
            sensor_id,
            start_time,
            start_time,
            end_time,
            end_time,
            limit.unwrap_or(1000) as i64
        )
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_milliseconds(row.timestamp_ms);
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
        let rows = sqlx::query!(
            r#"
            SELECT timestamp_ms, value FROM json_values
            WHERE sensor_id = ?
            AND (? IS NULL OR timestamp_ms >= ?)
            AND (? IS NULL OR timestamp_ms <= ?)
            ORDER BY timestamp_ms ASC
            LIMIT ?
            "#,
            sensor_id,
            start_time,
            start_time,
            end_time,
            end_time,
            limit.unwrap_or(1000) as i64
        )
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_milliseconds(row.timestamp_ms);
            let value: serde_json::Value =
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
        let rows = sqlx::query!(
            r#"
            SELECT timestamp_ms, value FROM blob_values
            WHERE sensor_id = ?
            AND (? IS NULL OR timestamp_ms >= ?)
            AND (? IS NULL OR timestamp_ms <= ?)
            ORDER BY timestamp_ms ASC
            LIMIT ?
            "#,
            sensor_id,
            start_time,
            start_time,
            end_time,
            end_time,
            limit.unwrap_or(1000) as i64
        )
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_milliseconds(row.timestamp_ms);
            let value = row.value;
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::Blob(samples))
    }
}
