use super::sqlite_publishers::*;
use super::sqlite_utilities::get_sensor_id_or_create_sensor;
use crate::datamodel::TypedSamples;
use crate::datamodel::batch::{Batch, SingleSensorBatch};
use crate::storage::StorageInstance;
use anyhow::{Context, Result};
use async_broadcast::Sender;
use async_trait::async_trait;
use sqlx::{Sqlite, Transaction, prelude::*};
use sqlx::{SqlitePool, sqlite::SqliteConnectOptions};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;

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
        if sync_sender.receiver_count() > 0 && !sync_sender.is_closed() {
            let _ = timeout(Duration::from_secs(15), sync_sender.broadcast(())).await?;
        }
        Ok(())
    }

    async fn vacuum(&self) -> Result<()> {
        self.vacuum().await?;
        Ok(())
    }

    async fn list_sensors(&self) -> Result<Vec<String>> {
        unimplemented!();
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
}
