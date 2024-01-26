use crate::datamodel::batch::{Batch, Sample, SingleSensorBatch, TypedSamples};
use crate::datamodel::SensorType;
use crate::storage::storage::{GenericStorage, SensorData, StorageInstance};
use anyhow::{Context, Result};
use async_broadcast::Sender;
use async_trait::async_trait;
use cached::proc_macro::once;
use sqlx::{prelude::*, Sqlite, Transaction};
use sqlx::{sqlite::SqliteConnectOptions, SqlitePool};
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;
use tokio::time::timeout;
use uuid::Uuid;

// SQLite implementation
#[derive(Debug)]
pub struct SqliteStorage {
    pool: SqlitePool,
}

#[async_trait]
impl GenericStorage for SqliteStorage {
    type StorageInstance = Self;

    async fn connect(connection_string: &str) -> Result<Self::StorageInstance> {
        let connect_options = SqliteConnectOptions::from_str(connection_string)
            .context("Failed to create sqlite connection options")?
            // Create the database file if it doesn't exist
            .create_if_missing(true)
            // The Wall mode should perform better for SensApp
            // It is the default in sqlx, but we want to make sure it stays that way
            .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal)
            // Foreign keys have a performance impact, they are disabled by default
            // in SQLite, but we want to make sure they stay disabled.
            .foreign_keys(false);

        let pool = sqlx::SqlitePool::connect_with(connect_options)
            .await
            .context("Failed to create sqlite pool")?;

        Ok(SqliteStorage { pool })
    }

    async fn create_or_migrate(&self) -> Result<()> {
        // Implement schema creation or migration logic here
        sqlx::migrate!("src/storage/sqlite/migrations")
            .run(&self.pool)
            .await
            .context("Failed to migrate database")?;

        Ok(())
    }

    async fn publish_batch(&self, batch: crate::datamodel::batch::Batch) -> Result<()> {
        // Implement batch publishing logic here
        Ok(())
    }
}

#[async_trait]
impl StorageInstance for SqliteStorage {
    async fn create_sensor(&self, sensor_data: &SensorData) -> Result<()> {
        // Implement sensor creation logic here
        Ok(())
    }

    async fn publish(&self, batch: Arc<Batch>, sync_sender: Sender<()>) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        for single_sensor_batch in batch.sensor_batches.as_ref() {
            self.publish_single_sensor_batch(&mut transaction, &single_sensor_batch)
                .await?;
        }
        transaction.commit().await?;
        self.sync(sync_sender).await?;
        Ok(())
    }

    async fn sync(&self, sync_sender: Sender<()>) -> Result<()> {
        // Implement sync logic here
        //println!("Syncing");
        //println!("Receiver count: {}", sync_sender.receiver_count());
        if sync_sender.receiver_count() > 0 && !sync_sender.is_closed() {
            let _ = timeout(Duration::from_secs(5), sync_sender.broadcast(())).await?;
        }
        Ok(())
    }

    async fn vacuum(&self) -> Result<()> {
        self.vacuum().await?;
        Ok(())
    }
}

#[once(time = 120, result = true, sync_writes = true)]
async fn once_get_sensor_id(
    pool: &SqlitePool,
    sensor_uuid: Uuid,
    sensor_name: String,
    sensor_type: SensorType,
) -> Result<i64> {
    println!("aaah");
    let uuid_string = sensor_uuid.to_string();
    let sensor_id = sqlx::query!(
        r#"
            SELECT sensor_id FROM sensors WHERE uuid = ?
            "#,
        uuid_string
    )
    .fetch_optional(pool)
    .await?
    .map(|row| row.sensor_id);

    // If the sensor exists, it's returned
    if let Some(Some(sensor_id)) = sensor_id {
        return Ok(sensor_id);
    }

    let sensor_type_string = sensor_type.to_string();

    let create_sensor_query = sqlx::query!(
        r#"
            INSERT INTO sensors (uuid, name, type)
            VALUES (?, ?, ?)
            "#,
        uuid_string,
        sensor_name,
        sensor_type_string
    );

    // Execute the query
    let sensor_id = create_sensor_query.execute(pool).await?.last_insert_rowid();

    Ok(sensor_id)
}

impl SqliteStorage {
    async fn get_sensor_id(
        &self,
        sensor_uuid: Uuid,
        sensor_name: String,
        sensor_type: SensorType,
    ) -> Result<i64> {
        once_get_sensor_id(&self.pool, sensor_uuid, sensor_name, sensor_type).await
    }

    async fn publish_single_sensor_batch(
        &self,
        mut transaction: &mut Transaction<'_, Sqlite>,
        single_sensor_batch: &SingleSensorBatch,
    ) -> Result<()> {
        match single_sensor_batch.samples.as_ref() {
            TypedSamples::Integer(samples) => {
                let sensor_id = self
                    .get_sensor_id(
                        single_sensor_batch.sensor_uuid,
                        single_sensor_batch.sensor_name.clone(),
                        SensorType::Integer,
                    )
                    .await?;
                self.publish_integer_values(&mut transaction, sensor_id, &samples)
                    .await?;
            }
            _ => {
                return Err(anyhow::anyhow!(
                    "Unsupported sample type: {:?}",
                    single_sensor_batch.samples
                ))
            }
        }
        Ok(())
    }

    async fn publish_integer_values(
        &self,
        transaction: &mut Transaction<'_, Sqlite>,
        sensor_id: i64,
        values: &[Sample<i64>],
    ) -> Result<()> {
        for value in values {
            let query = sqlx::query!(
                r#"
                INSERT INTO integer_values (sensor_id, timestamp_ms, value)
                VALUES (?, ?, ?)
                "#,
                sensor_id,
                value.timestamp_ms,
                value.value
            );
            transaction.execute(query).await?;
        }
        Ok(())
    }

    async fn publish_float_values(&self, sensor_id: i64, values: &[Sample<f64>]) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        for value in values {
            let query = sqlx::query!(
                r#"
                INSERT INTO float_values (sensor_id, timestamp_ms, value)
                VALUES (?, ?, ?)
                "#,
                sensor_id,
                value.timestamp_ms,
                value.value
            );
            transaction.execute(query).await?;
        }
        transaction.commit().await?;
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
