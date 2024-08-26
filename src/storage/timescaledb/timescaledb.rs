use super::{
    super::storage::StorageInstance, timescaledb_publishers::*,
    timescaledb_utilities::get_sensor_id_or_create_sensor,
};
use crate::crud::list_cursor::ListCursor;
use crate::crud::viewmodel::sensor_viewmodel::SensorViewModel;
use crate::datamodel::matchers::SensorMatcher;
use crate::datamodel::{batch::Batch, TypedSamples};
use anyhow::{Context, Result};
use async_broadcast::Sender;
use async_trait::async_trait;
use sqlx::{postgres::PgConnectOptions, PgPool};
use std::time::Duration;
use std::{str::FromStr, sync::Arc};
use tokio::time::timeout;

#[derive(Debug)]
pub struct TimeScaleDBStorage {
    pool: PgPool,
}

impl TimeScaleDBStorage {
    pub async fn connect(connection_string: &str) -> Result<Self> {
        let connect_options = PgConnectOptions::from_str(connection_string)
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
        // timescaledb doesn't need to do anything special for sync
        // as we use transaction
        if sync_sender.receiver_count() > 0 && !sync_sender.is_closed() {
            let _ = timeout(Duration::from_secs(15), sync_sender.broadcast(())).await?;
        }
        Ok(())
    }

    async fn vacuum(&self) -> Result<()> {
        self.vacuum().await?;
        Ok(())
    }

    async fn list_sensors(
        &self,
        matcher: SensorMatcher,
        cursor: ListCursor,
        limit: usize,
    ) -> Result<(Vec<SensorViewModel>, Option<ListCursor>)> {
        super::super::postgresql::postgresql_crud::list_sensors(&self.pool, matcher, cursor, limit)
            .await
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
}
