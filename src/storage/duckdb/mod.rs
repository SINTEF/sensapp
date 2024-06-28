use crate::datamodel::batch::{Batch, SingleSensorBatch};
use crate::datamodel::TypedSamples;
use anyhow::{Context, Result};
use async_broadcast::Sender;
use async_trait::async_trait;
use duckdb::Connection;
use duckdb_publishers::*;
use duckdb_utilities::get_sensor_id_or_create_sensor;
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::Mutex;
use tokio::task::spawn_blocking;
use tokio::time::timeout;

use super::storage::{GenericStorage, StorageInstance};

mod duckdb_publishers;
mod duckdb_utilities;

#[derive(Debug)]
pub struct DuckDBStorage {
    connection: Arc<Mutex<Connection>>,
}

const INIT_SQL: &str = include_str!("./migrations/20240223133248_init.sql");

#[async_trait]
impl GenericStorage for DuckDBStorage {
    type StorageInstance = Self;

    async fn connect(connection_string: &str) -> Result<Self::StorageInstance> {
        let connection =
            Connection::open(connection_string).context("Failed to open DuckDB connection")?;
        let connection = Arc::new(Mutex::new(connection));
        Ok(DuckDBStorage { connection })
    }

    async fn create_or_migrate(&self) -> Result<()> {
        let connection = self.connection.lock().await;
        connection
            .execute_batch(INIT_SQL)
            .context("Failed to initialise database")?;
        Ok(())
    }
}

#[async_trait]
impl StorageInstance for DuckDBStorage {
    async fn publish(&self, batch: Arc<Batch>, sync_sender: Sender<()>) -> Result<()> {
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
        let mut connection = self.connection.lock().await;
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
