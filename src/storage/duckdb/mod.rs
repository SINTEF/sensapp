use crate::datamodel::TypedSamples;
use crate::datamodel::batch::{Batch, SingleSensorBatch};
use crate::config;
use anyhow::{Context, Result, bail};
use async_broadcast::Sender;
use async_trait::async_trait;
use duckdb::Connection;
use duckdb_publishers::*;
use duckdb_utilities::get_sensor_id_or_create_sensor;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio::task::spawn_blocking;

use super::{StorageInstance, common::sync_with_timeout};

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
        // DuckDB doesn't need to do anything special for sync
        // As we use transactions
        let config = config::get().context("Failed to get configuration")?;
        sync_with_timeout(&sync_sender, config.storage_sync_timeout_seconds).await
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

    async fn list_series(&self, _metric_filter: Option<&str>) -> Result<Vec<crate::datamodel::Sensor>> {
        unimplemented!();
    }

    async fn query_sensor_data(
        &self,
        _sensor_name: &str,
        _start_time: Option<i64>,
        _end_time: Option<i64>,
        _limit: Option<usize>,
    ) -> Result<Option<crate::datamodel::SensorData>> {
        unimplemented!("DuckDB sensor data querying not yet implemented");
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
        connection.execute("DELETE FROM strings_values_dictionary", []).ok();
        connection.execute("DELETE FROM labels_description_dictionary", []).ok();
        connection.execute("DELETE FROM labels_name_dictionary", []).ok();
        connection.execute("DELETE FROM units", []).ok();

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
