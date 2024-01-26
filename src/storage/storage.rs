use anyhow::Result;
use async_broadcast::Sender;
use async_trait::async_trait;
use std::sync::Arc;

use crate::datamodel::batch::Batch;

#[async_trait]
pub trait GenericStorage {
    type StorageInstance: StorageInstance + Sync + Send;

    async fn connect(connection_string: &str) -> Result<Self::StorageInstance>;
    async fn create_or_migrate(&self) -> Result<()>;
    async fn publish_batch(&self, batch: Batch) -> Result<()>;
}

#[async_trait]
pub trait StorageInstance {
    async fn create_sensor(&self, sensor_data: &SensorData) -> Result<()>;
    async fn publish(&self, batch: Arc<Batch>, sync_sender: Sender<()>) -> Result<()>;
    async fn sync(&self, sync_sender: Sender<()>) -> Result<()>;
    async fn vacuum(&self) -> Result<()>;
}

pub struct SensorData {
    // Define sensor data structure here
}

pub struct SensorSample {
    // time series sample
    pub timestamp_ms: i64,
    pub value: f64,
}

#[derive(Debug)]
enum GenericStorages {
    Sqlite(crate::storage::sqlite::SqliteStorage),
    Postgres(crate::storage::postgresql::PostgresStorage),
}

#[derive(Debug)]
pub struct Storage {
    generic_storage: GenericStorages,
}

impl Storage {
    pub async fn publish_batch(&self, batch: Batch) -> Result<()> {
        match self.generic_storage {
            GenericStorages::Sqlite(ref sqlite_storage) => {
                sqlite_storage.publish_batch(batch).await?
            }
            GenericStorages::Postgres(ref postgres_storage) => {
                postgres_storage.publish_batch(batch).await?
            }
        }
        // Implement batch publishing logic here
        Ok(())
    }
}
