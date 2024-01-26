use std::sync::Arc;

use super::storage::{GenericStorage, SensorData, StorageInstance};
use crate::datamodel::batch::Batch;
use anyhow::Result;
use async_broadcast::Sender;
use async_trait::async_trait;

#[derive(Debug)]
pub struct PostgresStorage {}

#[async_trait]
impl GenericStorage for PostgresStorage {
    type StorageInstance = Self;

    async fn connect(connection_string: &str) -> Result<Self::StorageInstance> {
        Ok(Self::StorageInstance {})
    }
    async fn create_or_migrate(&self) -> Result<()> {
        Ok(())
    }
    async fn publish_batch(&self, batch: crate::datamodel::batch::Batch) -> Result<()> {
        Ok(())
    }
}

#[async_trait]
impl StorageInstance for PostgresStorage {
    async fn create_sensor(&self, sensor_data: &SensorData) -> Result<()> {
        // Implement sensor creation logic here
        Ok(())
    }
    async fn publish(&self, batch: Arc<Batch>, sync_sender: Sender<()>) -> Result<()> {
        // Implement batch publishing logic here
        Ok(())
    }

    async fn sync(&self, sync_sender: Sender<()>) -> Result<()> {
        // Implement sync logic here
        Ok(())
    }

    async fn vacuum(&self) -> Result<()> {
        // TODO: Implement vacuum logic here
        Ok(())
    }
}
