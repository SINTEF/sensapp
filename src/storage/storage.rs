use anyhow::{bail, Result};
use async_broadcast::Sender;
use async_trait::async_trait;
use sqlx::Sqlite;
use std::sync::Arc;

use crate::datamodel::batch::Batch;

#[async_trait]
//#[enum_delegate::register]
pub trait StorageInstance: Send + Sync {
    async fn create_or_migrate(&self) -> Result<()>;
    async fn publish(
        &self,
        batch: std::sync::Arc<crate::datamodel::batch::Batch>,
        sync_sender: async_broadcast::Sender<()>,
    ) -> Result<()>;
    async fn sync(&self, sync_sender: async_broadcast::Sender<()>) -> Result<()>;
    async fn vacuum(&self) -> Result<()>;
}

/*#[derive(Debug)]
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
}*/
