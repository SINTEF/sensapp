// Core storage traits and factory - always available
use anyhow::Result;
use async_trait::async_trait;
use std::fmt::Debug;

#[async_trait]
pub trait StorageInstance: Send + Sync + Debug {
    #[allow(dead_code)]
    async fn create_or_migrate(&self) -> Result<()>;
    async fn publish(
        &self,
        batch: std::sync::Arc<crate::datamodel::batch::Batch>,
        sync_sender: async_broadcast::Sender<()>,
    ) -> Result<()>;
    async fn sync(&self, sync_sender: async_broadcast::Sender<()>) -> Result<()>;
    #[allow(dead_code)]
    async fn vacuum(&self) -> Result<()>;

    async fn list_sensors(&self) -> Result<Vec<String>>;
}

pub mod storage_factory;

// Storage backends - conditionally compiled based on features
#[cfg(feature = "postgres")]
pub mod postgresql;

#[cfg(feature = "sqlite")]
pub mod sqlite;

#[cfg(feature = "timescaledb")]
pub mod timescaledb;

#[cfg(feature = "duckdb")]
pub mod duckdb;

#[cfg(feature = "bigquery")]
pub mod bigquery;

#[cfg(feature = "rrdcached")]
pub mod rrdcached;
