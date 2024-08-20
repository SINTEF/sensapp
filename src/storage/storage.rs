use anyhow::Result;
use async_trait::async_trait;
use std::fmt::Debug;

use crate::crud::{list_cursor::ListCursor, viewmodel::sensor_viewmodel::SensorViewModel};

#[async_trait]
pub trait StorageInstance: Send + Sync + Debug {
    async fn create_or_migrate(&self) -> Result<()>;
    async fn publish(
        &self,
        batch: std::sync::Arc<crate::datamodel::batch::Batch>,
        sync_sender: async_broadcast::Sender<()>,
    ) -> Result<()>;
    async fn sync(&self, sync_sender: async_broadcast::Sender<()>) -> Result<()>;
    async fn vacuum(&self) -> Result<()>;

    async fn list_sensors(
        &self,
        cursor: ListCursor,
        limit: usize,
    ) -> Result<(Vec<SensorViewModel>, Option<ListCursor>)>;
}
