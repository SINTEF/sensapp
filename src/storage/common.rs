use anyhow::Result;
use std::time::Duration;
use async_broadcast::Sender;
use tokio::time::timeout;

/// Performs a sync operation with configurable timeout
/// This is the common sync pattern used across all storage backends
pub async fn sync_with_timeout(
    sync_sender: &Sender<()>,
    timeout_seconds: u64,
) -> Result<()> {
    if sync_sender.receiver_count() > 0 && !sync_sender.is_closed() {
        let _ = timeout(
            Duration::from_secs(timeout_seconds),
            sync_sender.broadcast(()),
        )
        .await?;
    }
    Ok(())
}