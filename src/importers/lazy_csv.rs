use crate::bus::EventBus;
use anyhow::Result;
use futures::io;
use std::sync::Arc;

pub async fn publish_csv_async<R: io::AsyncRead + Unpin + Send>(
    mut async_reader: R,
    batch_size: usize,
    event_bus: Arc<EventBus>,
) -> Result<()> {
    Ok(())
}
