use crate::datamodel::batch::Batch;
use std::sync::Arc;

#[derive(Debug, Clone)]
pub enum Message {
    Publish(PublishMessage),
}

#[derive(Debug, Clone)]
pub struct PublishMessage {
    pub batch: Arc<Batch>,
    // A request sync message is sent to ask the storage backends
    // to sync. This is done to ensure that the data is persisted.
    //
    // However, some storage backends many support syncing, or may lie about
    // syncing. This is also true for some storage hardware nowadays.
    pub sync_sender: async_broadcast::Sender<()>,
    pub sync_receiver: async_broadcast::InactiveReceiver<()>,
}
