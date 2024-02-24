use super::message::{Message, PublishMessage};
use crate::datamodel::batch::Batch;
use anyhow::Result;
use std::sync::Arc;

#[derive(Debug)]
pub struct EventBus {
    pub name: String,
    pub main_bus_sender: async_broadcast::Sender<Message>,
    pub main_bus_receiver: async_broadcast::InactiveReceiver<Message>,
}

impl EventBus {
    // Create a new event bus.
    // Please note that the receiver is inactive by default as it may be cloned many times.
    // Consider using .activate() or .activate_cloned() to activate it.
    pub fn init(name: String) -> Self {
        let (s, r) = async_broadcast::broadcast(128);
        let r = r.deactivate();
        Self {
            name,
            main_bus_sender: s,
            main_bus_receiver: r,
        }
    }

    async fn broadcast(&self, message: Message) -> Result<()> {
        self.main_bus_sender.broadcast(message).await?;
        Ok(())
    }

    pub async fn publish(&self, batch: Batch) -> Result<async_broadcast::InactiveReceiver<()>> {
        // We create a new broadcast channel to receive the sync message.
        // It can technically have multiple emitters and multiple receivers.
        // In most cases, it should be a one to one relationship, but
        // it could be possible to have multiple storage backends and a single
        // receiver that waits for the first one to sync, or all.
        let (sync_sender, sync_receiver) = async_broadcast::broadcast(1);
        let sync_receiver = sync_receiver.deactivate();

        self.broadcast(Message::Publish(PublishMessage {
            batch: Arc::new(batch),
            sync_sender,
            sync_receiver: sync_receiver.clone(),
        }))
        .await?;

        Ok(sync_receiver)
    }
}

pub fn init_event_bus() -> Arc<EventBus> {
    Arc::new(EventBus::init("SensApp".to_string()))
}
