use super::message::{Message, PublishMessage};
use crate::datamodel::batch::Batch;
use anyhow::Result;
use std::sync::Arc;

#[derive(Debug)]
pub struct EventBus {
    pub name: String,
    //main_bus_sender: mpsc::Sender<u8>,
    //main_bus_receiver: mpsc::Receiver<u8>,
    //pub main_bus_sender: async_channel::Sender<u8>,
    //pub main_bus_receiver: async_channel::Receiver<u8>,
    //pub main_bus_sender: tokio::sync::broadcast::Sender<u8>,
    //pub main_bus_receiver: tokio::sync::broadcast::Receiver<u8>,
    pub main_bus_sender: async_broadcast::Sender<Message>,
    pub main_bus_receiver: async_broadcast::InactiveReceiver<Message>,
}

impl EventBus {
    // Create a new event bus.
    // Please note that the receiver is inactive by default as it may be cloned many times.
    // Consider using .activate() or .activate_cloned() to activate it.
    pub fn init(name: String) -> Self {
        // let (tx, rx) = mpsc::channel(10);
        //let (s, _) = tokio::sync::broadcast::channel::<u8>(1000);
        //let (s, r) = async_broadcast::broadcast(128);
        let (s, r) = async_broadcast::broadcast(128);
        let r = r.deactivate();
        Self {
            name,
            main_bus_sender: s,
            main_bus_receiver: r,
        }
    }

    async fn broadcast(&self, message: Message) -> Result<()> {
        //self.main_bus_sender.send(event).await?;
        //self.main_bus_sender.send(event)?;
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

    // receive
    /*pub async fn receive_one(&mut self) -> Result<u8> {
        self.main_bus_receiver
            .recv()
            .await
            .map_err(|e| anyhow::anyhow!("Failed to receive event: {}", e))
    }*/
}

pub fn init_event_bus() -> Arc<EventBus> {
    Arc::new(EventBus::init("SensApp".to_string()))
}
