use super::message::{Message, PublishMessage};
use crate::datamodel::batch::Batch;
use anyhow::Result;
use std::sync::Arc;

#[derive(Debug)]
pub struct EventBus {
    pub main_bus_sender: async_broadcast::Sender<Message>,
    pub main_bus_receiver: async_broadcast::InactiveReceiver<Message>,
}

impl EventBus {
    // Create a new event bus.
    // Please note that the receiver is inactive by default as it may be cloned many times.
    // Consider using .activate() or .activate_cloned() to activate it.
    pub fn new() -> Self {
        let (s, r) = async_broadcast::broadcast(128);
        let r = r.deactivate();
        Self {
            main_bus_sender: s,
            main_bus_receiver: r,
        }
    }

    async fn broadcast(&self, message: Message) -> Result<()> {
        println!("aad: publish event bus");
        println!(
            "aad: number of receivers: {}",
            self.main_bus_sender.receiver_count(),
        );
        println!(
            "aad: number of inactive receivers: {}",
            self.main_bus_sender.inactive_receiver_count(),
        );
        println!(
            "aad: number of senders: {}",
            self.main_bus_sender.sender_count(),
        );
        println!("aad: is full ? {}", self.main_bus_sender.is_full(),);
        println!("aad: len: {}", self.main_bus_sender.len(),);
        self.main_bus_sender.broadcast(message).await?;
        println!("aae: publish event bus");
        Ok(())
    }

    pub async fn publish(&self, batch: Batch) -> Result<async_broadcast::InactiveReceiver<()>> {
        // We create a new broadcast channel to receive the sync message.
        // It can technically have multiple emitters and multiple receivers.
        // In most cases, it should be a one to one relationship, but
        // it could be possible to have multiple storage backends and a single
        // receiver that waits for the first one to sync, or all.
        println!("aaa: publish event bus");
        let (sync_sender, sync_receiver) = async_broadcast::broadcast(1);
        let sync_receiver = sync_receiver.deactivate();
        println!("aab: publish event bus");

        self.broadcast(Message::Publish(PublishMessage {
            batch: Arc::new(batch),
            sync_sender,
            sync_receiver: sync_receiver.clone(),
        }))
        .await?;

        println!("aac: publish event bus");

        Ok(sync_receiver)
    }
}

#[cfg(test)]
mod tests {
    use tokio::{spawn, sync::Mutex};

    use super::*;
    use crate::datamodel::batch::Batch;

    #[tokio::test]
    async fn test_event_bus_init() {
        let _ = EventBus::new();
    }

    #[tokio::test]
    async fn test_event_bus_publish() {
        let event_bus = EventBus::new();
        let batch = Batch::default();

        let has_received = Arc::new(Mutex::new(false));
        let receiver_spawn_clone = event_bus.main_bus_receiver.clone();
        let has_received_spawn_clone = has_received.clone();

        spawn(async move {
            let received_message = receiver_spawn_clone.activate().recv().await;

            match received_message {
                Ok(Message::Publish(publish_message)) => {
                    publish_message.sync_sender.broadcast(()).await.unwrap();
                    {
                        let mut has_received = has_received_spawn_clone.lock().await;
                        *has_received = true;
                    }
                }
                _ => panic!("Unexpected message type"),
            }
        });

        let sync_receiver = event_bus.publish(batch).await.unwrap();

        sync_receiver.clone().activate().recv().await.unwrap();

        assert!(*has_received.lock().await);
    }
}
