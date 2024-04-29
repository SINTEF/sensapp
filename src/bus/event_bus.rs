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

#[cfg(test)]
mod tests {
    use tokio::{spawn, sync::Mutex};

    use super::*;
    use crate::datamodel::batch::Batch;

    #[tokio::test]
    async fn test_event_bus_init() {
        let event_bus = EventBus::init("TestBus".to_string());
        assert_eq!(event_bus.name, "TestBus");
    }

    #[tokio::test]
    async fn test_init_event_bus() {
        let event_bus = init_event_bus();
        assert_eq!(event_bus.name, "SensApp");
    }

    #[tokio::test]
    async fn test_event_bus_publish() {
        let event_bus = EventBus::init("TestBus".to_string());
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
