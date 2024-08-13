use crate::{bus::EventBus, storage::storage::StorageInstance};
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct HttpServerState {
    pub name: Arc<String>,
    pub event_bus: Arc<EventBus>,
    pub storage: Arc<dyn StorageInstance>,
}
