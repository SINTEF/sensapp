use std::sync::Arc;
use crate::bus::EventBus;

#[derive(Clone, Debug)]
pub struct HttpServerState {
    pub name: String,
    pub event_bus: Arc<EventBus>,
}
