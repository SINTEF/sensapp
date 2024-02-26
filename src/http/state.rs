use crate::bus::EventBus;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct HttpServerState {
    pub name: Arc<String>,
    pub event_bus: Arc<EventBus>,
}
