use crate::storage::StorageInstance;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub struct HttpServerState {
    pub name: Arc<String>,
    pub storage: Arc<dyn StorageInstance>,
}
