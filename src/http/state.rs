use crate::http::auth::{AccessContext, AuthConfig};
use crate::http::authorized_storage::AuthorizedStorage;
use crate::http::metrics::HttpMetrics;
use crate::storage::StorageInstance;
use std::sync::Arc;

/// HTTP server state shared across all request handlers.
#[derive(Clone, Debug)]
pub struct HttpServerState {
    /// Server instance name (used in responses)
    pub name: Arc<String>,
    /// Storage backend (PostgreSQL, SQLite, etc.)
    pub storage: Arc<dyn StorageInstance>,
    /// Shared Prometheus metrics registry and counters
    pub metrics: Arc<HttpMetrics>,
    /// If true, InfluxDB numeric types are stored as Decimal/Numeric instead of Integer/Float
    pub influxdb_with_numeric: bool,
    /// Optional JWT authentication configuration.
    /// When `None`, all endpoints are open (no security).
    pub auth: Option<AuthConfig>,
}

impl HttpServerState {
    pub fn with_access(mut self, access: Option<AccessContext>) -> Self {
        if let Some(access) = access {
            self.storage = Arc::new(AuthorizedStorage::new(self.storage, access));
        }
        self
    }
}
