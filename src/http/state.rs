use crate::storage::StorageInstance;
use std::sync::Arc;

/// HTTP server state shared across all request handlers.
#[derive(Clone, Debug)]
pub struct HttpServerState {
    /// Server instance name (used in responses)
    pub name: Arc<String>,
    /// Storage backend (PostgreSQL, SQLite, etc.)
    pub storage: Arc<dyn StorageInstance>,
    /// If true, InfluxDB numeric types are stored as Decimal/Numeric instead of Integer/Float
    pub influxdb_with_numeric: bool,
}
