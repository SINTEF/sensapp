use thiserror::Error;
use uuid::Uuid;

/// Storage-specific errors that can occur during database operations
#[derive(Error, Debug)]
pub enum StorageError {
    /// Database connection or query execution error
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    /// Data integrity issues - missing required fields in database views
    #[cfg(any(
        feature = "postgres",
        feature = "sqlite",
        feature = "timescaledb",
        feature = "bigquery"
    ))]
    #[error("Data integrity error: Missing {field} for sensor {context}")]
    MissingRequiredField { field: String, context: String },

    /// Invalid data format in database
    #[error("Invalid data format: {message} for sensor {sensor_context}")]
    InvalidDataFormat {
        message: String,
        sensor_context: String,
    },

    /// Sensor not found
    #[error("Sensor not found: {sensor_id}")]
    SensorNotFound { sensor_id: String },

    /// Metric not found
    #[error("Metric not found: {metric_name}")]
    MetricNotFound { metric_name: String },

    /// A write names a sensor UUID that is stored, or given in the same batch, with another name
    /// or type. The message does not tell the stored name: the writer may not be allowed to see it.
    #[error("Sensor {uuid} exists with another name or type than '{name}' ({sensor_type})")]
    SensorIdentityMismatch {
        uuid: Uuid,
        name: String,
        sensor_type: String,
    },

    /// A new sensor UUID whose 64-bit storage key (ClickHouse, BigQuery) is the key of another
    /// stored sensor, or of another sensor of the same batch. Such a sensor cannot be stored.
    #[error(
        "Sensor {uuid} has the same storage key as another sensor of this database: use another UUID"
    )]
    SensorKeyCollision { uuid: Uuid },

    /// Configuration error
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// The storage backend does not implement this operation
    #[error("Operation not supported by this storage backend: {0}")]
    Unsupported(String),

    /// The storage backend cannot be reached, timed out, or is overloaded: the operation may
    /// succeed later
    #[error("Storage backend unavailable: {0}")]
    Unavailable(String),

    /// Generic storage operation error with context
    #[error("Storage operation failed: {operation} - {details}")]
    OperationFailed { operation: String, details: String },
}

impl StorageError {
    /// Create a missing field error with sensor context
    #[cfg(any(
        feature = "postgres",
        feature = "sqlite",
        feature = "timescaledb",
        feature = "bigquery"
    ))]
    pub fn missing_field(
        field: &str,
        sensor_uuid: Option<Uuid>,
        sensor_name: Option<&str>,
    ) -> Self {
        let context = match (sensor_uuid, sensor_name) {
            (Some(uuid), Some(name)) => format!("UUID={}, name='{}'", uuid, name),
            (Some(uuid), None) => format!("UUID={}", uuid),
            (None, Some(name)) => format!("name='{}'", name),
            (None, None) => "unknown sensor".to_string(),
        };

        StorageError::MissingRequiredField {
            field: field.to_string(),
            context,
        }
    }

    /// Create an invalid data format error with sensor context
    pub fn invalid_data_format(
        message: &str,
        sensor_uuid: Option<Uuid>,
        sensor_name: Option<&str>,
    ) -> Self {
        let sensor_context = match (sensor_uuid, sensor_name) {
            (Some(uuid), Some(name)) => format!("UUID={}, name='{}'", uuid, name),
            (Some(uuid), None) => format!("UUID={}", uuid),
            (None, Some(name)) => format!("name='{}'", name),
            (None, None) => "unknown sensor".to_string(),
        };

        StorageError::InvalidDataFormat {
            message: message.to_string(),
            sensor_context,
        }
    }
}
