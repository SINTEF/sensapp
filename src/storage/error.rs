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
    #[allow(dead_code)] // Part of error API, will be used for sensor operations
    SensorNotFound { sensor_id: String },

    /// Metric not found
    #[error("Metric not found: {metric_name}")]
    #[allow(dead_code)] // Part of error API, will be used for metric operations
    MetricNotFound { metric_name: String },

    /// Configuration error
    #[error("Configuration error: {0}")]
    #[allow(dead_code)] // Part of error API, will be used for storage config errors
    Configuration(String),

    /// Generic storage operation error with context
    #[error("Storage operation failed: {operation} - {details}")]
    #[allow(dead_code)] // Part of error API, will be used for generic storage operations
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
