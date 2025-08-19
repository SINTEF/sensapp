use thiserror::Error;
use uuid::Uuid;

/// Storage-specific errors that can occur during database operations
#[derive(Error, Debug)]
pub enum StorageError {
    /// Database connection or query execution error
    #[error("Database error: {0}")]
    Database(#[from] sqlx::Error),

    /// Data integrity issues - missing required fields in database views
    #[error("Data integrity error: Missing {field} for sensor {context}")]
    MissingRequiredField {
        field: String,
        context: String,
    },

    /// Invalid data format in database
    #[error("Invalid data format: {message} for sensor {sensor_context}")]
    InvalidDataFormat {
        message: String,
        sensor_context: String,
    },

    /// Sensor not found
    #[error("Sensor not found: {sensor_id}")]
    SensorNotFound {
        sensor_id: String,
    },

    /// Metric not found  
    #[error("Metric not found: {metric_name}")]
    MetricNotFound {
        metric_name: String,
    },

    /// Configuration error
    #[error("Configuration error: {0}")]
    Configuration(String),

    /// Generic storage operation error with context
    #[error("Storage operation failed: {operation} - {details}")]
    OperationFailed {
        operation: String,
        details: String,
    },
}

impl StorageError {
    /// Create a missing field error with sensor context
    pub fn missing_field(field: &str, sensor_uuid: Option<Uuid>, sensor_name: Option<&str>) -> Self {
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
    pub fn invalid_data_format(message: &str, sensor_uuid: Option<Uuid>, sensor_name: Option<&str>) -> Self {
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

    /// Create a sensor not found error
    pub fn sensor_not_found(sensor_id: &str) -> Self {
        StorageError::SensorNotFound {
            sensor_id: sensor_id.to_string(),
        }
    }

    /// Create a metric not found error
    pub fn metric_not_found(metric_name: &str) -> Self {
        StorageError::MetricNotFound {
            metric_name: metric_name.to_string(),
        }
    }

    /// Create an operation failed error
    pub fn operation_failed(operation: &str, details: &str) -> Self {
        StorageError::OperationFailed {
            operation: operation.to_string(),
            details: details.to_string(),
        }
    }
}

/// Result type alias for storage operations
pub type StorageResult<T> = Result<T, StorageError>;