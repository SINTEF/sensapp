use crate::storage::StorageError;
use axum::Json;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use serde_json::json;
use tracing::error;
use utoipa::ToSchema;

// Anyhow error handling with axum
// https://github.com/tokio-rs/axum/blob/d3112a40d55f123bc5e65f995e2068e245f12055/examples/anyhow-error-response/src/main.rs
#[derive(Debug, ToSchema)]
pub enum AppError {
    #[schema(example = "Internal Server Error", value_type = String)]
    InternalServerError(anyhow::Error),
    #[schema(example = "Bad Request", value_type = String)]
    BadRequest(anyhow::Error),
    #[schema(example = "Not Found", value_type = String)]
    NotFound(anyhow::Error),
    #[schema(example = "Storage Error", value_type = String)]
    Storage(StorageError),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            AppError::InternalServerError(error) => {
                error!("Internal Server Error: {}", error);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal Server Error".to_string(),
                )
            }
            AppError::BadRequest(error) => (StatusCode::BAD_REQUEST, error.to_string()),
            AppError::NotFound(error) => (StatusCode::NOT_FOUND, error.to_string()),
            AppError::Storage(storage_error) => match &storage_error {
                StorageError::SensorNotFound { .. } | StorageError::MetricNotFound { .. } => {
                    (StatusCode::NOT_FOUND, storage_error.to_string())
                }
                #[cfg(any(
                    feature = "postgres",
                    feature = "sqlite",
                    feature = "timescaledb",
                    feature = "bigquery"
                ))]
                StorageError::MissingRequiredField { .. } => {
                    error!("Missing required field: {}", storage_error);
                    (StatusCode::BAD_REQUEST, storage_error.to_string())
                }
                StorageError::InvalidDataFormat { .. } => {
                    error!("Invalid data format: {}", storage_error);
                    (StatusCode::BAD_REQUEST, storage_error.to_string())
                }
                StorageError::Configuration(_) => {
                    error!("Storage configuration error: {}", storage_error);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Storage configuration error".to_string(),
                    )
                }
                StorageError::Database(_) | StorageError::OperationFailed { .. } => {
                    error!("Storage operation failed: {}", storage_error);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Storage operation failed".to_string(),
                    )
                }
            },
        };
        let body = Json(json!({ "error": message }));
        (status, body).into_response()
    }
}
// Specific conversion for StorageError to maintain error categorization
impl From<StorageError> for AppError {
    fn from(err: StorageError) -> Self {
        Self::Storage(err)
    }
}

// Generic conversion for anyhow::Error specifically
impl From<anyhow::Error> for AppError {
    fn from(err: anyhow::Error) -> Self {
        Self::InternalServerError(err)
    }
}

impl AppError {
    pub fn bad_request(err: impl Into<anyhow::Error>) -> Self {
        Self::BadRequest(err.into())
    }

    pub fn internal_server_error(err: impl Into<anyhow::Error>) -> Self {
        Self::InternalServerError(err.into())
    }

    pub fn not_found(err: impl Into<anyhow::Error>) -> Self {
        Self::NotFound(err.into())
    }
}
