use crate::storage::StorageError;
use axum::Json;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use serde_json::json;
use tracing::error;
use utoipa::ToSchema;

fn storage_error_is_unavailable(storage_error: &StorageError) -> bool {
    let details = storage_error.to_string().to_ascii_lowercase();

    details.contains("connection refused")
        || details.contains("failed to connect")
        || details.contains("pool timed out")
        || details.contains("timed out")
        || details.contains("connection closed")
        || details.contains("connection reset")
        || details.contains("broken pipe")
        || details.contains("no such file or directory")
        || details.contains("database is unavailable")
}

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
    #[schema(example = "Unauthorized", value_type = String)]
    Unauthorized(String),
    #[schema(example = "Forbidden", value_type = String)]
    Forbidden(String),
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
            AppError::Unauthorized(message) => (StatusCode::UNAUTHORIZED, message),
            AppError::Forbidden(message) => (StatusCode::FORBIDDEN, message),
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
                    if storage_error_is_unavailable(&storage_error) {
                        error!("Storage backend unavailable: {}", storage_error);
                        (
                            StatusCode::SERVICE_UNAVAILABLE,
                            "Database unavailable".to_string(),
                        )
                    } else {
                        error!("Storage operation failed: {}", storage_error);
                        (
                            StatusCode::INTERNAL_SERVER_ERROR,
                            "Storage operation failed".to_string(),
                        )
                    }
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
        match err.downcast::<StorageError>() {
            Ok(storage_error) => Self::Storage(storage_error),
            Err(err) => Self::InternalServerError(err),
        }
    }
}

impl AppError {
    pub fn bad_request(err: impl Into<anyhow::Error>) -> Self {
        Self::BadRequest(err.into())
    }

    pub fn internal_server_error(err: impl Into<anyhow::Error>) -> Self {
        Self::from(err.into())
    }

    pub fn not_found(err: impl Into<anyhow::Error>) -> Self {
        Self::NotFound(err.into())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::body::to_bytes;

    #[tokio::test]
    async fn test_internal_server_error_preserves_storage_error_category() {
        let response =
            AppError::internal_server_error(anyhow::Error::new(StorageError::OperationFailed {
                operation: "publish batch".to_string(),
                details: "connection refused".to_string(),
            }))
            .into_response();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            json!({ "error": "Database unavailable" })
        );
    }
}
