use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::Json;
use serde_json::json;
use std::fmt;

// Anyhow error handling with axum
// https://github.com/tokio-rs/axum/blob/d3112a40d55f123bc5e65f995e2068e245f12055/examples/anyhow-error-response/src/main.rs
#[derive(Debug)]
pub enum AppError {
    InternalServerError(anyhow::Error),
    BadRequest(anyhow::Error),
    NotFound(anyhow::Error),
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            AppError::InternalServerError(error) => {
                eprintln!("Internal Server Error: {}", error.backtrace());
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal Server Error".to_string(),
                )
            }
            AppError::NotFound(error) => (StatusCode::NOT_FOUND, error.to_string()),
            AppError::BadRequest(error) => (StatusCode::BAD_REQUEST, error.to_string()),
        };
        let body = Json(json!({ "error": message }));
        (status, body).into_response()
    }
}
impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self::InternalServerError(err.into())
    }
}

impl fmt::Display for AppError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            AppError::InternalServerError(error) => write!(f, "Internal Server Error: {}", error),
            AppError::NotFound(error) => write!(f, "Not Found: {}", error),
            AppError::BadRequest(error) => write!(f, "Bad Request: {}", error),
        }
    }
}
