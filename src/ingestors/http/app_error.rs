use axum::Json;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use serde_json::json;
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
}

impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        let (status, message) = match self {
            AppError::InternalServerError(error) => {
                eprintln!("Internal Server Error: {}", error);
                (
                    StatusCode::INTERNAL_SERVER_ERROR,
                    "Internal Server Error".to_string(),
                )
            }
            AppError::BadRequest(error) => (StatusCode::BAD_REQUEST, error.to_string()),
            AppError::NotFound(error) => (StatusCode::NOT_FOUND, error.to_string()),
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
