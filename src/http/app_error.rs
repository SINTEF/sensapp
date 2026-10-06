use crate::storage::StorageError;
use axum::Json;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use serde_json::json;
use tracing::error;
use utoipa::ToSchema;

/// PostgreSQL error codes of a database that cannot serve the request now but may later: the
/// connection exception class (`08`), too many connections, and the shutdown family (admin
/// shutdown, crash shutdown, cannot connect now). A statement timeout (`57014`) is not one of
/// them: the database is up, the query is slow, and retrying it only adds load.
fn sqlstate_is_unavailable(code: &str) -> bool {
    code.starts_with("08") || matches!(code, "53300" | "57P01" | "57P02" | "57P03")
}

/// Whether a SQLite error is `SQLITE_BUSY` or `SQLITE_LOCKED`: another connection holds the write
/// lock for longer than the busy timeout. The database is fine, the same request can work later.
/// sqlx reports the extended result code, whose low byte is the primary code (`SQLITE_BUSY_SNAPSHOT`
/// is 517, so 5).
#[cfg(feature = "sqlite")]
fn sqlite_error_is_busy(error: &dyn sqlx::error::DatabaseError) -> bool {
    use sqlx::error::DatabaseError;

    error
        .try_downcast_ref::<sqlx::sqlite::SqliteError>()
        .and_then(|error| error.code()?.parse::<i32>().ok())
        .is_some_and(|code| matches!(code & 0xff, 5 | 6))
}

/// Whether a sqlx error means that the database cannot be reached, is out of connections, or is
/// busy with another writer (SQLite). Everything else (a slow or cancelled statement, a constraint, a missing file, a bad
/// configuration) is a failure of the operation, reported as a 500 that clients do not retry.
fn sqlx_error_is_unavailable(error: &sqlx::Error) -> bool {
    match error {
        sqlx::Error::PoolTimedOut
        | sqlx::Error::PoolClosed
        | sqlx::Error::WorkerCrashed
        | sqlx::Error::Io(_) => true,
        sqlx::Error::Database(error) => {
            #[cfg(feature = "sqlite")]
            if sqlite_error_is_busy(error.as_ref()) {
                return true;
            }
            error
                .code()
                .is_some_and(|code| sqlstate_is_unavailable(&code))
        }
        _ => false,
    }
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
                StorageError::Unsupported(_) => {
                    (StatusCode::NOT_IMPLEMENTED, storage_error.to_string())
                }
                StorageError::Configuration(_) => {
                    error!("Storage configuration error: {}", storage_error);
                    (
                        StatusCode::INTERNAL_SERVER_ERROR,
                        "Storage configuration error".to_string(),
                    )
                }
                StorageError::Unavailable(_) => {
                    error!("Storage backend unavailable: {}", storage_error);
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        "Database unavailable".to_string(),
                    )
                }
                StorageError::Database(error) if sqlx_error_is_unavailable(error) => {
                    error!("Storage backend unavailable: {}", storage_error);
                    (
                        StatusCode::SERVICE_UNAVAILABLE,
                        "Database unavailable".to_string(),
                    )
                }
                // The backends sort their own errors before building an `OperationFailed`
                // (ClickHouse sends the transient ones to `Unavailable`), so what is left is a
                // failure of the operation whatever the words in its message.
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
        if err.is::<crate::http::authorized_storage::SensorAccessDenied>() {
            return Self::Forbidden(err.to_string());
        }
        // Errors of the ClickHouse client travel inside `anyhow` through `?`: classify them here
        // so that an outage is a 503 and not an anonymous 500.
        #[cfg(feature = "clickhouse")]
        if let Some(error) = err.downcast_ref::<clickhouse::error::Error>() {
            return Self::Storage(crate::storage::clickhouse::classify_clickhouse_error(error));
        }
        // The same for the sqlx errors that come through `?`: a pool that times out or a
        // refused connection is a 503, not an anonymous 500.
        if err.chain().any(|cause| {
            cause
                .downcast_ref::<sqlx::Error>()
                .is_some_and(sqlx_error_is_unavailable)
        }) {
            return Self::Storage(StorageError::Unavailable(format!("{err:#}")));
        }
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
    use anyhow::Context;
    use axum::body::to_bytes;

    async fn status_of(error: anyhow::Error) -> StatusCode {
        AppError::internal_server_error(error)
            .into_response()
            .status()
    }

    #[tokio::test]
    async fn test_internal_server_error_preserves_storage_error_category() {
        let response = AppError::internal_server_error(anyhow::Error::new(
            StorageError::Unavailable("connection refused".to_string()),
        ))
        .into_response();

        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let body = to_bytes(response.into_body(), usize::MAX).await.unwrap();
        assert_eq!(
            serde_json::from_slice::<serde_json::Value>(&body).unwrap(),
            json!({ "error": "Database unavailable" })
        );
    }

    #[tokio::test]
    async fn test_unreachable_database_is_a_503_whatever_the_wrapping() {
        let refused = || std::io::Error::from(std::io::ErrorKind::ConnectionRefused);

        // Through `?` in a function that returns `anyhow`, with and without context
        assert_eq!(
            status_of(sqlx::Error::PoolTimedOut.into()).await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        let with_context: anyhow::Result<()> =
            Err(anyhow::Error::new(sqlx::Error::Io(refused()))).context("while publishing");
        assert_eq!(
            status_of(with_context.unwrap_err()).await,
            StatusCode::SERVICE_UNAVAILABLE
        );
        // Through the storage error
        assert_eq!(
            status_of(StorageError::Database(sqlx::Error::PoolClosed).into()).await,
            StatusCode::SERVICE_UNAVAILABLE
        );
    }

    #[tokio::test]
    async fn test_slow_or_failing_operations_are_not_a_503() {
        // The words of a message do not make a database unavailable
        for details in [
            "statement timed out",
            "canceling statement due to statement timeout",
            "no such file or directory",
            "connection refused",
        ] {
            let error = StorageError::OperationFailed {
                operation: "query".to_string(),
                details: details.to_string(),
            };
            assert_eq!(
                status_of(error.into()).await,
                StatusCode::INTERNAL_SERVER_ERROR,
                "{details}"
            );
        }
        // Nor does an error of the driver that is not about the connection
        assert_eq!(
            status_of(StorageError::Database(sqlx::Error::RowNotFound).into()).await,
            StatusCode::INTERNAL_SERVER_ERROR
        );
        assert_eq!(
            status_of(sqlx::Error::Protocol("timed out".to_string()).into()).await,
            StatusCode::INTERNAL_SERVER_ERROR
        );
    }

    #[test]
    fn test_sqlstates_of_an_unavailable_database() {
        for code in [
            "08000", "08006", "08001", "53300", "57P01", "57P02", "57P03",
        ] {
            assert!(sqlstate_is_unavailable(code), "{code}");
        }
        // A statement timeout (query canceled), a deadlock, a constraint, and the SQLite codes
        for code in ["57014", "40P01", "23505", "42P01", "5", "14", ""] {
            assert!(!sqlstate_is_unavailable(code), "{code}");
        }
    }

    #[tokio::test]
    async fn test_unavailable_storage_is_a_503() {
        let response = AppError::from(StorageError::Unavailable("timeout expired".to_string()))
            .into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);
    }

    /// Two connections to one file, the first holding the write lock, the second not waiting.
    #[cfg(feature = "sqlite")]
    #[tokio::test]
    async fn test_sqlite_busy_is_a_503_and_other_errors_are_not() {
        use sqlx::Connection;
        use sqlx::sqlite::SqliteConnectOptions;
        use std::str::FromStr;
        use std::time::Duration;

        let path = std::env::temp_dir().join(format!("sensapp-busy-{}.db", uuid::Uuid::new_v4()));
        let options = SqliteConnectOptions::from_str(&format!("sqlite://{}", path.display()))
            .unwrap()
            .create_if_missing(true)
            .busy_timeout(Duration::ZERO);
        let mut holder = sqlx::SqliteConnection::connect_with(&options)
            .await
            .unwrap();
        let mut waiter = sqlx::SqliteConnection::connect_with(&options)
            .await
            .unwrap();

        sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut holder)
            .await
            .unwrap();
        let busy = sqlx::query("BEGIN IMMEDIATE")
            .execute(&mut waiter)
            .await
            .unwrap_err();
        assert_eq!(
            status_of(busy.into()).await,
            StatusCode::SERVICE_UNAVAILABLE
        );

        let syntax = sqlx::query("NOT SQL")
            .execute(&mut waiter)
            .await
            .unwrap_err();
        assert_eq!(
            status_of(syntax.into()).await,
            StatusCode::INTERNAL_SERVER_ERROR
        );

        drop(holder);
        drop(waiter);
        for suffix in ["", "-wal", "-shm"] {
            let _ = std::fs::remove_file(format!("{}{suffix}", path.display()));
        }
    }

    #[cfg(feature = "clickhouse")]
    #[tokio::test]
    async fn test_clickhouse_client_errors_are_classified_through_anyhow() {
        use anyhow::Context;

        let timed_out: anyhow::Result<()> =
            Err(clickhouse::error::Error::TimedOut).context("while counting samples");
        let response = AppError::from(timed_out.unwrap_err()).into_response();
        assert_eq!(response.status(), StatusCode::SERVICE_UNAVAILABLE);

        let broken: anyhow::Error =
            clickhouse::error::Error::BadResponse("Code: 62. DB::Exception: Syntax error".into())
                .into();
        let response = AppError::from(broken).into_response();
        assert_eq!(response.status(), StatusCode::INTERNAL_SERVER_ERROR);
    }
}
