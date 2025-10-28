use crate::parsing::prometheus::remote_read_parser::{
    create_empty_read_response, parse_remote_read_request, serialize_read_response,
};

use super::{app_error::AppError, state::HttpServerState};
use axum::{
    debug_handler,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Response,
};
use tokio_util::bytes::Bytes;
use tracing::{debug, info, warn};

fn verify_read_headers(headers: &HeaderMap) -> Result<(), AppError> {
    // Check that we have the right content encoding, that must be snappy
    match headers.get("content-encoding") {
        Some(content_encoding) => match content_encoding.to_str() {
            Ok("snappy") | Ok("SNAPPY") => {}
            _ => {
                return Err(AppError::bad_request(anyhow::anyhow!(
                    "Unsupported content-encoding, must be snappy"
                )));
            }
        },
        None => {
            return Err(AppError::bad_request(anyhow::anyhow!(
                "Missing content-encoding header"
            )));
        }
    }

    // Check that the content type is protocol buffer
    match headers.get("content-type") {
        Some(content_type) => match content_type.to_str() {
            Ok("application/x-protobuf") | Ok("APPLICATION/X-PROTOBUF") => {}
            _ => {
                return Err(AppError::bad_request(anyhow::anyhow!(
                    "Unsupported content-type, must be application/x-protobuf"
                )));
            }
        },
        None => {
            return Err(AppError::bad_request(anyhow::anyhow!(
                "Missing content-type header"
            )));
        }
    }

    // Check that the remote read version is supported
    match headers.get("x-prometheus-remote-read-version") {
        Some(version) => match version.to_str() {
            Ok("0.1.0") => {}
            _ => {
                return Err(AppError::bad_request(anyhow::anyhow!(
                    "Unsupported x-prometheus-remote-read-version, must be 0.1.0"
                )));
            }
        },
        None => {
            return Err(AppError::bad_request(anyhow::anyhow!(
                "Missing x-prometheus-remote-read-version header"
            )));
        }
    }

    Ok(())
}

/// Prometheus Remote Read API.
///
/// Allows you to read data from SensApp using Prometheus remote read protocol.
///
/// It follows the [Prometheus Remote Read specification](https://prometheus.io/docs/prometheus/latest/querying/remote_read_api/).
#[utoipa::path(
    post,
    path = "/api/v1/prometheus_remote_read",
    tag = "Prometheus",
    request_body(
        content_type = "application/x-protobuf",
        description = "Prometheus Remote Read endpoint. [Reference](https://prometheus.io/docs/prometheus/latest/querying/remote_read_api/)",
    ),
    params(
        ("content-encoding" = String, Header, format = "snappy", description = "Content encoding, must be snappy"),
        ("content-type" = String, Header, format = "application/x-protobuf", description = "Content type, must be application/x-protobuf"),
        ("x-prometheus-remote-read-version" = String, Header, format = "0.1.0", description = "Prometheus Remote Read version, must be 0.1.0"),
    ),
    responses(
        (status = 200, description = "Read Response", content_type = "application/x-protobuf"),
        (status = 400, description = "Bad Request", body = AppError),
        (status = 500, description = "Internal Server Error", body = AppError),
    )
)]
#[debug_handler]
pub async fn prometheus_remote_read(
    State(_state): State<HttpServerState>,
    headers: HeaderMap,
    bytes: Bytes,
) -> Result<Response<axum::body::Body>, AppError> {
    debug!("Prometheus remote read: received {} bytes", bytes.len());

    // Verify headers
    verify_read_headers(&headers)?;

    // Parse the read request
    let read_request = parse_remote_read_request(&bytes).map_err(|e| {
        AppError::bad_request(anyhow::anyhow!("Failed to parse read request: {}", e))
    })?;

    info!(
        "Prometheus remote read: Processing {} queries",
        read_request.queries.len()
    );

    // Log detailed information about each query for debugging
    for (i, query) in read_request.queries.iter().enumerate() {
        info!(
            "Query {}: time range {}ms - {}ms ({} matchers)",
            i,
            query.start_timestamp_ms,
            query.end_timestamp_ms,
            query.matchers.len()
        );

        for matcher in &query.matchers {
            debug!(
                "  Matcher: {}={} (type={})",
                matcher.name, matcher.value, matcher.r#type
            );
        }

        if let Some(hints) = &query.hints {
            //debug!("  Hints: step={}ms, func='{}'", hints.step_ms, hints.func);
            debug!("  Hints: {:?}", hints);
        }
    }

    debug!(
        "Accepted response types: {:?}",
        read_request.accepted_response_types
    );

    // For now, create an empty response
    warn!("Returning empty response - data fetching not yet implemented");
    let empty_response = create_empty_read_response(&read_request).map_err(|e| {
        AppError::internal_server_error(anyhow::anyhow!("Failed to create response: {}", e))
    })?;

    // Serialize and compress the response
    let response_bytes = serialize_read_response(&empty_response).map_err(|e| {
        AppError::internal_server_error(anyhow::anyhow!("Failed to serialize response: {}", e))
    })?;

    info!(
        "Prometheus remote read: Returning response with {} bytes",
        response_bytes.len()
    );

    // Build HTTP response with appropriate headers
    let response = Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "snappy")
        .body(axum::body::Body::from(response_bytes))
        .map_err(|e| {
            AppError::internal_server_error(anyhow::anyhow!("Failed to build response: {}", e))
        })?;

    Ok(response)
}

#[cfg(any(test, feature = "test-utils"))]
mod tests {
    #![allow(dead_code)]
    use super::*;
    use axum::http::HeaderValue;

    fn create_test_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("content-encoding", HeaderValue::from_static("snappy"));
        headers.insert(
            "content-type",
            HeaderValue::from_static("application/x-protobuf"),
        );
        headers.insert(
            "x-prometheus-remote-read-version",
            HeaderValue::from_static("0.1.0"),
        );
        headers
    }

    #[test]
    fn test_verify_read_headers_valid() {
        let headers = create_test_headers();
        assert!(verify_read_headers(&headers).is_ok());
    }

    #[test]
    fn test_verify_read_headers_missing_content_encoding() {
        let mut headers = create_test_headers();
        headers.remove("content-encoding");
        assert!(verify_read_headers(&headers).is_err());
    }

    #[test]
    fn test_verify_read_headers_invalid_content_encoding() {
        let mut headers = create_test_headers();
        headers.insert("content-encoding", HeaderValue::from_static("gzip"));
        assert!(verify_read_headers(&headers).is_err());
    }

    #[test]
    fn test_verify_read_headers_missing_content_type() {
        let mut headers = create_test_headers();
        headers.remove("content-type");
        assert!(verify_read_headers(&headers).is_err());
    }

    #[test]
    fn test_verify_read_headers_invalid_version() {
        let mut headers = create_test_headers();
        headers.insert(
            "x-prometheus-remote-read-version",
            HeaderValue::from_static("2.0.0"),
        );
        assert!(verify_read_headers(&headers).is_err());
    }
}
