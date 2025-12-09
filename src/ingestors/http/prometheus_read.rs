use crate::datamodel::SensAppDateTime;
use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
use crate::parsing::prometheus::chunk_encoder::ChunkEncoder;
use crate::parsing::prometheus::converter::{build_prometheus_labels, sensor_data_to_timeseries};
use crate::parsing::prometheus::remote_read_models::{
    QueryResult, ReadResponse, read_request::ResponseType,
};
use crate::parsing::prometheus::remote_read_parser::{
    parse_remote_read_request, serialize_read_response,
};
use crate::parsing::prometheus::remote_write_models::Sample as PromSample;
use crate::parsing::prometheus::stream_writer::StreamWriter;
use crate::storage::query::LabelMatcher;

use super::{app_error::AppError, state::HttpServerState};
use axum::{
    debug_handler,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Response,
};
use tokio_util::bytes::Bytes;
use tracing::{debug, info};

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
    State(state): State<HttpServerState>,
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
            debug!("  Hints: step={}ms, func='{}'", hints.step_ms, hints.func);
        }
    }

    debug!(
        "Accepted response types: {:?}",
        read_request.accepted_response_types
    );

    // Check if client accepts streamed XOR chunks
    let use_streaming = read_request
        .accepted_response_types
        .contains(&(ResponseType::StreamedXorChunks as i32));

    if use_streaming {
        // Return streamed chunked response
        handle_streamed_response(&state, &read_request).await
    } else {
        // Return standard SAMPLES response
        handle_samples_response(&state, &read_request).await
    }
}

/// Handle standard SAMPLES response type
async fn handle_samples_response(
    state: &HttpServerState,
    read_request: &crate::parsing::prometheus::remote_read_models::ReadRequest,
) -> Result<Response<axum::body::Body>, AppError> {
    let mut results = Vec::with_capacity(read_request.queries.len());

    for query in &read_request.queries {
        // Convert Prometheus matchers to SensApp matchers
        let matchers: Vec<LabelMatcher> = query.matchers.iter().map(LabelMatcher::from).collect();

        // Convert timestamps from milliseconds to SensAppDateTime
        let start_time = SensAppDateTime::from_unix_milliseconds_i64(query.start_timestamp_ms);
        let end_time = SensAppDateTime::from_unix_milliseconds_i64(query.end_timestamp_ms);

        // Query storage (numeric_only=true for Prometheus compatibility)
        let sensor_data = state
            .storage
            .query_sensors_by_labels(&matchers, Some(start_time), Some(end_time), None, true)
            .await
            .map_err(|e| {
                AppError::internal_server_error(anyhow::anyhow!("Storage query failed: {}", e))
            })?;

        debug!("Query returned {} sensors", sensor_data.len());

        // Convert to Prometheus TimeSeries
        let timeseries = sensor_data
            .iter()
            .filter_map(sensor_data_to_timeseries)
            .collect();

        results.push(QueryResult { timeseries });
    }

    let response = ReadResponse { results };

    // Serialize and compress the response
    let response_bytes = serialize_read_response(&response).map_err(|e| {
        AppError::internal_server_error(anyhow::anyhow!("Failed to serialize response: {}", e))
    })?;

    info!(
        "Prometheus remote read: Returning SAMPLES response with {} bytes",
        response_bytes.len()
    );

    // Build HTTP response with appropriate headers
    Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "snappy")
        .body(axum::body::Body::from(response_bytes))
        .map_err(|e| {
            AppError::internal_server_error(anyhow::anyhow!("Failed to build response: {}", e))
        })
}

/// Handle STREAMED_XOR_CHUNKS response type
async fn handle_streamed_response(
    state: &HttpServerState,
    read_request: &crate::parsing::prometheus::remote_read_models::ReadRequest,
) -> Result<Response<axum::body::Body>, AppError> {
    let mut chunked_responses = Vec::with_capacity(read_request.queries.len());

    for (query_index, query) in read_request.queries.iter().enumerate() {
        // Convert Prometheus matchers to SensApp matchers
        let matchers: Vec<LabelMatcher> = query.matchers.iter().map(LabelMatcher::from).collect();

        // Convert timestamps from milliseconds to SensAppDateTime
        let start_time = SensAppDateTime::from_unix_milliseconds_i64(query.start_timestamp_ms);
        let end_time = SensAppDateTime::from_unix_milliseconds_i64(query.end_timestamp_ms);

        // Query storage (numeric_only=true for Prometheus compatibility)
        let sensor_data = state
            .storage
            .query_sensors_by_labels(&matchers, Some(start_time), Some(end_time), None, true)
            .await
            .map_err(|e| {
                AppError::internal_server_error(anyhow::anyhow!("Storage query failed: {}", e))
            })?;

        debug!(
            "Query {} returned {} sensors",
            query_index,
            sensor_data.len()
        );

        // Convert to ChunkedSeries
        let chunked_series: Vec<_> = sensor_data
            .iter()
            .filter_map(|sd| {
                // Extract labels
                let labels = build_prometheus_labels(&sd.sensor);

                // Extract samples as Prometheus format
                let samples = extract_prom_samples_for_chunks(sd)?;

                // Encode as XOR chunks
                ChunkEncoder::encode_series(labels, samples).ok()
            })
            .collect();

        let chunked_response = ChunkEncoder::create_response(query_index as i64, chunked_series);
        chunked_responses.push(chunked_response);
    }

    // Create the streaming response body
    let body = StreamWriter::create_stream_body(&chunked_responses).map_err(|e| {
        AppError::internal_server_error(anyhow::anyhow!("Failed to create stream body: {}", e))
    })?;

    info!(
        "Prometheus remote read: Returning STREAMED_XOR_CHUNKS response with {} bytes",
        body.len()
    );

    // Build HTTP response with appropriate headers for streaming
    Response::builder()
        .status(StatusCode::OK)
        .header(
            "content-type",
            "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse",
        )
        .body(axum::body::Body::from(body))
        .map_err(|e| {
            AppError::internal_server_error(anyhow::anyhow!("Failed to build response: {}", e))
        })
}

/// Extract Prometheus samples from SensorData for chunk encoding.
/// Returns None for non-numeric types.
fn extract_prom_samples_for_chunks(
    sensor_data: &crate::datamodel::SensorData,
) -> Option<Vec<PromSample>> {
    use crate::datamodel::TypedSamples;

    match &sensor_data.samples {
        TypedSamples::Float(samples) => {
            let prom_samples = samples
                .iter()
                .map(|s| PromSample {
                    value: s.value,
                    timestamp: s.datetime.to_unix_milliseconds().floor() as i64,
                })
                .collect();
            Some(prom_samples)
        }
        TypedSamples::Integer(samples) => {
            let prom_samples = samples
                .iter()
                .map(|s| PromSample {
                    value: s.value as f64,
                    timestamp: s.datetime.to_unix_milliseconds().floor() as i64,
                })
                .collect();
            Some(prom_samples)
        }
        TypedSamples::Numeric(samples) => {
            use rust_decimal::prelude::ToPrimitive;
            let prom_samples = samples
                .iter()
                .filter_map(|s| {
                    s.value.to_f64().map(|value| PromSample {
                        value,
                        timestamp: s.datetime.to_unix_milliseconds().floor() as i64,
                    })
                })
                .collect();
            Some(prom_samples)
        }
        // Non-numeric types cannot be represented in Prometheus format
        TypedSamples::String(_)
        | TypedSamples::Boolean(_)
        | TypedSamples::Location(_)
        | TypedSamples::Blob(_)
        | TypedSamples::Json(_) => None,
    }
}

#[cfg(test)]
mod tests {
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
