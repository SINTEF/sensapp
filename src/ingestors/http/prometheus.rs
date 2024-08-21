use super::{app_error::AppError, state::HttpServerState};
use crate::{
    datamodel::batch_builder::BatchBuilder,
    parsing::{prometheus::PrometheusParser, ParseData},
};
use anyhow::Result;
use axum::{
    debug_handler,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use tokio_util::bytes::Bytes;

fn verify_headers(headers: &HeaderMap) -> Result<(), AppError> {
    // Check that we have the right content encoding, that must be snappy
    match headers.get("content-encoding") {
        Some(content_encoding) => match content_encoding.to_str() {
            Ok("snappy") | Ok("SNAPPY") => {}
            _ => {
                return Err(AppError::BadRequest(anyhow::anyhow!(
                    "Unsupported content-encoding, must be snappy"
                )));
            }
        },
        None => {
            return Err(AppError::BadRequest(anyhow::anyhow!(
                "Missing content-encoding header"
            )));
        }
    }

    // Check that the content type is protocol buffer
    match headers.get("content-type") {
        Some(content_type) => match content_type.to_str() {
            Ok("application/x-protobuf") | Ok("APPLICATION/X-PROTOBUF") => {}
            _ => {
                return Err(AppError::BadRequest(anyhow::anyhow!(
                    "Unsupported content-type, must be application/x-protobuf"
                )));
            }
        },
        None => {
            return Err(AppError::BadRequest(anyhow::anyhow!(
                "Missing content-type header"
            )));
        }
    }

    // Check that the remote write version is supported
    match headers.get("x-prometheus-remote-write-version") {
        Some(version) => match version.to_str() {
            Ok("0.1.0") => {}
            _ => {
                return Err(AppError::BadRequest(anyhow::anyhow!(
                    "Unsupported x-prometheus-remote-write-version, must be 0.1.0"
                )));
            }
        },
        None => {
            return Err(AppError::BadRequest(anyhow::anyhow!(
                "Missing x-prometheus-remote-write-version header"
            )));
        }
    }

    Ok(())
}

/// Prometheus Remote Write API.
///
/// Allows you to write data from Prometheus to SensApp.
///
/// It follows the [Prometheus Remote Write specification](https://prometheus.io/docs/concepts/remote_write_spec/).
#[utoipa::path(
    post,
    path = "/api/v1/prometheus_remote_write",
    tag = "Prometheus",
    request_body(
        content = Bytes,
        content_type = "application/x-protobuf",
        description = "Prometheus Remote Write endpoint. [Reference](https://prometheus.io/docs/concepts/remote_write_spec/)",
    ),
    params(
        ("content-encoding" = String, Header, format = "snappy", description = "Content encoding, must be snappy"),
        ("content-type" = String, Header, format = "application/x-protobuf", description = "Content type, must be application/x-protobuf"),
        ("x-prometheus-remote-write-version" = String, Header, format = "0.1.0", description = "Prometheus Remote Write version, must be 0.1.0"),
    ),
    responses(
        (status = 204, description = "No Content"),
        (status = 400, description = "Bad Request", body = AppError),
        (status = 500, description = "Internal Server Error", body = AppError),
    )
)]
#[debug_handler]
pub async fn publish_prometheus(
    State(state): State<HttpServerState>,
    headers: HeaderMap,
    bytes: Bytes,
) -> Result<StatusCode, AppError> {
    verify_headers(&headers)?;

    let mut batch_builder = BatchBuilder::new()?;
    let parser = PrometheusParser;
    parser.parse_data(&bytes, None, &mut batch_builder).await?;

    match batch_builder.send_what_is_left(state.event_bus).await {
        Ok(Some(mut receiver)) => {
            receiver.wait().await?;
        }
        Ok(None) => {}
        Err(error) => {
            return Err(AppError::InternalServerError(anyhow::anyhow!(error)));
        }
    }

    Ok(StatusCode::NO_CONTENT)
}
