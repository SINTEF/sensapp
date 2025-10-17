use std::sync::Arc;

use crate::{
    datamodel::{
        Sample, SensAppDateTime, Sensor, SensorType, TypedSamples, batch_builder::BatchBuilder,
        sensapp_datetime::SensAppDateTimeExt, sensapp_vec::SensAppLabels, unit::Unit,
    },
    parsing::prometheus::remote_write_parser::parse_remote_write_request,
};

use super::{app_error::AppError, state::HttpServerState};
use anyhow::Result;
use axum::{
    debug_handler,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use tokio_util::bytes::Bytes;
use tracing::{debug, info};

/// Validates required Prometheus Remote Write API headers.
///
/// Prometheus sends data with specific headers:
/// - `content-encoding`: must be "snappy" (compression format)
/// - `content-type`: must be "application/x-protobuf" (protobuf format)
/// - `x-prometheus-remote-write-version`: must be "0.1.0" (API version)
fn verify_headers(headers: &HeaderMap) -> Result<(), AppError> {
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

    // Check that the remote write version is supported
    match headers.get("x-prometheus-remote-write-version") {
        Some(version) => match version.to_str() {
            Ok("0.1.0") => {}
            _ => {
                return Err(AppError::bad_request(anyhow::anyhow!(
                    "Unsupported x-prometheus-remote-write-version, must be 0.1.0"
                )));
            }
        },
        None => {
            return Err(AppError::bad_request(anyhow::anyhow!(
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
    debug!("Prometheus remote write: received {} bytes", bytes.len());

    // Verify headers
    verify_headers(&headers)?;

    // Parse the content
    let write_request = parse_remote_write_request(&bytes)?;

    // Regularly, prometheus sends metadata on the undocumented reserved field,
    // so we stop immediately when it happens.
    if write_request.timeseries.is_empty() {
        return Ok(StatusCode::NO_CONTENT);
    }

    debug!("Processing {} timeseries", write_request.timeseries.len());

    let mut batch_builder = BatchBuilder::new()?;
    for time_serie in write_request.timeseries {
        let mut labels = SensAppLabels::with_capacity(time_serie.labels.len());
        let mut name: Option<String> = None;
        let mut unit: Option<Unit> = None;
        // Extract special labels: __name__ (metric name) and "unit" (custom SensApp field)
        // All labels including these are stored as-is in SensApp for full metadata preservation
        for label in time_serie.labels {
            match label.name.as_str() {
                "__name__" => {
                    name = Some(label.value.clone());
                }
                "unit" => {
                    unit = Some(Unit::new(label.value.clone(), None));
                }
                _ => {}
            }
            labels.push((label.name, label.value));
        }
        let name = match name {
            Some(name) => name,
            None => {
                return Err(AppError::bad_request(anyhow::anyhow!(
                    "A time serie is missing its __name__ label"
                )));
            }
        };

        // Prometheus has a very simple model, it's always a float.
        let sensor = Sensor::new_without_uuid(name, SensorType::Float, unit, Some(labels))?;

        // We can now add the samples
        let samples = TypedSamples::Float(
            time_serie
                .samples
                .into_iter()
                .map(|sample| Sample {
                    datetime: SensAppDateTime::from_unix_milliseconds_i64(sample.timestamp),
                    value: sample.value,
                })
                .collect(),
        );

        batch_builder.add(Arc::new(sensor), samples).await?;
    }

    match batch_builder.send_what_is_left(state.storage.clone()).await {
        Ok(true) => {
            info!("Prometheus: Batch sent successfully");
        }
        Ok(false) => {
            debug!("Prometheus: No data to send");
        }
        Err(error) => {
            return Err(AppError::internal_server_error(error));
        }
    }

    // OK no content
    Ok(StatusCode::NO_CONTENT)
}
