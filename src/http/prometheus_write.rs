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
use std::time::Instant;
use tokio_util::bytes::Bytes;
use tracing::{debug, info};

/// Validates required Prometheus Remote Write API headers.
///
/// Prometheus sends data with specific headers:
/// - `content-encoding`: must be "snappy" (compression format)
/// - `content-type`: must be "application/x-protobuf" (protobuf format)
/// - `x-prometheus-remote-write-version`: must be "0.1.0" (API version)
fn verify_headers(headers: &HeaderMap) -> Result<(), AppError> {
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
    let metrics = state.metrics.clone();
    let started = Instant::now();

    let result = async move {
        debug!("Prometheus remote write: received {} bytes", bytes.len());

        verify_headers(&headers)?;

        let write_request = parse_remote_write_request(&bytes)?;

        if write_request.timeseries.is_empty() {
            return Ok::<_, AppError>((StatusCode::NO_CONTENT, 0usize, 0usize));
        }

        debug!("Processing {} timeseries", write_request.timeseries.len());

        let mut batch_builder = BatchBuilder::new()?;
        let mut series = 0usize;
        let mut sample_count = 0usize;
        for time_serie in write_request.timeseries {
            let mut labels = SensAppLabels::with_capacity(time_serie.labels.len());
            let mut name: Option<String> = None;
            let mut unit: Option<Unit> = None;
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

            let sensor = Sensor::new_without_uuid(name, SensorType::Float, unit, Some(labels))?;

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

            series += 1;
            sample_count += samples.len();
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

        Ok((StatusCode::NO_CONTENT, series, sample_count))
    }
    .await;

    metrics.observe_operation_result(
        "write",
        "prometheus_remote_write",
        started.elapsed(),
        result.is_ok(),
    );
    if let Ok((_, series, samples)) = &result {
        metrics.observe_series("write", "prometheus_remote_write", *series);
        metrics.observe_samples("write", "prometheus_remote_write", *samples);
    }

    result.map(|(status, _, _)| status)
}
