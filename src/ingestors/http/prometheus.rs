use std::sync::Arc;

use crate::{
    datamodel::{
        batch_builder::BatchBuilder, sensapp_datetime::SensAppDateTimeExt,
        sensapp_vec::SensAppLabels, unit::Unit, Sample, SensAppDateTime, Sensor, SensorType,
        TypedSamples,
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

#[debug_handler]
pub async fn publish_prometheus(
    State(state): State<HttpServerState>,
    headers: HeaderMap,
    bytes: Bytes,
) -> Result<StatusCode, AppError> {
    // println!("InfluxDB publish");
    // println!("bucket: {}", bucket);
    // println!("org: {:?}", org);
    // println!("org_id: {:?}", org_id);
    // println!("precision: {:?}", precision);
    // println!("bytes: {:?}", bytes);

    println!("Received {} bytes", bytes.len());

    // Verify headers
    verify_headers(&headers)?;

    // Parse the content
    let write_request = parse_remote_write_request(&bytes)?;

    // Regularly, prometheus sends metadata on the undocumented reserved field,
    // so we stop immediately when it happens.
    if write_request.timeseries.is_empty() {
        return Ok(StatusCode::NO_CONTENT);
    }

    println!("Received {} timeseries", write_request.timeseries.len());

    let mut batch_builder = BatchBuilder::new()?;
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
                return Err(AppError::BadRequest(anyhow::anyhow!(
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
        // batch_builder.send_if_batch_full(event_bus.clone()).await?;
    }

    match batch_builder.send_what_is_left(state.event_bus).await {
        Ok(Some(mut receiver)) => {
            receiver.wait().await?;
        }
        Ok(None) => {}
        Err(error) => {
            return Err(AppError::InternalServerError(anyhow::anyhow!(error)));
        }
    }

    // OK no content
    Ok(StatusCode::NO_CONTENT)
}
