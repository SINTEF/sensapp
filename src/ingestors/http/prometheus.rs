use std::sync::Arc;

use super::{app_error::AppError, state::HttpServerState};
use crate::{
    datamodel::{
        batch::SingleSensorBatch, batch_builder::BatchBuilder,
        sensapp_datetime::SensAppDateTimeExt, SensAppDateTime, TypedSamples,
    },
    parsing::{
        prometheus::{
            remote_read_request_models::{Query, ResponseType},
            remote_read_request_parser::parse_remote_read_request,
            remote_read_response::{
                ChunkedReadResponse, ChunkedSeries, QueryResult, ReadResponse, TimeSeries,
            },
            PrometheusParser,
        },
        ParseData,
    },
};
use anyhow::Result;
use async_stream::stream;
use axum::{
    body::Body,
    debug_handler,
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
};
use futures::stream::StreamExt;
use futures::Stream;
use prost::Message;
use tokio_util::bytes::Bytes;

#[derive(Debug, PartialEq)]
enum VerifyHeadersMode {
    Read,
    Write,
}

fn verify_headers(headers: &HeaderMap, mode: VerifyHeadersMode) -> Result<(), AppError> {
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

    if mode == VerifyHeadersMode::Write {
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
    } else {
        // Check that the remote read version is supported
        match headers.get("x-prometheus-remote-read-version") {
            Some(version) => match version.to_str() {
                Ok("0.1.0") => {}
                _ => {
                    return Err(AppError::BadRequest(anyhow::anyhow!(
                        "Unsupported x-prometheus-remote-read-version, must be 0.1.0"
                    )));
                }
            },
            None => {
                return Err(AppError::BadRequest(anyhow::anyhow!(
                    "Missing x-prometheus-remote-read-version header"
                )));
            }
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
        description = "Prometheus Remote Write data. [Reference](https://prometheus.io/docs/concepts/remote_write_spec/)",
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
pub async fn prometheus_remote_write(
    State(state): State<HttpServerState>,
    headers: HeaderMap,
    bytes: Bytes,
) -> Result<StatusCode, AppError> {
    verify_headers(&headers, VerifyHeadersMode::Write)?;

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

/// Prometheus Remote Read API.
///
/// Read data from SensApp in Prometheus.
///
/// It follows the [Prometheus Remote Read specification](https://prometheus.io/docs/prometheus/latest/querying/remote_read_api/).
#[utoipa::path(
    post,
    path = "/api/v1/prometheus_remote_read",
    tag = "Prometheus",
    request_body(
        content = String,
        content_type = "application/x-protobuf",
        description = "Prometheus Remote Read query. [Reference](https://prometheus.io/docs/prometheus/latest/querying/remote_read_api/).",
    ),
    params(
        ("content-encoding" = String, Header, format = "snappy", description = "Content encoding, must be snappy"),
        ("content-type" = String, Header, format = "application/x-protobuf", description = "Content type, must be application/x-protobuf"),
    ),
    responses(
        (status = 200, description = "Prometheus Remote Read data"),
        (status = 400, description = "Bad Request", body = AppError),
        (status = 500, description = "Internal Server Error", body = AppError),
    )
)]
#[debug_handler]
pub async fn prometheus_remote_read(
    State(state): State<HttpServerState>,
    headers: HeaderMap,
    bytes: Bytes,
) -> Result<(StatusCode, HeaderMap, Body), AppError> {
    println!("Prometheus Remote Write API");
    println!("bytes: {:?}", bytes);
    println!("headers: {:?}", headers);
    verify_headers(&headers, VerifyHeadersMode::Read)?;

    let read_request = parse_remote_read_request(&bytes)?;
    println!("read_request: {:?}", read_request);
    let xor_response_type =
        read_request
            .accepted_response_types
            .iter()
            .any(|accepted_response_type| {
                *accepted_response_type == ResponseType::StreamedXorChunks as i32
            });

    let stream = prometheus_read_stream(read_request.queries);

    if xor_response_type {
        prometheus_read_xor(Box::pin(stream))
    } else {
        prometheus_read_protobuf(Box::pin(stream)).await
    }
}

fn prometheus_read_stream(queries: Vec<Query>) -> impl Stream<Item = (usize, SingleSensorBatch)> {
    stream! {
        for (i, query) in queries.into_iter().enumerate() {
            let start = query.start_timestamp_ms;
            let end = query.end_timestamp_ms;

            // create 100 samples
            let n_samples = 10_000_i64;
            let mut samples = Vec::with_capacity(n_samples as usize);
            let step = (end - start) / n_samples;
            for j in 0..n_samples {
                let timestamp = start + step * j;
                let value = (i + 1) as f64 * j as f64;
                samples.push(crate::datamodel::sample::Sample {
                    datetime: SensAppDateTime::from_unix_milliseconds_i64(timestamp),
                    value,
                });
            }

            // create the single sensor batch
            let batch = SingleSensorBatch::new(
                Arc::new(crate::datamodel::Sensor::new_without_uuid(
                    "canard".to_string(),
                    crate::datamodel::SensorType::Float,
                    None,
                    None,
                )
                .unwrap()),
                TypedSamples::Float(samples.into()),
            );

            yield (i, batch);
        }
    }
}

async fn prometheus_read_protobuf<S>(
    mut chunk_stream: S,
) -> Result<(StatusCode, HeaderMap, Body), AppError>
where
    S: Stream<Item = (usize, SingleSensorBatch)> + Unpin,
{
    let mut headers = HeaderMap::new();
    headers.insert(
        "Content-Type",
        HeaderValue::from_static("application/x-protobuf"),
    );
    headers.insert("Content-Encoding", HeaderValue::from_static("snappy"));

    let mut current_query_index = 0;
    let mut current_query_result = QueryResult {
        timeseries: Vec::new(),
    };
    let mut read_response = ReadResponse { results: vec![] };

    while let Some((query_index, batch)) = chunk_stream.next().await {
        if query_index != current_query_index {
            read_response.results.push(current_query_result);
            current_query_index = query_index;
            current_query_result = QueryResult {
                timeseries: Vec::new(),
            };
        }
        let timeserie = TimeSeries::from_single_sensor_batch(&batch).await;
        current_query_result.timeseries.push(timeserie);
    }
    read_response.results.push(current_query_result);

    // Serialise to protobuf binary
    let mut proto_buffer: Vec<u8> = Vec::new();
    read_response.encode(&mut proto_buffer).unwrap();

    // Snappy it
    let mut encoder = snap::raw::Encoder::new();
    let buffer = encoder.compress_vec(&proto_buffer).unwrap();

    // It could be possible to have some performance gains by writing the
    // buffer directly to the stream instead of a temporary buffer.
    // But the XOR stream should be preffered if performance is a concern.

    Ok((StatusCode::OK, headers, buffer.into()))
}

fn prometheus_read_xor<S>(chunk_stream: S) -> Result<(StatusCode, HeaderMap, Body), AppError>
where
    S: Stream<Item = (usize, SingleSensorBatch)> + Unpin + Send + 'static,
{
    let body_stream = chunk_stream.then(|(query_index, batch)| async move {
        let chunked_serie = ChunkedSeries::from_single_sensor_batch(&batch).await;

        let chunked_read_response = ChunkedReadResponse {
            chunked_series: vec![chunked_serie],
            query_index: query_index as i64,
        };

        let mut buffer: Vec<u8> = Vec::new();
        chunked_read_response.promotheus_stream_encode(&mut buffer)?;

        Ok::<Vec<u8>, anyhow::Error>(buffer)
    });

    let mut headers = HeaderMap::new();
    headers.insert(
        "Content-Type",
        HeaderValue::from_static(
            "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse",
        ),
    );
    headers.insert("Content-Encoding", HeaderValue::from_static(""));

    let body = Body::from_stream(body_stream);

    Ok((StatusCode::OK, headers, body))
}
