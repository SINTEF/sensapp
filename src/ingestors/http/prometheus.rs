use std::io::Write;

use super::{app_error::AppError, state::HttpServerState};
use crate::{
    datamodel::{
        batch_builder::BatchBuilder, sensapp_datetime::SensAppDateTimeExt, SensAppDateTime,
    },
    parsing::{
        prometheus::{
            remote_read_request_models::ResponseType,
            remote_read_request_parser::parse_remote_read_request,
            remote_read_response::{
                chunk, Chunk, ChunkedReadResponse, ChunkedSeries, Label, QueryResult, ReadResponse,
                Sample, TimeSeries,
            },
            PrometheusParser,
        },
        ParseData,
    },
};
use anyhow::Result;
use axum::{
    debug_handler,
    extract::State,
    http::{HeaderMap, HeaderValue, StatusCode},
};
use prost::Message;
use rusty_chunkenc::{crc32c::write_crc32c, uvarint::write_uvarint, xor};
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
) -> Result<(StatusCode, HeaderMap, Bytes), AppError> {
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

    println!("xor_response_type: {:?}", xor_response_type);

    // Write status header
    // content-type: application/x-protobuf

    let mut headers = HeaderMap::new();
    if xor_response_type {
        headers.insert(
            "Content-Type",
            HeaderValue::from_static(
                "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse",
            ),
        );
        headers.insert("Content-Encoding", HeaderValue::from_static(""));
    } else {
        headers.insert(
            "Content-Type",
            HeaderValue::from_static("application/x-protobuf"),
        );
        headers.insert("Content-Encoding", HeaderValue::from_static("snappy"));
    }

    for (i, query) in read_request.queries.iter().enumerate() {
        println!("query matcher: {:?}", query.to_sensor_matcher());
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

        if xor_response_type {
            let mut chunk_buffer: Vec<u8> = Vec::new();
            let chunk = rusty_chunkenc::xor::XORChunk::new(
                samples
                    .into_iter()
                    .map(|sample| rusty_chunkenc::XORSample {
                        timestamp: sample.datetime.to_unix_milliseconds().floor() as i64,
                        value: sample.value,
                    })
                    .collect(),
            );
            chunk.write(&mut chunk_buffer).unwrap();

            println!("buffer: {}", base64::encode(&chunk_buffer));

            let chunked_read_response = ChunkedReadResponse {
                chunked_series: vec![ChunkedSeries {
                    labels: vec![Label {
                        name: "__name__".to_string(),
                        value: "canard".to_string(),
                    }],
                    chunks: vec![Chunk {
                        min_time_ms: start,
                        max_time_ms: end,
                        r#type: chunk::Encoding::Xor as i32,
                        data: chunk_buffer,
                    }],
                }],
                query_index: i as i64,
            };

            // convert to bytes
            let mut proto_buffer: Vec<u8> = Vec::new();
            chunked_read_response.encode(&mut proto_buffer).unwrap();

            let mut buffer: Vec<u8> = Vec::new();
            write_uvarint(proto_buffer.len() as u64, &mut buffer).unwrap();

            write_crc32c(&proto_buffer, &mut buffer).unwrap();
            buffer.write_all(&proto_buffer).unwrap();

            // just to let it go
            if buffer.len() > 0 {
                return Ok((StatusCode::OK, headers.clone(), buffer.into()));
            }
        } else {
            let read_response = ReadResponse {
                results: vec![QueryResult {
                    timeseries: vec![TimeSeries {
                        labels: vec![Label {
                            name: "__name__".to_string(),
                            value: "canard".to_string(),
                        }],
                        samples: samples
                            .into_iter()
                            .map(|sample| Sample {
                                timestamp: sample.datetime.to_unix_milliseconds().floor() as i64,
                                value: sample.value,
                            })
                            .collect(),
                    }],
                }],
            };

            // convert to bytes
            let mut proto_buffer: Vec<u8> = Vec::new();
            read_response.encode(&mut proto_buffer).unwrap();

            // snappy it
            let mut encoder = snap::raw::Encoder::new();
            let buffer = encoder.compress_vec(&proto_buffer).unwrap();

            if buffer.len() > 0 {
                return Ok((StatusCode::OK, headers.clone(), buffer.into()));
            }
        }
    }

    Ok((StatusCode::NO_CONTENT, headers, Bytes::new()))
}
