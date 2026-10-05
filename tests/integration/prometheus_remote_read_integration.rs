//! Integration tests for Prometheus Remote Read API.
//!
//! These tests verify that the remote read endpoint correctly:
//! - Parses incoming requests
//! - Queries the storage backend
//! - Returns properly formatted responses (both SAMPLES and STREAMED_XOR_CHUNKS)

use crate::common;

use anyhow::Result;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::post;
use common::TestDb;
use prost::Message;
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch_builder::BatchBuilder;
use sensapp::datamodel::sensapp_vec::SensAppLabels;
use sensapp::datamodel::{Sample, Sensor, SensorType, TypedSamples};
use sensapp::http::metrics::HttpMetrics;
use sensapp::http::prometheus_read::prometheus_remote_read;
use sensapp::http::state::HttpServerState;
use sensapp::parsing::prometheus::remote_read_models::{
    ChunkedReadResponse, LabelMatcher as PromLabelMatcher, Query, ReadHints, ReadRequest,
    ReadResponse, label_matcher, read_request,
};
use serial_test::serial;
use std::io::Cursor;
use std::sync::Arc;
use tower::ServiceExt;
use uuid::Uuid;

/// Base timestamp for tests: Jan 1, 2024 00:00:00 UTC in milliseconds
const BASE_TS_MS: i64 = 1704067200 * 1000;

// Ensure configuration is loaded once for all tests in this module
static INIT: std::sync::Once = std::sync::Once::new();
fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

/// Helper to create a test sensor with labels
fn create_sensor_with_labels(
    name: &str,
    sensor_type: SensorType,
    labels: Vec<(String, String)>,
) -> Sensor {
    let labels: SensAppLabels = labels.into_iter().collect();
    Sensor {
        uuid: Uuid::new_v4(),
        name: name.to_string(),
        sensor_type,
        unit: None,
        labels,
    }
}

/// Helper to create float samples at specific timestamps (in milliseconds)
fn create_float_samples_at_times(times_ms: &[i64], values: &[f64]) -> TypedSamples {
    use sensapp::datamodel::sensapp_datetime::SensAppDateTimeExt;
    let samples: smallvec::SmallVec<[Sample<f64>; 4]> = times_ms
        .iter()
        .zip(values.iter())
        .map(|(&time_ms, &value)| Sample {
            datetime: sensapp::datamodel::SensAppDateTime::from_unix_milliseconds_i64(time_ms),
            value,
        })
        .collect();
    TypedSamples::Float(samples)
}

/// Helper to create integer samples at specific timestamps (in milliseconds)
fn create_integer_samples_at_times(times_ms: &[i64], values: &[i64]) -> TypedSamples {
    use sensapp::datamodel::sensapp_datetime::SensAppDateTimeExt;
    let samples: smallvec::SmallVec<[Sample<i64>; 4]> = times_ms
        .iter()
        .zip(values.iter())
        .map(|(&time_ms, &value)| Sample {
            datetime: sensapp::datamodel::SensAppDateTime::from_unix_milliseconds_i64(time_ms),
            value,
        })
        .collect();
    TypedSamples::Integer(samples)
}

/// Helper to publish sensors with samples to the storage
async fn publish_test_sensors(
    storage: &Arc<dyn sensapp::storage::StorageInstance>,
    sensors_with_samples: Vec<(Sensor, TypedSamples)>,
) -> Result<()> {
    let mut batch_builder = BatchBuilder::new()?;

    for (sensor, samples) in sensors_with_samples {
        let sensor_arc = Arc::new(sensor);
        batch_builder.add(sensor_arc.clone(), samples).await?;
    }

    batch_builder.send_what_is_left(storage.clone()).await?;

    Ok(())
}

/// Create a test app with the Prometheus remote read endpoint
fn create_test_app(storage: Arc<dyn sensapp::storage::StorageInstance>) -> Router {
    create_test_app_with_limit(storage, sensapp::http::limits::DEFAULT_MAX_QUERY_SAMPLES)
}

/// The same, with `SENSAPP_HTTP_MAX_QUERY_SAMPLES` set to `max_query_samples`
fn create_test_app_with_limit(
    storage: Arc<dyn sensapp::storage::StorageInstance>,
    max_query_samples: usize,
) -> Router {
    let state = HttpServerState {
        name: Arc::new("SensApp Test".to_string()),
        storage,
        metrics: Arc::new(HttpMetrics::new()),
        influxdb_with_numeric: false,
        max_query_samples,
        auth: None,
    };

    Router::new()
        .route(
            "/api/v1/prometheus_remote_read",
            post(prometheus_remote_read),
        )
        .with_state(state)
}

/// Build a remote read request body (snappy-compressed protobuf)
fn build_remote_read_request(queries: Vec<Query>, response_types: Vec<i32>) -> Result<Vec<u8>> {
    let request = ReadRequest {
        queries,
        accepted_response_types: response_types,
    };

    let encoded = request.encode_to_vec();
    let compressed = snap::raw::Encoder::new().compress_vec(&encoded)?;
    Ok(compressed)
}

/// Parse a remote read response (snappy-compressed protobuf)
fn parse_remote_read_response(body: &[u8]) -> Result<ReadResponse> {
    let decompressed = snap::raw::Decoder::new().decompress_vec(body)?;
    let response = ReadResponse::decode(&mut Cursor::new(decompressed))?;
    Ok(response)
}

fn parse_streamed_chunked_response(body: &[u8]) -> Result<Vec<ChunkedReadResponse>> {
    let mut cursor = 0usize;
    let mut responses = Vec::new();

    while cursor < body.len() {
        let mut length = 0usize;
        let mut shift = 0usize;

        loop {
            let byte = body[cursor];
            cursor += 1;
            length |= ((byte & 0x7F) as usize) << shift;
            if byte & 0x80 == 0 {
                break;
            }
            shift += 7;
        }

        cursor += 4; // Skip CRC32C.
        let message_end = cursor + length;
        responses.push(ChunkedReadResponse::decode(&body[cursor..message_end])?);
        cursor = message_end;
    }

    Ok(responses)
}

/// Send a remote read request and return the response
async fn send_remote_read_request(app: &Router, body: Vec<u8>) -> Result<(StatusCode, Vec<u8>)> {
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/prometheus_remote_read")
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "snappy")
        .header("x-prometheus-remote-read-version", "0.1.0")
        .body(Body::from(body))?;

    let response = app.clone().oneshot(request).await?;
    let status = response.status();
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await?
        .to_vec();

    Ok((status, body_bytes))
}

// ============================================================================
// Basic Remote Read Tests (SAMPLES response type)
// ============================================================================

/// Test basic remote read with exact name match
#[tokio::test]
#[serial]
async fn test_remote_read_basic() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create test sensors with samples
    // Use realistic timestamps: Jan 1, 2024 + offsets in seconds, converted to ms
    let sensor = create_sensor_with_labels("cpu_usage", SensorType::Float, vec![]);
    let times_ms = vec![BASE_TS_MS, BASE_TS_MS + 1000, BASE_TS_MS + 2000]; // 0, +1s, +2s
    let values = vec![10.0, 20.0, 30.0];

    publish_test_sensors(
        &storage,
        vec![(sensor, create_float_samples_at_times(&times_ms, &values))],
    )
    .await?;

    // Build and send remote read request
    let app = create_test_app(storage);
    let query = Query {
        start_timestamp_ms: BASE_TS_MS - 1000, // 1 second before
        end_timestamp_ms: BASE_TS_MS + 10000,  // 10 seconds after
        matchers: vec![PromLabelMatcher {
            r#type: label_matcher::Type::Eq as i32,
            name: "__name__".to_string(),
            value: "cpu_usage".to_string(),
        }],
        hints: None,
    };

    let body = build_remote_read_request(
        vec![query],
        vec![read_request::ResponseType::Samples as i32],
    )?;

    let (status, response_body) = send_remote_read_request(&app, body).await?;

    assert_eq!(status, StatusCode::OK, "Response should be OK");

    // Parse response
    let response = parse_remote_read_response(&response_body)?;
    assert_eq!(response.results.len(), 1, "Should have one query result");

    let query_result = &response.results[0];
    assert_eq!(
        query_result.timeseries.len(),
        1,
        "Should have one time series"
    );

    let timeseries = &query_result.timeseries[0];

    // Check labels - should have __name__ label
    let name_label = timeseries.labels.iter().find(|l| l.name == "__name__");
    assert!(name_label.is_some(), "Should have __name__ label");
    assert_eq!(name_label.unwrap().value, "cpu_usage");

    // Check samples
    assert_eq!(timeseries.samples.len(), 3, "Should have 3 samples");
    assert_eq!(timeseries.samples[0].timestamp, BASE_TS_MS);
    assert_eq!(timeseries.samples[0].value, 10.0);
    assert_eq!(timeseries.samples[1].timestamp, BASE_TS_MS + 1000);
    assert_eq!(timeseries.samples[1].value, 20.0);
    assert_eq!(timeseries.samples[2].timestamp, BASE_TS_MS + 2000);
    assert_eq!(timeseries.samples[2].value, 30.0);

    Ok(())
}

/// Test remote read with label matchers
#[tokio::test]
#[serial]
async fn test_remote_read_with_labels() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create sensors with different labels
    let sensor_prod = create_sensor_with_labels(
        "http_requests",
        SensorType::Float,
        vec![("environment".to_string(), "production".to_string())],
    );
    let sensor_staging = create_sensor_with_labels(
        "http_requests",
        SensorType::Float,
        vec![("environment".to_string(), "staging".to_string())],
    );

    let times_ms = vec![BASE_TS_MS, BASE_TS_MS + 1000];
    publish_test_sensors(
        &storage,
        vec![
            (
                sensor_prod,
                create_float_samples_at_times(&times_ms, &[100.0, 200.0]),
            ),
            (
                sensor_staging,
                create_float_samples_at_times(&times_ms, &[50.0, 60.0]),
            ),
        ],
    )
    .await?;

    // Query for production environment only
    let app = create_test_app(storage);
    let query = Query {
        start_timestamp_ms: BASE_TS_MS - 1000,
        end_timestamp_ms: BASE_TS_MS + 10000,
        matchers: vec![
            PromLabelMatcher {
                r#type: label_matcher::Type::Eq as i32,
                name: "__name__".to_string(),
                value: "http_requests".to_string(),
            },
            PromLabelMatcher {
                r#type: label_matcher::Type::Eq as i32,
                name: "environment".to_string(),
                value: "production".to_string(),
            },
        ],
        hints: None,
    };

    let body = build_remote_read_request(
        vec![query],
        vec![read_request::ResponseType::Samples as i32],
    )?;

    let (status, response_body) = send_remote_read_request(&app, body).await?;
    assert_eq!(status, StatusCode::OK);

    let response = parse_remote_read_response(&response_body)?;
    assert_eq!(response.results.len(), 1);
    assert_eq!(
        response.results[0].timeseries.len(),
        1,
        "Should find only production sensor"
    );

    let timeseries = &response.results[0].timeseries[0];

    // Verify it's the production sensor by checking sample values
    assert_eq!(timeseries.samples[0].value, 100.0);
    assert_eq!(timeseries.samples[1].value, 200.0);

    Ok(())
}

/// Test remote read with regex matcher
#[tokio::test]
#[serial]
async fn test_remote_read_regex_matcher() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create sensors with different names
    let sensor1 = create_sensor_with_labels("cpu_usage", SensorType::Float, vec![]);
    let sensor2 = create_sensor_with_labels("cpu_temperature", SensorType::Float, vec![]);
    let sensor3 = create_sensor_with_labels("memory_usage", SensorType::Float, vec![]);

    let times_ms = vec![BASE_TS_MS];
    publish_test_sensors(
        &storage,
        vec![
            (sensor1, create_float_samples_at_times(&times_ms, &[10.0])),
            (sensor2, create_float_samples_at_times(&times_ms, &[50.0])),
            (sensor3, create_float_samples_at_times(&times_ms, &[80.0])),
        ],
    )
    .await?;

    // Query with regex matching "cpu.*"
    let app = create_test_app(storage);
    let query = Query {
        start_timestamp_ms: BASE_TS_MS - 1000,
        end_timestamp_ms: BASE_TS_MS + 10000,
        matchers: vec![PromLabelMatcher {
            r#type: label_matcher::Type::Re as i32,
            name: "__name__".to_string(),
            value: "cpu.*".to_string(),
        }],
        hints: None,
    };

    let body = build_remote_read_request(
        vec![query],
        vec![read_request::ResponseType::Samples as i32],
    )?;

    let (status, response_body) = send_remote_read_request(&app, body).await?;
    assert_eq!(status, StatusCode::OK);

    let response = parse_remote_read_response(&response_body)?;
    assert_eq!(response.results.len(), 1);
    assert_eq!(
        response.results[0].timeseries.len(),
        2,
        "Should find two cpu sensors"
    );

    Ok(())
}

/// Test remote read with time range filtering
#[tokio::test]
#[serial]
async fn test_remote_read_time_range() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create sensor with samples spread over time
    let sensor = create_sensor_with_labels("time_test", SensorType::Float, vec![]);
    // Use realistic timestamps: 0s, +1s, +2s, +3s, +4s from base
    let times_ms = vec![
        BASE_TS_MS,
        BASE_TS_MS + 1000,
        BASE_TS_MS + 2000,
        BASE_TS_MS + 3000,
        BASE_TS_MS + 4000,
    ];
    let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];

    publish_test_sensors(
        &storage,
        vec![(sensor, create_float_samples_at_times(&times_ms, &values))],
    )
    .await?;

    // Query for specific time range: samples at +1s, +2s, +3s (indices 1, 2, 3)
    let app = create_test_app(storage);
    let query = Query {
        start_timestamp_ms: BASE_TS_MS + 1000,
        end_timestamp_ms: BASE_TS_MS + 3000,
        matchers: vec![PromLabelMatcher {
            r#type: label_matcher::Type::Eq as i32,
            name: "__name__".to_string(),
            value: "time_test".to_string(),
        }],
        hints: None,
    };

    let body = build_remote_read_request(
        vec![query],
        vec![read_request::ResponseType::Samples as i32],
    )?;

    let (status, response_body) = send_remote_read_request(&app, body).await?;
    assert_eq!(status, StatusCode::OK);

    let response = parse_remote_read_response(&response_body)?;
    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].timeseries.len(), 1);

    let samples = &response.results[0].timeseries[0].samples;
    // Should have samples at +1s, +2s, +3s (3 samples)
    assert_eq!(samples.len(), 3, "Should have 3 samples within time range");

    // Verify timestamps
    assert_eq!(samples[0].timestamp, BASE_TS_MS + 1000);
    assert_eq!(samples[1].timestamp, BASE_TS_MS + 2000);
    assert_eq!(samples[2].timestamp, BASE_TS_MS + 3000);

    Ok(())
}

/// Test remote read with multiple queries
#[tokio::test]
#[serial]
async fn test_remote_read_multiple_queries() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create two different sensors
    let sensor1 = create_sensor_with_labels("metric_a", SensorType::Float, vec![]);
    let sensor2 = create_sensor_with_labels("metric_b", SensorType::Float, vec![]);

    let times_ms = vec![BASE_TS_MS, BASE_TS_MS + 1000];
    publish_test_sensors(
        &storage,
        vec![
            (
                sensor1,
                create_float_samples_at_times(&times_ms, &[10.0, 20.0]),
            ),
            (
                sensor2,
                create_float_samples_at_times(&times_ms, &[30.0, 40.0]),
            ),
        ],
    )
    .await?;

    // Send request with two queries
    let app = create_test_app(storage);
    let query1 = Query {
        start_timestamp_ms: BASE_TS_MS - 1000,
        end_timestamp_ms: BASE_TS_MS + 10000,
        matchers: vec![PromLabelMatcher {
            r#type: label_matcher::Type::Eq as i32,
            name: "__name__".to_string(),
            value: "metric_a".to_string(),
        }],
        hints: None,
    };
    let query2 = Query {
        start_timestamp_ms: BASE_TS_MS - 1000,
        end_timestamp_ms: BASE_TS_MS + 10000,
        matchers: vec![PromLabelMatcher {
            r#type: label_matcher::Type::Eq as i32,
            name: "__name__".to_string(),
            value: "metric_b".to_string(),
        }],
        hints: None,
    };

    let body = build_remote_read_request(
        vec![query1, query2],
        vec![read_request::ResponseType::Samples as i32],
    )?;

    let (status, response_body) = send_remote_read_request(&app, body).await?;
    assert_eq!(status, StatusCode::OK);

    let response = parse_remote_read_response(&response_body)?;
    assert_eq!(response.results.len(), 2, "Should have two query results");

    // Verify first query result
    assert_eq!(response.results[0].timeseries.len(), 1);
    assert_eq!(response.results[0].timeseries[0].samples[0].value, 10.0);

    // Verify second query result
    assert_eq!(response.results[1].timeseries.len(), 1);
    assert_eq!(response.results[1].timeseries[0].samples[0].value, 30.0);

    Ok(())
}

/// Test remote read with integer sensor (should convert to f64)
#[tokio::test]
#[serial]
async fn test_remote_read_integer_sensor() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create integer sensor
    let sensor = create_sensor_with_labels("request_count", SensorType::Integer, vec![]);
    let times_ms = vec![BASE_TS_MS, BASE_TS_MS + 1000, BASE_TS_MS + 2000];
    let values = vec![100, 200, 300];

    publish_test_sensors(
        &storage,
        vec![(sensor, create_integer_samples_at_times(&times_ms, &values))],
    )
    .await?;

    // Query the sensor
    let app = create_test_app(storage);
    let query = Query {
        start_timestamp_ms: BASE_TS_MS - 1000,
        end_timestamp_ms: BASE_TS_MS + 10000,
        matchers: vec![PromLabelMatcher {
            r#type: label_matcher::Type::Eq as i32,
            name: "__name__".to_string(),
            value: "request_count".to_string(),
        }],
        hints: None,
    };

    let body = build_remote_read_request(
        vec![query],
        vec![read_request::ResponseType::Samples as i32],
    )?;

    let (status, response_body) = send_remote_read_request(&app, body).await?;
    assert_eq!(status, StatusCode::OK);

    let response = parse_remote_read_response(&response_body)?;
    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].timeseries.len(), 1);

    // Verify samples are converted to f64
    let samples = &response.results[0].timeseries[0].samples;
    assert_eq!(samples.len(), 3);
    assert_eq!(samples[0].value, 100.0);
    assert_eq!(samples[1].value, 200.0);
    assert_eq!(samples[2].value, 300.0);

    Ok(())
}

/// Test remote read silently skips string sensors (non-numeric)
#[tokio::test]
#[serial]
async fn test_remote_read_skips_string_sensor() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create a string sensor (should be skipped)
    let string_sensor = create_sensor_with_labels(
        "status_log",
        SensorType::String,
        vec![("type".to_string(), "log".to_string())],
    );
    let string_samples: smallvec::SmallVec<[Sample<String>; 4]> = vec![Sample {
        // Use a realistic timestamp in seconds (Jan 1, 2024)
        datetime: hifitime::Epoch::from_unix_seconds(1704067200.0),
        value: "OK".to_string(),
    }]
    .into_iter()
    .collect();

    // Create a float sensor (should be returned)
    let float_sensor = create_sensor_with_labels(
        "cpu_usage",
        SensorType::Float,
        vec![("type".to_string(), "metric".to_string())],
    );

    let mut batch_builder = BatchBuilder::new()?;
    batch_builder
        .add(
            Arc::new(string_sensor),
            TypedSamples::String(string_samples),
        )
        .await?;
    batch_builder
        .add(
            Arc::new(float_sensor),
            create_float_samples_at_times(&[BASE_TS_MS], &[42.0]),
        )
        .await?;
    batch_builder.send_what_is_left(storage.clone()).await?;

    // Query for all sensors matching "type" label
    let app = create_test_app(storage);
    let query = Query {
        start_timestamp_ms: BASE_TS_MS - 1000,
        end_timestamp_ms: BASE_TS_MS + 10000,
        matchers: vec![PromLabelMatcher {
            r#type: label_matcher::Type::Re as i32,
            name: "__name__".to_string(),
            value: ".*".to_string(),
        }],
        hints: None,
    };

    let body = build_remote_read_request(
        vec![query],
        vec![read_request::ResponseType::Samples as i32],
    )?;

    let (status, response_body) = send_remote_read_request(&app, body).await?;
    assert_eq!(status, StatusCode::OK);

    let response = parse_remote_read_response(&response_body)?;
    // Should only have the float sensor, string sensor is silently skipped
    assert_eq!(
        response.results[0].timeseries.len(),
        1,
        "Should only have float sensor"
    );

    let name_label = response.results[0].timeseries[0]
        .labels
        .iter()
        .find(|l| l.name == "__name__");
    assert_eq!(name_label.unwrap().value, "cpu_usage");

    Ok(())
}

/// Test remote read returns empty response for non-matching query
#[tokio::test]
#[serial]
async fn test_remote_read_no_matches() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create a sensor
    let sensor = create_sensor_with_labels("existing_metric", SensorType::Float, vec![]);
    publish_test_sensors(
        &storage,
        vec![(sensor, create_float_samples_at_times(&[BASE_TS_MS], &[1.0]))],
    )
    .await?;

    // Query for non-existent metric
    let app = create_test_app(storage);
    let query = Query {
        start_timestamp_ms: BASE_TS_MS - 1000,
        end_timestamp_ms: BASE_TS_MS + 10000,
        matchers: vec![PromLabelMatcher {
            r#type: label_matcher::Type::Eq as i32,
            name: "__name__".to_string(),
            value: "nonexistent_metric".to_string(),
        }],
        hints: None,
    };

    let body = build_remote_read_request(
        vec![query],
        vec![read_request::ResponseType::Samples as i32],
    )?;

    let (status, response_body) = send_remote_read_request(&app, body).await?;
    assert_eq!(status, StatusCode::OK);

    let response = parse_remote_read_response(&response_body)?;
    assert_eq!(response.results.len(), 1);
    assert_eq!(
        response.results[0].timeseries.len(),
        0,
        "Should return empty timeseries for non-matching query"
    );

    Ok(())
}

// ============================================================================
// Header Validation Tests
// ============================================================================

/// Test that missing headers return bad request
#[tokio::test]
#[serial]
async fn test_remote_read_missing_headers() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = create_test_app(storage);

    // Request without required headers
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/prometheus_remote_read")
        .body(Body::empty())?;

    let response = app.clone().oneshot(request).await?;
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "Should return bad request for missing headers"
    );

    Ok(())
}

/// Test that wrong content-encoding returns bad request
#[tokio::test]
#[serial]
async fn test_remote_read_wrong_encoding() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = create_test_app(storage);

    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/prometheus_remote_read")
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "gzip") // Wrong encoding
        .header("x-prometheus-remote-read-version", "0.1.0")
        .body(Body::empty())?;

    let response = app.clone().oneshot(request).await?;
    assert_eq!(
        response.status(),
        StatusCode::BAD_REQUEST,
        "Should return bad request for wrong encoding"
    );

    Ok(())
}

// ============================================================================
// Streamed Response Tests (STREAMED_XOR_CHUNKS)
// ============================================================================

/// Test streamed XOR chunks response type
#[tokio::test]
#[serial]
async fn test_remote_read_streamed_xor_chunks() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create test sensor
    let sensor = create_sensor_with_labels("chunked_metric", SensorType::Float, vec![]);
    let times_ms = vec![
        BASE_TS_MS,
        BASE_TS_MS + 1000,
        BASE_TS_MS + 2000,
        BASE_TS_MS + 3000,
        BASE_TS_MS + 4000,
    ];
    let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];

    publish_test_sensors(
        &storage,
        vec![(sensor, create_float_samples_at_times(&times_ms, &values))],
    )
    .await?;

    // Request streamed XOR chunks response
    let app = create_test_app(storage);
    let query = Query {
        start_timestamp_ms: BASE_TS_MS - 1000,
        end_timestamp_ms: BASE_TS_MS + 10000,
        matchers: vec![PromLabelMatcher {
            r#type: label_matcher::Type::Eq as i32,
            name: "__name__".to_string(),
            value: "chunked_metric".to_string(),
        }],
        hints: None,
    };

    let body = build_remote_read_request(
        vec![query],
        vec![read_request::ResponseType::StreamedXorChunks as i32],
    )?;

    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/prometheus_remote_read")
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "snappy")
        .header("x-prometheus-remote-read-version", "0.1.0")
        .body(Body::from(body))?;

    let response = app.clone().oneshot(request).await?;

    assert_eq!(response.status(), StatusCode::OK);

    // Check content-type header for streamed response
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.contains("application/x-streamed-protobuf"),
        "Content-type should be streamed protobuf, got: {}",
        content_type
    );

    // The body should be non-empty (contains varint length + protobuf + CRC32)
    let body_bytes = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    assert!(!body_bytes.is_empty(), "Streamed response should have body");

    // Note: Full parsing of the streamed response would require implementing
    // the varint + CRC32 parsing logic, which is tested in stream_writer tests

    Ok(())
}

/// Test streamed XOR chunks response for a series larger than one XOR chunk.
#[tokio::test]
#[serial]
async fn test_remote_read_streamed_xor_chunks_large_series() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    let sensor = create_sensor_with_labels("large_chunked_metric", SensorType::Float, vec![]);
    let sample_count = 70_000usize;
    let times_ms: Vec<i64> = (0..sample_count)
        .map(|index| BASE_TS_MS + index as i64 * 1000)
        .collect();
    let values: Vec<f64> = (0..sample_count).map(|index| index as f64).collect();

    publish_test_sensors(
        &storage,
        vec![(sensor, create_float_samples_at_times(&times_ms, &values))],
    )
    .await?;

    let app = create_test_app(storage);
    let query = Query {
        start_timestamp_ms: BASE_TS_MS - 1000,
        end_timestamp_ms: BASE_TS_MS + sample_count as i64 * 1000,
        matchers: vec![PromLabelMatcher {
            r#type: label_matcher::Type::Eq as i32,
            name: "__name__".to_string(),
            value: "large_chunked_metric".to_string(),
        }],
        hints: None,
    };

    let body = build_remote_read_request(
        vec![query],
        vec![read_request::ResponseType::StreamedXorChunks as i32],
    )?;

    let (status, body_bytes) = send_remote_read_request(&app, body).await?;
    assert_eq!(status, StatusCode::OK);
    assert!(
        !body_bytes.is_empty(),
        "Large streamed response should have body"
    );

    let responses = parse_streamed_chunked_response(&body_bytes)?;
    assert_eq!(responses.len(), 1);
    assert_eq!(responses[0].chunked_series.len(), 1);
    assert_eq!(responses[0].chunked_series[0].chunks.len(), 2);

    Ok(())
}

/// Test remote read uses common Prometheus hints to return aggregated samples.
#[tokio::test]
#[serial]
async fn test_remote_read_samples_with_avg_over_time_hints() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    let sensor = create_sensor_with_labels("hinted_metric", SensorType::Float, vec![]);
    let times_ms = vec![
        BASE_TS_MS,
        BASE_TS_MS + 1000,
        BASE_TS_MS + 2000,
        BASE_TS_MS + 3000,
        BASE_TS_MS + 4000,
    ];
    let values = vec![1.0, 2.0, 3.0, 4.0, 5.0];

    publish_test_sensors(
        &storage,
        vec![(sensor, create_float_samples_at_times(&times_ms, &values))],
    )
    .await?;

    let app = create_test_app(storage);
    let query = Query {
        start_timestamp_ms: BASE_TS_MS,
        end_timestamp_ms: BASE_TS_MS + 5000,
        matchers: vec![PromLabelMatcher {
            r#type: label_matcher::Type::Eq as i32,
            name: "__name__".to_string(),
            value: "hinted_metric".to_string(),
        }],
        hints: Some(ReadHints {
            step_ms: 2000,
            func: "avg_over_time".to_string(),
            start_ms: BASE_TS_MS,
            end_ms: BASE_TS_MS + 5000,
            grouping: vec![],
            by: false,
            range_ms: 2000,
        }),
    };

    let body = build_remote_read_request(vec![query], vec![])?;
    let (status, response_body) = send_remote_read_request(&app, body).await?;

    assert_eq!(status, StatusCode::OK);

    let response = parse_remote_read_response(&response_body)?;
    let timeseries = &response.results[0].timeseries[0];
    assert_eq!(timeseries.samples.len(), 3);
    assert_eq!(timeseries.samples[0].timestamp, BASE_TS_MS);
    assert_eq!(timeseries.samples[1].timestamp, BASE_TS_MS + 2000);
    assert_eq!(timeseries.samples[2].timestamp, BASE_TS_MS + 4000);
    assert!((timeseries.samples[0].value - 1.5).abs() < f64::EPSILON);
    assert!((timeseries.samples[1].value - 3.5).abs() < f64::EPSILON);
    assert!((timeseries.samples[2].value - 5.0).abs() < f64::EPSILON);

    Ok(())
}

/// Test that default response type is SAMPLES when no type specified
#[tokio::test]
#[serial]
async fn test_remote_read_default_response_type() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create test sensor
    let sensor = create_sensor_with_labels("default_type_metric", SensorType::Float, vec![]);
    publish_test_sensors(
        &storage,
        vec![(
            sensor,
            create_float_samples_at_times(&[BASE_TS_MS], &[42.0]),
        )],
    )
    .await?;

    // Request with empty accepted_response_types (should default to SAMPLES)
    let app = create_test_app(storage);
    let query = Query {
        start_timestamp_ms: BASE_TS_MS - 1000,
        end_timestamp_ms: BASE_TS_MS + 10000,
        matchers: vec![PromLabelMatcher {
            r#type: label_matcher::Type::Eq as i32,
            name: "__name__".to_string(),
            value: "default_type_metric".to_string(),
        }],
        hints: None,
    };

    let body = build_remote_read_request(vec![query], vec![])?; // Empty response types

    let (status, response_body) = send_remote_read_request(&app, body).await?;
    assert_eq!(status, StatusCode::OK);

    // Should be able to parse as standard ReadResponse (SAMPLES format)
    let response = parse_remote_read_response(&response_body)?;
    assert_eq!(response.results.len(), 1);
    assert_eq!(response.results[0].timeseries.len(), 1);

    Ok(())
}

/// The sample limit of a remote read is `SENSAPP_HTTP_MAX_QUERY_SAMPLES`: a read of exactly the
/// limit works, one sample more is a 400 for the raw samples (and for a response of either type),
/// and the same series read with hints that give one bucket per step fits under the same limit.
#[tokio::test]
#[serial]
async fn test_remote_read_sample_limit_is_configurable() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    let limit = 50usize;
    let sensor = create_sensor_with_labels("limited_metric", SensorType::Float, vec![]);
    let sample_count = limit + 1;
    let times_ms: Vec<i64> = (0..sample_count)
        .map(|index| BASE_TS_MS + index as i64 * 1000)
        .collect();
    let values: Vec<f64> = (0..sample_count).map(|index| index as f64).collect();
    publish_test_sensors(
        &storage,
        vec![(sensor, create_float_samples_at_times(&times_ms, &values))],
    )
    .await?;

    let query = |end_offset_ms: i64, hints: Option<ReadHints>| Query {
        start_timestamp_ms: BASE_TS_MS,
        end_timestamp_ms: BASE_TS_MS + end_offset_ms,
        matchers: vec![PromLabelMatcher {
            r#type: label_matcher::Type::Eq as i32,
            name: "__name__".to_string(),
            value: "limited_metric".to_string(),
        }],
        hints,
    };
    let app = create_test_app_with_limit(storage, limit);

    // Exactly the limit: the samples at +0 s .. +49 s
    for response_type in [
        read_request::ResponseType::Samples,
        read_request::ResponseType::StreamedXorChunks,
    ] {
        let body =
            build_remote_read_request(vec![query(49_000, None)], vec![response_type as i32])?;
        let (status, _) = send_remote_read_request(&app, body).await?;
        assert_eq!(status, StatusCode::OK, "{response_type:?} at the limit");

        // One sample more
        let body =
            build_remote_read_request(vec![query(50_000, None)], vec![response_type as i32])?;
        let (status, body) = send_remote_read_request(&app, body).await?;
        assert_eq!(
            status,
            StatusCode::BAD_REQUEST,
            "{response_type:?} over the limit"
        );
        let body = String::from_utf8_lossy(&body);
        assert!(
            body.contains("Query exceeds 50 samples in total"),
            "unexpected body: {body}"
        );
    }

    // Buckets of 10 s: 6 buckets for the 51 samples, which fit under the same limit
    let hints = ReadHints {
        step_ms: 10_000,
        func: "max_over_time".to_string(),
        start_ms: BASE_TS_MS,
        end_ms: BASE_TS_MS + 50_000,
        grouping: vec![],
        by: false,
        range_ms: 10_000,
    };
    let body = build_remote_read_request(vec![query(50_000, Some(hints))], vec![])?;
    let (status, response_body) = send_remote_read_request(&app, body).await?;
    assert_eq!(status, StatusCode::OK);
    let response = parse_remote_read_response(&response_body)?;
    assert_eq!(response.results[0].timeseries[0].samples.len(), 6);

    Ok(())
}

/// A plain selector graphed over a long range at a coarse step gets the latest sample of each step,
/// with its own timestamp, whatever the number of samples: the raw read would be over the limit.
/// Prometheus keeps or drops each of them with its lookback, as it does with the raw samples.
#[tokio::test]
#[serial]
async fn test_remote_read_plain_selector_hints_return_the_latest_sample_of_each_step() -> Result<()>
{
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    const MINUTE_MS: i64 = 60_000;
    const HOUR_MS: i64 = 60 * MINUTE_MS;
    // One sample a minute for 5 hours, and one with samples only between minutes 10 and 19 of each hour
    let steady_times: Vec<i64> = (0..=300)
        .map(|minute| BASE_TS_MS + minute * MINUTE_MS)
        .collect();
    let steady_values: Vec<f64> = (0..=300).map(|minute| minute as f64).collect();
    let sparse_times: Vec<i64> = (0..5)
        .flat_map(|hour| {
            (10..20).map(move |minute| BASE_TS_MS + hour * HOUR_MS + minute * MINUTE_MS)
        })
        .collect();
    let sparse_values: Vec<f64> = (0..5)
        .flat_map(|hour| (10..20).map(move |minute| (hour * 100 + minute) as f64))
        .collect();
    publish_test_sensors(
        &storage,
        vec![
            (
                create_sensor_with_labels("steady_metric", SensorType::Float, vec![]),
                create_float_samples_at_times(&steady_times, &steady_values),
            ),
            (
                create_sensor_with_labels("sparse_metric", SensorType::Float, vec![]),
                create_float_samples_at_times(&sparse_times, &sparse_values),
            ),
        ],
    )
    .await?;

    // 301 samples would be over the limit: the evaluations are every hour, from 1 h to 4 h
    let app = create_test_app_with_limit(storage, 50);
    let read = |name: &str, func: &str, range_ms: i64, end_ms: i64| {
        let first_ms = BASE_TS_MS + HOUR_MS;
        let query = Query {
            start_timestamp_ms: first_ms - 5 * MINUTE_MS + 1,
            end_timestamp_ms: end_ms,
            matchers: vec![PromLabelMatcher {
                r#type: label_matcher::Type::Eq as i32,
                name: "__name__".to_string(),
                value: name.to_string(),
            }],
            hints: Some(ReadHints {
                step_ms: HOUR_MS,
                func: func.to_string(),
                start_ms: first_ms - 5 * MINUTE_MS + 1,
                end_ms,
                grouping: vec![],
                by: false,
                range_ms,
            }),
        };
        build_remote_read_request(vec![query], vec![])
    };

    for func in ["", "sum"] {
        let body = read("steady_metric", func, 0, BASE_TS_MS + 4 * HOUR_MS)?;
        let (status, response_body) = send_remote_read_request(&app, body).await?;
        assert_eq!(status, StatusCode::OK, "func {func:?}");
        let response = parse_remote_read_response(&response_body)?;
        let samples = &response.results[0].timeseries[0].samples;
        let got: Vec<(i64, f64)> = samples.iter().map(|s| (s.timestamp, s.value)).collect();
        let expected: Vec<(i64, f64)> = (1..=4)
            .map(|hour| (BASE_TS_MS + hour * HOUR_MS, (hour * 60) as f64))
            .collect();
        assert_eq!(got, expected, "func {func:?}");
    }

    // The sparse series keeps the real timestamp of its latest sample before each evaluation, 41
    // minutes before it: Prometheus drops that sample, as its lookback is 5 minutes
    let body = read("sparse_metric", "", 0, BASE_TS_MS + 4 * HOUR_MS)?;
    let (status, response_body) = send_remote_read_request(&app, body).await?;
    assert_eq!(status, StatusCode::OK);
    let response = parse_remote_read_response(&response_body)?;
    let got: Vec<(i64, f64)> = response.results[0].timeseries[0]
        .samples
        .iter()
        .map(|s| (s.timestamp, s.value))
        .collect();
    let expected: Vec<(i64, f64)> = (0..4)
        .map(|hour| {
            (
                BASE_TS_MS + hour * HOUR_MS + 19 * MINUTE_MS,
                (hour * 100 + 19) as f64,
            )
        })
        .collect();
    assert_eq!(got, expected);

    // A step that does not end on the last evaluation is answered with the raw samples, which are
    // over the limit here, with the reason in the message
    let body = read(
        "steady_metric",
        "",
        0,
        BASE_TS_MS + 4 * HOUR_MS + 30 * MINUTE_MS,
    )?;
    let (status, response_body) = send_remote_read_request(&app, body).await?;
    assert_eq!(status, StatusCode::BAD_REQUEST);
    let message = String::from_utf8_lossy(&response_body);
    assert!(
        message.contains("could not be answered with one value per step"),
        "unexpected body: {message}"
    );

    Ok(())
}
