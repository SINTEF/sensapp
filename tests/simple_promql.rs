//! Integration tests for the simple PromQL query endpoint.
//!
//! Tests the `GET /api/v1/query` endpoint with various PromQL queries.
//! The endpoint supports multiple export formats: SenML (default), CSV, JSONL, and Arrow.

mod common;

use anyhow::Result;
use arrow_ipc::reader::StreamReader;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::get;
use common::TestDb;
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch_builder::BatchBuilder;
use sensapp::datamodel::sensapp_vec::SensAppLabels;
use sensapp::datamodel::{Sample, Sensor, SensorType, TypedSamples};
use sensapp::http::metrics::HttpMetrics;
use sensapp::http::simple_promql::simple_promql_query;
use sensapp::http::state::HttpServerState;
use sensapp::storage::StorageInstance;
use serde_json::Value;
use serial_test::serial;
use std::io::Cursor;
use std::sync::Arc;
use tower::ServiceExt;
use uuid::Uuid;

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
    Sensor::new(
        Uuid::new_v4(),
        name.to_string(),
        sensor_type,
        None,
        Some(labels),
    )
}

/// Helper to create float samples with recent timestamps (within last hour)
fn create_float_samples(count: usize) -> TypedSamples {
    // Use current time minus some minutes to ensure samples are in the query range
    let now = hifitime::Epoch::now().expect("Failed to get current time");
    let samples: smallvec::SmallVec<[Sample<f64>; 4]> = (0..count)
        .map(|i| {
            // Create samples spaced 1 minute apart, starting from 30 minutes ago
            let offset_minutes = 30 - (i as i64);
            let sample_time = now - hifitime::Duration::from_seconds(offset_minutes as f64 * 60.0);
            Sample {
                datetime: sample_time,
                value: 20.0 + i as f64,
            }
        })
        .collect();
    TypedSamples::Float(samples)
}

/// Helper to publish sensors with samples to the storage
async fn publish_test_sensors(
    storage: &Arc<dyn StorageInstance>,
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

/// Create a test app with the simple_promql endpoint
async fn create_test_app(storage: Arc<dyn StorageInstance>) -> Router {
    let state = HttpServerState {
        name: Arc::new("SensApp Test".to_string()),
        storage,
        metrics: Arc::new(HttpMetrics::new()),
        influxdb_with_numeric: false,
        auth: None,
    };

    Router::new()
        .route("/api/v1/query", get(simple_promql_query))
        .with_state(state)
}

/// Test simple metric name query
#[tokio::test]
#[serial]
async fn test_simple_promql_metric_name() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create test sensors
    let sensor = create_sensor_with_labels("cpu_usage", SensorType::Float, vec![]);
    publish_test_sensors(&storage, vec![(sensor, create_float_samples(5))]).await?;

    let app = create_test_app(storage).await;

    // Query by metric name
    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/query?query=cpu_usage")
        .body(Body::empty())?;

    let response = app.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: Value = serde_json::from_slice(&body)?;

    // Response is SenML format - an array of records
    let records = json.as_array().expect("SenML response should be an array");
    assert!(!records.is_empty(), "Should have at least one record");

    // First record should have _name containing the sensor name (bn is now the UUID)
    let first_record = &records[0];
    let name = first_record["_name"]
        .as_str()
        .expect("First record should have _name");
    assert!(
        name.contains("cpu_usage"),
        "_name should contain sensor name"
    );

    Ok(())
}

/// Test query with label filters
#[tokio::test]
#[serial]
async fn test_simple_promql_with_labels() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create test sensors with different labels
    let sensor1 = create_sensor_with_labels(
        "http_requests",
        SensorType::Float,
        vec![("environment".to_string(), "production".to_string())],
    );
    let sensor2 = create_sensor_with_labels(
        "http_requests",
        SensorType::Float,
        vec![("environment".to_string(), "staging".to_string())],
    );

    publish_test_sensors(
        &storage,
        vec![
            (sensor1, create_float_samples(5)),
            (sensor2, create_float_samples(5)),
        ],
    )
    .await?;

    let app = create_test_app(storage).await;

    // Query with label filter - URL-encoded curly braces and quotes
    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/query?query=http_requests%7Benvironment%3D%22production%22%7D")
        .body(Body::empty())?;

    let response = app.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: Value = serde_json::from_slice(&body)?;

    // Response is SenML format
    let records = json.as_array().expect("SenML response should be an array");
    assert!(!records.is_empty(), "Should have at least one record");

    // Check that we only got the production sensor (all records should have the same _name)
    let first_record = &records[0];
    let name = first_record["_name"]
        .as_str()
        .expect("First record should have _name");
    assert!(
        name.contains("http_requests"),
        "_name should contain sensor name"
    );

    // Check that we have values
    assert!(
        records.iter().any(|r| r.get("v").is_some()),
        "Should have records with values"
    );

    Ok(())
}

/// Test matrix selector with time range
#[tokio::test]
#[serial]
async fn test_simple_promql_matrix_selector() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create test sensor
    let sensor = create_sensor_with_labels("memory_usage", SensorType::Float, vec![]);
    publish_test_sensors(&storage, vec![(sensor, create_float_samples(10))]).await?;

    let app = create_test_app(storage).await;

    // Query with matrix selector (range)
    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/query?query=memory_usage[5m]")
        .body(Body::empty())?;

    let response = app.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: Value = serde_json::from_slice(&body)?;

    // Response is SenML format
    let records = json.as_array().expect("SenML response should be an array");
    // May or may not have data depending on time range, but should be valid
    assert!(
        records.is_empty() || records[0].get("bn").is_some() || records[0].get("v").is_some(),
        "Response should be valid SenML"
    );

    Ok(())
}

/// Test rejected (unsupported) aggregation query
#[tokio::test]
#[serial]
async fn test_simple_promql_reject_aggregation() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = create_test_app(storage).await;

    // Try aggregation query
    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/query?query=stddev(cpu_usage)")
        .body(Body::empty())?;

    let response = app.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let body_str = String::from_utf8_lossy(&body);
    assert!(body_str.contains("Aggregation 'stddev' is not supported"));

    Ok(())
}

/// Test rejected function call
#[tokio::test]
#[serial]
async fn test_simple_promql_reject_function() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = create_test_app(storage).await;

    // Try rate function
    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/query?query=rate(http_requests[5m])")
        .body(Body::empty())?;

    let response = app.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let body_str = String::from_utf8_lossy(&body);
    assert!(body_str.contains("Function"));

    Ok(())
}

/// Test rejected binary operation
#[tokio::test]
#[serial]
async fn test_simple_promql_reject_binary() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = create_test_app(storage).await;

    // Try binary operation - URL-encode the + sign
    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/query?query=cpu_usage%2B1")
        .body(Body::empty())?;

    let response = app.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let body_str = String::from_utf8_lossy(&body);
    assert!(body_str.contains("Binary"));

    Ok(())
}

/// Test binary-operation error includes guidance for hyphenated metric names
#[tokio::test]
#[serial]
async fn test_simple_promql_hyphenated_metric_name_hint() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    let sensor = create_sensor_with_labels("demo-temperature", SensorType::Float, vec![]);
    publish_test_sensors(&storage, vec![(sensor, create_float_samples(3))]).await?;

    let app = create_test_app(storage).await;

    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/query?query=demo-temperature%5B5m%5D")
        .body(Body::empty())?;

    let response = app.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let body_str = String::from_utf8_lossy(&body);
    assert!(body_str.contains("demo-temperature"));
    assert!(body_str.contains("__name__"));

    Ok(())
}

/// Test quoted-name workaround for hyphenated metric names
#[tokio::test]
#[serial]
async fn test_simple_promql_hyphenated_metric_name_via_name_matcher() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    let sensor = create_sensor_with_labels("demo-temperature", SensorType::Float, vec![]);
    publish_test_sensors(&storage, vec![(sensor, create_float_samples(3))]).await?;

    let app = create_test_app(storage).await;

    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/query?query=%7B__name__%3D%22demo-temperature%22%7D%5B5m%5D")
        .body(Body::empty())?;

    let response = app.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: Value = serde_json::from_slice(&body)?;
    let records = json.as_array().expect("SenML response should be an array");
    assert!(
        !records.is_empty(),
        "Should have records for the hyphenated sensor"
    );

    Ok(())
}

/// Test query with regex matcher
#[tokio::test]
#[serial]
async fn test_simple_promql_regex_matcher() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create test sensors with different regions
    let sensor1 = create_sensor_with_labels(
        "requests",
        SensorType::Float,
        vec![("region".to_string(), "us-east-1".to_string())],
    );
    let sensor2 = create_sensor_with_labels(
        "requests",
        SensorType::Float,
        vec![("region".to_string(), "us-west-2".to_string())],
    );
    let sensor3 = create_sensor_with_labels(
        "requests",
        SensorType::Float,
        vec![("region".to_string(), "eu-central-1".to_string())],
    );

    publish_test_sensors(
        &storage,
        vec![
            (sensor1, create_float_samples(3)),
            (sensor2, create_float_samples(3)),
            (sensor3, create_float_samples(3)),
        ],
    )
    .await?;

    let app = create_test_app(storage).await;

    // Query with regex matcher for US regions - URL-encoded
    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/query?query=requests%7Bregion%3D~%22us-.*%22%7D")
        .body(Body::empty())?;

    let response = app.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: Value = serde_json::from_slice(&body)?;

    // Response is SenML format
    let records = json.as_array().expect("SenML response should be an array");

    // Should have records from 2 sensors (us-east-1 and us-west-2), each with 3 samples
    // Each sensor adds multiple records, so total should be around 6 (2 sensors * 3 samples each)
    assert!(!records.is_empty(), "Should have records");

    // In SenML, sensors with the same name but different labels are distinguished by the _labels field.
    // Count unique label combinations (should find us-east-1 and us-west-2)
    let unique_regions: std::collections::HashSet<_> = records
        .iter()
        .filter_map(|r| {
            r.get("_labels")
                .and_then(|l| l.get("region"))
                .and_then(|v| v.as_str())
        })
        .collect();
    assert_eq!(
        unique_regions.len(),
        2,
        "Should match 2 sensors (us-east-1 and us-west-2)"
    );
    assert!(
        unique_regions.contains("us-east-1"),
        "Should have us-east-1"
    );
    assert!(
        unique_regions.contains("us-west-2"),
        "Should have us-west-2"
    );

    Ok(())
}

/// Test empty result for non-matching query
#[tokio::test]
#[serial]
async fn test_simple_promql_no_matches() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create a sensor
    let sensor = create_sensor_with_labels("existing_metric", SensorType::Float, vec![]);
    publish_test_sensors(&storage, vec![(sensor, create_float_samples(3))]).await?;

    let app = create_test_app(storage).await;

    // Query for non-existent metric
    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/query?query=nonexistent_metric")
        .body(Body::empty())?;

    let response = app.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: Value = serde_json::from_slice(&body)?;

    // Response is SenML format - should be an empty array
    let records = json.as_array().expect("SenML response should be an array");
    assert!(records.is_empty(), "Should be empty for non-matching query");

    Ok(())
}

/// Test invalid PromQL query
#[tokio::test]
#[serial]
async fn test_simple_promql_invalid_query() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = create_test_app(storage).await;

    // Try invalid query
    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/query?query={{{invalid")
        .body(Body::empty())?;

    let response = app.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    Ok(())
}

/// Test CSV export format
#[tokio::test]
#[serial]
async fn test_simple_promql_csv_format() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create test sensors
    let sensor = create_sensor_with_labels("test_metric", SensorType::Float, vec![]);
    publish_test_sensors(&storage, vec![(sensor, create_float_samples(3))]).await?;

    let app = create_test_app(storage).await;

    // Query with CSV format
    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/query?query=test_metric&format=csv")
        .body(Body::empty())?;

    let response = app.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::OK);

    // Check content type
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.contains("text/csv"),
        "Content-Type should be text/csv"
    );

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let csv_str = String::from_utf8_lossy(&body);

    // CSV should have a header and data rows
    let lines: Vec<&str> = csv_str.lines().collect();
    assert!(!lines.is_empty(), "CSV should have content");

    // Header should contain typical columns
    let header = lines[0].to_lowercase();
    assert!(
        header.contains("timestamp") || header.contains("sensor"),
        "CSV should have proper header"
    );

    Ok(())
}

/// Test JSONL export format
#[tokio::test]
#[serial]
async fn test_simple_promql_jsonl_format() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create test sensors
    let sensor = create_sensor_with_labels("test_metric", SensorType::Float, vec![]);
    publish_test_sensors(&storage, vec![(sensor, create_float_samples(3))]).await?;

    let app = create_test_app(storage).await;

    // Query with JSONL format
    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/query?query=test_metric&format=jsonl")
        .body(Body::empty())?;

    let response = app.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::OK);

    // Check content type
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.contains("application/x-ndjson") || content_type.contains("jsonl"),
        "Content-Type should be JSONL type"
    );

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let jsonl_str = String::from_utf8_lossy(&body);

    // Each non-empty line should be valid JSON
    for line in jsonl_str.lines() {
        if !line.is_empty() {
            let _: Value =
                serde_json::from_str(line).expect("Each JSONL line should be valid JSON");
        }
    }

    Ok(())
}

/// Test Arrow export format
#[tokio::test]
#[serial]
async fn test_simple_promql_arrow_format() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create test sensors
    let sensor = create_sensor_with_labels("test_metric", SensorType::Float, vec![]);
    publish_test_sensors(&storage, vec![(sensor, create_float_samples(3))]).await?;

    let app = create_test_app(storage).await;

    // Query with Arrow format
    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/query?query=test_metric&format=arrow")
        .body(Body::empty())?;

    let response = app.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::OK);

    // Check content type
    let content_type = response
        .headers()
        .get("content-type")
        .and_then(|v| v.to_str().ok())
        .unwrap_or("");
    assert!(
        content_type.contains("arrow") || content_type.contains("octet-stream"),
        "Content-Type should be arrow or binary type"
    );

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;

    assert!(body.len() > 6, "Arrow stream should have content");

    let mut reader = StreamReader::try_new(Cursor::new(body), None)?;
    let first_batch = reader
        .next()
        .transpose()?
        .expect("Arrow stream should contain at least one record batch");

    assert!(
        first_batch.num_rows() > 0,
        "Arrow batch should contain rows"
    );
    assert!(
        first_batch
            .schema()
            .fields()
            .iter()
            .any(|field| field.name() == "timestamp"),
        "Arrow batch should contain a timestamp column"
    );

    Ok(())
}

/// Test invalid format parameter
#[tokio::test]
#[serial]
async fn test_simple_promql_invalid_format() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = create_test_app(storage).await;

    // Query with invalid format
    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/query?query=test_metric&format=invalid_format")
        .body(Body::empty())?;

    let response = app.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let body_str = String::from_utf8_lossy(&body);
    assert!(
        body_str.contains("Unsupported"),
        "Should mention unsupported format"
    );

    Ok(())
}

/// Test multi-sensor SenML export (tests sensor_name column functionality)
#[tokio::test]
#[serial]
async fn test_simple_promql_multi_sensor_senml() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create multiple test sensors with the same metric name but different labels
    let sensor1 = create_sensor_with_labels(
        "multi_metric",
        SensorType::Float,
        vec![("instance".to_string(), "server1".to_string())],
    );
    let sensor2 = create_sensor_with_labels(
        "multi_metric",
        SensorType::Float,
        vec![("instance".to_string(), "server2".to_string())],
    );

    publish_test_sensors(
        &storage,
        vec![
            (sensor1, create_float_samples(2)),
            (sensor2, create_float_samples(2)),
        ],
    )
    .await?;

    let app = create_test_app(storage).await;

    // Query all sensors with this metric name
    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/query?query=multi_metric")
        .body(Body::empty())?;

    let response = app.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let json: Value = serde_json::from_slice(&body)?;

    // Response is SenML format
    let records = json.as_array().expect("SenML response should be an array");

    // Should have records from both sensors
    assert!(
        !records.is_empty(),
        "Should have records from multiple sensors"
    );

    // In SenML, sensors with the same name but different labels are distinguished by the _labels field.
    // Count unique instance labels (should find server1 and server2)
    let unique_instances: std::collections::HashSet<_> = records
        .iter()
        .filter_map(|r| {
            r.get("_labels")
                .and_then(|l| l.get("instance"))
                .and_then(|v| v.as_str())
        })
        .collect();
    assert_eq!(
        unique_instances.len(),
        2,
        "Should have records from 2 different sensors"
    );
    assert!(unique_instances.contains("server1"), "Should have server1");
    assert!(unique_instances.contains("server2"), "Should have server2");

    Ok(())
}

/// Test multi-sensor CSV export (tests sensor_name column functionality)
#[tokio::test]
#[serial]
async fn test_simple_promql_multi_sensor_csv() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    // Create multiple test sensors
    let sensor1 = create_sensor_with_labels(
        "csv_metric",
        SensorType::Float,
        vec![("host".to_string(), "host1".to_string())],
    );
    let sensor2 = create_sensor_with_labels(
        "csv_metric",
        SensorType::Float,
        vec![("host".to_string(), "host2".to_string())],
    );

    publish_test_sensors(
        &storage,
        vec![
            (sensor1, create_float_samples(2)),
            (sensor2, create_float_samples(2)),
        ],
    )
    .await?;

    let app = create_test_app(storage).await;

    // Query all sensors with this metric name in CSV format
    let request = Request::builder()
        .method("GET")
        .uri("/api/v1/query?query=csv_metric&format=csv")
        .body(Body::empty())?;

    let response = app.oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::OK);

    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    let csv_str = String::from_utf8_lossy(&body);

    let lines: Vec<&str> = csv_str.lines().collect();
    assert!(lines.len() > 1, "CSV should have header and data rows");

    // Header should have sensor_name column
    let header = lines[0].to_lowercase();
    assert!(
        header.contains("sensor_name") || header.contains("sensor"),
        "CSV header should include sensor identification"
    );

    Ok(())
}

/// Float samples 1 minute apart within the last hour, starting at `base`
fn create_float_samples_from(count: usize, base: f64) -> TypedSamples {
    let TypedSamples::Float(mut samples) = create_float_samples(count) else {
        unreachable!("create_float_samples returns floats");
    };
    for (i, sample) in samples.iter_mut().enumerate() {
        sample.value = base + i as f64;
    }
    TypedSamples::Float(samples)
}

/// Two sensors in room a (values 20..=22 and 40..=42) and one in room b (100..=102)
async fn cross_series_app() -> Result<(TestDb, Router)> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    let sensor = |room: &str, id: &str| {
        create_sensor_with_labels(
            "cross_temp",
            SensorType::Float,
            vec![
                ("room".to_string(), room.to_string()),
                ("id".to_string(), id.to_string()),
            ],
        )
    };
    publish_test_sensors(
        &storage,
        vec![
            (sensor("a", "1"), create_float_samples_from(3, 20.0)),
            (sensor("a", "2"), create_float_samples_from(3, 40.0)),
            (sensor("b", "3"), create_float_samples_from(3, 100.0)),
        ],
    )
    .await?;

    let app = create_test_app(storage).await;
    Ok((test_db, app))
}

/// Run a query expression and return the JSONL lines
async fn jsonl_query(app: &Router, expr: &str, step: Option<&str>) -> Result<Vec<Value>> {
    let mut uri = format!(
        "/api/v1/query?format=jsonl&query={}",
        urlencoding::encode(expr)
    );
    if let Some(step) = step {
        uri.push_str(&format!("&step={step}"));
    }
    let request = Request::builder()
        .method("GET")
        .uri(uri)
        .body(Body::empty())?;
    let response = app.clone().oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::OK, "query: {expr}");
    let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
    String::from_utf8_lossy(&body)
        .lines()
        .filter(|line| !line.is_empty())
        .map(|line| Ok(serde_json::from_str(line)?))
        .collect()
}

fn values_for_room(lines: &[Value], room: &str) -> Vec<f64> {
    lines
        .iter()
        .filter(|line| line["labels"]["room"] == room)
        .map(|line| line["value"].as_f64().expect("numeric value"))
        .collect()
}

/// avg by (room) averages all samples of the room's sensors over the window
#[tokio::test]
#[serial]
async fn test_simple_promql_avg_by_label() -> Result<()> {
    let (_db, app) = cross_series_app().await?;

    let lines = jsonl_query(&app, "avg by (room) (cross_temp[1h])", None).await?;

    assert_eq!(values_for_room(&lines, "a"), vec![31.0]);
    assert_eq!(values_for_room(&lines, "b"), vec![101.0]);
    assert_eq!(lines.len(), 2, "one point per group without step");
    // Only the grouping label is kept
    assert!(lines[0]["labels"].get("id").is_none());
    assert_eq!(lines[0]["sensor_name"], "avg(cross_temp)");

    Ok(())
}

/// Without a grouping clause every series collapses into one
#[tokio::test]
#[serial]
async fn test_simple_promql_avg_all_series() -> Result<()> {
    let (_db, app) = cross_series_app().await?;

    let lines = jsonl_query(&app, "avg(cross_temp)", None).await?;

    assert_eq!(lines.len(), 1);
    let expected = (31.0 * 6.0 + 101.0 * 3.0) / 9.0;
    let value = lines[0]["value"].as_f64().unwrap();
    assert!((value - expected).abs() < 1e-9, "{value} != {expected}");

    Ok(())
}

/// With a step, the counts of every bucket add up to the number of samples
#[tokio::test]
#[serial]
async fn test_simple_promql_count_with_step() -> Result<()> {
    let (_db, app) = cross_series_app().await?;

    let lines = jsonl_query(&app, "count by (room) (cross_temp[1h])", Some("10m")).await?;

    assert_eq!(values_for_room(&lines, "a").iter().sum::<f64>(), 6.0);
    assert_eq!(values_for_room(&lines, "b").iter().sum::<f64>(), 3.0);
    assert!(lines.iter().all(|line| line["type"] == "integer"));

    Ok(())
}

/// min, max and sum over a selector with matchers
#[tokio::test]
#[serial]
async fn test_simple_promql_min_max_sum_with_matchers() -> Result<()> {
    let (_db, app) = cross_series_app().await?;

    let min = jsonl_query(&app, r#"min(cross_temp{room="a"})"#, None).await?;
    let max = jsonl_query(&app, r#"max(cross_temp{room="a"})"#, None).await?;
    let sum = jsonl_query(&app, r#"sum(cross_temp{room="a"})"#, None).await?;

    assert_eq!(min[0]["value"].as_f64(), Some(20.0));
    assert_eq!(max[0]["value"].as_f64(), Some(42.0));
    assert_eq!(sum[0]["value"].as_f64(), Some(186.0));

    Ok(())
}

/// Unsupported shapes are rejected with a 400
#[tokio::test]
#[serial]
async fn test_simple_promql_aggregation_errors() -> Result<()> {
    let (_db, app) = cross_series_app().await?;

    for query in [
        "query=cross_temp&step=5m",
        "query=avg(cross_temp)&step=nonsense",
        "query=topk(2,%20cross_temp)",
        "query=sum(rate(cross_temp[5m]))",
        "query=avg(cross_temp)%20*%202",
    ] {
        let request = Request::builder()
            .method("GET")
            .uri(format!("/api/v1/query?{query}"))
            .body(Body::empty())?;
        let response = app.clone().oneshot(request).await?;
        assert_eq!(response.status(), StatusCode::BAD_REQUEST, "query: {query}");
    }

    Ok(())
}

/// A non-numeric sensor matching the selector is ignored, not an error
#[tokio::test]
#[serial]
async fn test_simple_promql_aggregation_ignores_non_numeric_sensors() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    let now = hifitime::Epoch::now()?;
    let strings = TypedSamples::String(smallvec::smallvec![Sample {
        datetime: now - hifitime::Duration::from_seconds(60.0),
        value: "on".to_string(),
    }]);
    publish_test_sensors(
        &storage,
        vec![
            (
                create_sensor_with_labels("mixed", SensorType::Float, vec![]),
                create_float_samples_from(2, 10.0),
            ),
            (
                create_sensor_with_labels("mixed", SensorType::String, vec![]),
                strings,
            ),
        ],
    )
    .await?;
    let app = create_test_app(storage).await;

    let lines = jsonl_query(&app, "sum(mixed)", None).await?;

    assert_eq!(lines.len(), 1);
    assert_eq!(lines[0]["value"].as_f64(), Some(21.0));

    Ok(())
}

/// An aggregation matching nothing returns an empty result, like a selector
#[tokio::test]
#[serial]
async fn test_simple_promql_aggregation_no_matches() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let app = create_test_app(test_db.storage()).await;

    let lines = jsonl_query(&app, "avg by (room) (does_not_exist)", None).await?;

    assert!(lines.is_empty());

    Ok(())
}

/// Series lacking the grouping label fall into one group without that label
#[tokio::test]
#[serial]
async fn test_simple_promql_group_by_missing_label() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    publish_test_sensors(
        &storage,
        vec![
            (
                create_sensor_with_labels(
                    "partial",
                    SensorType::Float,
                    vec![("room".to_string(), "a".to_string())],
                ),
                create_float_samples_from(1, 1.0),
            ),
            (
                create_sensor_with_labels("partial", SensorType::Float, vec![]),
                create_float_samples_from(1, 3.0),
            ),
        ],
    )
    .await?;
    let app = create_test_app(storage).await;

    let lines = jsonl_query(&app, "sum by (room) (partial)", None).await?;

    assert_eq!(lines.len(), 2);
    assert_eq!(values_for_room(&lines, "a"), vec![1.0]);
    let unlabelled: Vec<_> = lines
        .iter()
        .filter(|line| line["labels"].get("room").is_none())
        .collect();
    assert_eq!(unlabelled.len(), 1);
    assert_eq!(unlabelled[0]["value"].as_f64(), Some(3.0));

    Ok(())
}

/// Synthetic aggregate series go through every exporter
#[tokio::test]
#[serial]
async fn test_simple_promql_aggregation_all_formats() -> Result<()> {
    let (_db, app) = cross_series_app().await?;

    for format in ["senml", "csv", "jsonl", "arrow"] {
        let request = Request::builder()
            .method("GET")
            .uri(format!(
                "/api/v1/query?format={format}&query={}",
                urlencoding::encode("avg by (room) (cross_temp[1h])")
            ))
            .body(Body::empty())?;
        let response = app.clone().oneshot(request).await?;
        assert_eq!(response.status(), StatusCode::OK, "format: {format}");
        let body = axum::body::to_bytes(response.into_body(), usize::MAX).await?;
        assert!(!body.is_empty(), "format: {format}");
        if format == "csv" {
            assert!(String::from_utf8_lossy(&body).contains("avg(cross_temp)"));
        }
    }

    Ok(())
}
