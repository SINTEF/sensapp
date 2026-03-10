mod common;

use anyhow::Result;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use axum::routing::post;
use common::TestDb;
use common::db::DbHelpers;
use prost::Message;
use sensapp::config::load_configuration_for_tests;
use sensapp::http::prometheus_write::publish_prometheus;
use sensapp::http::state::HttpServerState;
use sensapp::parsing::prometheus::remote_write_models::{
    Label, Sample as PromSample, TimeSeries, WriteRequest,
};
use serial_test::serial;
use std::sync::Arc;
use tower::ServiceExt;

static INIT: std::sync::Once = std::sync::Once::new();
fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

/// Create a test app with the Prometheus remote write endpoint
fn create_test_app(storage: Arc<dyn sensapp::storage::StorageInstance>) -> Router {
    let state = HttpServerState {
        name: Arc::new("SensApp Test".to_string()),
        storage,
        influxdb_with_numeric: false,
    };

    Router::new()
        .route("/api/v1/prometheus_remote_write", post(publish_prometheus))
        .with_state(state)
}

/// Build a Prometheus remote write request body (snappy-compressed protobuf)
fn build_write_request(timeseries: Vec<TimeSeries>) -> Result<Vec<u8>> {
    let request = WriteRequest { timeseries };
    let encoded = request.encode_to_vec();
    let compressed = snap::raw::Encoder::new().compress_vec(&encoded)?;
    Ok(compressed)
}

/// Send a remote write request
async fn send_write_request(app: &Router, body: Vec<u8>) -> Result<StatusCode> {
    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/prometheus_remote_write")
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "snappy")
        .header("x-prometheus-remote-write-version", "0.1.0")
        .body(Body::from(body))?;

    let response = app.clone().oneshot(request).await?;
    Ok(response.status())
}

/// Test basic Prometheus remote write with a single time series
#[tokio::test]
#[serial]
async fn test_prometheus_write_basic() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = create_test_app(storage.clone());

    let ts = TimeSeries {
        labels: vec![
            Label {
                name: "__name__".to_string(),
                value: "cpu_usage".to_string(),
            },
            Label {
                name: "host".to_string(),
                value: "server1".to_string(),
            },
        ],
        samples: vec![
            PromSample {
                value: 75.5,
                timestamp: 1704067200000, // Jan 1, 2024 in ms
            },
            PromSample {
                value: 80.2,
                timestamp: 1704067260000,
            },
            PromSample {
                value: 65.0,
                timestamp: 1704067320000,
            },
        ],
    };

    let body = build_write_request(vec![ts])?;
    let status = send_write_request(&app, body).await?;

    assert_eq!(status, StatusCode::NO_CONTENT);

    // Verify data was stored
    let sensor = DbHelpers::get_sensor_by_name(&storage, "cpu_usage").await?;
    assert!(sensor.is_some(), "cpu_usage sensor should exist");

    let sensor_data = DbHelpers::verify_sensor_data(&storage, "cpu_usage", 3).await?;

    if let sensapp::datamodel::TypedSamples::Float(samples) = &sensor_data.samples {
        assert_eq!(samples[0].value, 75.5);
        assert_eq!(samples[1].value, 80.2);
        assert_eq!(samples[2].value, 65.0);
    } else {
        panic!("Expected float samples for Prometheus data");
    }

    Ok(())
}

/// Test Prometheus remote write with multiple time series
#[tokio::test]
#[serial]
async fn test_prometheus_write_multiple_series() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = create_test_app(storage.clone());

    let ts1 = TimeSeries {
        labels: vec![Label {
            name: "__name__".to_string(),
            value: "up".to_string(),
        }],
        samples: vec![PromSample {
            value: 1.0,
            timestamp: 1704067200000,
        }],
    };

    let ts2 = TimeSeries {
        labels: vec![Label {
            name: "__name__".to_string(),
            value: "process_cpu_seconds_total".to_string(),
        }],
        samples: vec![PromSample {
            value: 42.5,
            timestamp: 1704067200000,
        }],
    };

    let body = build_write_request(vec![ts1, ts2])?;
    let status = send_write_request(&app, body).await?;

    assert_eq!(status, StatusCode::NO_CONTENT);

    DbHelpers::verify_sensor_data(&storage, "up", 1).await?;
    DbHelpers::verify_sensor_data(&storage, "process_cpu_seconds_total", 1).await?;

    Ok(())
}

/// Test Prometheus remote write with empty timeseries (metadata request)
#[tokio::test]
#[serial]
async fn test_prometheus_write_empty_timeseries() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = create_test_app(storage.clone());

    // Prometheus regularly sends empty timeseries for metadata
    let body = build_write_request(vec![])?;
    let status = send_write_request(&app, body).await?;

    assert_eq!(status, StatusCode::NO_CONTENT);

    Ok(())
}

/// Test Prometheus remote write rejects missing __name__ label
#[tokio::test]
#[serial]
async fn test_prometheus_write_missing_name_label() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = create_test_app(storage.clone());

    let ts = TimeSeries {
        labels: vec![Label {
            name: "host".to_string(),
            value: "server1".to_string(),
        }],
        samples: vec![PromSample {
            value: 42.0,
            timestamp: 1704067200000,
        }],
    };

    let body = build_write_request(vec![ts])?;
    let status = send_write_request(&app, body).await?;

    assert_eq!(status, StatusCode::BAD_REQUEST);

    Ok(())
}

/// Test Prometheus write rejects wrong content-encoding header
#[tokio::test]
#[serial]
async fn test_prometheus_write_wrong_content_encoding() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = create_test_app(storage.clone());

    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/prometheus_remote_write")
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "gzip") // Wrong! Should be snappy
        .header("x-prometheus-remote-write-version", "0.1.0")
        .body(Body::from(vec![0u8; 10]))?;

    let response = app.clone().oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    Ok(())
}

/// Test Prometheus write rejects missing content-type header
#[tokio::test]
#[serial]
async fn test_prometheus_write_missing_content_type() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = create_test_app(storage.clone());

    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/prometheus_remote_write")
        .header("content-encoding", "snappy")
        .header("x-prometheus-remote-write-version", "0.1.0")
        .body(Body::from(vec![0u8; 10]))?;

    let response = app.clone().oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    Ok(())
}

/// Test Prometheus write rejects missing version header
#[tokio::test]
#[serial]
async fn test_prometheus_write_missing_version_header() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = create_test_app(storage.clone());

    let request = Request::builder()
        .method("POST")
        .uri("/api/v1/prometheus_remote_write")
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "snappy")
        // Missing x-prometheus-remote-write-version
        .body(Body::from(vec![0u8; 10]))?;

    let response = app.clone().oneshot(request).await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    Ok(())
}

/// Test Prometheus write stores labels correctly
#[tokio::test]
#[serial]
async fn test_prometheus_write_preserves_labels() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = create_test_app(storage.clone());

    let ts = TimeSeries {
        labels: vec![
            Label {
                name: "__name__".to_string(),
                value: "http_requests_total".to_string(),
            },
            Label {
                name: "method".to_string(),
                value: "GET".to_string(),
            },
            Label {
                name: "status".to_string(),
                value: "200".to_string(),
            },
        ],
        samples: vec![PromSample {
            value: 1234.0,
            timestamp: 1704067200000,
        }],
    };

    let body = build_write_request(vec![ts])?;
    let status = send_write_request(&app, body).await?;
    assert_eq!(status, StatusCode::NO_CONTENT);

    let sensor = DbHelpers::get_sensor_by_name(&storage, "http_requests_total")
        .await?
        .expect("Sensor should exist");

    let label_names: Vec<&str> = sensor.labels.iter().map(|(n, _)| n.as_str()).collect();
    assert!(
        label_names.contains(&"method"),
        "Should have 'method' label, got: {:?}",
        label_names
    );
    assert!(
        label_names.contains(&"status"),
        "Should have 'status' label, got: {:?}",
        label_names
    );

    // Check label values
    let method_value = sensor
        .labels
        .iter()
        .find(|(n, _)| n == "method")
        .map(|(_, v)| v.as_str());
    assert_eq!(method_value, Some("GET"));

    let status_value = sensor
        .labels
        .iter()
        .find(|(n, _)| n == "status")
        .map(|(_, v)| v.as_str());
    assert_eq!(status_value, Some("200"));

    Ok(())
}
