//! A write whose payload can never be stored answers 4xx, through the real router. Prometheus and
//! Telegraf retry the 5xx answers: a bad payload answered with a 500 was sent again forever, and
//! blocked its remote-write shard. These tests run on the backend selected by `TEST_DATABASE_URL`.
//! A storage failure still answers 5xx: see `real_router::real_server_errors_are_still_logged_as_errors`.

use crate::common::TestDb;
use anyhow::Result;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use prost::Message;
use sensapp::config::load_configuration_for_tests;
use sensapp::http::metrics::HttpMetrics;
use sensapp::http::server::{RouterSettings, build_router};
use sensapp::http::state::HttpServerState;
use sensapp::parsing::prometheus::remote_read_models::{
    LabelMatcher as PromLabelMatcher, Query, ReadRequest, read_request,
};
use sensapp::parsing::prometheus::remote_write_models::{
    Label, Sample as PromSample, TimeSeries, WriteRequest,
};
use sensapp::storage::StorageInstance;
use serial_test::serial;
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;

static INIT: std::sync::Once = std::sync::Once::new();

fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

fn app(storage: Arc<dyn StorageInstance>) -> Router {
    let state = HttpServerState {
        name: Arc::new("SensApp Test".to_string()),
        storage,
        metrics: Arc::new(HttpMetrics::new()),
        influxdb_with_numeric: false,
        max_query_samples: sensapp::http::limits::DEFAULT_MAX_QUERY_SAMPLES,
        auth: None,
    };
    let settings = RouterSettings {
        max_body_bytes: 1024 * 1024,
        request_timeout: Duration::from_secs(30),
        write_timeout: Duration::from_secs(30),
        maintenance_timeout: Duration::from_secs(30),
        max_concurrent_writes: 4,
        body_read_timeout: Duration::from_secs(30),
        ui_dir: None,
    };
    build_router(state, &settings)
}

async fn send(app: &Router, request: Request<Body>) -> (StatusCode, String) {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&body).to_string())
}

fn publish(content_type: &str, body: impl Into<Body>) -> Request<Body> {
    Request::post("/publish")
        .header("content-type", content_type)
        .body(body.into())
        .unwrap()
}

fn influxdb(line_protocol: &str) -> Request<Body> {
    Request::post("/api/v2/write?bucket=b&org=o")
        .header("content-type", "text/plain")
        .body(Body::from(line_protocol.to_string()))
        .unwrap()
}

fn remote_write(compressed: Vec<u8>) -> Request<Body> {
    Request::post("/api/v1/prometheus_remote_write")
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "snappy")
        .header("x-prometheus-remote-write-version", "0.1.0")
        .body(Body::from(compressed))
        .unwrap()
}

fn snappy(bytes: &[u8]) -> Vec<u8> {
    snap::raw::Encoder::new().compress_vec(bytes).unwrap()
}

fn one_series(labels: &[(&str, &str)]) -> Vec<u8> {
    let request = WriteRequest {
        timeseries: vec![TimeSeries {
            labels: labels
                .iter()
                .map(|(name, value)| Label {
                    name: name.to_string(),
                    value: value.to_string(),
                })
                .collect(),
            samples: vec![PromSample {
                value: 1.5,
                timestamp: 1_700_000_000_000,
            }],
        }],
    };
    snappy(&request.encode_to_vec())
}

/// Each request, and the status class it must get: 4 for a client error.
async fn assert_client_errors(app: &Router, cases: Vec<(&str, Request<Body>)>) {
    let mut wrong = Vec::new();
    for (case, request) in cases {
        let (status, body) = send(app, request).await;
        // The answer says what is wrong, in a few words
        if !status.is_client_error() || body.len() > 1_000 {
            wrong.push(format!(
                "{case}: {status} ({} bytes) {:.200}",
                body.len(),
                body
            ));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
}

#[tokio::test]
#[serial]
async fn payloads_that_cannot_be_stored_are_client_errors() -> Result<()> {
    ensure_config();
    let db = TestDb::new().await?;
    let app = app(db.storage());

    let long_string = "x".repeat(5_000);
    let cases = vec![
        (
            "CSV with a row longer than its header",
            publish(
                "text/csv",
                "datetime,sensor_name,value\n2024-01-01T00:00:00Z,a,1,2\n",
            ),
        ),
        (
            "CSV without rows",
            publish("text/csv", "datetime,sensor_name,value\n"),
        ),
        (
            "CSV with a NUL in a string value",
            publish(
                "text/csv",
                "datetime,sensor_name,value\n2024-01-01T00:00:00Z,a,b\0c\n",
            ),
        ),
        (
            "CSV with a NUL in a sensor name",
            publish(
                "text/csv",
                "datetime,sensor_name,value\n2024-01-01T00:00:00Z,a\0b,1\n",
            ),
        ),
        (
            "SenML with a NUL in a string value",
            publish(
                "application/senml+json",
                r#"[{"n":"nul_string","t":1700000000,"vs":"a\u0000b"}]"#,
            ),
        ),
        (
            "SenML with a NUL in a JSON value",
            publish(
                "application/json",
                r#"[{"bn":"nul_json","bt":1700000000,"_type":"json"},{"vs":"{\"a\":\"b\\u0000c\"}"}]"#,
            ),
        ),
        (
            "SenML with a NUL in a label",
            publish(
                "application/json",
                r#"[{"n":"nul_label","t":1700000000,"v":1,"_labels":{"room":"a\u0000b"}}]"#,
            ),
        ),
        (
            "SenML with a long string and another type for the same series",
            publish(
                "application/json",
                format!(
                    r#"[{{"n":"two_types","u":"x","t":1700000000,"vs":"{long_string}"}},{{"n":"two_types","u":"x","t":1700000001,"v":1}}]"#
                ),
            ),
        ),
        ("bad line protocol", influxdb("not line protocol at all")),
        (
            "line protocol with a separator in a tag",
            influxdb("cpu,host=a\x1fb value=1 1700000000000000000"),
        ),
        (
            "bad snappy",
            remote_write(b"definitely not snappy".to_vec()),
        ),
        (
            "bad protobuf",
            remote_write(snappy(b"not a protobuf at all")),
        ),
        (
            "a label value with a separator",
            remote_write(one_series(&[("__name__", "up"), ("job", "a\x1fb")])),
        ),
        (
            "a label value with a NUL",
            remote_write(one_series(&[("__name__", "up"), ("job", "a\0b")])),
        ),
    ];
    assert_client_errors(&app, cases).await;

    // Nothing of these was stored
    let (status, body) = send(&app, Request::get("/metrics").body(Body::empty())?).await;
    assert_eq!(status, StatusCode::OK);
    for name in ["nul_string", "nul_json", "nul_label", "two_types", "\"up\""] {
        assert!(!body.contains(name), "{name} in {body}");
    }
    Ok(())
}

/// `application/senml+json` is the media type of RFC 8428: it is SenML, not CSV.
#[tokio::test]
#[serial]
async fn senml_json_is_read_as_senml() -> Result<()> {
    ensure_config();
    let db = TestDb::new().await?;
    let app = app(db.storage());
    let (status, body) = send(
        &app,
        publish(
            "application/senml+json",
            r#"[{"n":"senml_media_type","t":1700000000,"v":1.5}]"#,
        ),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    let (_, body) = send(&app, Request::get("/metrics").body(Body::empty())?).await;
    assert!(body.contains("senml_media_type"), "{body}");
    Ok(())
}

/// A matcher of a type Prometheus does not have is refused, instead of being read as `=`.
#[tokio::test]
#[serial]
async fn an_unknown_matcher_type_is_a_400() -> Result<()> {
    ensure_config();
    let db = TestDb::new().await?;
    let app = app(db.storage());
    let request = ReadRequest {
        queries: vec![Query {
            start_timestamp_ms: 0,
            end_timestamp_ms: 1_700_000_000_000,
            matchers: vec![PromLabelMatcher {
                r#type: 42,
                name: "__name__".to_string(),
                value: "up".to_string(),
            }],
            hints: None,
        }],
        accepted_response_types: vec![read_request::ResponseType::Samples as i32],
    };
    let request = Request::post("/api/v1/prometheus_remote_read")
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "snappy")
        .header("x-prometheus-remote-read-version", "0.1.0")
        .body(Body::from(snappy(&request.encode_to_vec())))?;
    let (status, body) = send(&app, request).await;
    assert_eq!(status, StatusCode::BAD_REQUEST, "{body}");
    assert!(body.contains("42"), "{body}");
    Ok(())
}
