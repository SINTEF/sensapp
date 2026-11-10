//! Requests that used to panic in a handler. The server binary exited on any panic, so one such
//! request stopped an instance, and a retrying client stopped every instance in turn. Each test
//! sends the request through the real router, expects an error answer, then checks that the next
//! request is still answered. These tests run on the backend selected by `TEST_DATABASE_URL`.

use crate::common::TestDb;
use anyhow::Result;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch::{Batch, SingleSensorBatch};
use sensapp::datamodel::sensapp_vec::SensAppVec;
use sensapp::datamodel::{Sample, Sensor, SensorType, TypedSamples};
use sensapp::http::metrics::HttpMetrics;
use sensapp::http::server::{RouterSettings, build_router};
use sensapp::http::state::HttpServerState;
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
    (status, String::from_utf8_lossy(&body).into_owned())
}

async fn post_csv(app: &Router, csv: &str) -> (StatusCode, String) {
    send(
        app,
        Request::post("/publish")
            .header("content-type", "text/csv")
            .body(Body::from(csv.to_string()))
            .unwrap(),
    )
    .await
}

/// The request is refused with a 4xx, and the server still answers the next one.
async fn assert_refused_then_alive(app: &Router, (status, body): (StatusCode, String)) {
    assert!(
        status.is_client_error(),
        "expected a 4xx, got {status}: {body}"
    );
    let (status, _) = send(
        app,
        Request::get("/health/live").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the server answers the next request"
    );
}

/// A series with one sample, through CSV; its UUID.
async fn one_series(app: &Router, storage: &Arc<dyn StorageInstance>) -> Result<String> {
    let name = format!("panic_series_{}", uuid::Uuid::new_v4().simple());
    let (status, body) =
        post_csv(app, &format!("datetime,{name}\n2024-01-01T00:00:00Z,1.5\n")).await;
    assert!(status.is_success(), "{status}: {body}");
    let listed = storage.list_series(Some(&name), None, None).await?;
    Ok(listed.series[0].uuid.to_string())
}

#[tokio::test]
#[serial]
async fn a_zero_step_is_refused() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = app(storage.clone());
    let uuid = one_series(&app, &storage).await?;

    // The window has no sample: the bucket count was computed in the handler, dividing by zero
    let availability = format!(
        "/series/{uuid}/availability?start=2025-01-01T00:00:00Z&end=2025-01-02T00:00:00Z&step=0s"
    );
    let response = send(
        &app,
        Request::get(&availability).body(Body::empty()).unwrap(),
    )
    .await;
    assert_refused_then_alive(&app, response).await;

    let data = format!("/series/{uuid}?step=0s&aggregation=avg");
    let response = send(&app, Request::get(&data).body(Body::empty()).unwrap()).await;
    assert_refused_then_alive(&app, response).await;
    Ok(())
}

#[tokio::test]
#[serial]
async fn a_csv_name_with_a_control_character_is_refused() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let app = app(test_db.storage());

    // Long format: the name is in a column
    let long = "datetime,sensor_name,value\n2024-01-01T00:00:00Z,bad\u{0B}name,1.5\n";
    assert_refused_then_alive(&app, post_csv(&app, long).await).await;

    // Wide format: the name is a header
    let wide = "datetime,bad\u{1F}name\n2024-01-01T00:00:00Z,1.5\n";
    assert_refused_then_alive(&app, post_csv(&app, wide).await).await;
    Ok(())
}

#[tokio::test]
#[serial]
async fn a_wide_csv_with_the_same_header_twice_is_refused() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let app = app(test_db.storage());

    // Two columns named `a` with different types
    let csv = "datetime,a,a\n2024-01-01T00:00:00Z,1,1.5\n";
    assert_refused_then_alive(&app, post_csv(&app, csv).await).await;
    Ok(())
}

#[tokio::test]
#[serial]
async fn a_numeric_sum_over_the_decimal_range_is_refused() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = app(storage.clone());

    // Two values of 5 × 10^28 in one bucket: each fits in a `Decimal` (up to 7.9 × 10^28), their
    // sum does not
    let sensor = Arc::new(Sensor::new_without_uuid(
        format!("panic_numeric_{}", uuid::Uuid::new_v4().simple()),
        SensorType::Numeric,
        None,
        None,
    )?);
    let large = rust_decimal::Decimal::from_i128_with_scale(5 * 10_i128.pow(28), 0);
    let samples = [0.0, 1.0].map(|seconds| Sample {
        datetime: hifitime::Epoch::from_unix_seconds(1_700_000_000.0 + seconds),
        value: large,
    });
    let mut batches = SensAppVec::new();
    batches.push(SingleSensorBatch::new(
        sensor.clone(),
        TypedSamples::Numeric(samples.into_iter().collect()),
    ));
    // Some backends cannot store such a value (ClickHouse keeps 8 digits after the point): then
    // there is nothing to sum
    if storage
        .publish(Arc::new(Batch::new(batches)))
        .await
        .is_err()
    {
        return Ok(());
    }

    let data = format!("/series/{}?step=1h&aggregation=sum", sensor.uuid);
    let (status, body) = send(&app, Request::get(&data).body(Body::empty()).unwrap()).await;
    assert!(
        !status.is_success(),
        "expected an error, got {status}: {body}"
    );
    let (status, _) = send(
        &app,
        Request::get("/health/live").body(Body::empty()).unwrap(),
    )
    .await;
    assert_eq!(
        status,
        StatusCode::OK,
        "the server answers the next request"
    );
    Ok(())
}

#[tokio::test]
#[serial]
async fn a_senml_time_out_of_range_is_refused() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let app = app(test_db.storage());

    // Negative times are relative to now; these are beyond what a date can hold
    for time in ["-1e13", "1e300", "-1e300"] {
        let senml = format!(r#"[{{"n":"panic_senml","t":{time},"v":1}}]"#);
        let request = Request::post("/publish")
            .header("content-type", "application/json")
            .body(Body::from(senml))
            .unwrap();
        let response = send(&app, request).await;
        assert_refused_then_alive(&app, response).await;
    }
    Ok(())
}

/// The PromQL parser is a recursive descent with no depth limit. Measured on a 2 MiB thread (a
/// Tokio worker) in a debug build: 150 nested parentheses or 700 unary minus signs overflow the
/// stack, which aborts the process. Queries and selectors are now refused above 4 KiB or 64 levels.
#[tokio::test]
#[serial]
async fn deeply_nested_or_long_queries_are_refused() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let app = app(test_db.storage());
    let queries = [
        format!("{}x{}", "(".repeat(300), ")".repeat(300)),
        format!("{}x", "-".repeat(1000)),
        // Over 4 KiB (a URI is at most 64 KiB)
        format!("x{}", " + x".repeat(1_250)),
    ];
    for query in &queries {
        let query = urlencoding::encode(query);
        for uri in [
            format!("/api/v1/query?query={query}"),
            format!("/series?selector={query}"),
            format!("/prometheus/metrics?include_latest_samples=true&selector={query}"),
        ] {
            let (status, body) = send(&app, Request::get(&uri).body(Body::empty()).unwrap()).await;
            assert_eq!(status, StatusCode::BAD_REQUEST, "{}: {body}", &uri[..40]);
            assert!(body.contains("PromQL"), "{body}");
        }
    }
    // A label value may hold any character
    let quoted = urlencoding::encode(r#"{name="((((((((((--------"}"#);
    let (status, body) = send(
        &app,
        Request::get(format!("/series?selector={quoted}"))
            .body(Body::empty())
            .unwrap(),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    Ok(())
}
