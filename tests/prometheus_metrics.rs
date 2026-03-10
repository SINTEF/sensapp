mod common;

use anyhow::Result;
use axum::http::StatusCode;
use common::TestDb;
use common::http::TestApp;
use sensapp::config::load_configuration_for_tests;
use serial_test::serial;

static INIT: std::sync::Once = std::sync::Once::new();

fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

#[tokio::test]
#[serial]
async fn test_prometheus_metrics_endpoint_exposes_http_metrics() -> Result<()> {
    ensure_config();

    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage).await;

    app.get("/health/live").await?.assert_status(StatusCode::OK);
    app.get("/health/live").await?.assert_status(StatusCode::OK);

    let response = app.get("/prometheus/metrics").await?;
    response.assert_status(StatusCode::OK);
    response.assert_content_type("text/plain; version=0.0.4; charset=utf-8");
    response.assert_body_contains(
        "sensapp_http_requests_total{method=\"GET\",path=\"/health/live\",status=\"200\"} 2",
    );
    response.assert_body_contains("sensapp_http_requests_in_flight 0");
    response.assert_body_contains("sensapp_storage_ready 1");
    response.assert_body_contains("sensapp_uptime_seconds");

    Ok(())
}
