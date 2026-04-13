mod common;

use anyhow::Result;
use axum::http::StatusCode;
use common::TestDb;
use common::http::TestApp;
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch_builder::BatchBuilder;
use sensapp::datamodel::sensapp_vec::SensAppLabels;
use sensapp::datamodel::{Sample, Sensor, SensorType, TypedSamples};
use serial_test::serial;
use std::sync::Arc;
use uuid::Uuid;

static INIT: std::sync::Once = std::sync::Once::new();

fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

fn create_sensor(name: &str, sensor_type: SensorType, labels: Vec<(&str, &str)>) -> Sensor {
    let labels: SensAppLabels = labels
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();

    Sensor {
        uuid: Uuid::new_v4(),
        name: name.to_string(),
        sensor_type,
        unit: None,
        labels,
    }
}

async fn publish_test_sensors(
    storage: &Arc<dyn sensapp::storage::StorageInstance>,
    sensors_with_samples: Vec<(Sensor, TypedSamples)>,
) -> Result<()> {
    let mut batch_builder = BatchBuilder::new()?;

    for (sensor, samples) in sensors_with_samples {
        batch_builder.add(Arc::new(sensor), samples).await?;
    }

    batch_builder.send_what_is_left(storage.clone()).await?;
    Ok(())
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

#[tokio::test]
#[serial]
async fn test_prometheus_metrics_can_append_latest_compatible_samples() -> Result<()> {
    ensure_config();

    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage.clone()).await;

    publish_test_sensors(
        &storage,
        vec![
            (
                create_sensor(
                    "room_temperature_celsius",
                    SensorType::Float,
                    vec![("room", "lab"), ("site", "alpha")],
                ),
                TypedSamples::Float(smallvec::smallvec![Sample {
                    datetime: sensapp::datamodel::SensAppDateTime::from_unix_seconds(
                        1_704_067_201.0
                    ),
                    value: 42.5,
                }]),
            ),
            (
                create_sensor("demo-temperature", SensorType::Float, vec![("room", "lab")]),
                TypedSamples::Float(smallvec::smallvec![Sample {
                    datetime: sensapp::datamodel::SensAppDateTime::from_unix_seconds(
                        1_704_067_202.0
                    ),
                    value: 99.0,
                }]),
            ),
            (
                create_sensor("status_text", SensorType::String, vec![("room", "lab")]),
                TypedSamples::String(smallvec::smallvec![Sample {
                    datetime: sensapp::datamodel::SensAppDateTime::from_unix_seconds(
                        1_704_067_203.0
                    ),
                    value: "ok".to_string(),
                }]),
            ),
        ],
    )
    .await?;

    let response = app
        .get("/prometheus/metrics?include_latest_samples=true")
        .await?;
    response.assert_status(StatusCode::OK);
    response.assert_body_contains("sensapp_storage_ready 1");
    response.assert_body_contains(
        "room_temperature_celsius{room=\"lab\",site=\"alpha\"} 42.5 1704067201000",
    );
    assert!(!response.body().contains("demo-temperature{"));
    assert!(!response.body().contains("status_text{"));

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_prometheus_metrics_latest_samples_honors_selector() -> Result<()> {
    ensure_config();

    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage.clone()).await;

    publish_test_sensors(
        &storage,
        vec![
            (
                create_sensor(
                    "room_temperature_celsius",
                    SensorType::Float,
                    vec![("site", "alpha")],
                ),
                TypedSamples::Float(smallvec::smallvec![Sample {
                    datetime: sensapp::datamodel::SensAppDateTime::from_unix_seconds(
                        1_704_067_210.0
                    ),
                    value: 21.0,
                }]),
            ),
            (
                create_sensor(
                    "room_temperature_celsius",
                    SensorType::Float,
                    vec![("site", "beta")],
                ),
                TypedSamples::Float(smallvec::smallvec![Sample {
                    datetime: sensapp::datamodel::SensAppDateTime::from_unix_seconds(
                        1_704_067_220.0
                    ),
                    value: 22.0,
                }]),
            ),
        ],
    )
    .await?;

    let response = app
        .get("/prometheus/metrics?include_latest_samples=true&selector=%7Bsite%3D%22alpha%22%7D")
        .await?;

    response.assert_status(StatusCode::OK);
    response.assert_body_contains("room_temperature_celsius{site=\"alpha\"} 21 1704067210000");
    assert!(!response.body().contains("site=\"beta\""));

    Ok(())
}
