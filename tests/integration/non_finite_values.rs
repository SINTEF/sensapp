//! Infinities are values: they are stored and read back on every backend, and the JSON exports
//! write them as text. NaN is no value: a write that holds one is refused, except Prometheus
//! remote write, which drops its NaN samples (stale markers). These tests run on the backend
//! selected by `TEST_DATABASE_URL`, through the real router.

use crate::common::{DatabaseType, TestDb};
use anyhow::Result;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use prost::Message;
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch_builder::BatchBuilder;
use sensapp::datamodel::{Sample, Sensor, SensorType, TypedSamples};
use sensapp::http::metrics::HttpMetrics;
use sensapp::http::server::{RouterSettings, build_router};
use sensapp::http::state::HttpServerState;
use sensapp::parsing::prometheus::remote_write_models::{
    Label, Sample as PromSample, TimeSeries, WriteRequest,
};
use sensapp::storage::{StorageError, StorageInstance};
use serde_json::Value;
use serial_test::serial;
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceExt;
use uuid::Uuid;

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

async fn get(app: &Router, path: &str) -> (StatusCode, String) {
    let response = app
        .clone()
        .oneshot(Request::get(path).body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&body).to_string())
}

const T0: f64 = 1_704_067_200.0;

async fn float_series(storage: &Arc<dyn StorageInstance>, values: &[f64]) -> Result<Arc<Sensor>> {
    let sensor = Arc::new(Sensor::new_without_uuid(
        format!("floats_{}", Uuid::new_v4().simple()),
        SensorType::Float,
        None,
        None,
    )?);
    let samples = values
        .iter()
        .enumerate()
        .map(|(i, &value)| Sample {
            datetime: hifitime::Epoch::from_unix_seconds(T0 + i as f64),
            value,
        })
        .collect::<Vec<_>>();
    let mut batch_builder = BatchBuilder::new()?;
    batch_builder
        .add(sensor.clone(), TypedSamples::Float(samples.into()))
        .await?;
    batch_builder.send_what_is_left(storage.clone()).await?;
    Ok(sensor)
}

#[tokio::test]
#[serial]
async fn infinities_are_stored_and_exported_as_text() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    if test_db.db_type == DatabaseType::RRDcached {
        return Ok(());
    }
    let storage = test_db.storage();
    let values = [f64::NEG_INFINITY, 1.5, 2.5, f64::INFINITY];
    let sensor = float_series(&storage, &values).await?;
    let uuid = sensor.uuid.to_string();

    let data = storage
        .query_sensor_data(&uuid, None, None, None)
        .await?
        .expect("the series");
    let TypedSamples::Float(read) = &data.samples else {
        panic!("not floats: {:?}", data.samples);
    };
    let read: Vec<f64> = read.iter().map(|sample| sample.value).collect();
    assert_eq!(read, values);

    let app = app(storage.clone());
    let (status, jsonl) = get(&app, &format!("/series/{uuid}?format=jsonl")).await;
    assert_eq!(status, StatusCode::OK, "{jsonl}");
    let exported: Vec<Value> = jsonl
        .lines()
        .map(|line| serde_json::from_str::<Value>(line).unwrap()["value"].clone())
        .collect();
    assert_eq!(
        exported,
        [
            Value::from("-Inf"),
            Value::from(1.5),
            Value::from(2.5),
            Value::from("+Inf")
        ]
    );

    let (status, last) = get(&app, &format!("/series/{uuid}/last")).await;
    assert_eq!(status, StatusCode::OK, "{last}");
    let last: Value = serde_json::from_str(&last)?;
    assert_eq!(last["value"], "+Inf", "{last}");

    // SenML has no way to write an infinity: the samples are left out, not written as null
    let (status, senml) = get(&app, &format!("/series/{uuid}?format=senml")).await;
    assert_eq!(status, StatusCode::OK, "{senml}");
    assert!(!senml.contains("null"), "{senml}");
    assert!(senml.contains("1.5") && senml.contains("2.5"), "{senml}");

    // Simplify keeps the infinite samples, and simplifies the others
    let (status, simplified) = get(
        &app,
        &format!("/series/{uuid}?simplify=true&simplify_tolerance=0.1&format=jsonl"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{simplified}");
    assert!(
        simplified.contains("-Inf") && simplified.contains("+Inf"),
        "{simplified}"
    );

    // The extremes of a window are the infinities, on every backend
    for (aggregation, expected) in [("min", "-Inf"), ("max", "+Inf"), ("count", "4")] {
        let (status, body) = get(
            &app,
            &format!("/series/{uuid}?format=jsonl&step=1h&aggregation={aggregation}"),
        )
        .await;
        assert_eq!(status, StatusCode::OK, "{aggregation}: {body}");
        let value: Value = serde_json::from_str(body.trim())?;
        assert_eq!(
            value["value"].to_string().trim_matches('"'),
            expected,
            "{aggregation}: {body}"
        );
    }
    Ok(())
}

#[tokio::test]
#[serial]
async fn nan_is_refused() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    for (name, samples) in [
        (
            "nan_float",
            TypedSamples::Float(
                vec![Sample {
                    datetime: hifitime::Epoch::from_unix_seconds(T0),
                    value: f64::NAN,
                }]
                .into(),
            ),
        ),
        (
            "nan_location",
            TypedSamples::Location(
                vec![Sample {
                    datetime: hifitime::Epoch::from_unix_seconds(T0),
                    value: geo::Point::new(f64::NAN, 10.0),
                }]
                .into(),
            ),
        ),
    ] {
        let sensor_type = match &samples {
            TypedSamples::Float(_) => SensorType::Float,
            _ => SensorType::Location,
        };
        let sensor = Arc::new(Sensor::new_without_uuid(
            name.to_string(),
            sensor_type,
            None,
            None,
        )?);
        let mut batch_builder = BatchBuilder::new()?;
        let result = match batch_builder.add(sensor, samples).await {
            Ok(()) => batch_builder.send_what_is_left(storage.clone()).await,
            Err(error) => Err(error),
        };
        let error = result.expect_err(name);
        assert!(
            matches!(
                error.downcast_ref::<StorageError>(),
                Some(StorageError::InvalidDataFormat { .. })
            ),
            "{name}: {error:#}"
        );
    }
    Ok(())
}

/// Prometheus sends a NaN stale marker when a series disappears: it is not a sample, and a 400
/// would make Prometheus drop the whole request.
#[tokio::test]
#[serial]
async fn remote_write_drops_nan_samples() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    // RRDcached reads consolidated rows of a window, not the samples
    if test_db.db_type == DatabaseType::RRDcached {
        return Ok(());
    }
    let app = app(test_db.storage());
    let stale_marker = f64::from_bits(0x7ff0_0000_0000_0002);
    let write = WriteRequest {
        timeseries: vec![TimeSeries {
            labels: vec![Label {
                name: "__name__".to_string(),
                value: "with_stale_marker".to_string(),
            }],
            samples: vec![
                PromSample {
                    value: 1.0,
                    timestamp: 1_700_000_000_000,
                },
                PromSample {
                    value: stale_marker,
                    timestamp: 1_700_000_015_000,
                },
                PromSample {
                    value: f64::NAN,
                    timestamp: 1_700_000_030_000,
                },
            ],
            ..Default::default()
        }],
        ..Default::default()
    };
    let response = app
        .clone()
        .oneshot(
            Request::post("/api/v1/prometheus_remote_write")
                .header("content-type", "application/x-protobuf")
                .header("content-encoding", "snappy")
                .header("x-prometheus-remote-write-version", "0.1.0")
                .body(Body::from(
                    snap::raw::Encoder::new().compress_vec(&write.encode_to_vec())?,
                ))?,
        )
        .await?;
    assert!(response.status().is_success(), "{}", response.status());

    let found = test_db
        .storage()
        .query_sensors_by_labels(
            &[sensapp::storage::query::LabelMatcher::eq(
                "__name__",
                "with_stale_marker",
            )],
            None,
            None,
            None,
            false,
        )
        .await?;
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].samples.len(), 1, "{:?}", found[0].samples);
    Ok(())
}
