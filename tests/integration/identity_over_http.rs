//! The sensor identity check seen from the HTTP API: a token limited to some series cannot write
//! to another one by naming its UUID, and an export imported again still goes to its series.
//! These tests run on the backend selected by `TEST_DATABASE_URL`.

use crate::common::{DatabaseType, TestDb};
use anyhow::Result;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch::{Batch, SingleSensorBatch};
use sensapp::datamodel::sensapp_vec::SensAppVec;
use sensapp::datamodel::{Sample, Sensor, SensorData, SensorType, TypedSamples};
use sensapp::exporters::arrow::ArrowConverter;
use sensapp::exporters::senml::SenMLConverter;
use sensapp::http::auth::AuthConfig;
use sensapp::http::metrics::HttpMetrics;
use sensapp::http::server::{RouterSettings, build_router};
use sensapp::http::state::HttpServerState;
use sensapp::storage::StorageInstance;
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

const SECRET: &str = "test-secret-0123456789abcdef012345";
const ARROW: &str = "application/vnd.apache.arrow.stream";
const SENML: &str = "application/json";

/// RRDcached stores no name nor type, only a file per UUID.
fn applies_to(test_db: &TestDb) -> bool {
    test_db.db_type != DatabaseType::RRDcached
}

fn app(storage: Arc<dyn StorageInstance>) -> Router {
    let state = HttpServerState {
        name: Arc::new("SensApp Test".to_string()),
        storage,
        metrics: Arc::new(HttpMetrics::new()),
        influxdb_with_numeric: false,
        max_query_samples: sensapp::http::limits::DEFAULT_MAX_QUERY_SAMPLES,
        auth: Some(AuthConfig::from_secret(SECRET).expect("secret")),
    };
    let settings = RouterSettings {
        max_body_bytes: 1024 * 1024,
        request_timeout: Duration::from_secs(30),
        write_timeout: Duration::from_secs(30),
        maintenance_timeout: Duration::from_secs(30),
        max_concurrent_writes: 4,
        ui_dir: None,
    };
    build_router(state, &settings)
}

fn token(sensors: Option<Vec<String>>) -> String {
    AuthConfig::from_secret(SECRET)
        .expect("secret")
        .issue_token("test", "read write", 3600, sensors)
        .expect("token")
        .token
}

async fn post(
    app: &Router,
    token: &str,
    content_type: &str,
    body: Vec<u8>,
) -> (StatusCode, String) {
    let response = app
        .clone()
        .oneshot(
            Request::post("/publish")
                .header("authorization", format!("Bearer {token}"))
                .header("content-type", content_type)
                .body(Body::from(body))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&body).into_owned())
}

/// A stored Float series `pressure_*` with one sample, read back.
async fn stored_pressure(storage: &Arc<dyn StorageInstance>) -> Result<SensorData> {
    let sensor = Arc::new(Sensor::new_without_uuid(
        format!("pressure_{}", Uuid::new_v4()),
        SensorType::Float,
        None,
        None,
    )?);
    let mut sensors = SensAppVec::new();
    sensors.push(SingleSensorBatch::new(
        sensor.clone(),
        TypedSamples::Float(
            vec![Sample {
                datetime: hifitime::Epoch::from_unix_seconds(1_700_000_000.0),
                value: 1013.0,
            }]
            .into(),
        ),
    ));
    storage.publish(Arc::new(Batch::new(sensors))).await?;
    Ok(storage
        .query_sensor_data(&sensor.uuid.to_string(), None, None, None)
        .await?
        .expect("the series should exist"))
}

async fn sample_count(storage: &Arc<dyn StorageInstance>, uuid: Uuid) -> Result<usize> {
    Ok(storage
        .query_sensor_data(&uuid.to_string(), None, None, None)
        .await?
        .map_or(0, |data| data.samples.len()))
}

/// The same series under another name: what a client sends to borrow the UUID of a series.
fn renamed(stored: &SensorData, name: &str) -> SensorData {
    let sensor = Sensor::new(
        stored.sensor.uuid,
        name.to_string(),
        SensorType::Float,
        None,
        None,
    );
    let samples = TypedSamples::Float(
        vec![Sample {
            datetime: hifitime::Epoch::from_unix_seconds(1_700_000_060.0),
            value: 0.0,
        }]
        .into(),
    );
    SensorData::new(sensor, samples)
}

#[tokio::test]
#[serial]
async fn a_restricted_token_cannot_write_to_another_series_by_its_uuid() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    if !applies_to(&test_db) {
        return Ok(());
    }
    let storage = test_db.storage();
    let app = app(storage.clone());
    let pressure = stored_pressure(&storage).await?;
    let allowed = format!("temperature_{}", Uuid::new_v4());
    let restricted = token(Some(vec![allowed.clone()]));

    let borrowed = renamed(&pressure, &allowed);
    let senml = serde_json::to_vec(&SenMLConverter::to_senml_json(&borrowed)?)?;
    let arrow = ArrowConverter::to_arrow_stream(&borrowed)?;
    for (content_type, body) in [(SENML, senml), (ARROW, arrow)] {
        let (status, message) = post(&app, &restricted, content_type, body).await;
        assert_eq!(status, StatusCode::CONFLICT, "{content_type}: {message}");
        assert!(
            !message.contains(&pressure.sensor.name),
            "the answer must not tell the stored name: {message}"
        );
    }
    assert_eq!(sample_count(&storage, pressure.sensor.uuid).await?, 1);
    Ok(())
}

#[tokio::test]
#[serial]
async fn an_export_imported_again_goes_to_its_series() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    if !applies_to(&test_db) {
        return Ok(());
    }
    let storage = test_db.storage();
    let app = app(storage.clone());
    let pressure = stored_pressure(&storage).await?;
    let unrestricted = token(None);

    let senml = serde_json::to_vec(&SenMLConverter::to_senml_json(&pressure)?)?;
    let arrow = ArrowConverter::to_arrow_stream(&pressure)?;
    for (content_type, body) in [(SENML, senml), (ARROW, arrow)] {
        let (status, message) = post(&app, &unrestricted, content_type, body).await;
        assert!(status.is_success(), "{content_type}: {status} {message}");
    }

    // Writes are appends: the series has its sample three times, and is still one series
    assert_eq!(sample_count(&storage, pressure.sensor.uuid).await?, 3);
    let listed = storage
        .list_series(Some(&pressure.sensor.name), None, None)
        .await?;
    assert_eq!(listed.series.len(), 1);
    Ok(())
}

/// SenML numbers came back as Float, and a Float write under the UUID of an Integer series is
/// refused (409): an Integer export could not be imported again.
#[tokio::test]
#[serial]
async fn an_integer_senml_export_imported_again_goes_to_its_series() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    if !applies_to(&test_db) {
        return Ok(());
    }
    let storage = test_db.storage();
    let app = app(storage.clone());
    let sensor = Arc::new(Sensor::new_without_uuid(
        format!("count_{}", Uuid::new_v4()),
        SensorType::Integer,
        None,
        None,
    )?);
    let mut sensors = SensAppVec::new();
    sensors.push(SingleSensorBatch::new(
        sensor.clone(),
        TypedSamples::Integer(
            vec![Sample {
                datetime: hifitime::Epoch::from_unix_seconds(1_700_000_000.0),
                value: 42,
            }]
            .into(),
        ),
    ));
    storage.publish(Arc::new(Batch::new(sensors))).await?;
    let stored = storage
        .query_sensor_data(&sensor.uuid.to_string(), None, None, None)
        .await?
        .expect("the series should exist");

    let senml = serde_json::to_vec(&SenMLConverter::to_senml_json(&stored)?)?;
    let (status, message) = post(&app, &token(None), SENML, senml).await;
    assert!(status.is_success(), "{status} {message}");
    assert_eq!(sample_count(&storage, sensor.uuid).await?, 2);
    Ok(())
}
