//! How CSV timestamps are read, through `/publish`: offsets, epoch units, the time column chosen,
//! and a long format without a time column. These tests run on the backend selected by
//! `TEST_DATABASE_URL`.

use crate::common::TestDb;
use anyhow::Result;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use sensapp::config::load_configuration_for_tests;
use sensapp::http::metrics::HttpMetrics;
use sensapp::http::server::{RouterSettings, build_router};
use sensapp::http::state::HttpServerState;
use sensapp::storage::StorageInstance;
use sensapp::storage::common::datetime_to_micros;
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

/// 2024-06-01T12:00:00Z
const NOON_US: i64 = 1_717_243_200_000_000;

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
        ui_dir: None,
    };
    build_router(state, &settings)
}

async fn post_csv(app: &Router, csv: &str) -> (StatusCode, String) {
    let request = Request::post("/publish")
        .header("content-type", "text/csv")
        .body(Body::from(csv.to_string()))
        .unwrap();
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, String::from_utf8_lossy(&body).into_owned())
}

/// The microseconds of the samples of the series named `name`.
async fn stored_micros(storage: &Arc<dyn StorageInstance>, name: &str) -> Result<Vec<i64>> {
    let listed = storage.list_series(Some(name), None, None).await?;
    assert_eq!(listed.series.len(), 1, "one series named {name}");
    let uuid = listed.series[0].uuid.to_string();
    let data = storage
        .query_sensor_data(&uuid, None, None, None)
        .await?
        .expect("the series exists");
    let micros = match &data.samples {
        sensapp::datamodel::TypedSamples::Float(s) => {
            s.iter().map(|s| datetime_to_micros(&s.datetime)).collect()
        }
        sensapp::datamodel::TypedSamples::Integer(s) => {
            s.iter().map(|s| datetime_to_micros(&s.datetime)).collect()
        }
        other => panic!("unexpected samples {other:?}"),
    };
    Ok(micros)
}

fn run_id() -> String {
    uuid::Uuid::new_v4().simple().to_string()[..8].to_string()
}

/// Each CSV is one sample at noon UTC, written in another way. The series is named `t_<run>`.
#[tokio::test]
#[serial]
async fn csv_times_are_read_at_the_right_instant() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = app(storage.clone());

    for (header, cell) in [
        // The offset was added instead of subtracted
        ("datetime", "2024-06-01T14:00:00+02:00"),
        ("datetime", "2024-06-01T07:00:00-05:00"),
        // pandas writes a space
        ("datetime", "2024-06-01 12:00:00"),
        // Epoch seconds, milliseconds (stored around year 55970), microseconds, nanoseconds
        ("timestamp", "1717243200"),
        ("timestamp", "1717243200000"),
        ("timestamp", "1717243200000000"),
        ("timestamp", "1717243200000000000"),
        // A float epoch column failed with a 500
        ("timestamp", "1717243200.0"),
    ] {
        let name = format!("t_{}", run_id());
        let csv = format!("{header},{name}\n{cell},1.5\n");
        let (status, body) = post_csv(&app, &csv).await;
        assert!(status.is_success(), "{cell}: {status} {body}");
        assert_eq!(
            stored_micros(&storage, &name).await?,
            vec![NOON_US],
            "{cell}"
        );
    }
    Ok(())
}

/// A date column of dates alone is the time axis; `uptime`, an integer column with "time" in its
/// name, was chosen instead and the dates were stored as strings.
#[tokio::test]
#[serial]
async fn a_date_column_wins_over_an_uptime_column() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = app(storage.clone());

    let run = run_id();
    let csv =
        format!("date,temperature_{run},uptime_{run}\n2024-06-01,20.5,100\n2024-06-02,21.0,200\n");
    let (status, body) = post_csv(&app, &csv).await;
    assert!(status.is_success(), "{status} {body}");
    let day_us = 86_400_000_000;
    let midnight = NOON_US - day_us / 2;
    for name in [format!("temperature_{run}"), format!("uptime_{run}")] {
        assert_eq!(
            stored_micros(&storage, &name).await?,
            vec![midnight, midnight + day_us],
            "{name}"
        );
    }
    Ok(())
}

/// A column of times that is not the time axis is a string series, with its cells as written.
#[tokio::test]
#[serial]
async fn times_in_a_value_column_are_kept_as_written() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = app(storage.clone());

    let name = format!("due_{}", run_id());
    let csv = format!("datetime,{name}\n2024-06-01T12:00:00Z,2024-07-01\n");
    let (status, body) = post_csv(&app, &csv).await;
    assert!(status.is_success(), "{status} {body}");
    let listed = storage.list_series(Some(&name), None, None).await?;
    let data = storage
        .query_sensor_data(&listed.series[0].uuid.to_string(), None, None, None)
        .await?
        .expect("the series exists");
    match &data.samples {
        sensapp::datamodel::TypedSamples::String(samples) => {
            assert_eq!(samples[0].value, "2024-07-01")
        }
        other => panic!("a string series was expected: {other:?}"),
    }
    Ok(())
}

/// Without a time column, the long format stored the samples at 1970 plus the row number, and an
/// integer column with "time" in its name became the time axis when the dates did not parse.
#[tokio::test]
#[serial]
async fn a_csv_without_a_time_column_is_refused() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let app = app(test_db.storage());

    let name = format!("long_{}", run_id());
    // A time column that is not a time: the cells are not dates
    for csv in [
        format!("sensor_name,value\n{name},1.5\n{name},2.5\n"),
        format!("when,sensor_name,value\nyesterday,{name},1.5\n"),
        format!("date,{name},uptime\n01/06/2024,20.5,100\n02/06/2024,21.0,200\n"),
    ] {
        let (status, body) = post_csv(&app, &csv).await;
        assert_eq!(status, StatusCode::BAD_REQUEST, "{csv}: {body}");
    }
    Ok(())
}
