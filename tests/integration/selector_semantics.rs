//! The selectors of the HTTP endpoints mean what they mean in Prometheus: the metric name and
//! `__name__` select the series by name, and what SensApp does not implement is refused instead of
//! ignored. These tests run on the backend selected by `TEST_DATABASE_URL`, through the real router.

use crate::common::{DatabaseType, TestDb};
use anyhow::Result;
use axum::Router;
use axum::body::Body;
use axum::http::{Request, StatusCode};
use prost::Message;
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch_builder::BatchBuilder;
use sensapp::datamodel::sensapp_vec::SensAppLabels;
use sensapp::datamodel::{Sample, Sensor, SensorType, TypedSamples};
use sensapp::http::metrics::HttpMetrics;
use sensapp::http::server::{RouterSettings, build_router};
use sensapp::http::state::HttpServerState;
use sensapp::parsing::prometheus::remote_read_models::{
    LabelMatcher as PromLabelMatcher, Query, ReadRequest, ReadResponse, label_matcher, read_request,
};
use sensapp::parsing::prometheus::remote_write_models::{
    Label, Sample as PromSample, TimeSeries, WriteRequest,
};
use sensapp::storage::StorageInstance;
use serde_json::Value;
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

async fn send(app: &Router, request: Request<Body>) -> (StatusCode, Vec<u8>) {
    let response = app.clone().oneshot(request).await.unwrap();
    let status = response.status();
    let body = axum::body::to_bytes(response.into_body(), usize::MAX)
        .await
        .unwrap();
    (status, body.to_vec())
}

async fn get(app: &Router, path: &str) -> (StatusCode, String) {
    let (status, body) = send(app, Request::get(path).body(Body::empty()).unwrap()).await;
    (status, String::from_utf8_lossy(&body).to_string())
}

/// The names and labels of the series a `/series?selector=` lists, sorted
async fn listed(app: &Router, selector: &str) -> Vec<String> {
    let path = format!("/series?selector={}", urlencoding::encode(selector));
    let (status, body) = get(app, &path).await;
    assert_eq!(status, StatusCode::OK, "{selector}: {body}");
    let catalog: Value = serde_json::from_str(&body).unwrap();
    let mut ids: Vec<String> = catalog["dcat:dataset"]
        .as_array()
        .unwrap()
        .iter()
        .map(|dataset| dataset["@id"].as_str().unwrap().to_string())
        .collect();
    ids.sort();
    ids
}

#[tokio::test]
#[serial]
async fn series_selectors_select_by_metric_name() -> Result<()> {
    ensure_config();
    let db = TestDb::new().await?;
    // RRDcached keeps no names nor labels
    if db.db_type == DatabaseType::RRDcached {
        return Ok(());
    }
    let storage = db.storage();
    let mut batch = BatchBuilder::new()?;
    for (name, room) in [
        ("temperature", "a"),
        ("temperature", "b"),
        ("humidity", "a"),
    ] {
        let labels: SensAppLabels = [("room".to_string(), room.to_string())]
            .into_iter()
            .collect();
        let sensor =
            Sensor::new_without_uuid(name.to_string(), SensorType::Float, None, Some(labels))?;
        let sample = Sample {
            datetime: hifitime::Epoch::from_unix_seconds(1_700_000_000.0),
            value: 1.0,
        };
        batch
            .add(Arc::new(sensor), TypedSamples::Float(vec![sample].into()))
            .await?;
    }
    batch.send_what_is_left(storage.clone()).await?;
    let app = app(storage);

    let temperature_a = r#"temperature{room="a"}"#;
    let temperature_b = r#"temperature{room="b"}"#;
    let humidity_a = r#"humidity{room="a"}"#;
    let cases: [(&str, Vec<&str>); 6] = [
        (
            r#"{__name__="temperature"}"#,
            vec![temperature_a, temperature_b],
        ),
        (r#"{__name__!="temperature"}"#, vec![humidity_a]),
        (r#"temperature{room="a"}"#, vec![temperature_a]),
        ("humidity", vec![humidity_a]),
        (r#"{__name__=~"temp.*",room="b"}"#, vec![temperature_b]),
        (r#"{room="a"}"#, vec![humidity_a, temperature_a]),
    ];
    for (selector, expected) in cases {
        assert_eq!(listed(&app, selector).await, expected, "{selector}");
    }

    // The latest-sample scrape selects the same series
    let selector = urlencoding::encode(r#"temperature{room="a"}"#);
    let (status, body) = get(
        &app,
        &format!("/prometheus/metrics?include_latest_samples=true&selector={selector}"),
    )
    .await;
    assert_eq!(status, StatusCode::OK, "{body}");
    assert!(body.contains(r#"temperature{room="a"}"#), "{body}");
    assert!(!body.contains(r#"room="b""#), "{body}");
    assert!(!body.contains("humidity{"), "{body}");
    Ok(())
}

/// What a selector cannot mean here is refused: an invalid regex, `offset` and `@`.
#[tokio::test]
#[serial]
async fn selectors_sensapp_cannot_honour_are_400() -> Result<()> {
    ensure_config();
    let db = TestDb::new().await?;
    let app = app(db.storage());

    let mut wrong = Vec::new();
    for selector in [
        r#"{room=~"("}"#,
        r#"{room!~"("}"#,
        "temperature offset 1d",
        "temperature @ 1700000000",
    ] {
        let encoded = urlencoding::encode(selector);
        for path in [
            format!("/series?selector={encoded}"),
            format!("/prometheus/metrics?include_latest_samples=true&selector={encoded}"),
        ] {
            let (status, body) = get(&app, &path).await;
            if status != StatusCode::BAD_REQUEST {
                wrong.push(format!("{path}: {status} {body:.200}"));
            }
        }
    }
    for query in [
        "temperature[5m] offset 1d",
        "temperature offset 1h",
        "temperature @ 1700000000",
        "avg(temperature[5m] @ end())",
    ] {
        let path = format!("/api/v1/query?query={}", urlencoding::encode(query));
        let (status, body) = get(&app, &path).await;
        if status != StatusCode::BAD_REQUEST {
            wrong.push(format!("{query}: {status} {body:.200}"));
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    Ok(())
}

/// A series written by remote write is read back with the labels it was written with, and one
/// `__name__`.
#[tokio::test]
#[serial]
async fn remote_write_then_read_keeps_one_metric_name() -> Result<()> {
    ensure_config();
    let db = TestDb::new().await?;
    // RRDcached keeps no names nor labels
    if db.db_type == DatabaseType::RRDcached {
        return Ok(());
    }
    let app = app(db.storage());

    let labels = [
        ("__name__", "node_load1"),
        ("job", "node"),
        ("unit", "load"),
    ];
    let write = WriteRequest {
        timeseries: vec![TimeSeries {
            labels: labels
                .iter()
                .map(|(name, value)| Label {
                    name: name.to_string(),
                    value: value.to_string(),
                })
                .collect(),
            samples: vec![PromSample {
                value: 0.5,
                timestamp: 1_700_000_000_000,
            }],
            ..Default::default()
        }],
        ..Default::default()
    };
    let request = Request::post("/api/v1/prometheus_remote_write")
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "snappy")
        .header("x-prometheus-remote-write-version", "0.1.0")
        .body(Body::from(
            snap::raw::Encoder::new().compress_vec(&write.encode_to_vec())?,
        ))?;
    let (status, body) = send(&app, request).await;
    assert!(
        status.is_success(),
        "{status} {}",
        String::from_utf8_lossy(&body)
    );

    let read = ReadRequest {
        queries: vec![Query {
            start_timestamp_ms: 1_699_999_000_000,
            end_timestamp_ms: 1_700_001_000_000,
            matchers: vec![PromLabelMatcher {
                r#type: label_matcher::Type::Eq as i32,
                name: "__name__".to_string(),
                value: "node_load1".to_string(),
            }],
            hints: None,
        }],
        accepted_response_types: vec![read_request::ResponseType::Samples as i32],
    };
    let request = Request::post("/api/v1/prometheus_remote_read")
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "snappy")
        .header("x-prometheus-remote-read-version", "0.1.0")
        .body(Body::from(
            snap::raw::Encoder::new().compress_vec(&read.encode_to_vec())?,
        ))?;
    let (status, body) = send(&app, request).await;
    assert_eq!(status, StatusCode::OK);
    let response = ReadResponse::decode(&snap::raw::Decoder::new().decompress_vec(&body)?[..])?;
    let series = &response.results[0].timeseries;
    assert_eq!(series.len(), 1);
    let read_labels: Vec<(&str, &str)> = series[0]
        .labels
        .iter()
        .map(|label| (label.name.as_str(), label.value.as_str()))
        .collect();
    assert_eq!(read_labels, labels);

    // The catalog lists it once, with its name, and without `__name__` among its labels
    assert_eq!(
        listed(&app, "node_load1").await,
        [r#"node_load1{job="node",unit="load"}"#]
    );
    Ok(())
}

/// A label name may hold a comma: it is one label key in the list of metrics. SQLite joined the
/// keys with commas and split them again.
#[tokio::test]
#[serial]
async fn a_label_name_with_a_comma_is_one_label_key() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    if test_db.db_type == DatabaseType::RRDcached {
        // RRDcached lists no metrics
        return Ok(());
    }
    let storage = test_db.storage();
    let labels: SensAppLabels = [("a,b", "1"), ("c", "2")]
        .into_iter()
        .map(|(name, value)| (name.to_string(), value.to_string()))
        .collect();
    let sensor = Arc::new(Sensor::new_without_uuid(
        "comma_keys".to_string(),
        SensorType::Float,
        None,
        Some(labels),
    )?);
    let mut batch = BatchBuilder::new()?;
    batch
        .add(
            sensor,
            TypedSamples::Float(
                vec![Sample {
                    datetime: hifitime::Epoch::from_unix_seconds(1_704_067_200.0),
                    value: 1.0,
                }]
                .into(),
            ),
        )
        .await?;
    batch.send_what_is_left(storage.clone()).await?;

    let metrics = storage.list_metrics().await?;
    let metric = metrics
        .iter()
        .find(|metric| metric.name == "comma_keys")
        .expect("the metric is listed");
    let mut keys = metric.label_keys.clone();
    keys.sort();
    assert_eq!(keys, vec!["a,b", "c"]);
    Ok(())
}
