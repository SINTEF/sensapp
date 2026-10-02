//! A window that starts and ends in the middle of buckets only counts the samples inside it: the
//! buckets start at the beginning of the window and the samples before it and after it are not
//! read. These tests run on the backend selected by `TEST_DATABASE_URL`.
//!
//! ClickHouse used to test the start of the bucket instead of the time of the sample, because the
//! bucket was selected as `timestamp_us` and an alias is resolved in `WHERE`.

use crate::common::TestDb;
use anyhow::Result;
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch_builder::BatchBuilder;
use sensapp::datamodel::sensapp_vec::SensAppLabels;
use sensapp::datamodel::{Sample, SensAppDateTime, Sensor, SensorType, TypedSamples};
use sensapp::storage::common::datetime_to_micros;
use sensapp::storage::query::LabelMatcher;
use sensapp::storage::{Aggregation, SensorDataQueryOptions};
use serial_test::serial;
use std::sync::Arc;
use uuid::Uuid;

static INIT: std::sync::Once = std::sync::Once::new();

fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

/// 2024-01-01T00:00:00Z
const T0: f64 = 1_704_067_200.0;

fn at(seconds: f64) -> SensAppDateTime {
    hifitime::Epoch::from_unix_seconds(T0 + seconds)
}

#[tokio::test]
#[serial]
async fn a_window_inside_a_bucket_only_counts_its_samples() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let run = Uuid::new_v4().to_string();
    let labels: SensAppLabels = [("run".to_string(), run.clone())].into_iter().collect();
    let sensor = Arc::new(Sensor::new_without_uuid(
        "xs_window".to_string(),
        SensorType::Float,
        None,
        Some(labels),
    )?);
    let samples = (0..12)
        .map(|i| Sample {
            datetime: at(i as f64 * 60.0),
            value: i as f64,
        })
        .collect::<Vec<_>>();
    let mut batch_builder = BatchBuilder::new()?;
    batch_builder
        .add(sensor.clone(), TypedSamples::Float(samples.into()))
        .await?;
    batch_builder.send_what_is_left(storage.clone()).await?;

    // Samples at 0, 60, 120, ... seconds; the window [150, 500] holds the samples 3 to 8
    let options = SensorDataQueryOptions {
        start_time: Some(at(150.0)),
        end_time: Some(at(500.0)),
        limit: None,
        step_ms: Some(300_000),
        aggregation: Some(Aggregation::Sum),
        simplify: None,
    };
    let data = storage
        .query_sensor_data_advanced(&sensor.uuid.to_string(), &options)
        .await?
        .expect("the series");
    let TypedSamples::Float(samples) = &data.samples else {
        panic!("expected floats");
    };
    let buckets: Vec<(i64, f64)> = samples
        .iter()
        .map(|s| {
            (
                (datetime_to_micros(&s.datetime) - datetime_to_micros(&at(0.0))) / 1_000_000,
                s.value,
            )
        })
        .collect();
    // [150, 450): samples 3 to 7, [450, 750): sample 8
    assert_eq!(buckets, vec![(150, 25.0), (450, 8.0)]);

    // The same window read in bulk with other series
    let matchers = vec![LabelMatcher::eq("run", run)];
    let bulk = storage
        .query_selector_aggregated(&matchers, &options, 256, 1_000)
        .await?
        .expect("within limits");
    assert_eq!(bulk.len(), 1);
    let TypedSamples::Float(samples) = &bulk[0].samples else {
        panic!("expected floats");
    };
    let bulk: Vec<f64> = samples.iter().map(|s| s.value).collect();
    assert_eq!(bulk, vec![25.0, 8.0]);
    Ok(())
}
