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
use sensapp::datamodel::sensapp_datetime::SensAppDateTimeExt;
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

/// Publish `samples` under a new sensor labelled with a run id, and return the sensor and the
/// matchers that select it.
async fn publish_run(
    storage: &Arc<dyn sensapp::storage::StorageInstance>,
    name: &str,
    sensor_type: SensorType,
    samples: TypedSamples,
) -> Result<(Arc<Sensor>, Vec<LabelMatcher>)> {
    let run = Uuid::new_v4().to_string();
    let labels: SensAppLabels = [("run".to_string(), run.clone())].into_iter().collect();
    let sensor = Arc::new(Sensor::new_without_uuid(
        name.to_string(),
        sensor_type,
        None,
        Some(labels),
    )?);
    let mut batch_builder = BatchBuilder::new()?;
    batch_builder.add(sensor.clone(), samples).await?;
    batch_builder.send_what_is_left(storage.clone()).await?;
    Ok((sensor, vec![LabelMatcher::eq("run", run)]))
}

fn sum_by(step_ms: i64, start_time: Option<SensAppDateTime>) -> SensorDataQueryOptions {
    SensorDataQueryOptions {
        start_time,
        end_time: None,
        limit: None,
        step_ms: Some(step_ms),
        aggregation: Some(Aggregation::Sum),
        simplify: None,
    }
}

/// An integer sum over `i64` is an error, as on PostgreSQL. ClickHouse wrapped it to a negative.
#[tokio::test]
#[serial]
async fn an_integer_sum_over_i64_is_an_error() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let samples = [i64::MAX, 1]
        .into_iter()
        .enumerate()
        .map(|(i, value)| Sample {
            datetime: at(i as f64),
            value,
        })
        .collect::<Vec<_>>();
    let (sensor, matchers) = publish_run(
        &storage,
        "xs_overflow",
        SensorType::Integer,
        TypedSamples::Integer(samples.into()),
    )
    .await?;

    let options = sum_by(3_600_000, None);
    let result = storage
        .query_sensor_data_advanced(&sensor.uuid.to_string(), &options)
        .await;
    assert!(result.is_err(), "expected an error, got {result:?}");
    let bulk = storage
        .query_selector_aggregated(&matchers, &options, 256, 1_000)
        .await;
    assert!(bulk.is_err(), "expected an error, got {bulk:?}");
    Ok(())
}

/// Without a start, the buckets are aligned on the Unix epoch, and a sample before 1970 belongs to
/// the bucket that starts at or before it. ClickHouse truncated towards zero: -1.5 s went to the
/// bucket of -1 s.
#[tokio::test]
#[serial]
async fn buckets_before_1970_start_at_or_before_their_samples() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let samples = [(-1_500, 1.0), (-500, 2.0), (500, 4.0)]
        .into_iter()
        .map(|(ms, value)| Sample {
            datetime: SensAppDateTime::from_unix_milliseconds_i64(ms),
            value,
        })
        .collect::<Vec<_>>();
    let (sensor, matchers) = publish_run(
        &storage,
        "xs_before_1970",
        SensorType::Float,
        TypedSamples::Float(samples.into()),
    )
    .await?;

    let options = sum_by(1_000, None);
    let data = storage
        .query_sensor_data_advanced(&sensor.uuid.to_string(), &options)
        .await?
        .expect("the series");
    let buckets = |samples: &TypedSamples| -> Vec<(i64, f64)> {
        let TypedSamples::Float(samples) = samples else {
            panic!("expected floats");
        };
        samples
            .iter()
            .map(|s| (datetime_to_micros(&s.datetime) / 1_000, s.value))
            .collect()
    };
    let expected = vec![(-2_000, 1.0), (-1_000, 2.0), (0, 4.0)];
    assert_eq!(buckets(&data.samples), expected);

    let bulk = storage
        .query_selector_aggregated(&matchers, &options, 256, 1_000)
        .await?
        .expect("within limits");
    assert_eq!(buckets(&bulk[0].samples), expected);
    Ok(())
}

/// Two samples of a series at the same timestamp (written twice, deduplication off): `first`
/// keeps the one written first, `last` and `latest` the one written last, on the single series
/// read and on the bulk read alike. Only SQLite and DuckDB keep the order of the writes; the
/// other backends keep either sample.
#[tokio::test]
#[serial]
async fn equal_timestamps_follow_the_order_of_the_writes() -> Result<()> {
    use crate::common::DatabaseType;
    ensure_config();
    let test_db = TestDb::new().await?;
    if !matches!(test_db.db_type, DatabaseType::SQLite | DatabaseType::DuckDB) {
        return Ok(());
    }
    let storage = test_db.storage();
    let sample = |seconds: f64, value: f64| {
        TypedSamples::Float(
            vec![Sample {
                datetime: at(seconds),
                value,
            }]
            .into(),
        )
    };
    let (sensor, matchers) =
        publish_run(&storage, "xs_ties", SensorType::Float, sample(10.0, 1.0)).await?;
    for value in [2.0, 3.0] {
        let mut batch_builder = BatchBuilder::new()?;
        batch_builder
            .add(sensor.clone(), sample(10.0, value))
            .await?;
        batch_builder.send_what_is_left(storage.clone()).await?;
    }

    for (aggregation, expected) in [
        (Aggregation::First, 1.0),
        (Aggregation::Last, 3.0),
        (Aggregation::Latest, 3.0),
    ] {
        let options = SensorDataQueryOptions {
            aggregation: Some(aggregation),
            ..sum_by(60_000, Some(at(0.0)))
        };
        let value = |samples: &TypedSamples| {
            let TypedSamples::Float(samples) = samples else {
                panic!("expected floats");
            };
            samples.iter().map(|s| s.value).collect::<Vec<_>>()
        };
        let data = storage
            .query_sensor_data_advanced(&sensor.uuid.to_string(), &options)
            .await?
            .expect("the series");
        assert_eq!(value(&data.samples), vec![expected], "{aggregation:?}");
        let bulk = storage
            .query_selector_aggregated(&matchers, &options, 256, 1_000)
            .await?
            .expect("within limits");
        assert_eq!(
            value(&bulk[0].samples),
            vec![expected],
            "{aggregation:?} bulk"
        );
    }
    Ok(())
}
