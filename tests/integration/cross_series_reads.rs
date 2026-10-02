//! `read_cross_series` asks the storage for per-series buckets and merges them. It must give the
//! same answer as `aggregate_across_series` on the raw samples of the same series (the reference),
//! and it must not be limited by the number of raw samples. These tests run on the backend selected
//! by `TEST_DATABASE_URL`: each backend is compared with the reference on itself, which is not a
//! proof that the backends agree with each other.

use crate::common::TestDb;
use anyhow::Result;
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch_builder::BatchBuilder;
use sensapp::datamodel::sensapp_vec::SensAppLabels;
use sensapp::datamodel::{Sample, SensAppDateTime, Sensor, SensorData, SensorType, TypedSamples};
use sensapp::storage::SelectorLimitExceeded;
use sensapp::storage::common::datetime_to_micros;
use sensapp::storage::cross_series::{
    CrossSeriesQuery, Grouping, aggregate_across_series, read_cross_series,
};
use sensapp::storage::query::LabelMatcher;
use sensapp::storage::{Aggregation, StorageInstance};
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

fn samples_of<T>(count: usize, step_seconds: f64, value: impl Fn(usize) -> T) -> Vec<Sample<T>> {
    (0..count)
        .map(|i| Sample {
            datetime: at(i as f64 * step_seconds),
            value: value(i),
        })
        .collect()
}

fn sensor(name: &str, sensor_type: SensorType, run: &str, room: &str, id: usize) -> Arc<Sensor> {
    let labels: SensAppLabels = [
        ("run".to_string(), run.to_string()),
        ("room".to_string(), room.to_string()),
        ("id".to_string(), id.to_string()),
    ]
    .into_iter()
    .collect();
    Arc::new(
        Sensor::new_without_uuid(name.to_string(), sensor_type, None, Some(labels))
            .expect("a valid sensor"),
    )
}

async fn publish(
    storage: &Arc<dyn StorageInstance>,
    series: Vec<(Arc<Sensor>, TypedSamples)>,
) -> Result<()> {
    let mut batch_builder = BatchBuilder::new()?;
    for (sensor, samples) in series {
        batch_builder.add(sensor, samples).await?;
    }
    batch_builder.send_what_is_left(storage.clone()).await?;
    Ok(())
}

/// Rooms a, b and c: four float sensors of 12 samples a minute apart each. Room d: an integer, a
/// float and a decimal sensor sharing a group, 12 samples a minute apart as well.
async fn mixed_fixture(storage: &Arc<dyn StorageInstance>) -> Result<String> {
    let run = Uuid::new_v4().to_string();
    let mut series = Vec::new();
    for (room_index, room) in ["a", "b", "c"].into_iter().enumerate() {
        for id in 0..4 {
            let base = (room_index * 100 + id * 10) as f64;
            series.push((
                sensor("xs_temp", SensorType::Float, &run, room, id),
                TypedSamples::Float(
                    samples_of(12, 60.0, |i| base + (i as f64 * 1.5).sin() * 10.0).into(),
                ),
            ));
        }
    }
    series.push((
        sensor("xs_mixed", SensorType::Integer, &run, "d", 0),
        TypedSamples::Integer(samples_of(12, 60.0, |i| (i as i64 * 3) % 7 - 2).into()),
    ));
    series.push((
        sensor("xs_mixed", SensorType::Float, &run, "d", 1),
        TypedSamples::Float(samples_of(12, 60.0, |i| i as f64 * 0.25).into()),
    ));
    series.push((
        sensor("xs_mixed", SensorType::Numeric, &run, "d", 2),
        TypedSamples::Numeric(
            samples_of(12, 60.0, |i| {
                rust_decimal::Decimal::new(i as i64 * 7 + 3, 2)
            })
            .into(),
        ),
    ));
    publish(storage, series).await?;
    Ok(run)
}

/// Equal up to the rounding of a sum of floats added in another order
fn close(left: f64, right: f64) -> bool {
    (left - right).abs() <= 1e-9 * left.abs().max(right.abs()).max(1.0)
}

/// What an output series says, ordered: labels, name, then (time in micros, value).
type Projection = Vec<(Vec<(String, String)>, String, Vec<(i64, f64)>)>;

fn project(series: &[SensorData]) -> Projection {
    let mut projection: Projection = series
        .iter()
        .map(|data| {
            let values = match &data.samples {
                TypedSamples::Float(samples) => samples
                    .iter()
                    .map(|s| (datetime_to_micros(&s.datetime), s.value))
                    .collect(),
                TypedSamples::Integer(samples) => samples
                    .iter()
                    .map(|s| (datetime_to_micros(&s.datetime), s.value as f64))
                    .collect(),
                other => panic!("unexpected output samples {other:?}"),
            };
            (
                data.sensor
                    .labels
                    .iter()
                    .filter(|(name, _)| name != "run")
                    .cloned()
                    .collect(),
                data.sensor.name.clone(),
                values,
            )
        })
        .collect();
    projection.sort_by(|left, right| (&left.0, &left.1).cmp(&(&right.0, &right.1)));
    projection
}

fn assert_same(pushed: &Projection, reference: &Projection, context: &str) {
    assert_eq!(pushed.len(), reference.len(), "{context}: output series");
    for (pushed, reference) in pushed.iter().zip(reference) {
        assert_eq!(pushed.0, reference.0, "{context}: labels");
        assert_eq!(pushed.1, reference.1, "{context}: name");
        assert_eq!(pushed.2.len(), reference.2.len(), "{context}: buckets");
        for ((pushed_time, pushed_value), (reference_time, reference_value)) in
            pushed.2.iter().zip(&reference.2)
        {
            assert_eq!(pushed_time, reference_time, "{context}: bucket start");
            assert!(
                close(*pushed_value, *reference_value),
                "{context}: {pushed_value} against {reference_value} at {pushed_time}"
            );
        }
    }
}

#[tokio::test]
#[serial]
async fn buckets_merge_into_the_answer_of_the_raw_samples() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let run = mixed_fixture(&storage).await?;
    let matchers = vec![LabelMatcher::eq("run", run)];

    // A window that starts on the first sample and one that starts and ends between samples
    let windows = [(0.0, 700.0), (150.0, 500.0)];
    let groupings = [
        None,
        Some(Grouping::By(vec!["room".to_string()])),
        Some(Grouping::Without(vec!["id".to_string()])),
    ];
    let steps_seconds = [None, Some(60), Some(300), Some(7 * 60)];
    for (start, end) in windows {
        let (start, end) = (at(start), at(end));
        let origin_us = datetime_to_micros(&start);
        for aggregation in [
            Aggregation::Sum,
            Aggregation::Avg,
            Aggregation::Min,
            Aggregation::Max,
            Aggregation::Count,
        ] {
            for grouping in &groupings {
                for step in steps_seconds {
                    let query = CrossSeriesQuery {
                        aggregation,
                        grouping: grouping.clone(),
                        step_us: step.map(|seconds: i64| seconds * 1_000_000),
                        origin_us,
                    };
                    let context = format!(
                        "{} {grouping:?} step {step:?} window {}..{}",
                        aggregation.name(),
                        start,
                        end
                    );
                    let (pushed, stats) = read_cross_series(
                        storage.as_ref(),
                        &matchers,
                        Some(start),
                        Some(end),
                        &query,
                        10_000,
                        1_000_000,
                    )
                    .await?
                    .expect("within limits");
                    // The reference works on the raw samples of the same series
                    let raw = storage
                        .query_selector(&matchers, Some(start), Some(end), true, 1000, 1_000_000)
                        .await?
                        .expect("within limits");
                    let reference = aggregate_across_series(raw, &query)?;
                    assert_same(&project(&pushed), &project(&reference), &context);
                    assert_eq!(stats.series, 15, "{context}: series with samples");
                }
            }
        }
    }
    Ok(())
}

/// 300 series is more than the 256 a selector read allows, with a sample each in the same minute.
#[tokio::test]
#[serial]
async fn more_series_than_a_raw_read_allows() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let run = Uuid::new_v4().to_string();
    let count = 300usize;
    let series = (0..count)
        .map(|id| {
            (
                sensor(
                    "xs_many",
                    SensorType::Float,
                    &run,
                    if id % 2 == 0 { "even" } else { "odd" },
                    id,
                ),
                TypedSamples::Float(samples_of(4, 60.0, |i| (id + i) as f64).into()),
            )
        })
        .collect();
    publish(&storage, series).await?;
    let matchers = vec![LabelMatcher::eq("run", run)];
    let (start, end) = (at(0.0), at(600.0));

    // The raw read refuses it
    assert!(matches!(
        storage
            .query_selector(&matchers, Some(start), Some(end), true, 256, 1_000_000)
            .await?,
        Err(SelectorLimitExceeded::Series)
    ));

    let query = CrossSeriesQuery {
        aggregation: Aggregation::Sum,
        grouping: Some(Grouping::By(vec!["room".to_string()])),
        step_us: None,
        origin_us: datetime_to_micros(&start),
    };
    let read = |max_series, max_buckets| {
        let (storage, matchers, query) = (storage.clone(), matchers.clone(), query.clone());
        async move {
            read_cross_series(
                storage.as_ref(),
                &matchers,
                Some(start),
                Some(end),
                &query,
                max_series,
                max_buckets,
            )
            .await
        }
    };

    let (pushed, stats) = read(10_000, 1_000_000).await?.expect("within limits");
    assert_eq!(stats.series, count);
    let sum_of = |room: &str| -> f64 {
        let data = pushed
            .iter()
            .find(|data| {
                data.sensor
                    .labels
                    .iter()
                    .any(|(n, v)| n == "room" && v == room)
            })
            .expect("the room");
        let TypedSamples::Float(samples) = &data.samples else {
            panic!("expected floats");
        };
        assert_eq!(samples.len(), 1, "one bucket for the whole window");
        samples[0].value
    };
    let expected = |parity: usize| -> f64 {
        (0..count)
            .filter(|id| id % 2 == parity)
            .map(|id| (0..4).map(|i| (id + i) as f64).sum::<f64>())
            .sum()
    };
    assert_eq!(sum_of("even"), expected(0));
    assert_eq!(sum_of("odd"), expected(1));

    // The limits are exact: one series or one bucket more than allowed is refused
    assert!(read(count, 1_000_000).await?.is_ok());
    assert_eq!(
        read(count - 1, 1_000_000).await?.unwrap_err(),
        SelectorLimitExceeded::Series
    );
    assert!(read(10_000, count).await?.is_ok());
    assert_eq!(
        read(10_000, count - 1).await?.unwrap_err(),
        SelectorLimitExceeded::Samples
    );
    Ok(())
}
