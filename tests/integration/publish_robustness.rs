//! Publishing must behave the same on every backend whatever the shape of the data: samples
//! spread over many years in one request, the same series written again and again, or by many
//! writers at once. These tests run on the backend selected by `TEST_DATABASE_URL`.

use crate::common::TestDb;
use anyhow::Result;
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch_builder::BatchBuilder;
use sensapp::datamodel::sensapp_vec::SensAppLabels;
use sensapp::datamodel::{Sample, SensAppDateTime, Sensor, SensorType, TypedSamples};
use sensapp::storage::StorageInstance;
use serial_test::serial;
use std::sync::Arc;
use uuid::Uuid;

static INIT: std::sync::Once = std::sync::Once::new();

fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

/// 2000-01-15T00:00:00Z
const YEAR_2000: f64 = 947_894_400.0;

/// The 15th of every month starting in January 2000, one sample per month.
fn monthly(count: usize) -> Vec<Sample<f64>> {
    (0..count)
        .map(|index| {
            let (year, month) = (2000 + index / 12, 1 + index % 12);
            let datetime: SensAppDateTime =
                hifitime::Epoch::from_gregorian_utc_at_midnight(year as i32, month as u8, 15);
            Sample {
                datetime,
                value: index as f64,
            }
        })
        .collect()
}

#[tokio::test]
#[serial]
async fn one_request_can_span_many_years() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    // Some backends partition by month and limit the partitions touched by one insert
    // (ClickHouse: 100 by default). 150 months is 12.5 years of history.
    let months = 150;

    let sensor = Arc::new(Sensor::new_without_uuid(
        format!("one_request_can_span_many_years_{}", Uuid::new_v4()),
        SensorType::Float,
        None,
        None,
    )?);
    let mut batch_builder = BatchBuilder::new()?;
    batch_builder
        .add(sensor.clone(), TypedSamples::Float(monthly(months).into()))
        .await?;
    batch_builder.send_what_is_left(storage.clone()).await?;

    let data = storage
        .query_sensor_data(&sensor.uuid.to_string(), None, None, None)
        .await?
        .expect("the series should exist");
    let TypedSamples::Float(samples) = &data.samples else {
        panic!("expected float samples");
    };
    assert_eq!(samples.len(), months);
    assert!(
        samples
            .iter()
            .any(|sample| (sample.datetime.to_unix_seconds() - YEAR_2000).abs() < 1.0)
    );
    Ok(())
}

fn labeled_sensor(name_prefix: &str) -> Result<Arc<Sensor>> {
    let labels: SensAppLabels = [("room", "lab"), ("floor", "2")]
        .into_iter()
        .map(|(key, value)| (key.to_string(), value.to_string()))
        .collect();
    Ok(Arc::new(Sensor::new_without_uuid(
        format!("{name_prefix}_{}", Uuid::new_v4()),
        SensorType::Float,
        None,
        Some(labels),
    )?))
}

/// One sample at `minutes` minutes after 2024-01-01T00:00:00Z.
fn one_sample(minutes: usize) -> TypedSamples {
    TypedSamples::Float(
        vec![Sample {
            datetime: hifitime::Epoch::from_unix_seconds(1_704_067_200.0 + minutes as f64 * 60.0),
            value: minutes as f64,
        }]
        .into(),
    )
}

async fn publish_one_sample(
    storage: &Arc<dyn StorageInstance>,
    sensor: &Arc<Sensor>,
    minutes: usize,
) -> Result<()> {
    let mut batch_builder = BatchBuilder::new()?;
    batch_builder
        .add(sensor.clone(), one_sample(minutes))
        .await?;
    batch_builder.send_what_is_left(storage.clone()).await?;
    Ok(())
}

fn sorted_labels(sensor: &Sensor) -> Vec<(String, String)> {
    let mut labels: Vec<_> = sensor.labels.iter().cloned().collect();
    labels.sort();
    labels
}

async fn assert_single_series_with_its_labels(
    storage: &Arc<dyn StorageInstance>,
    sensor: &Sensor,
    expected_samples: usize,
) -> Result<()> {
    let expected_labels = vec![
        ("floor".to_string(), "2".to_string()),
        ("room".to_string(), "lab".to_string()),
    ];

    let listed = storage.list_series(Some(&sensor.name), None, None).await?;
    assert_eq!(listed.series.len(), 1, "the series is listed once");
    assert_eq!(sorted_labels(&listed.series[0]), expected_labels);

    let data = storage
        .query_sensor_data(&sensor.uuid.to_string(), None, None, None)
        .await?
        .expect("the series should exist");
    assert_eq!(sorted_labels(&data.sensor), expected_labels);
    assert_eq!(data.samples.len(), expected_samples);
    Ok(())
}

#[tokio::test]
#[serial]
async fn republishing_a_series_does_not_duplicate_its_labels() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let sensor = labeled_sensor("republishing")?;

    for minutes in 0..5 {
        publish_one_sample(&storage, &sensor, minutes).await?;
    }

    assert_single_series_with_its_labels(&storage, &sensor, 5).await
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn concurrent_first_writes_create_one_series() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let sensor = labeled_sensor("concurrent")?;
    let writers = 16;

    let tasks: Vec<_> = (0..writers)
        .map(|minutes| {
            let storage = storage.clone();
            let sensor = sensor.clone();
            tokio::spawn(async move { publish_one_sample(&storage, &sensor, minutes).await })
        })
        .collect();
    for task in tasks {
        task.await??;
    }

    assert_single_series_with_its_labels(&storage, &sensor, writers).await
}

/// Devices with a broken clock send dates far from today. They must be stored and read back
/// as they were sent, not rejected and not moved.
#[tokio::test]
#[serial]
async fn extreme_timestamps_round_trip() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    let dates = [
        (1900, 1, 1),
        (1969, 12, 31),
        (1970, 1, 1),
        (2262, 6, 1),
        (2500, 1, 1),
        (9999, 12, 31),
    ];
    let samples: Vec<Sample<f64>> = dates
        .iter()
        .enumerate()
        .map(|(index, (year, month, day))| Sample {
            datetime: hifitime::Epoch::from_gregorian_utc_at_midnight(*year, *month, *day),
            value: index as f64,
        })
        .collect();
    let expected: Vec<(i64, f64)> = samples
        .iter()
        .map(|sample| {
            (
                (sample.datetime.to_unix_seconds() * 1e6).round() as i64,
                sample.value,
            )
        })
        .collect();

    let sensor = Arc::new(Sensor::new_without_uuid(
        format!("extreme_timestamps_{}", Uuid::new_v4()),
        SensorType::Float,
        None,
        None,
    )?);
    let mut batch_builder = BatchBuilder::new()?;
    batch_builder
        .add(sensor.clone(), TypedSamples::Float(samples.into()))
        .await?;
    batch_builder.send_what_is_left(storage.clone()).await?;

    let data = storage
        .query_sensor_data(&sensor.uuid.to_string(), None, None, None)
        .await?
        .expect("the series should exist");
    let TypedSamples::Float(stored) = &data.samples else {
        panic!("expected float samples");
    };
    let mut stored: Vec<(i64, f64)> = stored
        .iter()
        .map(|sample| {
            (
                (sample.datetime.to_unix_seconds() * 1e6).round() as i64,
                sample.value,
            )
        })
        .collect();
    stored.sort_by_key(|(timestamp, _)| *timestamp);
    assert_eq!(stored, expected);
    Ok(())
}

/// Backends look sensors up in chunks: use more sensors than any chunk, with labels and a
/// shared unit, in one batch, then page through the catalog.
#[tokio::test]
#[serial]
async fn a_batch_of_many_new_sensors_registers_all_of_them() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let count = 2_100;
    let run = Uuid::new_v4();
    let unit = sensapp::datamodel::unit::Unit {
        name: "celsius".to_string(),
        description: Some("degrees".to_string()),
    };

    let mut batch_builder = BatchBuilder::new()?;
    for index in 0..count {
        let labels: SensAppLabels = [
            ("host".to_string(), format!("h{}", index % 50)),
            ("index".to_string(), index.to_string()),
        ]
        .into_iter()
        .collect();
        let sensor = Arc::new(Sensor::new_without_uuid(
            format!("many_sensors_{run}"),
            SensorType::Float,
            Some(unit.clone()),
            Some(labels),
        )?);
        batch_builder.add(sensor, one_sample(index)).await?;
    }
    batch_builder.send_what_is_left(storage.clone()).await?;

    let name = format!("many_sensors_{run}");
    let mut seen = std::collections::HashSet::new();
    let mut bookmark: Option<String> = None;
    loop {
        let page = storage
            .list_series(Some(&name), None, bookmark.as_deref())
            .await?;
        for sensor in &page.series {
            assert_eq!(sensor.labels.len(), 2, "labels of {}", sensor.uuid);
            // (SQLite does not keep unit descriptions)
            assert_eq!(
                sensor.unit.as_ref().map(|u| u.name.as_str()),
                Some("celsius")
            );
            assert!(seen.insert(sensor.uuid), "{} is listed twice", sensor.uuid);
        }
        bookmark = page.bookmark;
        if bookmark.is_none() {
            break;
        }
    }
    assert_eq!(seen.len(), count);
    Ok(())
}

/// A deleted series that is written again is a new, complete series: its metadata and labels
/// are registered again, and only the new samples exist.
#[tokio::test]
#[serial]
async fn a_series_written_again_after_its_deletion_is_registered_again() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let sensor = labeled_sensor("deleted_then_rewritten")?;

    for minutes in 0..3 {
        publish_one_sample(&storage, &sensor, minutes).await?;
    }
    assert!(storage.delete_series(&sensor.uuid.to_string()).await?);
    assert!(
        storage
            .list_series(Some(&sensor.name), None, None)
            .await?
            .series
            .is_empty(),
        "the deleted series is gone"
    );

    publish_one_sample(&storage, &sensor, 10).await?;
    assert_single_series_with_its_labels(&storage, &sensor, 1).await
}

/// Strings are stored through a dictionary on some backends: whatever their content, and however
/// many series and requests share them, they must read back exactly.
#[tokio::test]
#[serial]
async fn string_samples_round_trip_whatever_their_content() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let run = Uuid::new_v4();

    let long = "long ".repeat(4_000);
    let strings: Vec<String> = vec![
        "plain".into(),
        "".into(),
        " leading and trailing ".into(),
        "é ü ñ 日本語 🚀".into(),
        "quote \" apostrophe ' backslash \\ percent % underscore _".into(),
        "line\nbreak\ttab".into(),
        "plain".into(), // repeated within a series
        long,
    ];

    // Three series that share their strings, written in two requests
    let sensors: Vec<Arc<Sensor>> = (0..3)
        .map(|index| {
            Sensor::new_without_uuid(
                format!("strings_{run}_{index}"),
                SensorType::String,
                None,
                None,
            )
            .map(Arc::new)
        })
        .collect::<Result<_, _>>()?;
    for request in 0..2 {
        let mut batch_builder = BatchBuilder::new()?;
        for (index, sensor) in sensors.iter().enumerate() {
            let samples: Vec<Sample<String>> = strings
                .iter()
                .enumerate()
                .map(|(position, value)| Sample {
                    datetime: hifitime::Epoch::from_unix_seconds(
                        1_704_067_200.0 + (request * 100 + position) as f64 + index as f64 * 0.001,
                    ),
                    value: format!(
                        "{value}{}",
                        if request == 1 && position == 0 {
                            "!"
                        } else {
                            ""
                        }
                    ),
                })
                .collect();
            batch_builder
                .add(sensor.clone(), TypedSamples::String(samples.into()))
                .await?;
        }
        batch_builder.send_what_is_left(storage.clone()).await?;
    }

    for (index, sensor) in sensors.iter().enumerate() {
        let data = storage
            .query_sensor_data(&sensor.uuid.to_string(), None, None, None)
            .await?
            .expect("the series should exist");
        let TypedSamples::String(stored) = &data.samples else {
            panic!("expected string samples");
        };
        let mut stored: Vec<(i64, String)> = stored
            .iter()
            .map(|sample| {
                (
                    (sample.datetime.to_unix_seconds() * 1e6).round() as i64,
                    sample.value.clone(),
                )
            })
            .collect();
        stored.sort();
        let mut expected: Vec<(i64, String)> = Vec::new();
        for request in 0..2 {
            for (position, value) in strings.iter().enumerate() {
                let time =
                    1_704_067_200.0 + (request * 100 + position) as f64 + index as f64 * 0.001;
                let value = format!(
                    "{value}{}",
                    if request == 1 && position == 0 {
                        "!"
                    } else {
                        ""
                    }
                );
                expected.push(((time * 1e6).round() as i64, value));
            }
        }
        expected.sort();
        assert_eq!(stored.len(), expected.len(), "series {index}");
        assert_eq!(stored, expected, "series {index}");
    }
    Ok(())
}
