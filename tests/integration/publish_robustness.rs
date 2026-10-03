//! Publishing must behave the same on every backend whatever the shape of the data: samples
//! spread over many years in one request, the same series written again and again, or by many
//! writers at once. These tests run on the backend selected by `TEST_DATABASE_URL`.

use crate::common::TestDb;
use anyhow::Result;
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch_builder::BatchBuilder;
use sensapp::datamodel::sensapp_datetime::SensAppDateTimeExt;
use sensapp::datamodel::sensapp_vec::SensAppLabels;
use sensapp::datamodel::{Sample, SensAppDateTime, Sensor, SensorType, TypedSamples};
use sensapp::storage::StorageInstance;
use sensapp::storage::common::datetime_to_micros;
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

/// Different new series, whose first samples are in a time range that has no storage yet (a new
/// week of the TimescaleDB hypertables, which create their chunks, and so lock `sensors`, on the
/// first insert): none of the writers may fail because it met another one. Each writer sends
/// several types, in a different order, so that the hypertables are not met in the same order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn concurrent_first_writes_of_different_series_in_a_new_time_range() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let run = Uuid::new_v4();

    for round in 0..5 {
        // One more month every round, so that every round starts on empty storage
        let seconds = 1_704_067_200.0 + round as f64 * 30.0 * 86_400.0;
        let tasks: Vec<_> = (0..8)
            .map(|writer| {
                let storage = storage.clone();
                tokio::spawn(async move {
                    let datetime: SensAppDateTime = hifitime::Epoch::from_unix_seconds(seconds);
                    let mut sensors = Vec::new();
                    let mut batch_builder = BatchBuilder::new()?;
                    for kind in 0..4 {
                        // The order of the types depends on the writer
                        let kind = (kind + writer) % 4;
                        let (sensor_type, samples) = match kind {
                            0 => (
                                SensorType::Float,
                                TypedSamples::Float(
                                    vec![Sample {
                                        datetime,
                                        value: 1.5,
                                    }]
                                    .into(),
                                ),
                            ),
                            1 => (
                                SensorType::Integer,
                                TypedSamples::Integer(vec![Sample { datetime, value: 7 }].into()),
                            ),
                            2 => (
                                SensorType::String,
                                TypedSamples::String(
                                    vec![Sample {
                                        datetime,
                                        value: "on".to_string(),
                                    }]
                                    .into(),
                                ),
                            ),
                            _ => (
                                SensorType::Boolean,
                                TypedSamples::Boolean(
                                    vec![Sample {
                                        datetime,
                                        value: true,
                                    }]
                                    .into(),
                                ),
                            ),
                        };
                        let sensor = Arc::new(Sensor::new_without_uuid(
                            format!("new_range_{run}_{round}_{writer}_{kind}"),
                            sensor_type,
                            None,
                            None,
                        )?);
                        batch_builder.add(sensor.clone(), samples).await?;
                        sensors.push(sensor);
                    }
                    batch_builder.send_what_is_left(storage).await?;
                    anyhow::Ok(sensors)
                })
            })
            .collect();
        for task in tasks {
            for sensor in task.await?? {
                let data = storage
                    .query_sensor_data(&sensor.uuid.to_string(), None, None, None)
                    .await?
                    .expect("the series should exist");
                assert_eq!(data.samples.len(), 1, "{} in round {round}", sensor.name);
            }
        }
    }
    Ok(())
}

/// A writer that registers new series of two types, and a writer that only needs chunks of the same
/// two types, at the same moment: each can want the storage of the types in the other order.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[serial]
async fn concurrent_first_writes_meeting_the_types_in_the_other_order() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let run = Uuid::new_v4();

    let batch = |names: &[(String, bool)], seconds: f64| {
        let datetime: SensAppDateTime = hifitime::Epoch::from_unix_seconds(seconds);
        let mut sensors = Vec::new();
        let mut samples = Vec::new();
        for (name, is_boolean) in names {
            let sensor_type = if *is_boolean {
                SensorType::Boolean
            } else {
                SensorType::String
            };
            sensors.push(Arc::new(Sensor::new_without_uuid(
                format!("{name}_{run}"),
                sensor_type,
                None,
                None,
            )?));
            samples.push(if *is_boolean {
                TypedSamples::Boolean(
                    vec![Sample {
                        datetime,
                        value: true,
                    }]
                    .into(),
                )
            } else {
                TypedSamples::String(
                    vec![Sample {
                        datetime,
                        value: "on".to_string(),
                    }]
                    .into(),
                )
            });
        }
        anyhow::Ok((sensors, samples))
    };
    let publish = |storage: Arc<dyn StorageInstance>, sensors: Vec<Arc<Sensor>>, samples| async move {
        let mut batch_builder = BatchBuilder::new()?;
        for (sensor, samples) in sensors.into_iter().zip(samples) {
            batch_builder.add(sensor, samples).await?;
        }
        batch_builder.send_what_is_left(storage).await?;
        anyhow::Ok(())
    };

    for round in 0..5 {
        let start = 1_704_067_200.0;
        let names = |prefix: &str| {
            [
                (format!("{prefix}_boolean_{round}"), true),
                (format!("{prefix}_string_{round}"), false),
            ]
        };
        // The series of the first writer exist, in the first month
        let existing = names("existing");
        let (sensors, samples) = batch(&existing, start)?;
        publish(storage.clone(), sensors.clone(), samples).await?;
        // Then, in a month that has no storage: the first writer needs the chunks, in the order of
        // the code (strings first), the second one registers new series (booleans first)
        let seconds = start + (round + 1) as f64 * 30.0 * 86_400.0;
        let (old_sensors, old_samples) = batch(&existing, seconds)?;
        let fresh = names("fresh");
        let (new_sensors, new_samples) = batch(&fresh, seconds)?;
        let (a, b) = tokio::join!(
            tokio::spawn(publish(storage.clone(), old_sensors, old_samples)),
            tokio::spawn(publish(storage.clone(), new_sensors.clone(), new_samples)),
        );
        a??;
        b??;
        for sensor in new_sensors {
            let data = storage
                .query_sensor_data(&sensor.uuid.to_string(), None, None, None)
                .await?
                .expect("the series should exist");
            assert_eq!(data.samples.len(), 1, "{} in round {round}", sensor.name);
        }
    }
    Ok(())
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

/// Microseconds of the sample `position` of the series `index` in the request `request`: whole
/// milliseconds, a millisecond apart, exact on every backend (DuckDB reads milliseconds back through
/// a float conversion, which must not matter here).
fn string_sample_micros(request: usize, position: usize, index: usize) -> i64 {
    1_704_067_200_000_000 + ((request * 100 + position) * 1_000 + index) as i64 * 1_000
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
    // What the sample `position` of a request holds: the second request changes the first one
    let value_of = |request: usize, position: usize| {
        format!(
            "{}{}",
            strings[position],
            if request == 1 && position == 0 {
                "!"
            } else {
                ""
            }
        )
    };

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
            let samples: Vec<Sample<String>> = (0..strings.len())
                .map(|position| Sample {
                    datetime: SensAppDateTime::from_unix_microseconds_i64(string_sample_micros(
                        request, position, index,
                    )),
                    value: value_of(request, position),
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
            .map(|sample| (datetime_to_micros(&sample.datetime), sample.value.clone()))
            .collect();
        stored.sort();
        let mut expected: Vec<(i64, String)> = (0..2)
            .flat_map(|request| (0..strings.len()).map(move |position| (request, position)))
            .map(|(request, position)| {
                (
                    string_sample_micros(request, position, index),
                    value_of(request, position),
                )
            })
            .collect();
        expected.sort();
        assert_eq!(stored.len(), expected.len(), "series {index}");
        assert_eq!(stored, expected, "series {index}");
    }
    Ok(())
}

/// A window is inclusive at both ends, a limit keeps the oldest samples of the window, whatever
/// the type and wherever a backend applies them.
#[tokio::test]
#[serial]
async fn windows_and_limits_select_the_right_samples() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let run = Uuid::new_v4();
    let second = |index: i64| 1_704_067_200_000_000 + index * 1_000_000;
    let at = |index: i64| SensAppDateTime::from_unix_microseconds_i64(second(index));

    // 100 samples, one per second, of three types
    let make = |name: &str, sensor_type: SensorType, samples: TypedSamples| -> Result<_> {
        let sensor = Arc::new(Sensor::new_without_uuid(
            format!("{name}_{run}"),
            sensor_type,
            None,
            None,
        )?);
        Ok((sensor, samples))
    };
    let floats: Vec<Sample<f64>> = (0..100)
        .map(|i| Sample {
            datetime: at(i),
            value: i as f64,
        })
        .collect();
    let integers: Vec<Sample<i64>> = (0..100)
        .map(|i| Sample {
            datetime: at(i),
            value: i,
        })
        .collect();
    let strings: Vec<Sample<String>> = (0..100)
        .map(|i| Sample {
            datetime: at(i),
            value: format!("s{i}"),
        })
        .collect();
    let series = [
        make(
            "window_float",
            SensorType::Float,
            TypedSamples::Float(floats.into()),
        )?,
        make(
            "window_integer",
            SensorType::Integer,
            TypedSamples::Integer(integers.into()),
        )?,
        make(
            "window_string",
            SensorType::String,
            TypedSamples::String(strings.into()),
        )?,
    ];
    let mut batch_builder = BatchBuilder::new()?;
    let mut sensors = Vec::new();
    for (sensor, samples) in series {
        sensors.push(sensor.clone());
        batch_builder.add(sensor, samples).await?;
    }
    batch_builder.send_what_is_left(storage.clone()).await?;

    // (start, end, limit) -> the indexes that must come back, oldest first
    type WindowCase = (
        Option<i64>,
        Option<i64>,
        Option<usize>,
        std::ops::RangeInclusive<i64>,
    );
    let cases: [WindowCase; 7] = [
        (None, None, None, 0..=99),
        (Some(10), Some(19), None, 10..=19), // both bounds are inclusive
        (Some(10), None, None, 10..=99),
        (None, Some(4), None, 0..=4),
        (Some(10), Some(50), Some(5), 10..=14), // the limit keeps the oldest of the window
        (None, None, Some(3), 0..=2),
        (Some(98), Some(500), Some(10), 98..=99),
    ];
    for sensor in &sensors {
        for (start, end, limit, expected) in &cases {
            let data = storage
                .query_sensor_data(&sensor.uuid.to_string(), start.map(at), end.map(at), *limit)
                .await?
                .expect("the series exists");
            let mut indexes: Vec<i64> = match &data.samples {
                TypedSamples::Float(s) => s.iter().map(|x| x.value as i64).collect(),
                TypedSamples::Integer(s) => s.iter().map(|x| x.value).collect(),
                TypedSamples::String(s) => {
                    s.iter().map(|x| x.value[1..].parse().unwrap()).collect()
                }
                other => panic!("unexpected {other:?}"),
            };
            // The order the backend returns is the order of the time
            let in_order = indexes.windows(2).all(|pair| pair[0] < pair[1]);
            indexes.sort();
            assert_eq!(
                indexes,
                expected.clone().collect::<Vec<_>>(),
                "{} window {start:?}..{end:?} limit {limit:?}",
                sensor.name
            );
            assert!(in_order, "{} must be ordered by time", sensor.name);
        }
    }
    Ok(())
}

/// A sensor is identified by its UUID, and its labels are written once, when the sensor is
/// created. Publishing the same UUID again with other labels adds the samples and leaves the
/// labels alone, the same way on every backend.
///
/// The UUID that SensApp derives from a name, a type, a unit and labels covers the labels: a
/// Prometheus or InfluxDB series with a changed label is another sensor. Only a client that
/// chooses its UUIDs (an Arrow stream, SenML) can reach this case.
#[tokio::test]
#[serial]
async fn labels_are_written_when_the_sensor_is_created() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();

    let uuid = Uuid::new_v4();
    let name = format!("labels_are_written_once_{uuid}");
    let publish = |room: &'static str, value: f64| {
        let storage = storage.clone();
        let name = name.clone();
        async move {
            let labels: SensAppLabels = [("room".to_string(), room.to_string())]
                .into_iter()
                .collect();
            let sensor = Arc::new(Sensor::new(
                uuid,
                name,
                SensorType::Float,
                None,
                Some(labels),
            ));
            let samples = TypedSamples::Float(
                vec![Sample {
                    datetime: hifitime::Epoch::from_unix_seconds(YEAR_2000 + value),
                    value,
                }]
                .into(),
            );
            let mut batch_builder = BatchBuilder::new()?;
            batch_builder.add(sensor, samples).await?;
            batch_builder.send_what_is_left(storage).await?;
            anyhow::Ok(())
        }
    };
    publish("kitchen", 1.0).await?;
    publish("garage", 2.0).await?;

    let data = storage
        .query_sensor_data(&uuid.to_string(), None, None, None)
        .await?
        .expect("the series should exist");
    assert_eq!(
        data.sensor.labels.to_vec(),
        vec![("room".to_string(), "kitchen".to_string())],
        "the labels of the first publish stay"
    );
    let TypedSamples::Float(samples) = &data.samples else {
        panic!("expected float samples");
    };
    assert_eq!(samples.len(), 2, "both publishes add their sample");
    Ok(())
}
