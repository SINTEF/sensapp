//! A publish that does not finish (the write timeout or a client disconnect drops its future, or a
//! later statement fails) must leave nothing behind that a later publish can trip over. In
//! particular, an id given to a new sensor or a new string inside a transaction that rolls back
//! must not be reused for that sensor or string afterwards: the database gives the same id to the
//! next new row, and the samples would be filed under the wrong series or read back as another
//! string.
//!
//! The tests cut a publish at many points in time, because where it stops matters and cannot be
//! chosen from the outside. Whatever the point, every series must then read back its own samples.
//! These tests run on the backend selected by `TEST_DATABASE_URL`.

use crate::common::{DatabaseType, TestDb};
use anyhow::Result;
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch::{Batch, SingleSensorBatch};
use sensapp::datamodel::sensapp_vec::{SensAppLabels, SensAppVec};
use sensapp::datamodel::unit::Unit;
use sensapp::datamodel::{Sample, Sensor, SensorType, TypedSamples};
use sensapp::storage::StorageInstance;
use serial_test::serial;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

static INIT: std::sync::Once = std::sync::Once::new();

fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

/// Series per batch: enough that a cut often lands in the middle of the registrations.
const SERIES: usize = 100;

/// The publish is cut after 1, 2, ... ms.
const CUTS_MS: std::ops::RangeInclusive<u64> = 1..=25;

/// 2024-01-01T00:00:00Z
const START: f64 = 1_704_067_200.0;

/// RRDcached keeps one file per UUID and has no ids to reuse, nor strings.
fn applies_to(test_db: &TestDb) -> bool {
    test_db.db_type != DatabaseType::RRDcached
}

fn new_sensor(name: String, sensor_type: SensorType) -> Result<Arc<Sensor>> {
    let labels: SensAppLabels = [
        ("run".to_string(), name.clone()),
        ("room".to_string(), "lab".to_string()),
    ]
    .into_iter()
    .collect();
    let unit = Unit::new(format!("unit_{name}"), None);
    Ok(Arc::new(Sensor::new_without_uuid(
        name,
        sensor_type,
        Some(unit),
        Some(labels),
    )?))
}

fn float_batch(series: &[(Arc<Sensor>, f64)]) -> Arc<Batch> {
    let mut sensors = SensAppVec::new();
    for (sensor, value) in series {
        sensors.push(SingleSensorBatch::new(
            sensor.clone(),
            TypedSamples::Float(
                vec![Sample {
                    datetime: hifitime::Epoch::from_unix_seconds(START),
                    value: *value,
                }]
                .into(),
            ),
        ));
    }
    Arc::new(Batch::new(sensors))
}

fn string_batch(series: &[(Arc<Sensor>, String)]) -> Arc<Batch> {
    let mut sensors = SensAppVec::new();
    for (sensor, value) in series {
        sensors.push(SingleSensorBatch::new(
            sensor.clone(),
            TypedSamples::String(
                vec![Sample {
                    datetime: hifitime::Epoch::from_unix_seconds(START),
                    value: value.clone(),
                }]
                .into(),
            ),
        ));
    }
    Arc::new(Batch::new(sensors))
}

/// Start a publish and drop it after `cut`, as the write timeout does.
async fn cut_publish(storage: &Arc<dyn StorageInstance>, batch: Arc<Batch>, cut: Duration) {
    // Whether it finished in time or not does not matter here: an error is a failed publish too.
    let _ = tokio::time::timeout(cut, storage.publish(batch)).await;
}

async fn assert_float_series(
    storage: &Arc<dyn StorageInstance>,
    series: &[(Arc<Sensor>, f64)],
    cut: Duration,
) -> Result<()> {
    for (sensor, value) in series {
        let data = storage
            .query_sensor_data(&sensor.uuid.to_string(), None, None, None)
            .await?
            .unwrap_or_else(|| panic!("{} is missing (cut after {cut:?})", sensor.name));
        let TypedSamples::Float(samples) = &data.samples else {
            panic!("{} has {:?} samples", sensor.name, data.samples);
        };
        assert!(!samples.is_empty(), "{} has no samples", sensor.name);
        for sample in samples.iter() {
            assert_eq!(
                sample.value, *value,
                "{} reads back the samples of another series (cut after {cut:?})",
                sensor.name
            );
        }
    }
    Ok(())
}

async fn assert_string_series(
    storage: &Arc<dyn StorageInstance>,
    series: &[(Arc<Sensor>, String)],
    cut: Duration,
) -> Result<()> {
    for (sensor, value) in series {
        let data = storage
            .query_sensor_data(&sensor.uuid.to_string(), None, None, None)
            .await?
            .unwrap_or_else(|| panic!("{} is missing (cut after {cut:?})", sensor.name));
        let TypedSamples::String(samples) = &data.samples else {
            panic!("{} has {:?} samples", sensor.name, data.samples);
        };
        assert!(!samples.is_empty(), "{} has no samples", sensor.name);
        for sample in samples.iter() {
            assert_eq!(
                &sample.value, value,
                "{} reads back another string (cut after {cut:?})",
                sensor.name
            );
        }
    }
    Ok(())
}

/// A batch of new series is cut, then sent again with other new series. Each series must read
/// back its own samples: none may be missing, and none may have the samples of another.
#[tokio::test]
#[serial]
async fn series_of_a_cut_publish_are_registered_again_under_their_own_id() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    if !applies_to(&test_db) {
        return Ok(());
    }
    let storage = test_db.storage();
    let run = Uuid::new_v4();

    for cut_ms in CUTS_MS {
        let cut = Duration::from_millis(cut_ms);
        let named = |kind: &str, index: usize| format!("rolled_back_{kind}_{cut_ms}_{index}_{run}");
        // Distinct values, so that a series that reads back another one's sample is seen
        let first: Vec<_> = (0..SERIES)
            .map(|index| {
                let value = (cut_ms * 10_000 + index as u64) as f64;
                Ok((new_sensor(named("first", index), SensorType::Float)?, value))
            })
            .collect::<Result<_>>()?;
        let others: Vec<_> = (0..SERIES)
            .map(|index| {
                let value = (cut_ms * 10_000 + 5_000 + index as u64) as f64;
                Ok((new_sensor(named("other", index), SensorType::Float)?, value))
            })
            .collect::<Result<_>>()?;

        cut_publish(&storage, float_batch(&first), cut).await;

        let retry: Vec<_> = first.iter().chain(others.iter()).cloned().collect();
        storage.publish(float_batch(&retry)).await?;

        assert_float_series(&storage, &retry, cut).await?;
    }
    Ok(())
}

/// New strings are written by a batch that is cut, then other new strings, then the first strings
/// again. Each series must read back its own string.
#[tokio::test]
#[serial]
async fn strings_of_a_cut_publish_are_stored_again_under_their_own_id() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    if !applies_to(&test_db) {
        return Ok(());
    }
    let storage = test_db.storage();
    let run = Uuid::new_v4();

    for cut_ms in CUTS_MS {
        let cut = Duration::from_millis(cut_ms);
        let named = |kind: &str, index: usize| format!("rolled_back_{kind}_{cut_ms}_{index}_{run}");
        let strings = |kind: &str| -> Result<Vec<(Arc<Sensor>, String)>> {
            (0..SERIES)
                .map(|index| {
                    let sensor = new_sensor(named(kind, index), SensorType::String)?;
                    let text = match kind {
                        "other" => format!("bar_{cut_ms}_{index}_{run}"),
                        _ => format!("foo_{cut_ms}_{index}_{run}"),
                    };
                    Ok((sensor, text))
                })
                .collect()
        };

        // New series every time, so that only the string ids can go wrong
        cut_publish(&storage, string_batch(&strings("first")?), cut).await;

        let others = strings("other")?;
        storage.publish(string_batch(&others)).await?;
        let again = strings("again")?;
        storage.publish(string_batch(&again)).await?;

        assert_string_series(&storage, &others, cut).await?;
        assert_string_series(&storage, &again, cut).await?;
    }
    Ok(())
}
