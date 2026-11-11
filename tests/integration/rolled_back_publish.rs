//! A publish that does not finish (the write timeout or a client disconnect drops its future, or a
//! later statement fails) must leave nothing behind that a later publish can trip over. In
//! particular, an id given to a new sensor or a new string inside a transaction that rolls back
//! must not be reused for that sensor or string afterwards: the database gives the same id to the
//! next new row, and the samples would be filed under the wrong series or read back as another
//! string.
//!
//! The tests cut a publish at a few points in time, because where it stops matters and cannot
//! be chosen from the outside. Whatever the point, every series must then read back its own
//! samples. These tests run on the backend selected by `TEST_DATABASE_URL`.

use crate::common::{DatabaseType, TestDb};
use anyhow::Result;
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch::{Batch, SingleSensorBatch};
use sensapp::datamodel::sensapp_vec::{SensAppLabels, SensAppVec};
use sensapp::datamodel::unit::Unit;
use sensapp::datamodel::{Sample, Sensor, SensorType, TypedSamples};
use sensapp::storage::{LabelMatcher, StorageInstance};
use serial_test::serial;
use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

static INIT: std::sync::Once = std::sync::Once::new();

fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

/// Series per batch: a few, so that a cut of a millisecond lands in the middle of the
/// registrations on SQLite.
const SERIES: usize = 20;

/// The publish is cut after these many milliseconds: where it stops cannot be chosen from the
/// outside, so a few points are tried.
const CUTS_MS: [u64; 3] = [1, 2, 5];

/// 2024-01-01T00:00:00Z
const START: f64 = 1_704_067_200.0;

/// RRDcached keeps one file per UUID and has no ids to reuse, nor strings.
fn applies_to(test_db: &TestDb) -> bool {
    test_db.db_type != DatabaseType::RRDcached
}

/// A new series, with a `round` label shared by the series of one round so that they are read
/// back with one query.
fn new_sensor(name: String, round: &str, sensor_type: SensorType) -> Result<Arc<Sensor>> {
    let labels: SensAppLabels = [
        ("round".to_string(), round.to_string()),
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

fn batch<T: Clone>(
    series: &[(Arc<Sensor>, T)],
    typed: fn(Vec<Sample<T>>) -> TypedSamples,
) -> Arc<Batch> {
    let mut sensors = SensAppVec::new();
    for (sensor, value) in series {
        sensors.push(SingleSensorBatch::new(
            sensor.clone(),
            typed(vec![Sample {
                datetime: hifitime::Epoch::from_unix_seconds(START),
                value: value.clone(),
            }]),
        ));
    }
    Arc::new(Batch::new(sensors))
}

fn floats(samples: Vec<Sample<f64>>) -> TypedSamples {
    TypedSamples::Float(samples.into())
}

fn strings(samples: Vec<Sample<String>>) -> TypedSamples {
    TypedSamples::String(samples.into())
}

/// Start a publish and drop it after `cut`, as the write timeout does.
async fn cut_publish(storage: &Arc<dyn StorageInstance>, batch: Arc<Batch>, cut: Duration) {
    // Whether it finished in time or not does not matter here: an error is a failed publish too.
    let _ = tokio::time::timeout(cut, storage.publish(batch)).await;
}

/// The series of a round, read with one selector (a bulk read on every backend): each of the
/// `expected` series must come back with only its own value. `value_of` gives the values of a
/// series as text.
async fn assert_round(
    storage: &Arc<dyn StorageInstance>,
    round: &str,
    expected: HashMap<Uuid, String>,
    value_of: fn(&TypedSamples) -> Vec<String>,
) -> Result<()> {
    let read = storage
        .query_selector(
            &[LabelMatcher::eq("round", round)],
            None,
            None,
            false,
            10 * SERIES,
            100 * SERIES,
        )
        .await?
        .map_err(|exceeded| anyhow::anyhow!("{exceeded:?}"))?;
    let mut found: HashMap<Uuid, Vec<String>> = HashMap::new();
    for data in &read {
        found
            .entry(data.sensor.uuid)
            .or_default()
            .extend(value_of(&data.samples));
    }
    for (uuid, value) in &expected {
        let values = found
            .get(uuid)
            .unwrap_or_else(|| panic!("series {uuid} is missing (round {round})"));
        assert!(!values.is_empty(), "series {uuid} has no samples");
        for found_value in values {
            assert_eq!(
                found_value, value,
                "series {uuid} reads back the sample of another series (round {round})"
            );
        }
    }
    Ok(())
}

fn float_values(samples: &TypedSamples) -> Vec<String> {
    match samples {
        TypedSamples::Float(samples) => samples.iter().map(|s| s.value.to_string()).collect(),
        other => panic!("expected float samples, got {other:?}"),
    }
}

fn string_values(samples: &TypedSamples) -> Vec<String> {
    match samples {
        TypedSamples::String(samples) => samples.iter().map(|s| s.value.clone()).collect(),
        other => panic!("expected string samples, got {other:?}"),
    }
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
        let round = format!("series_{cut_ms}_{run}");
        // Distinct values, so that a series that reads back another one's sample is seen
        let series = |kind: &str, offset: u64| -> Result<Vec<(Arc<Sensor>, f64)>> {
            (0..SERIES)
                .map(|index| {
                    let name = format!("rolled_back_{kind}_{index}_{round}");
                    let value = (offset + index as u64) as f64;
                    Ok((new_sensor(name, &round, SensorType::Float)?, value))
                })
                .collect()
        };
        let first = series("first", 0)?;
        let others = series("other", 1_000)?;

        cut_publish(
            &storage,
            batch(&first, floats),
            Duration::from_millis(cut_ms),
        )
        .await;
        let retry: Vec<_> = first.iter().chain(others.iter()).cloned().collect();
        storage.publish(batch(&retry, floats)).await?;

        let expected = retry
            .iter()
            .map(|(sensor, value)| (sensor.uuid, value.to_string()))
            .collect();
        assert_round(&storage, &round, expected, float_values).await?;
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
        let round = format!("strings_{cut_ms}_{run}");
        // New series every time, so that only the string ids can go wrong
        let series = |kind: &str, text: &str| -> Result<Vec<(Arc<Sensor>, String)>> {
            (0..SERIES)
                .map(|index| {
                    let name = format!("rolled_back_{kind}_{index}_{round}");
                    let sensor = new_sensor(name, &round, SensorType::String)?;
                    Ok((sensor, format!("{text}_{index}_{round}")))
                })
                .collect()
        };

        let cut = Duration::from_millis(cut_ms);
        cut_publish(&storage, batch(&series("first", "foo")?, strings), cut).await;
        let others = series("other", "bar")?;
        storage.publish(batch(&others, strings)).await?;
        let again = series("again", "foo")?;
        storage.publish(batch(&again, strings)).await?;

        let expected = others
            .iter()
            .chain(again.iter())
            .map(|(sensor, value)| (sensor.uuid, value.clone()))
            .collect();
        assert_round(&storage, &round, expected, string_values).await?;
    }
    Ok(())
}
