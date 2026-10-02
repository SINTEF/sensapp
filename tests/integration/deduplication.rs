//! Duplicate samples (a retried write, a client that sends twice, a crash in the middle of a
//! request) are removed by the vacuum operation. Only exact duplicates go: same series, same
//! timestamp, same value. These tests run on the backend selected by `TEST_DATABASE_URL`.

use crate::common::TestDb;
use anyhow::Result;
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch_builder::BatchBuilder;
use sensapp::datamodel::{Sample, SensAppDateTime, Sensor, SensorType, TypedSamples};
use sensapp::storage::{StorageError, StorageInstance};
use serial_test::serial;
use std::sync::Arc;
use uuid::Uuid;

static INIT: std::sync::Once = std::sync::Once::new();

fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

fn at(index: usize) -> SensAppDateTime {
    hifitime::Epoch::from_unix_seconds(1_704_067_200.0 + index as f64)
}

fn samples_of<T>(values: Vec<T>) -> Vec<Sample<T>> {
    values
        .into_iter()
        .enumerate()
        .map(|(index, value)| Sample {
            datetime: at(index),
            value,
        })
        .collect()
}

/// Three samples of every type.
fn samples(sensor_type: SensorType) -> TypedSamples {
    match sensor_type {
        SensorType::Integer => TypedSamples::Integer(samples_of(vec![1, 2, 3]).into()),
        SensorType::Numeric => TypedSamples::Numeric(
            samples_of(vec![
                rust_decimal::Decimal::new(101, 2),
                rust_decimal::Decimal::new(202, 2),
                rust_decimal::Decimal::new(303, 2),
            ])
            .into(),
        ),
        SensorType::Float => TypedSamples::Float(samples_of(vec![1.5, 2.5, 3.5]).into()),
        SensorType::String => {
            TypedSamples::String(samples_of(vec!["a".to_string(), "b".into(), "é".into()]).into())
        }
        SensorType::Boolean => TypedSamples::Boolean(samples_of(vec![true, false, true]).into()),
        SensorType::Location => TypedSamples::Location(
            samples_of(vec![
                geo::Point::new(1.0, 2.0),
                geo::Point::new(3.0, 4.0),
                geo::Point::new(5.0, 6.0),
            ])
            .into(),
        ),
        SensorType::Json => TypedSamples::Json(
            samples_of(vec![
                serde_json::json!({"a": 1}),
                serde_json::json!([1, 2]),
                serde_json::json!("three"),
            ])
            .into(),
        ),
        SensorType::Blob => {
            TypedSamples::Blob(samples_of(vec![vec![1u8], vec![2, 3], vec![4, 5, 6]]).into())
        }
    }
}

const ALL_TYPES: [SensorType; 8] = [
    SensorType::Integer,
    SensorType::Numeric,
    SensorType::Float,
    SensorType::String,
    SensorType::Boolean,
    SensorType::Location,
    SensorType::Json,
    SensorType::Blob,
];

async fn publish(
    storage: &Arc<dyn StorageInstance>,
    sensor: &Arc<Sensor>,
    samples: TypedSamples,
) -> Result<()> {
    let mut batch_builder = BatchBuilder::new()?;
    batch_builder.add(sensor.clone(), samples).await?;
    batch_builder.send_what_is_left(storage.clone()).await?;
    Ok(())
}

async fn count(storage: &Arc<dyn StorageInstance>, sensor: &Sensor) -> Result<usize> {
    Ok(storage
        .query_sensor_data(&sensor.uuid.to_string(), None, None, None)
        .await?
        .map_or(0, |data| data.samples.len()))
}

fn sensor(name: &str, sensor_type: SensorType, run: Uuid) -> Result<Arc<Sensor>> {
    Ok(Arc::new(Sensor::new_without_uuid(
        format!("{name}_{run}"),
        sensor_type,
        None,
        None,
    )?))
}

/// A backend that cannot deduplicate says so; the tests then have nothing to check.
async fn deduplicate(storage: &Arc<dyn StorageInstance>) -> Result<Option<u64>> {
    match storage.deduplicate_samples().await {
        Ok(removed) => Ok(Some(removed)),
        Err(error)
            if matches!(
                error.downcast_ref::<StorageError>(),
                Some(StorageError::Unsupported(_))
            ) =>
        {
            Ok(None)
        }
        Err(error) => Err(error),
    }
}

#[tokio::test]
#[serial]
async fn duplicates_of_every_type_are_removed_and_counted() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let run = Uuid::new_v4();

    let mut sensors = Vec::new();
    for sensor_type in ALL_TYPES {
        let sensor = sensor(&format!("dedup_{sensor_type}"), sensor_type, run)?;
        // The same three samples written three times: 6 duplicates
        for _ in 0..3 {
            publish(&storage, &sensor, samples(sensor_type)).await?;
        }
        assert_eq!(count(&storage, &sensor).await?, 9, "{sensor_type} before");
        sensors.push((sensor_type, sensor));
    }

    let Some(removed) = deduplicate(&storage).await? else {
        return Ok(());
    };
    // Other tests of the same database may have left duplicates of their own: at least ours
    assert!(removed >= 6 * ALL_TYPES.len() as u64, "removed {removed}");
    for (sensor_type, sensor) in &sensors {
        assert_eq!(count(&storage, sensor).await?, 3, "{sensor_type} after");
    }

    // Nothing is left to remove, and nothing changes the second time
    assert_eq!(deduplicate(&storage).await?, Some(0));
    for (sensor_type, sensor) in &sensors {
        assert_eq!(count(&storage, sensor).await?, 3, "{sensor_type} again");
    }
    Ok(())
}

#[tokio::test]
#[serial]
async fn only_exact_duplicates_are_removed() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let run = Uuid::new_v4();

    // Two different values at the same timestamp stay, and so does the same value at two
    // timestamps; the same value of another series is not a duplicate
    let first = sensor("exact_first", SensorType::Float, run)?;
    let second = sensor("exact_second", SensorType::Float, run)?;
    let same_time = |value: f64| Sample {
        datetime: at(0),
        value,
    };
    let float = |samples: Vec<Sample<f64>>| TypedSamples::Float(samples.into());
    publish(
        &storage,
        &first,
        float(vec![
            same_time(1.0),
            same_time(2.0),
            same_time(1.0), // duplicate of the first
            Sample {
                datetime: at(1),
                value: 1.0, // same value, other timestamp
            },
        ]),
    )
    .await?;
    publish(&storage, &second, float(vec![same_time(1.0)])).await?;

    let Some(removed) = deduplicate(&storage).await? else {
        return Ok(());
    };
    assert!(removed >= 1);
    assert_eq!(count(&storage, &first).await?, 3);
    assert_eq!(count(&storage, &second).await?, 1);

    let data = storage
        .query_sensor_data(&first.uuid.to_string(), None, None, None)
        .await?
        .expect("series");
    let TypedSamples::Float(stored) = &data.samples else {
        panic!("float samples");
    };
    let mut values: Vec<(i64, i64)> = stored
        .iter()
        .map(|sample| {
            (
                (sample.datetime.to_unix_seconds() * 1e6).round() as i64,
                sample.value as i64,
            )
        })
        .collect();
    values.sort();
    let start = (at(0).to_unix_seconds() * 1e6).round() as i64;
    assert_eq!(values, vec![(start, 1), (start, 2), (start + 1_000_000, 1)]);
    Ok(())
}

#[tokio::test]
#[serial]
async fn deduplicating_a_database_without_duplicates_changes_nothing() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let run = Uuid::new_v4();

    let sensor = sensor("clean", SensorType::Integer, run)?;
    publish(&storage, &sensor, samples(SensorType::Integer)).await?;
    let Some(_) = deduplicate(&storage).await? else {
        return Ok(());
    };
    assert_eq!(count(&storage, &sensor).await?, 3);
    assert_eq!(deduplicate(&storage).await?, Some(0));
    Ok(())
}
