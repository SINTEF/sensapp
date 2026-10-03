//! Duplicate samples (a retried write, a client that sends twice, a crash in the middle of a
//! request) are removed by the vacuum operation, or not written at all when the deduplication at
//! ingestion is on. Only exact duplicates go: same series, same timestamp, same value. These tests
//! run on the backend selected by `TEST_DATABASE_URL`.

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

/// Switches the deduplication at ingestion on. A backend that cannot do it says so; the tests then
/// have nothing to check.
async fn deduplicate_on_ingest(storage: &Arc<dyn StorageInstance>, enabled: bool) -> Result<bool> {
    match storage.set_deduplicate_on_ingest(enabled).await {
        Ok(()) => Ok(true),
        Err(error)
            if matches!(
                error.downcast_ref::<StorageError>(),
                Some(StorageError::Unsupported(_))
            ) =>
        {
            Ok(false)
        }
        Err(error) => Err(error),
    }
}

fn floats(samples: Vec<(usize, f64)>) -> TypedSamples {
    TypedSamples::Float(
        samples
            .into_iter()
            .map(|(index, value)| Sample {
                datetime: at(index),
                value,
            })
            .collect::<Vec<_>>()
            .into(),
    )
}

/// The `(second, value)` pairs of a float series, sorted.
async fn stored_floats(
    storage: &Arc<dyn StorageInstance>,
    sensor: &Sensor,
) -> Result<Vec<(usize, i64)>> {
    let data = storage
        .query_sensor_data(&sensor.uuid.to_string(), None, None, None)
        .await?
        .expect("series");
    let TypedSamples::Float(stored) = &data.samples else {
        panic!("float samples");
    };
    let start = at(0).to_unix_seconds().round();
    let mut values: Vec<(usize, i64)> = stored
        .iter()
        .map(|sample| {
            (
                (sample.datetime.to_unix_seconds().round() - start) as usize,
                (sample.value * 10.0).round() as i64,
            )
        })
        .collect();
    values.sort();
    Ok(values)
}

#[tokio::test]
#[serial]
async fn at_ingestion_the_same_samples_of_every_type_are_written_once() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    if !deduplicate_on_ingest(&storage, true).await? {
        return Ok(());
    }
    let run = Uuid::new_v4();

    for sensor_type in ALL_TYPES {
        let sensor = sensor(&format!("ingest_{sensor_type}"), sensor_type, run)?;
        // The same three samples written three times: stored once
        for _ in 0..3 {
            publish(&storage, &sensor, samples(sensor_type)).await?;
        }
        assert_eq!(count(&storage, &sensor).await?, 3, "{sensor_type}");
    }
    Ok(())
}

#[tokio::test]
#[serial]
async fn at_ingestion_repeats_inside_one_request_are_written_once() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    if !deduplicate_on_ingest(&storage, true).await? {
        return Ok(());
    }
    let run = Uuid::new_v4();

    let sensor = sensor("ingest_inside", SensorType::Float, run)?;
    publish(
        &storage,
        &sensor,
        floats(vec![(0, 1.0), (1, 2.0), (0, 1.0), (1, 2.0), (2, 3.0)]),
    )
    .await?;
    assert_eq!(
        stored_floats(&storage, &sensor).await?,
        vec![(0, 10), (1, 20), (2, 30)]
    );
    Ok(())
}

#[tokio::test]
#[serial]
async fn at_ingestion_only_exact_duplicates_are_dropped() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    if !deduplicate_on_ingest(&storage, true).await? {
        return Ok(());
    }
    let run = Uuid::new_v4();

    // Two values at one timestamp both stay, so does one value at two timestamps, and the same
    // sample of another series is not a duplicate. Written twice: nothing more is stored.
    let first = sensor("ingest_exact_first", SensorType::Float, run)?;
    let second = sensor("ingest_exact_second", SensorType::Float, run)?;
    for _ in 0..2 {
        publish(&storage, &first, floats(vec![(0, 1.0), (0, 2.0), (1, 1.0)])).await?;
        publish(&storage, &second, floats(vec![(0, 1.0)])).await?;
    }
    assert_eq!(
        stored_floats(&storage, &first).await?,
        vec![(0, 10), (0, 20), (1, 10)]
    );
    assert_eq!(stored_floats(&storage, &second).await?, vec![(0, 10)]);
    Ok(())
}

#[tokio::test]
#[serial]
async fn at_ingestion_a_partial_overlap_adds_only_what_is_new() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    if !deduplicate_on_ingest(&storage, true).await? {
        return Ok(());
    }
    let run = Uuid::new_v4();

    let sensor = sensor("ingest_overlap", SensorType::Float, run)?;
    publish(
        &storage,
        &sensor,
        floats(vec![(0, 1.0), (1, 2.0), (2, 3.0)]),
    )
    .await?;
    // 1 and 2 are known, 3 and 4 are new, and the value at 2 is a different one: kept
    publish(
        &storage,
        &sensor,
        floats(vec![(1, 2.0), (2, 3.0), (2, 9.0), (3, 4.0), (4, 5.0)]),
    )
    .await?;
    assert_eq!(
        stored_floats(&storage, &sensor).await?,
        vec![(0, 10), (1, 20), (2, 30), (2, 90), (3, 40), (4, 50)]
    );
    Ok(())
}

/// SensApp runs as several instances: a retry can reach another instance while the first request is
/// still being written. Writers of the same new samples at the same moment must store them once.
#[tokio::test]
#[serial]
async fn at_ingestion_concurrent_writers_of_the_same_samples_store_them_once() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    if !deduplicate_on_ingest(&storage, true).await? {
        return Ok(());
    }
    let run = Uuid::new_v4();

    // Several rounds: a race does not show every time. Half of the rounds use a series that is
    // registered already, the others a series that the writers register at the same time, except
    // on TimescaleDB where eight writers that register the same new series at the same moment can
    // deadlock, with or without deduplication (ideas/timescaledb-concurrent-first-write-deadlock.md).
    let register_first = |round: usize| {
        round.is_multiple_of(2) || test_db.db_type == crate::common::DatabaseType::TimescaleDB
    };
    for round in 0..6 {
        let sensor = sensor(&format!("ingest_race_{round}"), SensorType::Float, run)?;
        if register_first(round) {
            publish(&storage, &sensor, floats(vec![(1000, 0.0)])).await?;
        }
        let mut writers = Vec::new();
        for _ in 0..8 {
            let (storage, sensor) = (storage.clone(), sensor.clone());
            writers.push(tokio::spawn(async move {
                publish(
                    &storage,
                    &sensor,
                    floats((0..50).map(|index| (index, index as f64)).collect()),
                )
                .await
            }));
        }
        for writer in writers {
            writer.await??;
        }
        let expected = 50 + usize::from(register_first(round));
        assert_eq!(count(&storage, &sensor).await?, expected, "round {round}");
    }
    Ok(())
}

#[tokio::test]
#[serial]
async fn switching_the_deduplication_off_writes_duplicates_again() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    if !deduplicate_on_ingest(&storage, true).await? {
        return Ok(());
    }
    let run = Uuid::new_v4();

    let sensor = sensor("ingest_switch", SensorType::Float, run)?;
    publish(&storage, &sensor, floats(vec![(0, 1.0)])).await?;
    publish(&storage, &sensor, floats(vec![(0, 1.0)])).await?;
    assert_eq!(count(&storage, &sensor).await?, 1);
    deduplicate_on_ingest(&storage, false).await?;
    publish(&storage, &sensor, floats(vec![(0, 1.0)])).await?;
    assert_eq!(count(&storage, &sensor).await?, 2);
    Ok(())
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

/// TimescaleDB compresses the chunks older than a week with a background policy, and a chunk of
/// the test data is that old. Duplicates in compressed chunks are removed like the others, whether
/// the policy already ran or not. This test compresses the chunks itself so that it does not wait
/// for the policy.
#[cfg(feature = "timescaledb")]
#[tokio::test]
#[serial]
async fn duplicates_in_compressed_chunks_are_removed() -> Result<()> {
    use crate::common::DatabaseType;

    ensure_config();
    let test_db = TestDb::new().await?;
    if test_db.db_type != DatabaseType::TimescaleDB {
        return Ok(());
    }
    let storage = test_db.storage();
    let run = Uuid::new_v4();

    let mut sensors = Vec::new();
    for sensor_type in ALL_TYPES {
        let sensor = sensor(&format!("dedup_compressed_{sensor_type}"), sensor_type, run)?;
        // The same three samples written twice: 3 duplicates
        for _ in 0..2 {
            publish(&storage, &sensor, samples(sensor_type)).await?;
        }
        sensors.push((sensor_type, sensor));
    }
    // A series without duplicates, in the same chunks
    let clean = sensor("dedup_compressed_clean", SensorType::Float, run)?;
    publish(&storage, &clean, samples(SensorType::Float)).await?;

    let pool = sqlx::PgPool::connect(&test_db.connection_string.replacen(
        "timescaledb://",
        "postgres://",
        1,
    ))
    .await?;
    for table in sensapp::storage::common::VALUE_TABLES {
        let compressed: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
            "SELECT count(compress_chunk(chunk, if_not_compressed => true)) \
             FROM show_chunks('{table}') chunk"
        )))
        .fetch_one(&pool)
        .await?;
        assert!(compressed > 0, "{table} has a chunk to compress");
    }

    let removed = deduplicate(&storage)
        .await?
        .expect("TimescaleDB deduplicates");
    assert!(removed >= 3 * ALL_TYPES.len() as u64, "removed {removed}");
    for (sensor_type, sensor) in &sensors {
        assert_eq!(count(&storage, sensor).await?, 3, "{sensor_type} after");
    }
    assert_eq!(
        count(&storage, &clean).await?,
        3,
        "the clean series is untouched"
    );

    // The chunks are compressed again, as they were found
    for table in sensapp::storage::common::VALUE_TABLES {
        let decompressed: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM timescaledb_information.chunks \
             WHERE hypertable_name = $1 AND NOT is_compressed",
        )
        .bind(table)
        .fetch_one(&pool)
        .await?;
        assert_eq!(decompressed, 0, "{table}: every chunk is compressed again");
    }

    // Nothing is left to remove
    assert_eq!(deduplicate(&storage).await?, Some(0));
    Ok(())
}
