//! A write that names an existing sensor UUID must give the stored type: samples of another type
//! would go to a table no read of the series looks at. The name, the unit and the labels are not
//! compared: the write appends and the stored ones stay (who may write to which series is decided by
//! the allow list of the token, see `jwt_auth`). These tests run on the backend selected by
//! `TEST_DATABASE_URL`.

use crate::common::{DatabaseType, TestDb};
use anyhow::Result;
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch::{Batch, SingleSensorBatch};
use sensapp::datamodel::sensapp_vec::{SensAppLabels, SensAppVec};
use sensapp::datamodel::unit::Unit;
use sensapp::datamodel::{Sample, Sensor, SensorType, TypedSamples};
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

/// RRDcached stores no name nor type, only a file per UUID.
fn applies_to(test_db: &TestDb) -> bool {
    test_db.db_type != DatabaseType::RRDcached
}

/// 2024-01-01T00:00:00Z plus `minutes`
fn at(minutes: i64) -> hifitime::Epoch {
    hifitime::Epoch::from_unix_seconds(1_704_067_200.0 + minutes as f64 * 60.0)
}

fn samples_for(sensor_type: SensorType, minutes: i64) -> TypedSamples {
    let datetime = at(minutes);
    match sensor_type {
        SensorType::Integer => TypedSamples::Integer(vec![Sample { datetime, value: 7 }].into()),
        _ => TypedSamples::Float(
            vec![Sample {
                datetime,
                value: 7.0,
            }]
            .into(),
        ),
    }
}

async fn publish(storage: &Arc<dyn StorageInstance>, sensors: &[(Arc<Sensor>, i64)]) -> Result<()> {
    let mut batch = SensAppVec::new();
    for (sensor, minutes) in sensors {
        batch.push(SingleSensorBatch::new(
            sensor.clone(),
            samples_for(sensor.sensor_type, *minutes),
        ));
    }
    storage.publish(Arc::new(Batch::new(batch))).await
}

fn assert_identity_mismatch(result: Result<()>, what: &str) {
    let error = result.expect_err(what);
    assert!(
        matches!(
            error.downcast_ref::<StorageError>(),
            Some(StorageError::SensorIdentityMismatch { .. })
        ),
        "{what}: expected an identity mismatch, got {error:#}"
    );
}

/// The stored series keeps its name, its type and its single sample.
async fn assert_unchanged(storage: &Arc<dyn StorageInstance>, stored: &Sensor) -> Result<()> {
    let data = storage
        .query_sensor_data(&stored.uuid.to_string(), None, None, None)
        .await?
        .expect("the stored series should exist");
    assert_eq!(data.sensor.name, stored.name);
    assert_eq!(data.sensor.sensor_type, stored.sensor_type);
    assert_eq!(data.samples.len(), 1, "no sample may be added");
    Ok(())
}

fn stored_sensor(prefix: &str) -> Result<Arc<Sensor>> {
    let labels: SensAppLabels = [("room".to_string(), "lab".to_string())]
        .into_iter()
        .collect();
    Ok(Arc::new(Sensor::new_without_uuid(
        format!("{prefix}_{}", Uuid::new_v4()),
        SensorType::Float,
        Some(Unit::new("Cel".to_string(), None)),
        Some(labels),
    )?))
}

#[tokio::test]
#[serial]
async fn a_write_under_an_existing_uuid_with_another_name_appends_and_the_stored_name_stays()
-> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    if !applies_to(&test_db) {
        return Ok(());
    }
    let storage = test_db.storage();
    let stored = stored_sensor("identity_name")?;
    publish(&storage, &[(stored.clone(), 0)]).await?;

    let renamed = Arc::new(Sensor::new(
        stored.uuid,
        format!("another_name_{}", Uuid::new_v4()),
        SensorType::Float,
        None,
        None,
    ));
    publish(&storage, &[(renamed, 1)]).await?;

    let data = storage
        .query_sensor_data(&stored.uuid.to_string(), None, None, None)
        .await?
        .expect("the series exists");
    assert_eq!(data.sensor.name, stored.name, "the stored name stays");
    assert_eq!(data.samples.len(), 2);
    Ok(())
}

#[tokio::test]
#[serial]
async fn a_write_under_an_existing_uuid_with_another_type_is_refused() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    if !applies_to(&test_db) {
        return Ok(());
    }
    let storage = test_db.storage();
    let stored = stored_sensor("identity_type")?;
    publish(&storage, &[(stored.clone(), 0)]).await?;

    let retyped = Arc::new(Sensor::new(
        stored.uuid,
        stored.name.clone(),
        SensorType::Integer,
        None,
        None,
    ));
    assert_identity_mismatch(
        publish(&storage, &[(retyped, 1)]).await,
        "another type under the same UUID",
    );
    assert_unchanged(&storage, &stored).await
}

/// Two series of one batch with the same new UUID and different types: nothing is stored. With
/// different names and one type the series is created under the first name.
#[tokio::test]
#[serial]
async fn one_batch_cannot_give_two_identities_to_a_new_uuid() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    if !applies_to(&test_db) {
        return Ok(());
    }
    let storage = test_db.storage();
    let uuid = Uuid::new_v4();
    let first = Arc::new(Sensor::new(
        uuid,
        format!("identity_first_{uuid}"),
        SensorType::Float,
        None,
        None,
    ));
    let second = Arc::new(Sensor::new(
        uuid,
        format!("identity_second_{uuid}"),
        SensorType::Integer,
        None,
        None,
    ));
    assert_identity_mismatch(
        publish(&storage, &[(first, 0), (second, 1)]).await,
        "two identities for one UUID in a batch",
    );
    assert!(
        storage
            .query_sensor_data(&uuid.to_string(), None, None, None)
            .await?
            .is_none(),
        "the series must not be created"
    );
    Ok(())
}

/// Two names for one new UUID in a batch, one type: the series is created, under the first name.
#[tokio::test]
#[serial]
async fn one_batch_with_two_names_for_a_new_uuid_creates_it_under_the_first() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    if !applies_to(&test_db) {
        return Ok(());
    }
    let storage = test_db.storage();
    let uuid = Uuid::new_v4();
    let sensor = |name: &str| {
        Arc::new(Sensor::new(
            uuid,
            format!("{name}_{uuid}"),
            SensorType::Float,
            None,
            None,
        ))
    };
    publish(&storage, &[(sensor("first"), 0), (sensor("second"), 1)]).await?;
    let data = storage
        .query_sensor_data(&uuid.to_string(), None, None, None)
        .await?
        .expect("the series exists");
    assert_eq!(data.sensor.name, format!("first_{uuid}"));
    assert_eq!(data.samples.len(), 2);
    Ok(())
}

/// The same name and type with another unit and no labels, as an import that leaves them out
/// sends: the samples are appended and the stored unit and labels stay.
#[tokio::test]
#[serial]
async fn a_write_with_the_same_name_and_type_appends_whatever_the_unit_and_labels() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    if !applies_to(&test_db) {
        return Ok(());
    }
    let storage = test_db.storage();
    let stored = stored_sensor("identity_same")?;
    publish(&storage, &[(stored.clone(), 0)]).await?;

    let bare = Arc::new(Sensor::new(
        stored.uuid,
        stored.name.clone(),
        SensorType::Float,
        Some(Unit::new("K".to_string(), None)),
        None,
    ));
    publish(&storage, &[(bare, 1)]).await?;

    let data = storage
        .query_sensor_data(&stored.uuid.to_string(), None, None, None)
        .await?
        .expect("the series should exist");
    assert_eq!(data.samples.len(), 2);
    assert_eq!(
        data.sensor.unit.map(|unit| unit.name).as_deref(),
        Some("Cel")
    );
    assert_eq!(
        data.sensor.labels.to_vec(),
        vec![("room".to_string(), "lab".to_string())]
    );
    Ok(())
}

/// A write that names no series, only its UUID (the importers then use the UUID as the name, as
/// SenML without `_name` or Arrow with only `sensor_id` do), appends to the stored series whatever
/// its name. The type must still be the stored one.
#[tokio::test]
#[serial]
async fn a_write_with_only_the_uuid_appends_to_the_stored_series() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    if !applies_to(&test_db) {
        return Ok(());
    }
    let storage = test_db.storage();
    let stored = stored_sensor("identity_unnamed")?;
    publish(&storage, &[(stored.clone(), 0)]).await?;

    let unnamed = |sensor_type| {
        Arc::new(Sensor::new(
            stored.uuid,
            stored.uuid.to_string(),
            sensor_type,
            None,
            None,
        ))
    };
    assert_identity_mismatch(
        publish(&storage, &[(unnamed(SensorType::Integer), 1)]).await,
        "only the UUID, with another type",
    );
    publish(&storage, &[(unnamed(SensorType::Float), 1)]).await?;

    let data = storage
        .query_sensor_data(&stored.uuid.to_string(), None, None, None)
        .await?
        .expect("the series should exist");
    assert_eq!(data.sensor.name, stored.name, "the stored name stays");
    assert_eq!(data.samples.len(), 2);
    Ok(())
}

/// SenML and Arrow writers choose their UUIDs, often with a structure: two numbers in the two
/// halves, or the same number twice. ClickHouse and BigQuery key a series by 64 bits of its UUID;
/// with the XOR of the halves that was, `(a, b)` and `(b, a)` had the same key and every UUID with
/// equal halves the key `0`, and the second one was refused. Every backend stores them all, against
/// a stored sensor and within one batch.
#[tokio::test]
#[serial]
async fn structured_uuids_are_separate_series() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    if !applies_to(&test_db) {
        return Ok(());
    }
    let storage = test_db.storage();
    let (a, b) = (fastrand::u64(..), fastrand::u64(..));
    // The same name, so that only the UUID tells the sensors apart
    let sensor = |high: u64, low: u64| {
        Arc::new(Sensor::new(
            Uuid::from_u64_pair(high, low),
            "structured".to_string(),
            SensorType::Float,
            None,
            None,
        ))
    };
    let stored = sensor(a, b);
    publish(&storage, &[(stored.clone(), 0)]).await?;
    // Against a stored sensor
    let swapped = sensor(b, a);
    publish(&storage, &[(swapped.clone(), 1)]).await?;
    // Within one batch
    let (same, other_same) = (sensor(a, a), sensor(b, b));
    publish(&storage, &[(same.clone(), 2), (other_same.clone(), 3)]).await?;

    for (sensor, minutes) in [(stored, 0), (swapped, 1), (same, 2), (other_same, 3)] {
        let data = storage
            .query_sensor_data(&sensor.uuid.to_string(), None, None, None)
            .await?
            .unwrap_or_else(|| panic!("{} should be stored", sensor.uuid));
        assert_eq!(data.sensor.uuid, sensor.uuid);
        let TypedSamples::Float(samples) = data.samples else {
            panic!("a float series");
        };
        let times: Vec<_> = samples.iter().map(|sample| sample.datetime).collect();
        assert_eq!(times, [at(minutes)], "{} reads its own sample", sensor.uuid);
    }
    Ok(())
}

/// Two writers that register one new UUID at the same time, with two types: exactly one of them
/// creates the series and the other is refused, whoever is first. Registration is where two
/// instances meet, so the writers use one storage each way: a pool of one connection would
/// serialise them and prove nothing (the suite runs with the default pool).
#[tokio::test]
#[serial]
async fn two_writers_registering_one_uuid_with_two_types_cannot_both_succeed() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    // ClickHouse and BigQuery look the sensor up and then insert it, with nothing in between:
    // both writers can win (measured on ClickHouse, round 0). See
    // `ideas/clickhouse-concurrent-registration.md`.
    if !applies_to(&test_db)
        || matches!(
            test_db.db_type,
            DatabaseType::ClickHouse | DatabaseType::BigQuery
        )
    {
        return Ok(());
    }
    let storage = test_db.storage();
    for round in 0..10 {
        let uuid = Uuid::new_v4();
        let name = format!("identity_race_{uuid}");
        let sensor =
            |sensor_type| Arc::new(Sensor::new(uuid, name.clone(), sensor_type, None, None));
        let writers = [SensorType::Float, SensorType::Integer].map(|sensor_type| {
            let storage = storage.clone();
            let sensor = sensor(sensor_type);
            tokio::spawn(async move { publish(&storage, &[(sensor, round)]).await })
        });
        let mut succeeded = 0;
        for writer in writers {
            match writer.await? {
                Ok(()) => succeeded += 1,
                Err(error) => assert!(
                    matches!(
                        error.downcast_ref::<StorageError>(),
                        Some(StorageError::SensorIdentityMismatch { .. })
                    ),
                    "round {round}: expected an identity mismatch, got {error:#}"
                ),
            }
        }
        assert_eq!(succeeded, 1, "round {round}");
        let stored = storage
            .query_sensor_data(&uuid.to_string(), None, None, None)
            .await?
            .expect("the series exists");
        assert_eq!(stored.samples.len(), 1, "round {round}: one sample only");
    }
    Ok(())
}

async fn write(
    storage: &Arc<dyn StorageInstance>,
    sensor: &Arc<Sensor>,
    samples: TypedSamples,
) -> Result<()> {
    let mut builder = sensapp::datamodel::batch_builder::BatchBuilder::new()?;
    builder.add(sensor.clone(), samples).await?;
    builder.send_what_is_left(storage.clone()).await?;
    Ok(())
}

fn float_samples(values: &[f64], from: i64) -> TypedSamples {
    TypedSamples::Float(
        values
            .iter()
            .enumerate()
            .map(|(i, &value)| Sample {
                datetime: at(from + i as i64),
                value,
            })
            .collect(),
    )
}

fn integer_samples(values: &[i64], from: i64) -> TypedSamples {
    TypedSamples::Integer(
        values
            .iter()
            .enumerate()
            .map(|(i, &value)| Sample {
                datetime: at(from + i as i64),
                value,
            })
            .collect(),
    )
}

/// SenML only has floating point numbers, and pandas turns an integer column with gaps into
/// `float64`: a write whose type is not the stored one is converted when every value converts
/// exactly, in the shared writer, so every backend does the same. The series stays what it was.
#[tokio::test]
#[serial]
async fn a_write_of_another_type_is_converted_when_exact_and_refused_otherwise() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    if !applies_to(&test_db) {
        return Ok(());
    }
    let storage = test_db.storage();
    let sensor = |sensor_type, uuid: Uuid| {
        Arc::new(Sensor::new(
            uuid,
            format!("conversion_{uuid}"),
            sensor_type,
            None,
            None,
        ))
    };
    let read = |uuid: Uuid| {
        let storage = storage.clone();
        async move {
            storage
                .query_sensor_data(&uuid.to_string(), None, None, None)
                .await
                .unwrap()
                .expect("the series")
        }
    };

    // An Integer series takes whole floats, as integers
    let counter = Uuid::new_v4();
    write(
        &storage,
        &sensor(SensorType::Integer, counter),
        integer_samples(&[1], 0),
    )
    .await?;
    write(
        &storage,
        &sensor(SensorType::Float, counter),
        float_samples(&[2.0, 3.0], 1),
    )
    .await?;
    let data = read(counter).await;
    assert_eq!(data.sensor.sensor_type, SensorType::Integer);
    let TypedSamples::Integer(samples) = &data.samples else {
        panic!("not Integer: {:?}", data.samples);
    };
    assert_eq!(
        samples.iter().map(|s| s.value).collect::<Vec<_>>(),
        [1, 2, 3]
    );

    // One value that is not whole refuses the whole write, with a 409 and the stored type
    let error = write(
        &storage,
        &sensor(SensorType::Float, counter),
        float_samples(&[4.0, 4.5], 3),
    )
    .await
    .expect_err("4.5 is not an integer");
    let Some(StorageError::SensorTypeConversion { stored_type, .. }) =
        error.downcast_ref::<StorageError>()
    else {
        panic!("not a conversion error: {error:#}");
    };
    assert_eq!(stored_type, "Integer");
    assert_eq!(read(counter).await.samples.len(), 3, "nothing was stored");

    // A Float series takes integers; a Numeric one takes both
    let level = Uuid::new_v4();
    write(
        &storage,
        &sensor(SensorType::Float, level),
        float_samples(&[0.5], 0),
    )
    .await?;
    write(
        &storage,
        &sensor(SensorType::Integer, level),
        integer_samples(&[7], 1),
    )
    .await?;
    assert_eq!(read(level).await.samples.len(), 2);

    let price = Uuid::new_v4();
    write(
        &storage,
        &sensor(SensorType::Numeric, price),
        TypedSamples::Numeric(
            vec![Sample {
                datetime: at(0),
                value: rust_decimal::Decimal::new(125, 2),
            }]
            .into(),
        ),
    )
    .await?;
    write(
        &storage,
        &sensor(SensorType::Integer, price),
        integer_samples(&[3], 1),
    )
    .await?;
    write(
        &storage,
        &sensor(SensorType::Float, price),
        float_samples(&[0.1], 2),
    )
    .await?;
    let data = read(price).await;
    assert_eq!(data.sensor.sensor_type, SensorType::Numeric);
    assert_eq!(data.samples.len(), 3);

    // Other types are never converted
    let text = TypedSamples::String(
        vec![Sample {
            datetime: at(9),
            value: "3".to_string(),
        }]
        .into(),
    );
    let error = write(&storage, &sensor(SensorType::String, counter), text)
        .await
        .expect_err("a String is not an Integer");
    assert!(
        matches!(
            error.downcast_ref::<StorageError>(),
            Some(StorageError::SensorTypeConversion { .. })
        ),
        "{error:#}"
    );
    Ok(())
}
