//! A write that names an existing sensor UUID must name the stored sensor: same name, same type.
//! Otherwise a token allowed to write `temperature` could append to `pressure` by sending the
//! UUID of `pressure` with the name `temperature`, and samples of the wrong type would go to a
//! table no read looks at. The unit and the labels are not compared: an import may leave them
//! out, and the stored ones stay. These tests run on the backend selected by `TEST_DATABASE_URL`.

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
async fn a_write_under_an_existing_uuid_with_another_name_is_refused() -> Result<()> {
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
    assert_identity_mismatch(
        publish(&storage, &[(renamed, 1)]).await,
        "another name under the same UUID",
    );
    assert_unchanged(&storage, &stored).await
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

/// Two series of one batch with the same new UUID and different names: nothing is stored.
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
