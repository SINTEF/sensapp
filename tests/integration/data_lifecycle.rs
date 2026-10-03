#![cfg_attr(
    not(any(
        feature = "postgres",
        feature = "sqlite",
        feature = "duckdb",
        feature = "timescaledb",
        feature = "clickhouse"
    )),
    allow(dead_code, unused_imports)
)]

//! Deleting series and samples, on every backend that supports it.

use crate::common;

use anyhow::Result;
use common::{DatabaseType, TestDb};
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch_builder::BatchBuilder;
use sensapp::datamodel::{
    Sample, SensAppDateTime, Sensor, SensorType, TypedSamples, sensapp_vec::SensAppLabels,
};
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

fn backend_is_explicitly_configured(db_type: &DatabaseType) -> bool {
    std::env::var("TEST_DATABASE_URL")
        .is_ok_and(|url| DatabaseType::from_connection_string(&url) == *db_type)
}

/// One sample per minute starting at 2024-01-01T00:00:00Z.
const T0: f64 = 1_704_067_200.0;

fn at(minute: u32) -> SensAppDateTime {
    hifitime::Epoch::from_unix_seconds(T0 + 60.0 * f64::from(minute))
}

fn float_samples(minutes: &[u32]) -> TypedSamples {
    TypedSamples::Float(
        minutes
            .iter()
            .map(|minute| Sample {
                datetime: at(*minute),
                value: f64::from(*minute),
            })
            .collect(),
    )
}

fn integer_samples(minutes: &[u32]) -> TypedSamples {
    TypedSamples::Integer(
        minutes
            .iter()
            .map(|minute| Sample {
                datetime: at(*minute),
                value: i64::from(*minute),
            })
            .collect(),
    )
}

fn string_samples(minutes: &[u32]) -> TypedSamples {
    TypedSamples::String(
        minutes
            .iter()
            .map(|minute| Sample {
                datetime: at(*minute),
                value: format!("state-{minute}"),
            })
            .collect(),
    )
}

fn sensor(name: &str, sensor_type: SensorType, site: &str) -> Arc<Sensor> {
    let labels: SensAppLabels = smallvec::smallvec![("site".to_string(), site.to_string())];
    Arc::new(
        Sensor::new_without_uuid(name.to_string(), sensor_type, None, Some(labels))
            .expect("sensor should build"),
    )
}

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

/// Timestamps, in whole minutes since T0, of the samples stored for a series.
async fn stored_minutes(storage: &Arc<dyn StorageInstance>, sensor: &Sensor) -> Result<Vec<u32>> {
    let Some(data) = storage
        .query_sensor_data(&sensor.uuid.to_string(), None, None, None)
        .await?
    else {
        return Ok(vec![]);
    };
    let datetimes: Vec<SensAppDateTime> = match &data.samples {
        TypedSamples::Float(samples) => samples.iter().map(|s| s.datetime).collect(),
        TypedSamples::Integer(samples) => samples.iter().map(|s| s.datetime).collect(),
        TypedSamples::String(samples) => samples.iter().map(|s| s.datetime).collect(),
        other => panic!("unexpected sample type {other:?}"),
    };
    let mut minutes: Vec<u32> = datetimes
        .iter()
        .map(|datetime| ((datetime.to_unix_seconds() - T0) / 60.0).round() as u32)
        .collect();
    minutes.sort_unstable();
    Ok(minutes)
}

async fn series_uuids(storage: &Arc<dyn StorageInstance>) -> Result<Vec<Uuid>> {
    Ok(storage
        .list_series(None, None, None)
        .await?
        .series
        .into_iter()
        .map(|sensor| sensor.uuid)
        .collect())
}

async fn open(db_type: &DatabaseType) -> Result<Option<TestDb>> {
    ensure_config();
    match TestDb::new_with_type(db_type.clone()).await {
        Ok(test_db) => Ok(Some(test_db)),
        Err(error) if backend_is_explicitly_configured(db_type) => Err(error),
        Err(error) => {
            eprintln!("skipping {db_type:?} backend test: {error:#}");
            Ok(None)
        }
    }
}

async fn assert_delete_samples_range_for_backend(db_type: DatabaseType) -> Result<()> {
    let Some(test_db) = open(&db_type).await? else {
        return Ok(());
    };
    let storage = test_db.storage();

    let target = sensor("lifecycle_range_float", SensorType::Float, "a");
    let same_timestamps = sensor("lifecycle_range_integer", SensorType::Integer, "a");
    publish(&storage, &target, float_samples(&[0, 1, 2, 3, 4])).await?;
    publish(
        &storage,
        &same_timestamps,
        integer_samples(&[0, 1, 2, 3, 4]),
    )
    .await?;
    let target_uuid = target.uuid.to_string();

    // Both bounds are inclusive
    let deleted = storage
        .delete_series_samples(&target_uuid, at(1), at(3))
        .await?;
    assert_eq!(deleted, Some(3));
    assert_eq!(stored_minutes(&storage, &target).await?, vec![0, 4]);

    // A window without samples deletes nothing
    let deleted = storage
        .delete_series_samples(&target_uuid, at(10), at(20))
        .await?;
    assert_eq!(deleted, Some(0));

    // start == end targets one exact timestamp
    let deleted = storage
        .delete_series_samples(&target_uuid, at(0), at(0))
        .await?;
    assert_eq!(deleted, Some(1));
    assert_eq!(stored_minutes(&storage, &target).await?, vec![4]);

    // The sensor is kept, and so are the other series, even with the same timestamps
    assert!(series_uuids(&storage).await?.contains(&target.uuid));
    assert_eq!(
        stored_minutes(&storage, &same_timestamps).await?,
        vec![0, 1, 2, 3, 4]
    );

    // Unknown series
    let unknown = Uuid::new_v4().to_string();
    assert_eq!(
        storage
            .delete_series_samples(&unknown, at(0), at(10))
            .await?,
        None
    );

    Ok(())
}

async fn assert_delete_samples_of_string_series_for_backend(db_type: DatabaseType) -> Result<()> {
    let Some(test_db) = open(&db_type).await? else {
        return Ok(());
    };
    let storage = test_db.storage();

    let states = sensor("lifecycle_states", SensorType::String, "a");
    publish(&storage, &states, string_samples(&[0, 1, 2])).await?;

    let deleted = storage
        .delete_series_samples(&states.uuid.to_string(), at(1), at(2))
        .await?;
    assert_eq!(deleted, Some(2));
    assert_eq!(stored_minutes(&storage, &states).await?, vec![0]);

    Ok(())
}

async fn assert_delete_series_for_backend(db_type: DatabaseType) -> Result<()> {
    let Some(test_db) = open(&db_type).await? else {
        return Ok(());
    };
    let storage = test_db.storage();

    let doomed = sensor("lifecycle_doomed", SensorType::Float, "a");
    let bystander = sensor("lifecycle_bystander", SensorType::Float, "a");
    publish(&storage, &doomed, float_samples(&[0, 1, 2])).await?;
    publish(&storage, &bystander, float_samples(&[0, 1, 2])).await?;

    assert!(storage.delete_series(&doomed.uuid.to_string()).await?);

    // Gone from queries and from the catalog
    assert!(
        storage
            .query_sensor_data(&doomed.uuid.to_string(), None, None, None)
            .await?
            .is_none()
    );
    let uuids = series_uuids(&storage).await?;
    assert!(!uuids.contains(&doomed.uuid));
    assert!(uuids.contains(&bystander.uuid));
    assert_eq!(stored_minutes(&storage, &bystander).await?, vec![0, 1, 2]);

    // Deleting it again, or deleting an unknown series, reports "not found"
    assert!(!storage.delete_series(&doomed.uuid.to_string()).await?);
    assert!(!storage.delete_series(&Uuid::new_v4().to_string()).await?);

    Ok(())
}

/// The documented correction recipe: delete the bad range, then publish the fixed samples.
async fn assert_correction_recipe_for_backend(db_type: DatabaseType) -> Result<()> {
    let Some(test_db) = open(&db_type).await? else {
        return Ok(());
    };
    let storage = test_db.storage();

    let original = sensor("lifecycle_corrected", SensorType::Float, "a");
    publish(&storage, &original, float_samples(&[0, 1, 2])).await?;

    storage
        .delete_series_samples(&original.uuid.to_string(), at(1), at(1))
        .await?;
    publish(&storage, &original, float_samples(&[1])).await?;
    assert_eq!(stored_minutes(&storage, &original).await?, vec![0, 1, 2]);

    // Deleting the whole series and ingesting again brings it back under the same UUID
    assert!(storage.delete_series(&original.uuid.to_string()).await?);
    let again = sensor("lifecycle_corrected", SensorType::Float, "a");
    assert_eq!(again.uuid, original.uuid);
    publish(&storage, &again, float_samples(&[5])).await?;
    assert_eq!(stored_minutes(&storage, &again).await?, vec![5]);
    assert!(series_uuids(&storage).await?.contains(&again.uuid));

    Ok(())
}

async fn assert_invalid_uuid_is_an_error_for_backend(db_type: DatabaseType) -> Result<()> {
    let Some(test_db) = open(&db_type).await? else {
        return Ok(());
    };
    let storage = test_db.storage();

    assert!(storage.delete_series("not-a-uuid").await.is_err());
    assert!(
        storage
            .delete_series_samples("not-a-uuid", at(0), at(1))
            .await
            .is_err()
    );
    Ok(())
}

/// Another SensApp instance, or someone with SQL access, deletes a sensor that this
/// instance has cached: publishing it again must recreate it instead of failing.
#[cfg(any(feature = "postgres", feature = "timescaledb"))]
async fn assert_publish_recovers_from_stale_sensor_id_for_backend(
    db_type: DatabaseType,
) -> Result<()> {
    use sqlx::Executor;

    let Some(test_db) = open(&db_type).await? else {
        return Ok(());
    };
    let storage = test_db.storage();

    let cached = sensor("lifecycle_stale_cache", SensorType::Float, "a");
    publish(&storage, &cached, float_samples(&[0, 1])).await?;

    // Delete it with raw SQL, behind the back of the storage and its sensor id cache
    let url = test_db
        .connection_string
        .replacen("timescaledb://", "postgres://", 1);
    let pool = sqlx::PgPool::connect(&url).await?;
    let uuid = cached.uuid;
    for table in sensapp::storage::common::VALUE_TABLES {
        let sql = format!(
            "DELETE FROM {table} WHERE sensor_id IN (SELECT sensor_id FROM sensors WHERE uuid = $1)"
        );
        pool.execute(sqlx::query(sqlx::AssertSqlSafe(sql)).bind(uuid))
            .await?;
    }
    pool.execute(
        sqlx::query(
            "DELETE FROM labels WHERE sensor_id IN (SELECT sensor_id FROM sensors WHERE uuid = $1)",
        )
        .bind(uuid),
    )
    .await?;
    pool.execute(sqlx::query("DELETE FROM sensors WHERE uuid = $1").bind(uuid))
        .await?;
    pool.close().await;

    publish(&storage, &cached, float_samples(&[5])).await?;
    assert_eq!(stored_minutes(&storage, &cached).await?, vec![5]);

    Ok(())
}

macro_rules! backend_tests {
    ($feature:literal, $db_type:expr, $prefix:ident) => {
        #[cfg(feature = $feature)]
        mod $prefix {
            use super::*;

            #[tokio::test]
            #[serial]
            async fn delete_samples_range() -> Result<()> {
                assert_delete_samples_range_for_backend($db_type).await
            }

            #[tokio::test]
            #[serial]
            async fn delete_samples_of_string_series() -> Result<()> {
                assert_delete_samples_of_string_series_for_backend($db_type).await
            }

            #[tokio::test]
            #[serial]
            async fn delete_series() -> Result<()> {
                assert_delete_series_for_backend($db_type).await
            }

            #[tokio::test]
            #[serial]
            async fn correction_recipe() -> Result<()> {
                assert_correction_recipe_for_backend($db_type).await
            }

            #[tokio::test]
            #[serial]
            async fn invalid_uuid_is_an_error() -> Result<()> {
                assert_invalid_uuid_is_an_error_for_backend($db_type).await
            }
        }
    };
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[serial]
async fn postgresql_publish_recovers_from_stale_sensor_id() -> Result<()> {
    assert_publish_recovers_from_stale_sensor_id_for_backend(DatabaseType::PostgreSQL).await
}

#[cfg(feature = "timescaledb")]
#[tokio::test]
#[serial]
async fn timescaledb_publish_recovers_from_stale_sensor_id() -> Result<()> {
    assert_publish_recovers_from_stale_sensor_id_for_backend(DatabaseType::TimescaleDB).await
}

backend_tests!("postgres", DatabaseType::PostgreSQL, postgresql);
backend_tests!("sqlite", DatabaseType::SQLite, sqlite);
backend_tests!("duckdb", DatabaseType::DuckDB, duckdb);
backend_tests!("timescaledb", DatabaseType::TimescaleDB, timescaledb);
backend_tests!("clickhouse", DatabaseType::ClickHouse, clickhouse);
backend_tests!("bigquery", DatabaseType::BigQuery, bigquery);
