#![cfg_attr(
    not(any(feature = "sqlite", feature = "duckdb", feature = "timescaledb", feature = "clickhouse")),
    allow(dead_code, unused_imports)
)]

mod common;

use anyhow::Result;
use common::{DatabaseType, TestDb};
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch_builder::BatchBuilder;
use sensapp::datamodel::{Sample, Sensor, SensorType, TypedSamples};
use sensapp::storage::{Aggregation, SensorDataQueryOptions};
use serial_test::serial;
use std::sync::Arc;
use uuid::Uuid;

static INIT: std::sync::Once = std::sync::Once::new();

fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

async fn assert_bucketed_average_for_backend(db_type: DatabaseType) -> Result<()> {
    ensure_config();

    let test_db = match TestDb::new_with_type(db_type.clone()).await {
        Ok(test_db) => test_db,
        Err(error) => {
            eprintln!("skipping {db_type:?} backend test: {error:#}");
            return Ok(());
        }
    };
    let storage = test_db.storage();
    let sensor = Arc::new(Sensor::new(
        Uuid::new_v4(),
        format!("advanced_avg_{:?}", test_db.db_type),
        SensorType::Float,
        None,
        None,
    ));

    let samples = TypedSamples::Float(smallvec::smallvec![
        Sample {
            datetime: hifitime::Epoch::from_unix_seconds(1_704_067_200.0),
            value: 20.5,
        },
        Sample {
            datetime: hifitime::Epoch::from_unix_seconds(1_704_067_260.0),
            value: 21.0,
        },
        Sample {
            datetime: hifitime::Epoch::from_unix_seconds(1_704_067_320.0),
            value: 21.5,
        },
        Sample {
            datetime: hifitime::Epoch::from_unix_seconds(1_704_067_380.0),
            value: 22.0,
        },
        Sample {
            datetime: hifitime::Epoch::from_unix_seconds(1_704_067_440.0),
            value: 20.8,
        },
    ]);

    let mut batch_builder = BatchBuilder::new()?;
    batch_builder.add(sensor.clone(), samples).await?;
    batch_builder.send_what_is_left(storage.clone()).await?;

    let sensor_data = storage
        .query_sensor_data_advanced(
            &sensor.uuid.to_string(),
            &SensorDataQueryOptions {
                start_time: None,
                end_time: None,
                limit: None,
                step_ms: Some(120_000),
                aggregation: Some(Aggregation::Avg),
                simplify: None,
            },
        )
        .await?
        .expect("sensor data should exist");

    match sensor_data.samples {
        sensapp::datamodel::TypedSamples::Float(samples) => {
            assert_eq!(samples.len(), 3, "expected 3 aggregated rows");
            assert_eq!(samples[0].datetime.to_unix_milliseconds().floor() as i64, 1_704_067_200_000);
            assert_eq!(samples[0].value, 20.75);
            assert_eq!(samples[1].datetime.to_unix_milliseconds().floor() as i64, 1_704_067_320_000);
            assert_eq!(samples[1].value, 21.75);
            assert_eq!(samples[2].datetime.to_unix_milliseconds().floor() as i64, 1_704_067_440_000);
            assert_eq!(samples[2].value, 20.8);
        }
        other => panic!("expected float samples, got {other:?}"),
    }

    Ok(())
}

async fn assert_latest_and_availability_for_backend(db_type: DatabaseType) -> Result<()> {
    ensure_config();

    let test_db = match TestDb::new_with_type(db_type.clone()).await {
        Ok(test_db) => test_db,
        Err(error) => {
            eprintln!("skipping {db_type:?} backend test: {error:#}");
            return Ok(());
        }
    };
    let storage = test_db.storage();
    let sensor = Arc::new(Sensor::new(
        Uuid::new_v4(),
        format!("advanced_latest_{:?}", test_db.db_type),
        SensorType::Float,
        None,
        None,
    ));

    let samples = TypedSamples::Float(smallvec::smallvec![
        Sample {
            datetime: hifitime::Epoch::from_unix_seconds(1_704_067_200.0),
            value: 20.5,
        },
        Sample {
            datetime: hifitime::Epoch::from_unix_seconds(1_704_067_260.0),
            value: 21.0,
        },
        Sample {
            datetime: hifitime::Epoch::from_unix_seconds(1_704_067_320.0),
            value: 21.5,
        },
        Sample {
            datetime: hifitime::Epoch::from_unix_seconds(1_704_067_380.0),
            value: 22.0,
        },
        Sample {
            datetime: hifitime::Epoch::from_unix_seconds(1_704_067_440.0),
            value: 20.8,
        },
    ]);

    let mut batch_builder = BatchBuilder::new()?;
    batch_builder.add(sensor.clone(), samples).await?;
    batch_builder.send_what_is_left(storage.clone()).await?;

    let latest = storage
        .query_sensor_data_latest(&sensor.uuid.to_string(), None, None)
        .await?
        .expect("latest sample should exist");

    match latest.samples {
        sensapp::datamodel::TypedSamples::Float(samples) => {
            assert_eq!(samples.len(), 1, "expected a single latest sample");
            assert_eq!(samples[0].datetime.to_unix_milliseconds().floor() as i64, 1_704_067_440_000);
            assert_eq!(samples[0].value, 20.8);
        }
        other => panic!("expected float samples, got {other:?}"),
    }

    let availability = storage
        .query_sensor_data_availability(
            &sensor.uuid.to_string(),
            hifitime::Epoch::from_unix_seconds(1_704_067_200.0),
            hifitime::Epoch::from_unix_seconds(1_704_067_440.0),
            Some(120_000),
        )
        .await?
        .expect("availability summary should exist");

    assert_eq!(availability.sample_count, 5);
    assert_eq!(availability.covered_buckets, Some(3));
    assert_eq!(
        availability
            .first_sample_at
            .expect("first sample timestamp should exist")
            .to_unix_milliseconds()
            .floor() as i64,
        1_704_067_200_000
    );
    assert_eq!(
        availability
            .last_sample_at
            .expect("last sample timestamp should exist")
            .to_unix_milliseconds()
            .floor() as i64,
        1_704_067_440_000
    );

    Ok(())
}

#[cfg(feature = "postgres")]
#[tokio::test]
#[serial]
async fn test_postgresql_native_latest_and_availability_queries() -> Result<()> {
    assert_latest_and_availability_for_backend(DatabaseType::PostgreSQL).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
#[serial]
async fn test_sqlite_native_bucketed_average_query() -> Result<()> {
    assert_bucketed_average_for_backend(DatabaseType::SQLite).await
}

#[cfg(feature = "sqlite")]
#[tokio::test]
#[serial]
async fn test_sqlite_native_latest_and_availability_queries() -> Result<()> {
    assert_latest_and_availability_for_backend(DatabaseType::SQLite).await
}

#[cfg(feature = "duckdb")]
#[tokio::test]
#[serial]
async fn test_duckdb_native_bucketed_average_query() -> Result<()> {
    assert_bucketed_average_for_backend(DatabaseType::DuckDB).await
}

#[cfg(feature = "duckdb")]
#[tokio::test]
#[serial]
async fn test_duckdb_native_latest_and_availability_queries() -> Result<()> {
    assert_latest_and_availability_for_backend(DatabaseType::DuckDB).await
}

#[cfg(feature = "timescaledb")]
#[tokio::test]
#[serial]
async fn test_timescaledb_native_bucketed_average_query() -> Result<()> {
    assert_bucketed_average_for_backend(DatabaseType::TimescaleDB).await
}

#[cfg(feature = "timescaledb")]
#[tokio::test]
#[serial]
async fn test_timescaledb_native_latest_and_availability_queries() -> Result<()> {
    assert_latest_and_availability_for_backend(DatabaseType::TimescaleDB).await
}

#[cfg(feature = "clickhouse")]
#[tokio::test]
#[serial]
async fn test_clickhouse_native_bucketed_average_query() -> Result<()> {
    assert_bucketed_average_for_backend(DatabaseType::ClickHouse).await
}

#[cfg(feature = "clickhouse")]
#[tokio::test]
#[serial]
async fn test_clickhouse_native_latest_and_availability_queries() -> Result<()> {
    assert_latest_and_availability_for_backend(DatabaseType::ClickHouse).await
}
