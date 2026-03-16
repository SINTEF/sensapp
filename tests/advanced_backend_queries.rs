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

    let test_db = TestDb::new_with_type(db_type).await?;
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

#[cfg(feature = "sqlite")]
#[tokio::test]
#[serial]
async fn test_sqlite_native_bucketed_average_query() -> Result<()> {
    assert_bucketed_average_for_backend(DatabaseType::SQLite).await
}

#[cfg(feature = "duckdb")]
#[tokio::test]
#[serial]
async fn test_duckdb_native_bucketed_average_query() -> Result<()> {
    assert_bucketed_average_for_backend(DatabaseType::DuckDB).await
}

#[cfg(feature = "timescaledb")]
#[tokio::test]
#[serial]
async fn test_timescaledb_native_bucketed_average_query() -> Result<()> {
    assert_bucketed_average_for_backend(DatabaseType::TimescaleDB).await
}

#[cfg(feature = "clickhouse")]
#[tokio::test]
#[serial]
async fn test_clickhouse_native_bucketed_average_query() -> Result<()> {
    assert_bucketed_average_for_backend(DatabaseType::ClickHouse).await
}
