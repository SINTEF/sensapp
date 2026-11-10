//! Numeric values come back as written. These tests run on the backend selected by
//! `TEST_DATABASE_URL`.

use crate::common::{DatabaseType, TestDb};
use anyhow::Result;
use rust_decimal::Decimal;
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch_builder::BatchBuilder;
use sensapp::datamodel::{Sample, Sensor, SensorType, TypedSamples};
use sensapp::storage::{Aggregation, SensorDataQueryOptions, StorageError, StorageInstance};
use serial_test::serial;
use std::sync::Arc;
use uuid::Uuid;

static INIT: std::sync::Once = std::sync::Once::new();

fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

/// The largest values a `Decimal` holds, with up to 8 digits after the point. ClickHouse stored a
/// value of 10^21 or more divided by a power of ten (`Decimal::rescale` stopped at a smaller
/// scale), then refused it.
#[tokio::test]
#[serial]
async fn large_numerics_round_trip() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    // RRDcached stores floats
    if test_db.db_type == DatabaseType::RRDcached {
        return Ok(());
    }
    let storage = test_db.storage();
    let values = [
        Decimal::from_i128_with_scale(10_i128.pow(21), 0),
        Decimal::MAX,
        Decimal::MIN,
        Decimal::from_i128_with_scale(1_234_567_890_123_456_789_012_345_678, 8),
        Decimal::new(-1, 8),
    ];
    let samples = values
        .iter()
        .enumerate()
        .map(|(i, &value)| Sample {
            datetime: hifitime::Epoch::from_unix_seconds(1_704_067_200.0 + i as f64),
            value,
        })
        .collect::<Vec<_>>();
    let sensor = Arc::new(Sensor::new_without_uuid(
        format!("large_numerics_{}", Uuid::new_v4().simple()),
        SensorType::Numeric,
        None,
        None,
    )?);
    let mut batch_builder = BatchBuilder::new()?;
    batch_builder
        .add(sensor.clone(), TypedSamples::Numeric(samples.into()))
        .await?;
    batch_builder.send_what_is_left(storage.clone()).await?;

    let data = storage
        .query_sensor_data(&sensor.uuid.to_string(), None, None, None)
        .await?
        .expect("the series");
    let TypedSamples::Numeric(read) = data.samples else {
        panic!("expected numerics");
    };
    let read: Vec<Decimal> = read.iter().map(|sample| sample.value).collect();
    assert_eq!(read, values);
    Ok(())
}

async fn numeric_series(
    storage: &Arc<dyn StorageInstance>,
    values: &[(f64, Decimal)],
) -> Result<Arc<Sensor>> {
    let sensor = Arc::new(Sensor::new_without_uuid(
        format!("numerics_{}", Uuid::new_v4().simple()),
        SensorType::Numeric,
        None,
        None,
    )?);
    let samples = values
        .iter()
        .map(|&(seconds, value)| Sample {
            datetime: hifitime::Epoch::from_unix_seconds(seconds),
            value,
        })
        .collect::<Vec<_>>();
    let mut batch_builder = BatchBuilder::new()?;
    batch_builder
        .add(sensor.clone(), TypedSamples::Numeric(samples.into()))
        .await?;
    batch_builder.send_what_is_left(storage.clone()).await?;
    Ok(sensor)
}

/// The values of one-hour buckets
async fn aggregated(
    storage: &Arc<dyn StorageInstance>,
    sensor: &Sensor,
    aggregation: Aggregation,
) -> Result<Vec<String>> {
    let options = SensorDataQueryOptions {
        start_time: Some(hifitime::Epoch::from_unix_seconds(T0)),
        end_time: Some(hifitime::Epoch::from_unix_seconds(T0 + 7200.0)),
        limit: None,
        step_ms: Some(3_600_000),
        aggregation: Some(aggregation),
        simplify: None,
    };
    let data = storage
        .query_sensor_data_advanced(&sensor.uuid.to_string(), &options)
        .await?
        .expect("the series");
    Ok(match data.samples {
        TypedSamples::Numeric(samples) => samples
            .iter()
            .map(|s| s.value.normalize().to_string())
            .collect(),
        TypedSamples::Integer(samples) => samples.iter().map(|s| s.value.to_string()).collect(),
        other => panic!("unexpected {other:?}"),
    })
}

const T0: f64 = 1_704_067_200.0;

/// Numeric aggregations are exact: compared as numbers, not as text, and not through a float.
#[tokio::test]
#[serial]
async fn numeric_aggregations_are_exact() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    if test_db.db_type == DatabaseType::RRDcached {
        return Ok(());
    }
    let storage = test_db.storage();
    let big = Decimal::from_str_exact("1234567890123456789012345")?;
    let sensor = numeric_series(
        &storage,
        &[
            (T0, Decimal::from(9)),
            (T0 + 1.0, Decimal::from(10)),
            (T0 + 2.0, Decimal::from(-5)),
            (T0 + 3.0, Decimal::from(-10)),
            (T0 + 4.0, big),
            (T0 + 3600.0, Decimal::from(9)),
            (T0 + 3601.0, Decimal::from(10)),
        ],
    )
    .await?;

    let expected = [
        (Aggregation::Min, ["-10", "9"]),
        (Aggregation::Max, ["1234567890123456789012345", "10"]),
        (Aggregation::Sum, ["1234567890123456789012349", "19"]),
        (Aggregation::Avg, ["246913578024691357802469.8", "9.5"]),
        (Aggregation::Count, ["5", "2"]),
    ];
    let mut wrong = Vec::new();
    for (aggregation, expected) in expected {
        match aggregated(&storage, &sensor, aggregation).await {
            Ok(values) if values == expected => {}
            Ok(values) => wrong.push(format!("{aggregation:?}: {values:?}, not {expected:?}")),
            Err(error) => wrong.push(format!("{aggregation:?}: {error:#}")),
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    Ok(())
}

/// A sum that a `Decimal` cannot hold, or an Integer sum over i64, is an error of the request
/// (400), not of the server.
#[tokio::test]
#[serial]
async fn sums_that_overflow_are_invalid_requests() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    if test_db.db_type == DatabaseType::RRDcached {
        return Ok(());
    }
    let storage = test_db.storage();
    let numeric = numeric_series(&storage, &[(T0, Decimal::MAX), (T0 + 1.0, Decimal::MAX)]).await?;

    let integer = Arc::new(Sensor::new_without_uuid(
        format!("integers_{}", Uuid::new_v4().simple()),
        SensorType::Integer,
        None,
        None,
    )?);
    let mut batch_builder = BatchBuilder::new()?;
    batch_builder
        .add(
            integer.clone(),
            TypedSamples::Integer(
                vec![
                    Sample {
                        datetime: hifitime::Epoch::from_unix_seconds(T0),
                        value: i64::MAX,
                    },
                    Sample {
                        datetime: hifitime::Epoch::from_unix_seconds(T0 + 1.0),
                        value: i64::MAX,
                    },
                ]
                .into(),
            ),
        )
        .await?;
    batch_builder.send_what_is_left(storage.clone()).await?;

    let mut wrong = Vec::new();
    for (sensor, aggregation) in [
        (&numeric, Aggregation::Sum),
        (&numeric, Aggregation::Avg),
        (&integer, Aggregation::Sum),
    ] {
        match aggregated(&storage, sensor, aggregation).await {
            Err(error)
                if matches!(
                    error.downcast_ref::<StorageError>(),
                    Some(StorageError::InvalidDataFormat { .. })
                ) => {}
            other => wrong.push(format!("{} {aggregation:?}: {other:?}", sensor.sensor_type)),
        }
    }
    assert!(wrong.is_empty(), "{}", wrong.join("\n"));
    Ok(())
}

/// Digits after the point are kept on every backend, except where the column has a fixed scale:
/// ClickHouse stores `Decimal(38, 8)`, DuckDB `DECIMAL(38, 9)` and BigQuery `NUMERIC` (9 digits),
/// which round the others.
#[tokio::test]
#[serial]
async fn numerics_keep_their_digits_after_the_point() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    if test_db.db_type == DatabaseType::RRDcached {
        return Ok(());
    }
    let storage = test_db.storage();
    let value = Decimal::from_str_exact("0.123456789012345678901234567")?;
    let sensor = numeric_series(&storage, &[(T0, value)]).await?;
    let data = storage
        .query_sensor_data(&sensor.uuid.to_string(), None, None, None)
        .await?
        .expect("the series");
    let TypedSamples::Numeric(read) = data.samples else {
        panic!("expected numerics");
    };
    let expected = match test_db.db_type {
        DatabaseType::ClickHouse => value.round_dp(8),
        DatabaseType::DuckDB | DatabaseType::BigQuery => value.round_dp(9),
        _ => value,
    };
    assert_eq!(read[0].value, expected);
    Ok(())
}
