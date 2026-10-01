#![cfg_attr(
    not(any(feature = "postgres", feature = "sqlite", feature = "timescaledb")),
    allow(dead_code, unused_imports)
)]

//! Samples are written with one statement per chunk instead of one per sample.
//! Publish more samples than one SQLite statement can carry (and more than one
//! batch), of every type, and check that they all come back unchanged.

use crate::common;

use anyhow::Result;
use common::{DatabaseType, TestDb};
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch_builder::BatchBuilder;
use sensapp::datamodel::{Sample, SensAppDateTime, Sensor, SensorType, TypedSamples};
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

/// More than the default batch size (8192), so that the samples are split in several
/// batches, and more than the number of rows of one SQLite statement.
const SAMPLE_COUNT: usize = 20_000;

/// One sample per second starting at 2024-01-01T00:00:00Z.
const T0: f64 = 1_704_067_200.0;

fn at(index: usize) -> SensAppDateTime {
    hifitime::Epoch::from_unix_seconds(T0 + index as f64)
}

fn samples_of<T>(count: usize, value: impl Fn(usize) -> T) -> Vec<Sample<T>> {
    (0..count)
        .map(|index| Sample {
            datetime: at(index),
            value: value(index),
        })
        .collect()
}

/// Timestamp in microseconds and printed value of every sample, ordered by timestamp.
/// Decimals are normalized because backends may return a different scale.
fn canonical(samples: &TypedSamples) -> Vec<(i64, String)> {
    fn rows<T>(samples: &[Sample<T>], print: impl Fn(&T) -> String) -> Vec<(i64, String)> {
        let mut rows: Vec<(i64, String)> = samples
            .iter()
            .map(|sample| {
                (
                    (sample.datetime.to_unix_seconds() * 1e6).round() as i64,
                    print(&sample.value),
                )
            })
            .collect();
        rows.sort();
        rows
    }
    match samples {
        TypedSamples::Integer(s) => rows(s, |v| format!("{v:?}")),
        TypedSamples::Numeric(s) => rows(s, |v| v.normalize().to_string()),
        TypedSamples::Float(s) => rows(s, |v| format!("{v:?}")),
        TypedSamples::String(s) => rows(s, |v| format!("{v:?}")),
        TypedSamples::Boolean(s) => rows(s, |v| format!("{v:?}")),
        TypedSamples::Location(s) => rows(s, |v| format!("{:?} {:?}", v.x(), v.y())),
        TypedSamples::Blob(s) => rows(s, |v| format!("{v:?}")),
        TypedSamples::Json(s) => rows(s, |v| v.to_string()),
    }
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

/// Publishes the samples as one series, reads the series back and compares.
async fn assert_round_trip(
    storage: &Arc<dyn StorageInstance>,
    sensor_type: SensorType,
    expected: TypedSamples,
    stored_expected: Option<&TypedSamples>,
) -> Result<()> {
    let sensor = Arc::new(Sensor::new_without_uuid(
        format!("batched_inserts_{sensor_type}_{}", Uuid::new_v4()),
        sensor_type,
        None,
        None,
    )?);
    let expected_rows = canonical(stored_expected.unwrap_or(&expected));

    let mut batch_builder = BatchBuilder::new()?;
    batch_builder.add(sensor.clone(), expected).await?;
    batch_builder.send_what_is_left(storage.clone()).await?;

    let data = storage
        .query_sensor_data(&sensor.uuid.to_string(), None, None, None)
        .await?
        .expect("the series should exist");
    let stored_rows = canonical(&data.samples);
    assert_eq!(
        stored_rows.len(),
        expected_rows.len(),
        "{sensor_type} count"
    );
    assert_eq!(stored_rows, expected_rows, "{sensor_type} content");
    Ok(())
}

async fn assert_every_type_round_trips_for_backend(db_type: DatabaseType) -> Result<()> {
    let Some(test_db) = open(&db_type).await? else {
        return Ok(());
    };
    let storage = test_db.storage();
    let n = SAMPLE_COUNT;

    assert_round_trip(
        &storage,
        SensorType::Integer,
        TypedSamples::Integer(samples_of(n, |i| i as i64 * 3 - 1_000).into()),
        None,
    )
    .await?;
    assert_round_trip(
        &storage,
        SensorType::Numeric,
        TypedSamples::Numeric(samples_of(n, |i| rust_decimal::Decimal::new(i as i64, 3)).into()),
        None,
    )
    .await?;
    assert_round_trip(
        &storage,
        SensorType::Float,
        TypedSamples::Float(samples_of(n, |i| i as f64 * 0.5 - 17.25).into()),
        None,
    )
    .await?;
    // Few distinct strings, repeated many times
    assert_round_trip(
        &storage,
        SensorType::String,
        TypedSamples::String(samples_of(n, |i| format!("state-{}", i % 7)).into()),
        None,
    )
    .await?;
    assert_round_trip(
        &storage,
        SensorType::Boolean,
        TypedSamples::Boolean(samples_of(n, |i| i % 3 == 0).into()),
        None,
    )
    .await?;
    // Four columns per row: the widest table, the first one to hit SQLite's variable limit
    assert_round_trip(
        &storage,
        SensorType::Location,
        TypedSamples::Location(
            samples_of(n, |i| {
                geo::Point::new(i as f64 * 0.001, 60.0 + i as f64 * 0.0001)
            })
            .into(),
        ),
        None,
    )
    .await?;
    assert_round_trip(
        &storage,
        SensorType::Blob,
        TypedSamples::Blob(
            samples_of(n, |i| vec![(i % 256) as u8, (i / 256 % 256) as u8, 0, 255]).into(),
        ),
        None,
    )
    .await?;
    assert_round_trip(
        &storage,
        SensorType::Json,
        TypedSamples::Json(
            samples_of(
                n,
                |i| serde_json::json!({"index": i, "even": i % 2 == 0, "tag": "x"}),
            )
            .into(),
        ),
        None,
    )
    .await?;

    Ok(())
}

/// Fewer samples than one chunk, and a single one.
async fn assert_small_batches_for_backend(db_type: DatabaseType) -> Result<()> {
    let Some(test_db) = open(&db_type).await? else {
        return Ok(());
    };
    let storage = test_db.storage();

    for count in [1, 2, 10] {
        assert_round_trip(
            &storage,
            SensorType::Float,
            TypedSamples::Float(samples_of(count, |i| i as f64 + 0.5).into()),
            None,
        )
        .await?;
        assert_round_trip(
            &storage,
            SensorType::Location,
            TypedSamples::Location(samples_of(count, |i| geo::Point::new(i as f64, 1.0)).into()),
            None,
        )
        .await?;
    }
    Ok(())
}

/// SQLite cannot store NaN and Infinity in a REAL column: they are skipped.
#[cfg(feature = "sqlite")]
#[tokio::test]
#[serial]
async fn sqlite_skips_non_finite_floats() -> Result<()> {
    let Some(test_db) = open(&DatabaseType::SQLite).await? else {
        return Ok(());
    };
    let storage = test_db.storage();

    let samples = TypedSamples::Float(
        vec![
            Sample {
                datetime: at(0),
                value: 1.0,
            },
            Sample {
                datetime: at(1),
                value: f64::NAN,
            },
            Sample {
                datetime: at(2),
                value: f64::INFINITY,
            },
            Sample {
                datetime: at(3),
                value: f64::NEG_INFINITY,
            },
            Sample {
                datetime: at(4),
                value: 2.0,
            },
        ]
        .into(),
    );
    let finite = TypedSamples::Float(
        vec![
            Sample {
                datetime: at(0),
                value: 1.0,
            },
            Sample {
                datetime: at(4),
                value: 2.0,
            },
        ]
        .into(),
    );
    assert_round_trip(&storage, SensorType::Float, samples, Some(&finite)).await
}

macro_rules! backend_tests {
    ($feature:literal, $db_type:expr, $prefix:ident) => {
        #[cfg(feature = $feature)]
        mod $prefix {
            use super::*;

            #[tokio::test]
            #[serial]
            async fn every_type_round_trips() -> Result<()> {
                assert_every_type_round_trips_for_backend($db_type).await
            }

            #[tokio::test]
            #[serial]
            async fn small_batches() -> Result<()> {
                assert_small_batches_for_backend($db_type).await
            }
        }
    };
}

backend_tests!("postgres", DatabaseType::PostgreSQL, postgresql);
backend_tests!("sqlite", DatabaseType::SQLite, sqlite);
backend_tests!("timescaledb", DatabaseType::TimescaleDB, timescaledb);
