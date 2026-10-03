//! Writers that meet on TimescaleDB must not fail because PostgreSQL found a deadlock between them.
//! A batch that registers new series locks the hypertables it writes to before it inserts the
//! sensors (see `register_sensors`); this file covers the one order that is not enough, and the
//! retry that covers it. They only run on TimescaleDB.

use crate::common::{DatabaseType, TestDb};
use anyhow::Result;
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch_builder::BatchBuilder;
use sensapp::datamodel::{Sample, SensAppDateTime, Sensor, SensorType, TypedSamples};
use serial_test::serial;
use std::sync::Arc;
use std::time::Duration;
use uuid::Uuid;

static INIT: std::sync::Once = std::sync::Once::new();

fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

/// A chunk creator holds the lock of `string_values` and wants the one of `boolean_values`; the
/// batch that registers a boolean and a string series locks them in the other order (alphabetical).
/// PostgreSQL aborts one of the two; the batch is written again and succeeds, whichever it was.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[serial]
async fn a_batch_aborted_by_a_deadlock_is_written_again() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    if test_db.db_type != DatabaseType::TimescaleDB {
        return Ok(());
    }
    let storage = test_db.storage();
    let pool = sqlx::PgPool::connect(&test_db.connection_string.replacen(
        "timescaledb://",
        "postgres://",
        1,
    ))
    .await?;
    let run = Uuid::new_v4();

    // The chunk creator, with the first of its two locks
    let mut creator = pool.begin().await?;
    sqlx::query("LOCK TABLE ONLY string_values IN SHARE UPDATE EXCLUSIVE MODE")
        .execute(&mut *creator)
        .await?;

    let datetime: SensAppDateTime = hifitime::Epoch::from_unix_seconds(1_704_067_200.0);
    let flag = Arc::new(Sensor::new_without_uuid(
        format!("deadlock_flag_{run}"),
        SensorType::Boolean,
        None,
        None,
    )?);
    let text = Arc::new(Sensor::new_without_uuid(
        format!("deadlock_text_{run}"),
        SensorType::String,
        None,
        None,
    )?);
    let writer = {
        let (storage, flag, text) = (storage.clone(), flag.clone(), text.clone());
        tokio::spawn(async move {
            let mut batch_builder = BatchBuilder::new()?;
            batch_builder
                .add(
                    flag,
                    TypedSamples::Boolean(
                        vec![Sample {
                            datetime,
                            value: true,
                        }]
                        .into(),
                    ),
                )
                .await?;
            batch_builder
                .add(
                    text,
                    TypedSamples::String(
                        vec![Sample {
                            datetime,
                            value: "on".to_string(),
                        }]
                        .into(),
                    ),
                )
                .await?;
            batch_builder.send_what_is_left(storage).await?;
            anyhow::Ok(())
        })
    };

    // The batch holds `boolean_values` and waits for `string_values`. The creator asks for
    // `boolean_values`: a deadlock, one of them is aborted after a second.
    tokio::time::sleep(Duration::from_millis(500)).await;
    let _ = sqlx::query("LOCK TABLE ONLY boolean_values IN SHARE UPDATE EXCLUSIVE MODE")
        .execute(&mut *creator)
        .await;
    creator.rollback().await?;

    writer.await??;
    for sensor in [&flag, &text] {
        let data = storage
            .query_sensor_data(&sensor.uuid.to_string(), None, None, None)
            .await?
            .expect("the series should exist");
        assert_eq!(data.samples.len(), 1, "{}", sensor.name);
    }
    Ok(())
}
