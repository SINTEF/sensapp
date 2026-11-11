//! The latest sample of a series is read in two statements: the newest time, then its samples.
//! A delete of that time between them used to leave the answer empty (`None`, or a series with no
//! sample) while older samples remained. The reader must see the newest remaining sample for as
//! long as there is one. The deleter and the reader use one storage with the default pool: a pool
//! of one connection would serialise them. The newest sample is found again by trying again
//! (`LATEST_ATTEMPTS`), so a deleter that never pauses between two deletes can still beat it.

use crate::common::{DatabaseType, TestDb};
use anyhow::Result;
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch::{Batch, SingleSensorBatch};
use sensapp::datamodel::sensapp_vec::SensAppVec;
use sensapp::datamodel::{Sample, SensAppDateTime, Sensor, SensorType, TypedSamples};
use serial_test::serial;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use uuid::Uuid;

static INIT: std::sync::Once = std::sync::Once::new();

fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

fn at(seconds: i64) -> SensAppDateTime {
    hifitime::Epoch::from_unix_seconds(1_704_067_200.0 + seconds as f64)
}

#[tokio::test]
#[serial]
async fn latest_finds_the_newest_remaining_sample_while_the_newest_ones_are_deleted() -> Result<()>
{
    ensure_config();
    let test_db = TestDb::new().await?;
    // RRDcached has no deletes, BigQuery bills the project, and DuckDB still fails after the
    // retry (its readers are connections of their own): see
    // `ideas/duckdb-latest-under-concurrent-delete.md`
    if matches!(
        test_db.db_type,
        DatabaseType::RRDcached | DatabaseType::BigQuery | DatabaseType::DuckDB
    ) {
        return Ok(());
    }
    let storage = test_db.storage();
    const SAMPLES: usize = 150;
    let sensor = Arc::new(Sensor::new(
        Uuid::new_v4(),
        format!("latest_race_{}", Uuid::new_v4()),
        SensorType::Float,
        None,
        None,
    ));
    let samples: SensAppVec<_> = (0..SAMPLES as i64)
        .map(|second| Sample {
            datetime: at(second),
            value: second as f64,
        })
        .collect();
    let mut batch = SensAppVec::new();
    batch.push(SingleSensorBatch::new(
        sensor.clone(),
        TypedSamples::Float(samples),
    ));
    storage.publish(Arc::new(Batch::new(batch))).await?;

    // Samples that are not deleted yet, or being deleted
    let remaining = Arc::new(AtomicUsize::new(SAMPLES));
    let deleter = {
        let (storage, uuid, remaining) =
            (storage.clone(), sensor.uuid.to_string(), remaining.clone());
        tokio::spawn(async move {
            for second in (0..SAMPLES as i64).rev() {
                storage
                    .delete_series_samples(&uuid, at(second), at(second))
                    .await?;
                remaining.fetch_sub(1, Ordering::SeqCst);
                // A range delete does not run back to back with itself
                tokio::time::sleep(std::time::Duration::from_millis(3)).await;
            }
            anyhow::Ok(())
        })
    };
    let uuid = sensor.uuid.to_string();
    let mut reads = 0;
    while !deleter.is_finished() {
        let latest = storage.query_sensor_data_latest(&uuid, None, None).await?;
        reads += 1;
        // The count is at most one ahead of the database: a delete may be done and not counted
        let missing = latest.as_ref().is_none_or(|data| data.samples.is_empty());
        if missing {
            let still = remaining.load(Ordering::SeqCst);
            assert!(
                still < 2,
                "read {reads}: no latest sample, {still} samples remain"
            );
        }
    }
    deleter.await??;
    assert!(reads > 0);
    Ok(())
}
