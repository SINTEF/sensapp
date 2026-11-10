//! Numeric values come back as written. These tests run on the backend selected by
//! `TEST_DATABASE_URL`.

use crate::common::{DatabaseType, TestDb};
use anyhow::Result;
use rust_decimal::Decimal;
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch_builder::BatchBuilder;
use sensapp::datamodel::{Sample, Sensor, SensorType, TypedSamples};
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
