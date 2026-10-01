//! Publishing must behave the same on every backend whatever the shape of the data: samples
//! spread over many years in one request, the same series written again and again, or by many
//! writers at once. These tests run on the backend selected by `TEST_DATABASE_URL`.

use crate::common::TestDb;
use anyhow::Result;
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch_builder::BatchBuilder;
use sensapp::datamodel::{Sample, SensAppDateTime, Sensor, SensorType, TypedSamples};
use serial_test::serial;
use std::sync::Arc;
use uuid::Uuid;

static INIT: std::sync::Once = std::sync::Once::new();

fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

/// 2000-01-15T00:00:00Z
const YEAR_2000: f64 = 947_894_400.0;

/// The 15th of every month starting in January 2000, one sample per month.
fn monthly(count: usize) -> Vec<Sample<f64>> {
    (0..count)
        .map(|index| {
            let (year, month) = (2000 + index / 12, 1 + index % 12);
            let datetime: SensAppDateTime =
                hifitime::Epoch::from_gregorian_utc_at_midnight(year as i32, month as u8, 15);
            Sample {
                datetime,
                value: index as f64,
            }
        })
        .collect()
}

#[tokio::test]
#[serial]
async fn one_request_can_span_many_years() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    // Some backends partition by month and limit the partitions touched by one insert
    // (ClickHouse: 100 by default). 150 months is 12.5 years of history.
    let months = 150;

    let sensor = Arc::new(Sensor::new_without_uuid(
        format!("one_request_can_span_many_years_{}", Uuid::new_v4()),
        SensorType::Float,
        None,
        None,
    )?);
    let mut batch_builder = BatchBuilder::new()?;
    batch_builder
        .add(sensor.clone(), TypedSamples::Float(monthly(months).into()))
        .await?;
    batch_builder.send_what_is_left(storage.clone()).await?;

    let data = storage
        .query_sensor_data(&sensor.uuid.to_string(), None, None, None)
        .await?
        .expect("the series should exist");
    let TypedSamples::Float(samples) = &data.samples else {
        panic!("expected float samples");
    };
    assert_eq!(samples.len(), months);
    assert!(
        samples
            .iter()
            .any(|sample| (sample.datetime.to_unix_seconds() - YEAR_2000).abs() < 1.0)
    );
    Ok(())
}
