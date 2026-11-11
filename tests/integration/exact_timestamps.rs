//! Timestamps come back exactly as written, for every type of series. Some reads rebuilt them
//! through `f64` seconds and lost a microsecond; an open end (`i64::MAX` milliseconds, as a
//! Prometheus remote read sends) became 1970 on TimescaleDB and the read returned nothing.
//! These tests run on the backend selected by `TEST_DATABASE_URL`.

use crate::common::{DatabaseType, TestDb};
use anyhow::Result;
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch::{Batch, SingleSensorBatch};
use sensapp::datamodel::sensapp_datetime::SensAppDateTimeExt;
use sensapp::datamodel::sensapp_vec::SensAppVec;
use sensapp::datamodel::{Sample, SensAppDateTime, Sensor, SensorType, TypedSamples};
use sensapp::storage::common::datetime_to_micros;
use serial_test::serial;
use std::sync::Arc;

static INIT: std::sync::Once = std::sync::Once::new();

fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

/// Millisecond timestamps in 2004, 2026 and 2039, where the `f64` conversions were wrong.
const MILLIS: [i64; 9] = [
    1_073_741_824_001,
    1_073_741_824_123,
    1_073_741_825_999,
    1_794_268_800_001,
    1_794_268_800_250,
    1_794_268_800_999,
    2_177_452_800_001,
    2_177_452_800_123,
    2_177_452_800_999,
];

fn samples_of(sensor_type: SensorType) -> TypedSamples {
    let at = |ms: i64| SensAppDateTime::from_unix_milliseconds_i64(ms);
    match sensor_type {
        SensorType::Integer => TypedSamples::Integer(
            MILLIS
                .iter()
                .map(|&ms| Sample {
                    datetime: at(ms),
                    value: 1,
                })
                .collect(),
        ),
        SensorType::Float => TypedSamples::Float(
            MILLIS
                .iter()
                .map(|&ms| Sample {
                    datetime: at(ms),
                    value: 1.5,
                })
                .collect(),
        ),
        SensorType::Numeric => TypedSamples::Numeric(
            MILLIS
                .iter()
                .map(|&ms| Sample {
                    datetime: at(ms),
                    value: rust_decimal::Decimal::new(15, 1),
                })
                .collect(),
        ),
        SensorType::String => TypedSamples::String(
            MILLIS
                .iter()
                .map(|&ms| Sample {
                    datetime: at(ms),
                    value: "on".to_string(),
                })
                .collect(),
        ),
        SensorType::Boolean => TypedSamples::Boolean(
            MILLIS
                .iter()
                .map(|&ms| Sample {
                    datetime: at(ms),
                    value: true,
                })
                .collect(),
        ),
        SensorType::Json => TypedSamples::Json(
            MILLIS
                .iter()
                .map(|&ms| Sample {
                    datetime: at(ms),
                    value: serde_json::json!({"on": true}),
                })
                .collect(),
        ),
        other => panic!("not used here: {other:?}"),
    }
}

fn micros_of(samples: &TypedSamples) -> Vec<i64> {
    let micros = |datetimes: Vec<&SensAppDateTime>| -> Vec<i64> {
        datetimes.into_iter().map(datetime_to_micros).collect()
    };
    match samples {
        TypedSamples::Integer(s) => micros(s.iter().map(|s| &s.datetime).collect()),
        TypedSamples::Float(s) => micros(s.iter().map(|s| &s.datetime).collect()),
        TypedSamples::Numeric(s) => micros(s.iter().map(|s| &s.datetime).collect()),
        TypedSamples::String(s) => micros(s.iter().map(|s| &s.datetime).collect()),
        TypedSamples::Boolean(s) => micros(s.iter().map(|s| &s.datetime).collect()),
        TypedSamples::Json(s) => micros(s.iter().map(|s| &s.datetime).collect()),
        other => panic!("not used here: {other:?}"),
    }
}

#[tokio::test]
#[serial]
async fn timestamps_read_back_exactly_for_every_type() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    // RRDcached keeps seconds, in a round-robin archive
    if test_db.db_type == DatabaseType::RRDcached {
        return Ok(());
    }
    let storage = test_db.storage();
    let expected: Vec<i64> = MILLIS.iter().map(|ms| ms * 1000).collect();
    // As far as a date goes: the open end of a Prometheus remote read
    let open_end = SensAppDateTime::from_unix_milliseconds_i64(i64::MAX);

    for sensor_type in [
        SensorType::Integer,
        SensorType::Float,
        SensorType::Numeric,
        SensorType::String,
        SensorType::Boolean,
        SensorType::Json,
    ] {
        let sensor = Arc::new(Sensor::new_without_uuid(
            format!("exact_{sensor_type}_{}", uuid::Uuid::new_v4().simple()),
            sensor_type,
            None,
            None,
        )?);
        let mut batches = SensAppVec::new();
        batches.push(SingleSensorBatch::new(
            sensor.clone(),
            samples_of(sensor_type),
        ));
        storage.publish(Arc::new(Batch::new(batches))).await?;

        let uuid = sensor.uuid.to_string();
        let read = storage.query_sensor_data(&uuid, None, None, None).await?;
        assert_eq!(micros_of(&read.unwrap().samples), expected, "{sensor_type}");

        let start = SensAppDateTime::from_unix_milliseconds_i64(MILLIS[0]);
        let read = storage
            .query_sensor_data(&uuid, Some(start), Some(open_end), None)
            .await?;
        assert_eq!(
            micros_of(&read.unwrap().samples),
            expected,
            "{sensor_type}, open end"
        );
    }
    Ok(())
}
