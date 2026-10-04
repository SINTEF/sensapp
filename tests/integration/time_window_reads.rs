//! A read honours each time bound on its own: no bound, a start only, an end only, both, and a
//! limit. This holds for every sample type and for the two ways of reading raw samples (one
//! series by UUID, several by labels). These tests run on the backend selected by
//! `TEST_DATABASE_URL`.
//!
//! SQLite once wrote the bounds as `(? IS NULL OR timestamp_us >= ?)`, which keeps the planner from
//! using the time column of its index: a window of one hour read the whole series (175 ms against
//! 0.06 ms on 1.3 M samples). The bounds are `timestamp_us >= COALESCE(?, <lowest>)` now, and one
//! bind serves each, so a wrong bind would show here as a wrong window.

use crate::common::TestDb;
use anyhow::Result;
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch_builder::BatchBuilder;
use sensapp::datamodel::sensapp_vec::SensAppLabels;
use sensapp::datamodel::{Sample, SensAppDateTime, Sensor, SensorType, TypedSamples};
use sensapp::storage::common::datetime_to_micros;
use sensapp::storage::query::LabelMatcher;
use serial_test::serial;
use std::sync::Arc;
use uuid::Uuid;

static INIT: std::sync::Once = std::sync::Once::new();

fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

/// 2024-01-01T00:00:00Z
const T0: f64 = 1_704_067_200.0;

/// The sample `i` is at minute `i`, there are ten of them.
fn at(minute: f64) -> SensAppDateTime {
    hifitime::Epoch::from_unix_seconds(T0 + minute * 60.0)
}

fn samples_of<V>(value: impl Fn(i64) -> V) -> sensapp::datamodel::SensAppVec<Sample<V>> {
    (0..10)
        .map(|i| Sample {
            datetime: at(i as f64),
            value: value(i),
        })
        .collect::<Vec<_>>()
        .into()
}

/// One series of ten samples for each type.
fn series() -> Vec<(&'static str, SensorType, TypedSamples)> {
    vec![
        (
            "integer",
            SensorType::Integer,
            TypedSamples::Integer(samples_of(|i| i)),
        ),
        (
            "float",
            SensorType::Float,
            TypedSamples::Float(samples_of(|i| i as f64)),
        ),
        (
            "numeric",
            SensorType::Numeric,
            TypedSamples::Numeric(samples_of(rust_decimal::Decimal::from)),
        ),
        (
            "string",
            SensorType::String,
            TypedSamples::String(samples_of(|i| format!("value {i}"))),
        ),
        (
            "boolean",
            SensorType::Boolean,
            TypedSamples::Boolean(samples_of(|i| i % 2 == 0)),
        ),
        (
            "location",
            SensorType::Location,
            TypedSamples::Location(samples_of(|i| geo::Point::new(i as f64, 60.0))),
        ),
        (
            "blob",
            SensorType::Blob,
            TypedSamples::Blob(samples_of(|i| vec![i as u8])),
        ),
        (
            "json",
            SensorType::Json,
            TypedSamples::Json(samples_of(|i| serde_json::json!({ "i": i }))),
        ),
    ]
}

/// The minute of each sample.
fn minutes(samples: &TypedSamples) -> Vec<i64> {
    macro_rules! minutes_of {
        ($values:expr) => {
            $values
                .iter()
                .map(|sample| {
                    (datetime_to_micros(&sample.datetime) - datetime_to_micros(&at(0.0)))
                        / 60_000_000
                })
                .collect()
        };
    }
    match samples {
        TypedSamples::Integer(values) => minutes_of!(values),
        TypedSamples::Numeric(values) => minutes_of!(values),
        TypedSamples::Float(values) => minutes_of!(values),
        TypedSamples::String(values) => minutes_of!(values),
        TypedSamples::Boolean(values) => minutes_of!(values),
        TypedSamples::Location(values) => minutes_of!(values),
        TypedSamples::Blob(values) => minutes_of!(values),
        TypedSamples::Json(values) => minutes_of!(values),
    }
}

type Window = (
    &'static str,
    Option<f64>,
    Option<f64>,
    Option<usize>,
    Vec<i64>,
);

/// The bounds are inclusive.
fn windows() -> Vec<Window> {
    vec![
        ("no bound", None, None, None, (0..=9).collect()),
        ("a start", Some(3.0), None, None, (3..=9).collect()),
        ("an end", None, Some(5.0), None, (0..=5).collect()),
        ("both", Some(3.0), Some(5.0), None, vec![3, 4, 5]),
        (
            "between two samples",
            Some(2.5),
            Some(4.5),
            None,
            vec![3, 4],
        ),
        ("a start and a limit", Some(3.0), None, Some(2), vec![3, 4]),
        ("an end and a limit", None, Some(5.0), Some(2), vec![0, 1]),
        ("after the data", Some(20.0), None, None, vec![]),
        ("before the data", None, Some(-5.0), None, vec![]),
    ]
}

#[tokio::test]
#[serial]
async fn raw_reads_honour_each_time_bound_for_every_type() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let run = Uuid::new_v4().to_string();

    let mut sensors = Vec::new();
    for (name, sensor_type, samples) in series() {
        let labels: SensAppLabels = [("run".to_string(), run.clone())].into_iter().collect();
        let sensor = Arc::new(Sensor::new_without_uuid(
            format!("window_{name}"),
            sensor_type,
            None,
            Some(labels),
        )?);
        let mut batch_builder = BatchBuilder::new()?;
        batch_builder.add(sensor.clone(), samples).await?;
        batch_builder.send_what_is_left(storage.clone()).await?;
        sensors.push((name, sensor));
    }

    for (name, sensor) in &sensors {
        for (window, start, end, limit, expected) in windows() {
            let data = storage
                .query_sensor_data(&sensor.uuid.to_string(), start.map(at), end.map(at), limit)
                .await?;
            let found = data.map(|data| minutes(&data.samples)).unwrap_or_default();
            assert_eq!(found, expected, "{name}, {window}");
        }

        // The latest sample of the window
        for (window, start, end, limit, expected) in windows() {
            if limit.is_some() {
                continue;
            }
            let latest = storage
                .query_sensor_data_latest(&sensor.uuid.to_string(), start.map(at), end.map(at))
                .await?;
            let found = latest
                .map(|data| minutes(&data.samples))
                .unwrap_or_default();
            let expected: Vec<i64> = expected.last().copied().into_iter().collect();
            assert_eq!(found, expected, "{name}, latest sample, {window}");
        }
    }

    // Several series by labels (the bulk read). Its limit is not tested: it is a budget.
    let matchers = vec![LabelMatcher::eq("run", run)];
    for (window, start, end, limit, expected) in windows() {
        if limit.is_some() {
            continue;
        }
        let data = storage
            .query_sensors_by_labels(&matchers, start.map(at), end.map(at), None, false)
            .await?;
        for (name, sensor) in &sensors {
            let found = data
                .iter()
                .find(|data| data.sensor.uuid == sensor.uuid)
                .map(|data| minutes(&data.samples))
                .unwrap_or_default();
            assert_eq!(found, expected, "{name} by labels, {window}");
        }
    }
    Ok(())
}
