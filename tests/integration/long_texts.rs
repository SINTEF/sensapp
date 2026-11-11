//! Texts longer than a B-tree entry (about 2.7 kB on PostgreSQL) are stored and found again: a
//! string value, a label value and a sensor name of several kilobytes. PostgreSQL and TimescaleDB
//! refused them with "index row size exceeds btree maximum", and the whole batch with them. These
//! tests run on the backend selected by `TEST_DATABASE_URL`.

use crate::common::TestDb;
use anyhow::Result;
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch_builder::BatchBuilder;
use sensapp::datamodel::sensapp_vec::SensAppLabels;
use sensapp::datamodel::{Sample, Sensor, SensorType, TypedSamples};
use sensapp::storage::query::LabelMatcher;
use serial_test::serial;
use std::sync::Arc;

static INIT: std::sync::Once = std::sync::Once::new();

fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

/// A text of `length` characters that does not compress: TOAST compresses before the index
/// sees the value, so a repeated character would fit in a B-tree entry.
fn text(seed: u64, length: usize) -> String {
    let mut rng = fastrand::Rng::with_seed(seed);
    (0..length).map(|_| rng.alphanumeric()).collect()
}

#[tokio::test]
#[serial]
async fn long_strings_labels_and_names_are_stored_and_found() -> Result<()> {
    ensure_config();
    let db = TestDb::new().await?;
    let storage = db.storage();

    let name = format!("long_{}", text(1, 5_000));
    let label_value = text(2, 10_000);
    let string_value = text(3, 10_000);
    let labels: SensAppLabels = [("note".to_string(), label_value.clone())]
        .into_iter()
        .collect();
    let sensor = Arc::new(Sensor::new_without_uuid(
        name.clone(),
        SensorType::String,
        None,
        Some(labels),
    )?);
    let datetime = hifitime::Epoch::from_unix_seconds(1_704_067_200.0);
    let mut batch = BatchBuilder::new()?;
    batch
        .add(
            sensor.clone(),
            TypedSamples::String(
                vec![
                    Sample {
                        datetime,
                        value: string_value.clone(),
                    },
                    // The same long value again: one dictionary entry
                    Sample {
                        datetime: datetime + hifitime::Duration::from_seconds(1.0),
                        value: string_value.clone(),
                    },
                ]
                .into(),
            ),
        )
        .await?;
    batch.send_what_is_left(storage.clone()).await?;

    // Written a second time, by another batch: found in the dictionaries, not added twice
    let mut batch = BatchBuilder::new()?;
    batch
        .add(
            sensor.clone(),
            TypedSamples::String(
                vec![Sample {
                    datetime: datetime + hifitime::Duration::from_seconds(2.0),
                    value: string_value.clone(),
                }]
                .into(),
            ),
        )
        .await?;
    batch.send_what_is_left(storage.clone()).await?;

    let data = storage
        .query_sensor_data(&sensor.uuid.to_string(), None, None, None)
        .await?
        .expect("the series");
    assert_eq!(data.sensor.name, name);
    match &data.samples {
        TypedSamples::String(samples) => {
            assert_eq!(samples.len(), 3);
            assert!(samples.iter().all(|sample| sample.value == string_value));
        }
        other => panic!("not strings: {other:?}"),
    }

    // Found by its long name and its long label value
    let found = storage
        .query_sensors_by_labels(
            &[
                LabelMatcher::eq("__name__", name.clone()),
                LabelMatcher::eq("note", label_value.clone()),
            ],
            None,
            None,
            Some(1),
            false,
        )
        .await?;
    assert_eq!(found.len(), 1);
    assert_eq!(found[0].sensor.uuid, sensor.uuid);
    let labels: Vec<_> = found[0].sensor.labels.iter().collect();
    assert_eq!(labels, vec![&("note".to_string(), label_value)]);
    Ok(())
}
