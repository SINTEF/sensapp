//! `query_selector` must give the same answer as the portable sequential read, whatever a
//! backend does to make it cheaper: same series, same samples, same limits at their exact
//! boundaries. These tests run on the backend selected by `TEST_DATABASE_URL`.

use crate::common::TestDb;
use anyhow::Result;
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::Sensor;
use sensapp::datamodel::batch_builder::BatchBuilder;
use sensapp::datamodel::sensapp_vec::SensAppLabels;
use sensapp::datamodel::{Sample, SensAppDateTime, SensorData, SensorType, TypedSamples};
use sensapp::storage::query::LabelMatcher;
use sensapp::storage::selector::query_selector_sequential;
use sensapp::storage::{SelectorLimitExceeded, SelectorRead, StorageInstance};
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

fn at(seconds: f64) -> SensAppDateTime {
    hifitime::Epoch::from_unix_seconds(T0 + seconds)
}

fn samples_of<T>(count: usize, first_second: f64, value: impl Fn(usize) -> T) -> Vec<Sample<T>> {
    (0..count)
        .map(|index| Sample {
            datetime: at(first_second + index as f64 * 60.0),
            value: value(index),
        })
        .collect()
}

/// What a test sensor holds: how many samples of which type, from when.
struct Fixture {
    run: String,
    /// Total number of samples in all the series
    total_samples: usize,
    series: usize,
    numeric_series: usize,
    numeric_samples: usize,
}

async fn publish(storage: &Arc<dyn StorageInstance>) -> Result<Fixture> {
    let run = Uuid::new_v4().to_string();
    let mut fixture = Fixture {
        run: run.clone(),
        total_samples: 0,
        series: 0,
        numeric_series: 0,
        numeric_samples: 0,
    };

    let mut add = |name: String, sensor_type: SensorType, samples: TypedSamples, count: usize| {
        let labels: SensAppLabels = [
            ("run".to_string(), run.clone()),
            ("group".to_string(), name.clone()),
        ]
        .into_iter()
        .collect();
        let sensor = Arc::new(
            Sensor::new_without_uuid(name, sensor_type, None, Some(labels)).expect("sensor"),
        );
        fixture.series += 1;
        fixture.total_samples += count;
        if matches!(
            sensor_type,
            SensorType::Integer | SensorType::Numeric | SensorType::Float
        ) {
            fixture.numeric_series += 1;
            fixture.numeric_samples += count;
        }
        (sensor, samples)
    };

    let mut pending = Vec::new();
    for index in 0..12 {
        let count = index % 4 + 3;
        pending.push(add(
            format!("selector_float_{index}"),
            SensorType::Float,
            TypedSamples::Float(
                samples_of(count, 0.0, |i| index as f64 * 10.0 + i as f64 + 0.25).into(),
            ),
            count,
        ));
    }
    for index in 0..3 {
        pending.push(add(
            format!("selector_integer_{index}"),
            SensorType::Integer,
            TypedSamples::Integer(samples_of(4, 0.0, |i| index * 100 + i as i64).into()),
            4,
        ));
    }
    for index in 0..2 {
        pending.push(add(
            format!("selector_numeric_{index}"),
            SensorType::Numeric,
            TypedSamples::Numeric(
                samples_of(2, 0.0, |i| {
                    rust_decimal::Decimal::new(i as i64 * 7 + index, 2)
                })
                .into(),
            ),
            2,
        ));
    }
    for index in 0..2 {
        pending.push(add(
            format!("selector_string_{index}"),
            SensorType::String,
            TypedSamples::String(samples_of(3, 0.0, |i| format!("state-{index}-{i}")).into()),
            3,
        ));
    }
    pending.push(add(
        "selector_boolean".to_string(),
        SensorType::Boolean,
        TypedSamples::Boolean(samples_of(3, 0.0, |i| i % 2 == 0).into()),
        3,
    ));
    // Two series whose only samples are a day later than the others
    for index in 0..2 {
        pending.push(add(
            format!("selector_late_{index}"),
            SensorType::Float,
            TypedSamples::Float(samples_of(2, 86_400.0, |i| 900.0 + i as f64).into()),
            2,
        ));
    }

    let mut batch_builder = BatchBuilder::new()?;
    for (sensor, samples) in pending {
        batch_builder.add(sensor, samples).await?;
    }
    batch_builder.send_what_is_left(storage.clone()).await?;
    Ok(fixture)
}

type Canonical = Vec<(String, String, Vec<(String, String)>, Vec<(i64, String)>)>;

/// Series with their labels and samples, in a form that does not depend on the order of the
/// series, nor on the scale a backend gives to decimals.
fn canonical(read: &SelectorRead) -> Result<Canonical, SelectorLimitExceeded> {
    let series: &Vec<SensorData> = read.as_ref().map_err(|limit| *limit)?;
    let mut out: Canonical = series
        .iter()
        .map(|data| {
            let mut labels: Vec<_> = data.sensor.labels.iter().cloned().collect();
            labels.sort();
            let mut rows: Vec<(i64, String)> = match &data.samples {
                TypedSamples::Integer(s) => rows(s, |v| v.to_string()),
                TypedSamples::Numeric(s) => rows(s, |v| v.normalize().to_string()),
                TypedSamples::Float(s) => rows(s, |v| format!("{v:?}")),
                TypedSamples::String(s) => rows(s, |v| v.clone()),
                TypedSamples::Boolean(s) => rows(s, |v| v.to_string()),
                other => panic!("unexpected samples {other:?}"),
            };
            rows.sort();
            (
                data.sensor.uuid.to_string(),
                data.sensor.name.clone(),
                labels,
                rows,
            )
        })
        .collect();
    out.sort();
    Ok(out)
}

fn rows<T>(samples: &[Sample<T>], print: impl Fn(&T) -> String) -> Vec<(i64, String)> {
    samples
        .iter()
        .map(|sample| {
            (
                (sample.datetime.to_unix_seconds() * 1e6).round() as i64,
                print(&sample.value),
            )
        })
        .collect()
}

struct Case<'a> {
    storage: &'a Arc<dyn StorageInstance>,
    matchers: Vec<LabelMatcher>,
}

impl Case<'_> {
    /// The backend's read and the portable read, which must agree.
    async fn both(
        &self,
        start: Option<SensAppDateTime>,
        end: Option<SensAppDateTime>,
        numeric_only: bool,
        max_series: usize,
        max_samples: usize,
    ) -> Result<Result<Canonical, SelectorLimitExceeded>> {
        let actual = self
            .storage
            .query_selector(
                &self.matchers,
                start,
                end,
                numeric_only,
                max_series,
                max_samples,
            )
            .await?;
        let expected = query_selector_sequential(
            self.storage.as_ref(),
            &self.matchers,
            start,
            end,
            numeric_only,
            max_series,
            max_samples,
        )
        .await?;
        let (actual, expected) = (canonical(&actual), canonical(&expected));
        assert_eq!(
            actual, expected,
            "window {start:?}..{end:?}, numeric_only {numeric_only}, limits {max_series}/{max_samples}"
        );
        Ok(actual)
    }
}

async fn setup() -> Result<(TestDb, Fixture)> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let fixture = publish(&test_db.storage()).await?;
    Ok((test_db, fixture))
}

const BIG: usize = 1_000_000;

#[tokio::test]
#[serial]
async fn every_type_matches_the_sequential_read() -> Result<()> {
    let (test_db, fixture) = setup().await?;
    let storage = test_db.storage();
    let case = Case {
        storage: &storage,
        matchers: vec![LabelMatcher::eq("run", fixture.run.clone())],
    };

    let all = case
        .both(None, None, false, BIG, BIG)
        .await?
        .expect("within limits");
    assert_eq!(all.len(), fixture.series);
    assert_eq!(
        all.iter().map(|series| series.3.len()).sum::<usize>(),
        fixture.total_samples
    );

    let numeric = case
        .both(None, None, true, BIG, BIG)
        .await?
        .expect("within limits");
    assert_eq!(numeric.len(), fixture.numeric_series);
    assert_eq!(
        numeric.iter().map(|series| series.3.len()).sum::<usize>(),
        fixture.numeric_samples
    );
    Ok(())
}

#[tokio::test]
#[serial]
async fn time_windows_match_the_sequential_read() -> Result<()> {
    let (test_db, fixture) = setup().await?;
    let storage = test_db.storage();
    let case = Case {
        storage: &storage,
        matchers: vec![LabelMatcher::eq("run", fixture.run.clone())],
    };

    // Inside the first minutes: the late series are returned empty
    let narrow = case
        .both(Some(at(60.0)), Some(at(180.0)), false, BIG, BIG)
        .await?
        .expect("within limits");
    assert_eq!(narrow.len(), fixture.series);
    assert!(narrow.iter().any(|series| series.3.is_empty()));

    // Only the late samples
    let late = case
        .both(Some(at(86_000.0)), None, false, BIG, BIG)
        .await?
        .expect("within limits");
    assert_eq!(late.iter().map(|series| series.3.len()).sum::<usize>(), 4);

    // No data at all in the window: every series is still there, empty
    let none = case
        .both(Some(at(-1.0e7)), Some(at(-9.0e6)), false, BIG, BIG)
        .await?
        .expect("within limits");
    assert_eq!(none.len(), fixture.series);
    assert!(none.iter().all(|series| series.3.is_empty()));
    Ok(())
}

#[tokio::test]
#[serial]
async fn selectors_matching_nothing_or_one_series() -> Result<()> {
    let (test_db, fixture) = setup().await?;
    let storage = test_db.storage();

    let nothing = Case {
        storage: &storage,
        matchers: vec![LabelMatcher::eq("run", "no-such-run")],
    };
    assert_eq!(
        nothing.both(None, None, false, BIG, BIG).await?,
        Ok(Vec::new())
    );

    let one = Case {
        storage: &storage,
        matchers: vec![
            LabelMatcher::eq("run", fixture.run.clone()),
            LabelMatcher::eq("__name__", "selector_boolean"),
        ],
    };
    assert_eq!(
        one.both(None, None, false, BIG, BIG)
            .await?
            .expect("one")
            .len(),
        1
    );
    // A boolean series is not numeric
    assert_eq!(one.both(None, None, true, BIG, BIG).await?, Ok(Vec::new()));

    let regex = Case {
        storage: &storage,
        matchers: vec![
            LabelMatcher::eq("run", fixture.run.clone()),
            LabelMatcher::regex("__name__", "selector_(integer|numeric)_.*"),
        ],
    };
    assert_eq!(
        regex
            .both(None, None, false, BIG, BIG)
            .await?
            .expect("five")
            .len(),
        5
    );
    Ok(())
}

#[tokio::test]
#[serial]
async fn limits_are_exact() -> Result<()> {
    let (test_db, fixture) = setup().await?;
    let storage = test_db.storage();
    let case = Case {
        storage: &storage,
        matchers: vec![LabelMatcher::eq("run", fixture.run.clone())],
    };

    // Series: the limit itself is allowed, one less is not
    assert!(
        case.both(None, None, false, fixture.series, BIG)
            .await?
            .is_ok()
    );
    assert_eq!(
        case.both(None, None, false, fixture.series - 1, BIG)
            .await?,
        Err(SelectorLimitExceeded::Series)
    );

    // Samples, every type and numeric only
    assert!(
        case.both(None, None, false, BIG, fixture.total_samples)
            .await?
            .is_ok()
    );
    assert_eq!(
        case.both(None, None, false, BIG, fixture.total_samples - 1)
            .await?,
        Err(SelectorLimitExceeded::Samples)
    );
    assert!(
        case.both(None, None, true, BIG, fixture.numeric_samples)
            .await?
            .is_ok()
    );
    assert_eq!(
        case.both(None, None, true, BIG, fixture.numeric_samples - 1)
            .await?,
        Err(SelectorLimitExceeded::Samples)
    );

    // Both exceeded: the series limit is reported, as the sequential read does
    assert_eq!(
        case.both(None, None, false, 1, 1).await?,
        Err(SelectorLimitExceeded::Series)
    );
    Ok(())
}
