//! `query_selector_aggregated` must give the same answer as the portable read, series by series,
//! whatever a backend does to make it cheaper: same buckets, same values and value types, same
//! series left out, same limits at their exact boundaries. These tests run on the backend
//! selected by `TEST_DATABASE_URL`.
//!
//! Each backend is compared with the portable read **on itself**. That proves a fast path does
//! not change the answer of its own backend; it is not a proof that the backends agree with each
//! other. What they share is the contract of the shared reader: the limits are global to the
//! selector (a series cap, and one budget of buckets for all the series together), never per series.

use crate::common::TestDb;
use anyhow::Result;
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch_builder::BatchBuilder;
use sensapp::datamodel::sensapp_vec::SensAppLabels;
use sensapp::datamodel::{Sample, SensAppDateTime, Sensor, SensorData, SensorType, TypedSamples};
use sensapp::storage::query::LabelMatcher;
use sensapp::storage::selector::query_selector_aggregated_sequential;
use sensapp::storage::{
    Aggregation, SelectorLimitExceeded, SelectorRead, SensorDataQueryOptions, StorageInstance,
};
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

/// 40 samples, 15 seconds apart, starting at `first_second`.
fn forty<T>(first_second: f64, value: impl Fn(usize) -> T) -> Vec<Sample<T>> {
    (0..40)
        .map(|index| Sample {
            datetime: at(first_second + index as f64 * 15.0),
            value: value(index),
        })
        .collect()
}

struct Fixture {
    run: String,
    /// Numeric series that match the selector, whatever their data
    numeric_series: usize,
}

async fn publish(storage: &Arc<dyn StorageInstance>) -> Result<Fixture> {
    let run = Uuid::new_v4().to_string();
    let mut pending: Vec<(Arc<Sensor>, TypedSamples)> = Vec::new();
    let mut add = |name: String, sensor_type: SensorType, samples: TypedSamples| {
        let labels: SensAppLabels = [("run".to_string(), run.clone())].into_iter().collect();
        let sensor = Arc::new(
            Sensor::new_without_uuid(name, sensor_type, None, Some(labels)).expect("sensor"),
        );
        pending.push((sensor, samples));
    };

    for index in 0..6 {
        add(
            format!("agg_float_{index}"),
            SensorType::Float,
            TypedSamples::Float(
                forty(0.0, |i| {
                    index as f64 * 100.0 + (i as f64 * 1.7).sin() * 10.0
                })
                .into(),
            ),
        );
    }
    for index in 0..4 {
        add(
            format!("agg_integer_{index}"),
            SensorType::Integer,
            TypedSamples::Integer(forty(0.0, |i| index * 1000 + (i as i64 * 7) % 31 - 15).into()),
        );
    }
    for index in 0..2 {
        add(
            format!("agg_numeric_{index}"),
            SensorType::Numeric,
            TypedSamples::Numeric(
                forty(0.0, |i| {
                    rust_decimal::Decimal::new(i as i64 * 37 + index, 3)
                })
                .into(),
            ),
        );
    }
    // Not numeric: never part of an aggregated selector
    add(
        "agg_string".to_string(),
        SensorType::String,
        TypedSamples::String(forty(0.0, |i| format!("s{i}")).into()),
    );
    // Numeric, but with its samples a day later: matches, has nothing in the windows
    add(
        "agg_late".to_string(),
        SensorType::Float,
        TypedSamples::Float(forty(86_400.0, |i| i as f64).into()),
    );

    let mut batch_builder = BatchBuilder::new()?;
    for (sensor, samples) in pending {
        batch_builder.add(sensor, samples).await?;
    }
    batch_builder.send_what_is_left(storage.clone()).await?;
    Ok(Fixture {
        run,
        numeric_series: 6 + 4 + 2 + 1,
    })
}

type Canonical = Vec<(String, Vec<(i64, String)>)>;

fn canonical(read: &SelectorRead) -> Result<Canonical, SelectorLimitExceeded> {
    let series: &Vec<SensorData> = read.as_ref().map_err(|limit| *limit)?;
    fn rows<T>(
        samples: &[Sample<T>],
        kind: &str,
        print: impl Fn(&T) -> String,
    ) -> Vec<(i64, String)> {
        let mut rows: Vec<(i64, String)> = samples
            .iter()
            .map(|sample| {
                (
                    (sample.datetime.to_unix_seconds() * 1e6).round() as i64,
                    format!("{kind}:{}", print(&sample.value)),
                )
            })
            .collect();
        rows.sort();
        rows
    }
    let mut out: Canonical = series
        .iter()
        .map(|data| {
            let samples = match &data.samples {
                TypedSamples::Integer(s) => rows(s, "integer", |v| v.to_string()),
                TypedSamples::Float(s) => rows(s, "float", |v| format!("{v:.9}")),
                TypedSamples::Numeric(s) => {
                    rows(s, "numeric", |v| v.round_dp(6).normalize().to_string())
                }
                other => panic!("unexpected samples {other:?}"),
            };
            (data.sensor.name.clone(), samples)
        })
        .collect();
    out.sort();
    Ok(out)
}

fn options(
    start: Option<f64>,
    end: Option<f64>,
    step_ms: i64,
    aggregation: Aggregation,
) -> SensorDataQueryOptions {
    SensorDataQueryOptions {
        start_time: start.map(at),
        end_time: end.map(at),
        limit: None,
        step_ms: Some(step_ms),
        aggregation: Some(aggregation),
        simplify: None,
    }
}

const ALL: [Aggregation; 7] = [
    Aggregation::Avg,
    Aggregation::Min,
    Aggregation::Max,
    Aggregation::Sum,
    Aggregation::Count,
    Aggregation::First,
    Aggregation::Last,
];
const BIG: usize = 1_000_000;

struct Case<'a> {
    storage: &'a Arc<dyn StorageInstance>,
    matchers: Vec<LabelMatcher>,
}

impl Case<'_> {
    async fn both(
        &self,
        options: &SensorDataQueryOptions,
        max_series: usize,
        max_samples: usize,
    ) -> Result<Result<Canonical, SelectorLimitExceeded>> {
        let actual = self
            .storage
            .query_selector_aggregated(&self.matchers, options, max_series, max_samples)
            .await?;
        let expected = query_selector_aggregated_sequential(
            self.storage.as_ref(),
            &self.matchers,
            options,
            max_series,
            max_samples,
        )
        .await?;
        let (actual, expected) = (canonical(&actual), canonical(&expected));
        assert_eq!(
            actual, expected,
            "{:?} step {:?} window {:?}..{:?} limits {max_series}/{max_samples}",
            options.aggregation, options.step_ms, options.start_time, options.end_time
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

#[tokio::test]
#[serial]
async fn every_aggregation_matches_the_portable_read() -> Result<()> {
    let (test_db, fixture) = setup().await?;
    let storage = test_db.storage();
    let case = Case {
        storage: &storage,
        matchers: vec![LabelMatcher::eq("run", fixture.run.clone())],
    };

    for aggregation in ALL {
        let all = case
            .both(
                &options(Some(0.0), Some(600.0), 60_000, aggregation),
                BIG,
                BIG,
            )
            .await?
            .expect("within limits");
        // 12 series have data in the window; the string series and the late one are left out
        assert_eq!(all.len(), 12, "{aggregation:?}");
        // Ten one-minute buckets of 4 samples each
        assert!(
            all.iter().all(|(_, samples)| samples.len() == 10),
            "{aggregation:?}"
        );
    }
    Ok(())
}

#[tokio::test]
#[serial]
async fn steps_and_windows_that_do_not_line_up_match_too() -> Result<()> {
    let (test_db, fixture) = setup().await?;
    let storage = test_db.storage();
    let case = Case {
        storage: &storage,
        matchers: vec![LabelMatcher::eq("run", fixture.run.clone())],
    };

    for aggregation in ALL {
        // A step that does not divide the interval of the samples, a window that starts in the
        // middle of a bucket and ends before the last sample
        case.both(
            &options(Some(20.0), Some(500.0), 45_000, aggregation),
            BIG,
            BIG,
        )
        .await?
        .expect("within limits");
        // No start, no end
        case.both(&options(None, None, 120_000, aggregation), BIG, BIG)
            .await?
            .expect("within limits");
        // Steps shorter than the interval of the samples leave empty buckets, which do not exist
        case.both(
            &options(Some(0.0), Some(600.0), 5_000, aggregation),
            BIG,
            BIG,
        )
        .await?
        .expect("within limits");
    }
    Ok(())
}

#[tokio::test]
#[serial]
async fn only_the_late_samples_and_empty_windows() -> Result<()> {
    let (test_db, fixture) = setup().await?;
    let storage = test_db.storage();
    let case = Case {
        storage: &storage,
        matchers: vec![LabelMatcher::eq("run", fixture.run.clone())],
    };

    // A day later only the late series has data
    let late = case
        .both(
            &options(Some(86_000.0), None, 60_000, Aggregation::Avg),
            BIG,
            BIG,
        )
        .await?
        .expect("within limits");
    assert_eq!(late.len(), 1);
    // Nowhere: nothing is returned, the series are left out rather than empty
    let none = case
        .both(
            &options(Some(-1.0e7), Some(-9.0e6), 60_000, Aggregation::Max),
            BIG,
            BIG,
        )
        .await?
        .expect("within limits");
    assert!(none.is_empty());
    // No such series
    let nothing = Case {
        storage: &storage,
        matchers: vec![LabelMatcher::eq("run", "no-such-run")],
    };
    assert_eq!(
        nothing
            .both(&options(None, None, 60_000, Aggregation::Avg), BIG, BIG)
            .await?,
        Ok(Vec::new())
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
    let options = options(Some(0.0), Some(600.0), 60_000, Aggregation::Avg);

    // Series: every numeric series that matches counts, with or without data in the window
    assert!(
        case.both(&options, fixture.numeric_series, BIG)
            .await?
            .is_ok()
    );
    assert_eq!(
        case.both(&options, fixture.numeric_series - 1, BIG).await?,
        Err(SelectorLimitExceeded::Series)
    );

    // Buckets: 12 series with 10 buckets each
    assert!(case.both(&options, BIG, 120).await?.is_ok());
    assert_eq!(
        case.both(&options, BIG, 119).await?,
        Err(SelectorLimitExceeded::Samples)
    );
    // Both exceeded: the series limit is the one reported
    assert_eq!(
        case.both(&options, 1, 1).await?,
        Err(SelectorLimitExceeded::Series)
    );
    Ok(())
}
