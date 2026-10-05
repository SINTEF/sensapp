//! TimescaleDB compresses the chunks older than a week with a background policy, so data older
//! than that is normally compressed, and tests that run for a while see it happen under their
//! feet. Everything SensApp does with a series must give the same answers on compressed chunks as
//! on the same data uncompressed. These tests record the answers, compress every chunk, and ask
//! again. They only run on TimescaleDB.

use crate::common::{DatabaseType, TestDb};
use anyhow::{Result, anyhow};
use sensapp::config::load_configuration_for_tests;
use sensapp::datamodel::batch_builder::BatchBuilder;
use sensapp::datamodel::sensapp_vec::SensAppLabels;
use sensapp::datamodel::{Sample, SensAppDateTime, Sensor, SensorType, TypedSamples};
use sensapp::storage::query::LabelMatcher;
use sensapp::storage::{Aggregation, SensorDataQueryOptions, StorageInstance};
use serial_test::serial;
use std::sync::Arc;
use uuid::Uuid;

static INIT: std::sync::Once = std::sync::Once::new();

fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

/// 2024-01-01T00:00:00Z, a Monday
const T0: f64 = 1_704_067_200.0;
const DAY: f64 = 86_400.0;

fn at(seconds: f64) -> SensAppDateTime {
    hifitime::Epoch::from_unix_seconds(T0 + seconds)
}

/// A sample every 6 hours during 5 weeks: six chunks of seven days.
const SAMPLES: usize = 5 * 7 * 4;

fn sensor(name: &str, sensor_type: SensorType, run: &str) -> Result<Arc<Sensor>> {
    let labels: SensAppLabels = [("run".to_string(), run.to_string())].into_iter().collect();
    Ok(Arc::new(Sensor::new_without_uuid(
        name.to_string(),
        sensor_type,
        None,
        Some(labels),
    )?))
}

async fn publish(
    storage: &Arc<dyn StorageInstance>,
    series: Vec<(Arc<Sensor>, TypedSamples)>,
) -> Result<()> {
    let mut batch_builder = BatchBuilder::new()?;
    for (sensor, samples) in series {
        batch_builder.add(sensor, samples).await?;
    }
    batch_builder.send_what_is_left(storage.clone()).await?;
    Ok(())
}

/// `count` samples from the sample number `first`, one every 6 hours, the same instants for every
/// sensor. The values depend on the `seed` of the sensor.
fn float_samples(seed: usize, first: usize, count: usize) -> TypedSamples {
    TypedSamples::Float(
        (first..first + count)
            .map(|i| Sample {
                datetime: at(i as f64 * DAY / 4.0),
                value: ((i + seed * 5) as f64 * 0.7).sin() * 10.0 + 20.0,
            })
            .collect::<Vec<_>>()
            .into(),
    )
}

fn integer_samples(seed: usize, first: usize, count: usize) -> TypedSamples {
    TypedSamples::Integer(
        (first..first + count)
            .map(|i| Sample {
                datetime: at(i as f64 * DAY / 4.0),
                value: ((i + seed * 5) as i64 * 7) % 23,
            })
            .collect::<Vec<_>>()
            .into(),
    )
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

const AGGREGATIONS: [Aggregation; 8] = [
    Aggregation::Avg,
    Aggregation::Min,
    Aggregation::Max,
    Aggregation::Sum,
    Aggregation::Count,
    Aggregation::First,
    Aggregation::Last,
    Aggregation::Latest,
];

/// A series as text. Floats are rounded: a sum added in another order, as it is on compressed
/// data, differs in the last digit.
fn show(data: &sensapp::datamodel::SensorData) -> String {
    fn rows<T>(samples: &[Sample<T>], print: impl Fn(&T) -> String) -> String {
        samples
            .iter()
            .map(|s| format!("{}:{}", s.datetime.to_unix_seconds(), print(&s.value)))
            .collect::<Vec<_>>()
            .join(" ")
    }
    let samples = match &data.samples {
        TypedSamples::Float(s) => rows(s, |v| format!("{v:.6}")),
        TypedSamples::Integer(s) => rows(s, |v| v.to_string()),
        other => format!("{other:?}"),
    };
    format!("{} [{}]", data.sensor.name, samples)
}

fn show_series(data: &[sensapp::datamodel::SensorData]) -> String {
    data.iter().map(show).collect::<Vec<_>>().join(" | ")
}

fn show_read(read: &sensapp::storage::SelectorRead) -> String {
    match read {
        Ok(series) => show_series(series),
        Err(exceeded) => format!("{exceeded:?}"),
    }
}

/// Every answer SensApp gives about the two series and the selector, as labelled text.
async fn answers(
    storage: &Arc<dyn StorageInstance>,
    sensors: &[&Arc<Sensor>],
    run: &str,
) -> Result<Vec<(String, String)>> {
    let mut answers = Vec::new();
    let mut record = |label: String, answer: Result<String>| -> Result<()> {
        answers.push((
            label.clone(),
            answer.map_err(|e| anyhow!("{label}: {e:#}"))?,
        ));
        Ok(())
    };
    let window = (Some(at(3.0 * DAY)), Some(at(25.5 * DAY)));
    for sensor in sensors {
        let uuid = sensor.uuid.to_string();
        let name = &sensor.name;
        record(
            format!("{name}: everything"),
            storage
                .query_sensor_data(&uuid, None, None, None)
                .await
                .map(|data| data.as_ref().map(show).unwrap_or_default()),
        )?;
        record(
            format!("{name}: a window with a limit"),
            storage
                .query_sensor_data(&uuid, window.0, window.1, Some(10))
                .await
                .map(|data| data.as_ref().map(show).unwrap_or_default()),
        )?;
        record(
            format!("{name}: latest"),
            storage
                .query_sensor_data_latest(&uuid, None, None)
                .await
                .map(|data| data.as_ref().map(show).unwrap_or_default()),
        )?;
        record(
            format!("{name}: availability"),
            storage
                .query_sensor_data_availability(&uuid, at(0.0), at(36.0 * DAY), Some(86_400_000))
                .await
                .map(|summary| format!("{summary:?}")),
        )?;
        for aggregation in AGGREGATIONS {
            for (what, options) in [
                ("unbounded", options(None, None, 86_400_000, aggregation)),
                (
                    "a window that does not line up",
                    options(
                        Some(2.3 * DAY),
                        Some(30.1 * DAY),
                        3 * 86_400_000 + 1_000,
                        aggregation,
                    ),
                ),
            ] {
                record(
                    format!("{name}: {} {what}", aggregation.name()),
                    storage
                        .query_sensor_data_advanced(&uuid, &options)
                        .await
                        .map(|data| data.as_ref().map(show).unwrap_or_default()),
                )?;
            }
        }
    }
    let matchers = vec![LabelMatcher::eq("run", run.to_string())];
    record(
        "selector".to_string(),
        storage
            .query_selector(&matchers, None, None, false, 256, 1_000_000)
            .await
            .map(|read| show_read(&read)),
    )?;
    for aggregation in AGGREGATIONS {
        for (what, options) in [
            ("unbounded", options(None, None, 86_400_000, aggregation)),
            (
                "a window",
                options(
                    Some(1.0 * DAY),
                    Some(29.0 * DAY),
                    2 * 86_400_000,
                    aggregation,
                ),
            ),
        ] {
            record(
                format!("selector aggregated: {} {what}", aggregation.name()),
                storage
                    .query_selector_aggregated(&matchers, &options, 256, 1_000_000)
                    .await
                    .map(|read| show_read(&read)),
            )?;
        }
    }
    Ok(answers)
}

/// Compress the chunks that end before `older_than` (all of them without it).
async fn compress(test_db: &TestDb, older_than: Option<&str>) -> Result<()> {
    let pool = sqlx::PgPool::connect(&test_db.connection_string.replacen(
        "timescaledb://",
        "postgres://",
        1,
    ))
    .await?;
    let bound = older_than
        .map(|date| format!(", older_than => TIMESTAMPTZ '{date}'"))
        .unwrap_or_default();
    for table in sensapp::storage::common::VALUE_TABLES {
        sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(format!(
            "SELECT count(compress_chunk(chunk, if_not_compressed => true)) \
             FROM show_chunks('{table}'{bound}) chunk"
        )))
        .fetch_one(&pool)
        .await?;
    }
    Ok(())
}

/// Make some compressed chunks partially compressed without changing the data: deleting a sample
/// from a compressed chunk decompresses its batch, and writing it again puts the row in the
/// uncompressed part of the chunk. This is what a late or retried write does.
async fn make_chunks_partial(
    storage: &Arc<dyn StorageInstance>,
    sensor: &Arc<Sensor>,
    samples: impl Fn(usize) -> TypedSamples,
    sample_indexes: &[usize],
) -> Result<()> {
    for index in sample_indexes {
        let when = *index as f64 * DAY / 4.0;
        let deleted = storage
            .delete_series_samples(&sensor.uuid.to_string(), at(when - 60.0), at(when + 60.0))
            .await?;
        assert_eq!(deleted, Some(1), "the sample {index} is deleted");
        publish(storage, vec![(sensor.clone(), samples(*index))]).await?;
    }
    Ok(())
}

/// Six float and two integer series. The value tables are partitioned by time and by a hash of
/// the sensor over two slices: with several sensors every week has two chunks that overlap in
/// time, as in any real database.
struct Fixture {
    test_db: TestDb,
    run: String,
    floats: Vec<Arc<Sensor>>,
    integers: Vec<Arc<Sensor>>,
}

impl Fixture {
    fn sensors(&self) -> Vec<&Arc<Sensor>> {
        self.floats.iter().chain(&self.integers).collect()
    }
}

/// Compress one chunk of each pair of chunks that share a time range, and leave the other: the two
/// slices of the sensor hash are not in the same state, as when the policy compresses chunks one
/// after the other.
async fn compress_one_of_each_pair(test_db: &TestDb) -> Result<()> {
    let pool = sqlx::PgPool::connect(&test_db.connection_string.replacen(
        "timescaledb://",
        "postgres://",
        1,
    ))
    .await?;
    for table in sensapp::storage::common::VALUE_TABLES {
        sqlx::query_scalar::<_, i64>(sqlx::AssertSqlSafe(format!(
            "SELECT count(compress_chunk(first.chunk_id::regclass, if_not_compressed => true)) \
             FROM (SELECT DISTINCT ON (range_start) format('%I.%I', chunk_schema, chunk_name) AS chunk_id \
                   FROM timescaledb_information.chunks WHERE hypertable_name = '{table}' \
                   ORDER BY range_start, chunk_name) first"
        )))
        .fetch_one(&pool)
        .await?;
    }
    Ok(())
}

async fn setup() -> Result<Option<Fixture>> {
    ensure_config();
    let test_db = TestDb::new().await?;
    if test_db.db_type != DatabaseType::TimescaleDB {
        return Ok(None);
    }
    let run = Uuid::new_v4().to_string();
    let mut series = Vec::new();
    let mut floats = Vec::new();
    let mut integers = Vec::new();
    for index in 0..6 {
        let sensor = sensor(
            &format!("compressed_float_{index}"),
            SensorType::Float,
            &run,
        )?;
        series.push((sensor.clone(), float_samples(index, 0, SAMPLES)));
        floats.push(sensor);
    }
    for index in 0..2 {
        let sensor = sensor(
            &format!("compressed_integer_{index}"),
            SensorType::Integer,
            &run,
        )?;
        series.push((sensor.clone(), integer_samples(index, 0, SAMPLES)));
        integers.push(sensor);
    }
    publish(&test_db.storage(), series).await?;
    Ok(Some(Fixture {
        test_db,
        run,
        floats,
        integers,
    }))
}

#[tokio::test]
#[serial]
async fn reads_give_the_same_answers_on_compressed_chunks() -> Result<()> {
    let Some(fixture) = setup().await? else {
        return Ok(());
    };
    let (test_db, run) = (&fixture.test_db, &fixture.run);
    let storage = test_db.storage();
    let sensors = fixture.sensors();

    let uncompressed = answers(&storage, &sensors, run).await?;
    // Only one chunk of each week compressed: twin chunks in different states
    compress_one_of_each_pair(test_db).await?;
    let one_of_each_pair = answers(&storage, &sensors, run).await?;
    // The first three weeks compressed, the rest not: queries span both kinds of chunks, as they
    // do when the policy has compressed the old part of a series
    compress(test_db, Some("2024-01-22")).await?;
    let mixed = answers(&storage, &sensors, run).await?;
    compress(test_db, None).await?;
    let compressed = answers(&storage, &sensors, run).await?;

    // Chunks that were compressed and received a write since: partially compressed, in both
    // slices of the sensor hash. Several of them in one query, one per week
    for (seed, sensor) in fixture.floats.iter().enumerate() {
        let samples = |first| float_samples(seed, first, 1);
        make_chunks_partial(&storage, sensor, samples, &[2, 30, 58, 86, 114]).await?;
    }
    for (seed, sensor) in fixture.integers.iter().enumerate() {
        let samples = |first| integer_samples(seed, first, 1);
        make_chunks_partial(&storage, sensor, samples, &[3, 31, 59, 87]).await?;
    }
    let partial = answers(&storage, &sensors, run).await?;

    for (state, answers) in [
        ("one chunk of each pair compressed", &one_of_each_pair),
        ("half compressed", &mixed),
        ("compressed", &compressed),
        ("partially compressed", &partial),
    ] {
        assert_eq!(uncompressed.len(), answers.len());
        for ((label, expected), (_, got)) in uncompressed.iter().zip(answers) {
            assert_eq!(expected, got, "{state}: {label}");
        }
    }
    Ok(())
}

#[tokio::test]
#[serial]
async fn writes_reach_compressed_chunks() -> Result<()> {
    let Some(fixture) = setup().await? else {
        return Ok(());
    };
    let test_db = &fixture.test_db;
    let (floats, integers) = (fixture.floats[0].clone(), fixture.integers[0].clone());
    let storage = test_db.storage();
    compress(test_db, None).await?;
    let count = |sensor: &Arc<Sensor>| {
        let (storage, uuid) = (storage.clone(), sensor.uuid.to_string());
        async move {
            Ok::<_, anyhow::Error>(
                storage
                    .query_sensor_data(&uuid, None, None, None)
                    .await?
                    .map_or(0, |data| data.samples.len()),
            )
        }
    };
    assert_eq!(count(&floats).await?, SAMPLES);

    // A sample in the middle of the compressed history, and a retry of a sample that is there
    let late = TypedSamples::Float(
        vec![Sample {
            datetime: at(10.0 * DAY + 3600.0),
            value: 99.0,
        }]
        .into(),
    );
    publish(&storage, vec![(floats.clone(), late)]).await?;
    publish(&storage, vec![(floats.clone(), float_samples(0, 40, 1))]).await?;
    assert_eq!(count(&floats).await?, SAMPLES + 2);

    // Deleting a range of the history: the samples between day 7 and day 14
    let deleted = storage
        .delete_series_samples(&floats.uuid.to_string(), at(7.0 * DAY), at(14.0 * DAY))
        .await?;
    assert!(deleted.is_some_and(|deleted| deleted >= 28), "{deleted:?}");
    let remaining = count(&floats).await?;
    assert!(remaining < SAMPLES - 20, "{remaining} samples remain");
    assert_eq!(
        count(&integers).await?,
        SAMPLES,
        "the other series is untouched"
    );

    // Deleting a series, and the vacuum, on compressed data
    assert!(storage.delete_series(&integers.uuid.to_string()).await?);
    assert_eq!(count(&integers).await?, 0);
    storage.deduplicate_samples().await?;
    storage.vacuum().await?;
    assert_eq!(count(&floats).await?, remaining);
    Ok(())
}
