//! Tests of the backend against a scripted daemon: they pin what SensApp sends and how it
//! reads the answers. What the real daemon does with it is in `tests/integration/rrdcached_integration.rs`.

use super::connection::{ClientResult, RRDCachedClientTrait, RRDCachedConnectorTrait};
use super::*;
use crate::datamodel::{
    batch::{Batch, SingleSensorBatch},
    sensapp_vec::SensAppVec,
};
use rrdcached_client::batch_update::BatchUpdate;
use std::{
    collections::VecDeque,
    io,
    sync::{
        Mutex as StdMutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

const MISSING_FILE: &str =
    "RRD Error: opening '/var/lib/rrdcached/db/x.rrd': No such file or directory";

fn missing_file() -> RRDCachedClientError {
    RRDCachedClientError::UnexpectedResponse(-1, MISSING_FILE.to_string())
}

fn existing_file() -> RRDCachedClientError {
    RRDCachedClientError::UnexpectedResponse(
        -1,
        "RRD Error: creating '/var/lib/rrdcached/db/x.rrd': File exists".to_string(),
    )
}

fn io_error() -> RRDCachedClientError {
    RRDCachedClientError::Io(io::Error::new(io::ErrorKind::BrokenPipe, "boom"))
}

/// What the scripted daemon does, and what it was asked.
#[derive(Debug, Default)]
struct Script {
    /// Do the files exist? They do after a `CREATE`.
    files_exist: bool,
    last: VecDeque<ClientResult<usize>>,
    create: VecDeque<ClientResult<()>>,
    batch: VecDeque<ClientResult<()>>,
    list: VecDeque<ClientResult<Vec<String>>>,
    fetch: VecDeque<ClientResult<FetchResponse>>,
    /// The next batch never answers
    hang_batch: bool,

    lasts: usize,
    creates: Vec<String>,
    /// The commands of every batch received
    batches: Vec<Vec<String>>,
    /// Path, start and end of every fetch
    fetches: Vec<(String, i64, i64)>,
    lists: usize,
    pings: usize,
}

type SharedScript = Arc<StdMutex<Script>>;

#[derive(Debug)]
struct MockClient {
    script: SharedScript,
}

#[async_trait]
impl RRDCachedClientTrait for MockClient {
    async fn create(&mut self, args: CreateArguments) -> ClientResult<()> {
        let mut script = self.script.lock().unwrap();
        script.creates.push(args.to_str());
        // A daemon asked with `-O` does not replace a file that exists
        let exists = args.no_overwrite && script.files_exist;
        let result = script
            .create
            .pop_front()
            .unwrap_or_else(|| if exists { Err(existing_file()) } else { Ok(()) });
        if result.is_ok() {
            script.files_exist = true;
        }
        result
    }

    async fn batch(&mut self, batch_updates: Vec<BatchUpdate>) -> ClientResult<()> {
        let hang = {
            let mut script = self.script.lock().unwrap();
            script.batches.push(
                batch_updates
                    .iter()
                    .map(|update| update.to_command_string().unwrap().trim().to_string())
                    .collect(),
            );
            std::mem::take(&mut script.hang_batch)
        };
        if hang {
            std::future::pending::<()>().await;
        }
        self.script
            .lock()
            .unwrap()
            .batch
            .pop_front()
            .unwrap_or(Ok(()))
    }

    async fn list(&mut self, _recursive: bool, _path: Option<&str>) -> ClientResult<Vec<String>> {
        let mut script = self.script.lock().unwrap();
        script.lists += 1;
        script.list.pop_front().unwrap_or(Ok(Vec::new()))
    }

    async fn fetch(
        &mut self,
        path: &str,
        _consolidation_function: ConsolidationFunction,
        start: Option<i64>,
        end: Option<i64>,
    ) -> ClientResult<FetchResponse> {
        let mut script = self.script.lock().unwrap();
        script
            .fetches
            .push((path.to_string(), start.unwrap(), end.unwrap()));
        script
            .fetch
            .pop_front()
            .unwrap_or_else(|| Ok(response(10, &[])))
    }

    async fn last(&mut self, _path: &str) -> ClientResult<usize> {
        let mut script = self.script.lock().unwrap();
        script.lasts += 1;
        match script.last.pop_front() {
            Some(result) => result,
            None if script.files_exist => Ok(0),
            None => Err(missing_file()),
        }
    }

    async fn ping(&mut self) -> ClientResult<()> {
        self.script.lock().unwrap().pings += 1;
        Ok(())
    }
}

#[derive(Debug)]
struct MockConnector {
    script: SharedScript,
    connect_calls: AtomicUsize,
    /// How many of the next connections are refused
    refuse: StdMutex<usize>,
}

impl MockConnector {
    fn connect_calls(&self) -> usize {
        self.connect_calls.load(Ordering::SeqCst)
    }
}

#[async_trait]
impl RRDCachedConnectorTrait for MockConnector {
    async fn connect(&self) -> ClientResult<Box<dyn RRDCachedClientTrait>> {
        self.connect_calls.fetch_add(1, Ordering::SeqCst);
        {
            let mut refuse = self.refuse.lock().unwrap();
            if *refuse > 0 {
                *refuse -= 1;
                return Err(io_error());
            }
        }
        Ok(Box::new(MockClient {
            script: self.script.clone(),
        }))
    }
}

struct Fixture {
    storage: RrdCachedStorage,
    script: SharedScript,
    connector: Arc<MockConnector>,
}

fn fixture(script: Script) -> Fixture {
    let script = Arc::new(StdMutex::new(script));
    let connector = Arc::new(MockConnector {
        script: script.clone(),
        connect_calls: AtomicUsize::new(0),
        refuse: StdMutex::new(0),
    });
    let client = Box::new(MockClient {
        script: script.clone(),
    });
    Fixture {
        storage: RrdCachedStorage::new_for_test(client, connector.clone()),
        script,
        connector,
    }
}

impl Fixture {
    fn script(&self) -> std::sync::MutexGuard<'_, Script> {
        self.script.lock().unwrap()
    }
}

fn response(step: usize, rows: &[(usize, f64)]) -> FetchResponse {
    FetchResponse {
        flush_version: 1,
        start: rows.first().map_or(0, |row| row.0.saturating_sub(step)),
        end: rows.last().map_or(0, |row| row.0),
        step,
        ds_count: 1,
        ds_names: vec![DATA_SOURCE_NAME.to_string()],
        data: rows
            .iter()
            .map(|(time, value)| (*time, vec![*value]))
            .collect(),
    }
}

fn id(n: u128) -> Uuid {
    Uuid::from_u128(n)
}

fn sensor(uuid: Uuid, sensor_type: SensorType) -> Arc<Sensor> {
    Arc::new(Sensor {
        uuid,
        name: "name".to_string(),
        sensor_type,
        unit: None,
        labels: SmallVec::new(),
    })
}

fn float_batch(series: &[(Uuid, &[(i64, f64)])]) -> Arc<Batch> {
    let mut sensors = SensAppVec::new();
    for (uuid, points) in series {
        let samples = points
            .iter()
            .map(|(time, value)| Sample {
                datetime: SensAppDateTime::from_unix_seconds_i64(*time),
                value: *value,
            })
            .collect();
        sensors.push(SingleSensorBatch::new(
            sensor(*uuid, SensorType::Float),
            TypedSamples::Float(samples),
        ));
    }
    Arc::new(Batch::new(sensors))
}

fn at(seconds: i64) -> Option<SensAppDateTime> {
    Some(SensAppDateTime::from_unix_seconds_i64(seconds))
}

fn values(data: &SensorData) -> Vec<(i64, f64)> {
    match &data.samples {
        TypedSamples::Float(samples) => samples
            .iter()
            .map(|sample| (unix_seconds(&sample.datetime), sample.value))
            .collect(),
        other => panic!("expected float samples, got {other:?}"),
    }
}

fn storage_error_of(error: &anyhow::Error) -> &StorageError {
    error
        .downcast_ref::<StorageError>()
        .unwrap_or_else(|| panic!("not a storage error: {error:#}"))
}

// ---- connection string

fn settings(connection: &str) -> Result<Settings> {
    Settings::from_url(&Url::parse(connection).unwrap())
}

#[test]
fn settings_default_to_the_hoarder_preset_and_an_hour_of_heartbeat() {
    assert_eq!(
        settings("rrdcached://localhost:42217").unwrap(),
        Settings {
            preset: Preset::Hoarder,
            heartbeat_seconds: 3600,
        }
    );
}

#[test]
fn settings_read_the_preset_and_the_heartbeat() {
    assert_eq!(
        settings("rrdcached://localhost:42217?preset=munin&heartbeat=900").unwrap(),
        Settings {
            preset: Preset::Munin,
            heartbeat_seconds: 900,
        }
    );
}

#[test]
fn settings_refuse_what_they_do_not_understand() {
    for connection in [
        "rrdcached://localhost:42217?preset=nope",
        "rrdcached://localhost:42217?heartbeat=soon",
        "rrdcached://localhost:42217?heartbeat=5",
        "rrdcached://localhost:42217?heartbeat=-1",
        "rrdcached://localhost:42217?hartbeat=900",
    ] {
        assert!(settings(connection).is_err(), "{connection}");
    }
}

// ---- creation of the files

#[tokio::test]
async fn a_new_series_is_created_with_the_heartbeat_and_a_start_before_its_first_sample() {
    let f = fixture(Script::default());
    f.storage
        .publish(float_batch(&[(
            id(1),
            &[(1_000_050, 1.0), (1_000_000, 0.5)],
        )]))
        .await
        .unwrap();

    let script = f.script();
    assert_eq!(script.creates.len(), 1);
    let create = &script.creates[0];
    assert!(
        create.starts_with(&format!("{}.rrd -s 10 -b 999990 ", id(1))),
        "{create}"
    );
    assert!(create.contains("DS:sensapp:GAUGE:3600:U:U"), "{create}");
}

#[tokio::test]
async fn an_existing_file_is_not_replaced() {
    // A restart, or another instance, finds the files that are there already: the daemon is asked
    // not to overwrite, and says the file exists
    let f = fixture(Script {
        files_exist: true,
        ..Script::default()
    });
    f.storage
        .publish(float_batch(&[(id(1), &[(1_000_000, 1.0)])]))
        .await
        .unwrap();

    let script = f.script();
    assert_eq!(script.creates.len(), 1);
    assert!(script.creates[0].contains(" -O "), "{}", script.creates[0]);
    assert_eq!(script.batches.len(), 1);
}

#[tokio::test]
async fn a_known_series_is_not_created_again() {
    let f = fixture(Script::default());
    for time in [1_000_000, 1_000_010] {
        f.storage
            .publish(float_batch(&[(id(1), &[(time, 1.0)])]))
            .await
            .unwrap();
    }
    let script = f.script();
    assert_eq!((script.creates.len(), script.batches.len()), (1, 2));
}

#[tokio::test]
async fn concurrent_writes_of_a_new_series_never_replace_its_file() {
    let f = Arc::new(fixture(Script::default()));
    let writes = (0..8).map(|i| {
        let f = f.clone();
        tokio::spawn(async move {
            f.storage
                .publish(float_batch(&[(id(1), &[(1_000_000 + i * 10, 1.0)])]))
                .await
        })
    });
    for write in futures::future::join_all(writes).await {
        write.unwrap().unwrap();
    }
    // However many requests found the series new, each creation refuses to overwrite
    let script = f.script();
    assert!(!script.creates.is_empty());
    assert!(script.creates.iter().all(|create| create.contains(" -O ")));
}

#[tokio::test]
async fn a_file_created_by_another_instance_in_the_meantime_is_fine() {
    let f = fixture(Script {
        create: VecDeque::from([Err(RRDCachedClientError::UnexpectedResponse(
            -1,
            "RRD Error: creating '/var/lib/rrdcached/db/x.rrd': File exists".to_string(),
        ))]),
        ..Script::default()
    });
    f.storage
        .publish(float_batch(&[(id(1), &[(1_000_000, 1.0)])]))
        .await
        .unwrap();
    assert_eq!(f.script().batches.len(), 1);
}

#[tokio::test]
async fn a_failed_creation_is_an_error_and_nothing_is_written() {
    let f = fixture(Script {
        create: VecDeque::from([Err(RRDCachedClientError::UnexpectedResponse(
            -1,
            "/var/lib/rrdcached/db/x.rrd: Permission denied".to_string(),
        ))]),
        ..Script::default()
    });
    let error = f
        .storage
        .publish(float_batch(&[(id(1), &[(1_000_000, 1.0)])]))
        .await
        .unwrap_err();
    assert!(matches!(
        storage_error_of(&error),
        StorageError::OperationFailed { .. }
    ));
    assert!(f.script().batches.is_empty());
}

#[tokio::test]
async fn series_without_numbers_get_no_file() {
    let f = fixture(Script::default());
    let mut sensors = SensAppVec::new();
    sensors.push(SingleSensorBatch::new(
        sensor(id(1), SensorType::String),
        TypedSamples::String(SensAppVec::from_vec(vec![Sample {
            datetime: SensAppDateTime::from_unix_seconds_i64(1_000_000),
            value: "text".to_string(),
        }])),
    ));
    sensors.push(SingleSensorBatch::new(
        sensor(id(2), SensorType::Float),
        TypedSamples::Float(SensAppVec::new()),
    ));
    f.storage
        .publish(Arc::new(Batch::new(sensors)))
        .await
        .unwrap();

    let script = f.script();
    assert!(script.creates.is_empty());
    assert!(script.batches.is_empty());
}

// ---- updates

#[tokio::test]
async fn updates_are_sorted_deduplicated_and_in_whole_seconds() {
    let f = fixture(Script {
        files_exist: true,
        ..Script::default()
    });
    f.storage
        .publish(float_batch(&[(
            id(1),
            &[
                (1_000_020, 3.0),
                (1_000_000, 1.0),
                (1_000_010, 2.0),
                (1_000_010, 2.5),
            ],
        )]))
        .await
        .unwrap();

    let name = id(1);
    assert_eq!(
        f.script().batches,
        vec![vec![
            format!("UPDATE {name}.rrd 1000000:1"),
            format!("UPDATE {name}.rrd 1000010:2.5"),
            format!("UPDATE {name}.rrd 1000020:3"),
        ]]
    );
}

#[tokio::test]
async fn numbers_and_booleans_are_stored_as_floats() {
    use rust_decimal::Decimal;

    let f = fixture(Script {
        files_exist: true,
        ..Script::default()
    });
    let time = SensAppDateTime::from_unix_seconds_i64(1_000_000);
    let mut sensors = SensAppVec::new();
    sensors.push(SingleSensorBatch::new(
        sensor(id(1), SensorType::Integer),
        TypedSamples::Integer(SensAppVec::from_vec(vec![Sample {
            datetime: time,
            value: 42,
        }])),
    ));
    sensors.push(SingleSensorBatch::new(
        sensor(id(2), SensorType::Numeric),
        TypedSamples::Numeric(SensAppVec::from_vec(vec![Sample {
            datetime: time,
            value: Decimal::new(1234, 2),
        }])),
    ));
    sensors.push(SingleSensorBatch::new(
        sensor(id(3), SensorType::Boolean),
        TypedSamples::Boolean(SensAppVec::from_vec(vec![Sample {
            datetime: time,
            value: true,
        }])),
    ));
    f.storage
        .publish(Arc::new(Batch::new(sensors)))
        .await
        .unwrap();

    assert_eq!(
        f.script().batches,
        vec![vec![
            format!("UPDATE {}.rrd 1000000:42", id(1)),
            format!("UPDATE {}.rrd 1000000:12.34", id(2)),
            format!("UPDATE {}.rrd 1000000:1", id(3)),
        ]]
    );
}

fn stale_refusal() -> RRDCachedClientError {
    RRDCachedClientError::BatchUpdateErrorResponse(
        "errors".to_string(),
        vec!["1 illegal attempt to update using time 5 when last update time is 5 (minimum one second step)\n".to_string()],
    )
}

#[tokio::test]
async fn updates_older_than_the_file_are_not_a_failure() {
    // A request that is sent again, or samples that arrive late: the rest was stored
    let f = fixture(Script {
        files_exist: true,
        batch: VecDeque::from([Err(stale_refusal())]),
        ..Script::default()
    });
    f.storage
        .publish(float_batch(&[(id(1), &[(1_000_000, 1.0)])]))
        .await
        .unwrap();
}

#[tokio::test]
async fn another_refusal_fails_the_write_and_the_files_are_looked_for_again() {
    let f = fixture(Script {
        batch: VecDeque::from([Err(RRDCachedClientError::BatchUpdateErrorResponse(
            "errors".to_string(),
            vec!["1 /var/lib/rrdcached/db/x.rrd: No such file or directory\n".to_string()],
        ))]),
        ..Script::default()
    });
    let batch = float_batch(&[(id(1), &[(1_000_000, 1.0)])]);
    let error = f.storage.publish(batch.clone()).await.unwrap_err();
    assert!(matches!(
        storage_error_of(&error),
        StorageError::OperationFailed { .. }
    ));

    // The file was removed behind our back: the next write creates it again
    f.script().files_exist = false;
    f.storage.publish(batch).await.unwrap();
    let script = f.script();
    assert_eq!(script.creates.len(), 2);
}

#[tokio::test]
async fn unreachable_daemon_is_unavailable() {
    let f = fixture(Script {
        files_exist: true,
        batch: VecDeque::from([Err(io_error())]),
        ..Script::default()
    });
    *f.connector.refuse.lock().unwrap() = 1;
    let error = f
        .storage
        .publish(float_batch(&[(id(1), &[(1_000_000, 1.0)])]))
        .await
        .unwrap_err();
    assert!(matches!(
        storage_error_of(&error),
        StorageError::Unavailable(_)
    ));
}

// ---- the connection

#[tokio::test]
async fn a_broken_connection_is_replaced_and_the_request_is_repeated() {
    let f = fixture(Script {
        files_exist: true,
        batch: VecDeque::from([Err(io_error())]),
        ..Script::default()
    });
    f.storage
        .publish(float_batch(&[(id(1), &[(1_000_000, 1.0)])]))
        .await
        .unwrap();
    assert_eq!(f.connector.connect_calls(), 1);
    assert_eq!(f.script().batches.len(), 2);
}

#[tokio::test]
async fn a_refusal_of_the_daemon_does_not_replace_the_connection() {
    let f = fixture(Script::default());
    let _ = f
        .storage
        .query_sensor_data(&id(1).to_string(), None, None, None)
        .await;
    f.script()
        .fetch
        .push_back(Err(RRDCachedClientError::UnexpectedResponse(
            -1,
            "something else".to_string(),
        )));
    f.storage
        .query_sensor_data(&id(1).to_string(), at(0), at(100), None)
        .await
        .unwrap_err();
    assert_eq!(f.connector.connect_calls(), 0);
}

#[tokio::test]
async fn a_cancelled_request_does_not_leave_its_answer_to_the_next_one() {
    // The HTTP timeout drops a request after it sent its commands: its reply is still to come
    // on that stream. The next request must not read it.
    let f = fixture(Script {
        files_exist: true,
        hang_batch: true,
        ..Script::default()
    });
    let hanging = tokio::time::timeout(
        Duration::from_millis(50),
        f.storage
            .publish(float_batch(&[(id(1), &[(1_000_000, 1.0)])])),
    )
    .await;
    assert!(hanging.is_err(), "the write was meant to hang");
    assert_eq!(f.connector.connect_calls(), 0);

    f.storage.health_check().await.unwrap();
    assert_eq!(f.connector.connect_calls(), 1, "a fresh connection is used");
}

#[tokio::test(start_paused = true)]
async fn a_daemon_that_does_not_answer_is_abandoned() {
    let f = fixture(Script {
        files_exist: true,
        hang_batch: true,
        ..Script::default()
    });
    let started = tokio::time::Instant::now();
    // The first attempt hangs, the second is on a new connection and goes through
    f.storage
        .publish(float_batch(&[(id(1), &[(1_000_000, 1.0)])]))
        .await
        .unwrap();
    assert!(started.elapsed() >= super::connection::OPERATION_TIMEOUT);
    assert_eq!(f.connector.connect_calls(), 1);
}

#[tokio::test]
async fn the_health_check_is_a_ping() {
    let f = fixture(Script::default());
    f.storage.health_check().await.unwrap();
    let script = f.script();
    assert_eq!(script.pings, 1);
    assert!(script.batches.is_empty() && script.lists == 0);
}

// ---- reads

#[test]
fn a_row_is_in_the_window_when_its_interval_meets_it() {
    let rows = response(10, &[(100, 1.0), (110, 2.0), (120, 3.0), (130, 4.0)]);
    // A sample at 101 is in the row 110, one at 120 in the row 120, 121 in the row 130
    assert_eq!(
        rows_in_window(&rows, 101, 120),
        vec![(110, 2.0), (120, 3.0)]
    );
    assert_eq!(
        rows_in_window(&rows, 100, 121),
        vec![(100, 1.0), (110, 2.0), (120, 3.0), (130, 4.0)]
    );
    assert_eq!(rows_in_window(&rows, 111, 111), vec![(120, 3.0)]);
    assert!(rows_in_window(&rows, 131, 200).is_empty());
}

#[tokio::test]
async fn rows_come_back_as_samples_without_the_unknown_ones() {
    let f = fixture(Script {
        fetch: VecDeque::from([Ok(response(
            10,
            &[(100, f64::NAN), (110, 1.5), (120, f64::NAN), (130, 2.5)],
        ))]),
        ..Script::default()
    });
    let data = f
        .storage
        .query_sensor_data(&id(1).to_string(), at(101), at(130), None)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(data.sensor.uuid, id(1));
    assert_eq!(values(&data), vec![(110, 1.5), (130, 2.5)]);
    // One second before the start, so that the row of a sample at `start` is in the answer
    assert_eq!(f.script().fetches, vec![(id(1).to_string(), 100, 130)]);
}

#[tokio::test]
async fn the_limit_keeps_the_first_samples() {
    let f = fixture(Script {
        fetch: VecDeque::from([Ok(response(10, &[(110, 1.0), (120, 2.0), (130, 3.0)]))]),
        ..Script::default()
    });
    let data = f
        .storage
        .query_sensor_data(&id(1).to_string(), at(100), at(130), Some(2))
        .await
        .unwrap()
        .unwrap();
    assert_eq!(values(&data), vec![(110, 1.0), (120, 2.0)]);
}

#[tokio::test]
async fn a_series_with_nothing_in_the_window_is_empty_not_missing() {
    let f = fixture(Script {
        fetch: VecDeque::from([Ok(response(10, &[(110, f64::NAN), (120, f64::NAN)]))]),
        ..Script::default()
    });
    let data = f
        .storage
        .query_sensor_data(&id(1).to_string(), at(100), at(120), None)
        .await
        .unwrap()
        .unwrap();
    assert!(values(&data).is_empty());
}

#[tokio::test]
async fn a_series_without_a_file_is_missing_and_other_errors_are_errors() {
    let f = fixture(Script {
        fetch: VecDeque::from([Err(missing_file()), Err(io_error()), Err(io_error())]),
        ..Script::default()
    });
    let uuid = id(1).to_string();
    assert!(
        f.storage
            .query_sensor_data(&uuid, at(0), at(10), None)
            .await
            .unwrap()
            .is_none()
    );
    let error = f
        .storage
        .query_sensor_data(&uuid, at(0), at(10), None)
        .await
        .unwrap_err();
    assert!(matches!(
        storage_error_of(&error),
        StorageError::Unavailable(_)
    ));
}

#[tokio::test]
async fn a_window_that_ends_before_it_starts_is_empty() {
    let f = fixture(Script {
        files_exist: true,
        ..Script::default()
    });
    let data = f
        .storage
        .query_sensor_data(&id(1).to_string(), at(200), at(100), None)
        .await
        .unwrap()
        .unwrap();
    assert!(values(&data).is_empty());
    let script = f.script();
    assert!(script.fetches.is_empty());
    assert_eq!(script.lasts, 1);
}

#[tokio::test]
async fn a_window_without_end_stops_now_and_without_start_covers_the_retention() {
    let f = fixture(Script::default());
    f.storage
        .query_sensor_data(&id(1).to_string(), None, None, None)
        .await
        .unwrap();
    let (_, start, end) = f.script().fetches[0].clone();
    assert!((end - now_seconds()).abs() <= 2);
    assert_eq!(end - (start + 1), Preset::Hoarder.retention_seconds());
}

#[tokio::test]
async fn times_before_1970_are_clamped() {
    let f = fixture(Script::default());
    f.storage
        .query_sensor_data(&id(1).to_string(), at(-500), at(100), None)
        .await
        .unwrap();
    // A negative time would be a relative one for RRDtool
    let (_, start, _) = f.script().fetches[0].clone();
    assert_eq!(start, 1);
}

#[tokio::test]
async fn the_freshest_rows_of_a_long_window_come_from_finer_archives() {
    let f = fixture(Script {
        // The last update is at 150: the row 180 of the archive of minutes is not there yet
        last: VecDeque::from([Ok(150)]),
        fetch: VecDeque::from([
            Ok(response(60, &[(60, 1.0), (120, 2.0), (180, f64::NAN)])),
            // The seconds after the boundary of the minute, from the archive of 10 seconds
            Ok(response(
                10,
                &[(130, 2.2), (140, 2.4), (150, 2.6), (160, f64::NAN)],
            )),
        ]),
        ..Script::default()
    });
    let data = f
        .storage
        .query_sensor_data(&id(1).to_string(), at(0), at(180), None)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        values(&data),
        vec![(60, 1.0), (120, 2.0), (130, 2.2), (140, 2.4), (150, 2.6)]
    );
    let uuid = id(1).to_string();
    // The short window after the boundary (120) is what makes the daemon use the finer archive
    assert_eq!(
        f.script().fetches,
        vec![(uuid.clone(), 1, 180), (uuid, 120, 180)]
    );
}

#[tokio::test]
async fn the_fresh_rows_are_found_when_the_whole_window_is_incomplete() {
    // A new series: nothing is consolidated yet in the coarse archive
    let f = fixture(Script {
        last: VecDeque::from([Ok(95)]),
        fetch: VecDeque::from([
            Ok(response(60, &[(60, f64::NAN), (120, f64::NAN)])),
            Ok(response(10, &[(70, 1.0), (80, 2.0), (90, 3.0)])),
        ]),
        ..Script::default()
    });
    let data = f
        .storage
        .query_sensor_data(&id(1).to_string(), at(0), at(120), None)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(values(&data), vec![(70, 1.0), (80, 2.0), (90, 3.0)]);
    // Never longer than one coarse interval, whatever the end of the window
    let script = f.script();
    let (_, from, to) = &script.fetches[1];
    assert_eq!((*from, *to), (60, 120));
}

#[tokio::test]
async fn the_fresh_rows_search_goes_down_the_archives() {
    // Hours, then minutes, then seconds: each answer has its own last row incomplete
    let f = fixture(Script {
        last: VecDeque::from([Ok(7_300)]),
        fetch: VecDeque::from([
            Ok(response(
                3600,
                &[(3600, 1.0), (7200, 2.0), (10_800, f64::NAN)],
            )),
            Ok(response(60, &[(7260, 3.0), (7320, f64::NAN)])),
            Ok(response(
                10,
                &[
                    (7270, 3.1),
                    (7280, 3.2),
                    (7290, 3.3),
                    (7300, 3.4),
                    (7310, f64::NAN),
                ],
            )),
        ]),
        ..Script::default()
    });
    let data = f
        .storage
        .query_sensor_data(&id(1).to_string(), at(0), at(10_800), None)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(
        values(&data),
        vec![
            (3600, 1.0),
            (7200, 2.0),
            (7260, 3.0),
            (7270, 3.1),
            (7280, 3.2),
            (7290, 3.3),
            (7300, 3.4)
        ]
    );
    let uuid = id(1).to_string();
    assert_eq!(
        f.script().fetches,
        vec![
            (uuid.clone(), 1, 10_800),
            (uuid.clone(), 7200, 10_800),
            (uuid, 7260, 7320)
        ]
    );
    assert_eq!(f.script().lasts, 1, "the last update is asked once");
}

#[tokio::test]
async fn no_fresh_tail_is_read_when_the_window_is_complete_or_already_the_finest() {
    // The last row is known
    let f = fixture(Script {
        fetch: VecDeque::from([Ok(response(60, &[(60, 1.0), (120, 2.0)]))]),
        ..Script::default()
    });
    f.storage
        .query_sensor_data(&id(1).to_string(), at(0), at(120), None)
        .await
        .unwrap();
    assert_eq!(f.script().fetches.len(), 1);
    assert_eq!(f.script().lasts, 0);

    // The finest archive has no finer one to complete it
    let f = fixture(Script {
        fetch: VecDeque::from([Ok(response(10, &[(10, 1.0), (20, f64::NAN)]))]),
        ..Script::default()
    });
    f.storage
        .query_sensor_data(&id(1).to_string(), at(0), at(20), None)
        .await
        .unwrap();
    assert_eq!(f.script().fetches.len(), 1);

    // The window ends before the last update: the rows after a gap are complete
    let f = fixture(Script {
        last: VecDeque::from([Ok(10_000)]),
        fetch: VecDeque::from([Ok(response(60, &[(60, 1.0), (120, f64::NAN)]))]),
        ..Script::default()
    });
    f.storage
        .query_sensor_data(&id(1).to_string(), at(0), at(120), None)
        .await
        .unwrap();
    assert_eq!(f.script().fetches.len(), 1);
}

#[tokio::test]
async fn the_fresh_rows_search_stops_when_no_finer_archive_answers() {
    let f = fixture(Script {
        last: VecDeque::from([Ok(10)]),
        fetch: VecDeque::from([
            Ok(response(60, &[(60, f64::NAN), (120, f64::NAN)])),
            Ok(response(60, &[(60, f64::NAN)])),
        ]),
        ..Script::default()
    });
    let data = f
        .storage
        .query_sensor_data(&id(1).to_string(), at(0), at(120), None)
        .await
        .unwrap()
        .unwrap();
    assert!(values(&data).is_empty());
    assert_eq!(f.script().fetches.len(), 2);
}

// ---- listing

fn listing(names: &[&str]) -> Script {
    Script {
        list: VecDeque::from([Ok(names.iter().map(|name| name.to_string()).collect())]),
        ..Script::default()
    }
}

#[tokio::test]
async fn only_the_files_of_series_are_listed_in_order() {
    let a = id(0xa).to_string();
    let b = id(0xb).to_string();
    let c = id(0xc).to_string();
    let f = fixture(listing(&[
        &format!("{c}.rrd\n"),
        "munin-load.rrd\n",
        &format!("sub/{a}.rrd\n"),
        &format!("{}.rrd\n", id(0xd).to_string().to_uppercase()),
        &format!("{b}.rrd\n"),
        "notes.txt\n",
    ]));
    let listed = f.storage.list_series(None, None, None).await.unwrap();
    let uuids: Vec<Uuid> = listed.series.iter().map(|sensor| sensor.uuid).collect();
    assert_eq!(uuids, vec![id(0xa), id(0xb), id(0xc)]);
    assert!(listed.bookmark.is_none());
    assert_eq!(listed.series[0].name, a);
}

#[tokio::test]
async fn pages_follow_each_other_without_gaps_or_repeats() {
    let names: Vec<String> = [5u128, 1, 4, 2, 3]
        .iter()
        .map(|n| format!("{}.rrd", id(*n)))
        .collect();
    let names: Vec<&str> = names.iter().map(String::as_str).collect();
    let f = fixture(Script::default());

    let mut seen = Vec::new();
    let mut bookmark: Option<String> = None;
    let mut pages = 0;
    loop {
        f.script()
            .list
            .push_back(Ok(names.iter().map(|n| n.to_string()).collect()));
        let page = f
            .storage
            .list_series(None, Some(2), bookmark.as_deref())
            .await
            .unwrap();
        seen.extend(page.series.iter().map(|sensor| sensor.uuid));
        pages += 1;
        bookmark = page.bookmark;
        if bookmark.is_none() {
            break;
        }
    }
    assert_eq!(seen, (1..=5).map(id).collect::<Vec<_>>());
    assert_eq!(pages, 3);
}

#[tokio::test]
async fn a_page_that_ends_the_list_has_no_bookmark() {
    let names: Vec<String> = [1u128, 2]
        .iter()
        .map(|n| format!("{}.rrd", id(*n)))
        .collect();
    let f = fixture(listing(
        &names.iter().map(String::as_str).collect::<Vec<_>>(),
    ));
    let page = f.storage.list_series(None, Some(2), None).await.unwrap();
    assert_eq!(page.series.len(), 2);
    assert!(page.bookmark.is_none());
}

#[tokio::test]
async fn the_metric_filter_selects_by_name() {
    let f = fixture(listing(&[
        &format!("{}.rrd", id(0x1)),
        &format!("{}.rrd", id(0x2)),
    ]));
    let page = f
        .storage
        .list_series(Some(&id(0x2).to_string()), None, None)
        .await
        .unwrap();
    assert_eq!(page.series.len(), 1);
    assert_eq!(page.series[0].uuid, id(0x2));
}

#[tokio::test]
async fn a_listing_that_fails_is_an_error_not_a_guess() {
    let f = fixture(Script {
        list: VecDeque::from([Err(RRDCachedClientError::UnexpectedResponse(
            -1,
            "no".to_string(),
        ))]),
        ..Script::default()
    });
    assert!(f.storage.list_series(None, None, None).await.is_err());
}

// ---- selectors

#[tokio::test]
async fn a_selector_on_a_uuid_does_not_list_the_files() {
    let f = fixture(Script {
        fetch: VecDeque::from([Ok(response(10, &[(110, 1.0)]))]),
        ..Script::default()
    });
    let found = f
        .storage
        .query_sensors_by_labels(
            &[LabelMatcher::eq("__name__", id(7).to_string())],
            at(100),
            at(120),
            None,
            true,
        )
        .await
        .unwrap();
    assert_eq!(found.len(), 1);
    assert_eq!(f.script().lists, 0);
}

#[tokio::test]
async fn selectors_match_the_names_and_nothing_else() {
    let names: Vec<String> = [0x10u128, 0x11, 0x20]
        .iter()
        .map(|n| format!("{}.rrd", id(*n)))
        .collect();
    let all = || listing(&names.iter().map(String::as_str).collect::<Vec<_>>());
    let count = |matchers: Vec<LabelMatcher>| async move {
        let f = fixture(all());
        f.storage.matching_sensors(&matchers).await.unwrap().len()
    };

    // The names are UUIDs: `00000000-0000-0000-0000-0000000000XX`
    assert_eq!(
        count(vec![LabelMatcher::regex("__name__", ".*0010")]).await,
        1
    );
    assert_eq!(
        count(vec![LabelMatcher::regex("__name__", ".*00(10|11)")]).await,
        2
    );
    assert_eq!(
        count(vec![LabelMatcher::not_regex("__name__", ".*0010")]).await,
        2
    );
    assert_eq!(
        count(vec![LabelMatcher::neq("__name__", id(0x10).to_string())]).await,
        2
    );
    // Anchored: a part of the name is not the name
    assert_eq!(
        count(vec![LabelMatcher::regex("__name__", "0010")]).await,
        0
    );
    // The series have no labels
    assert_eq!(count(vec![LabelMatcher::eq("job", "x")]).await, 0);
    assert_eq!(count(vec![LabelMatcher::neq("job", "x")]).await, 3);
    assert_eq!(
        count(vec![
            LabelMatcher::regex("__name__", ".*"),
            LabelMatcher::eq("job", "x")
        ])
        .await,
        0
    );
    assert_eq!(count(vec![]).await, 0);
    // A pattern that is not a regex matches nothing, and its negation everything
    assert_eq!(count(vec![LabelMatcher::regex("__name__", "(")]).await, 0);
    assert_eq!(
        count(vec![LabelMatcher::not_regex("__name__", "(")]).await,
        3
    );
}

#[tokio::test]
async fn a_selector_reads_each_series_once() {
    let names: Vec<String> = [1u128, 2, 3]
        .iter()
        .map(|n| format!("{}.rrd", id(*n)))
        .collect();
    let f = fixture(listing(
        &names.iter().map(String::as_str).collect::<Vec<_>>(),
    ));
    for _ in 0..3 {
        f.script()
            .fetch
            .push_back(Ok(response(10, &[(110, 1.0), (120, 2.0)])));
    }
    let read = f
        .storage
        .query_selector(
            &[LabelMatcher::regex("__name__", ".*")],
            at(100),
            at(120),
            true,
            10,
            100,
        )
        .await
        .unwrap()
        .unwrap();
    assert_eq!(read.len(), 3);
    assert_eq!(f.script().fetches.len(), 3);
}

#[tokio::test]
async fn a_selector_stops_at_its_limits() {
    let names: Vec<String> = [1u128, 2, 3]
        .iter()
        .map(|n| format!("{}.rrd", id(*n)))
        .collect();
    let all = || listing(&names.iter().map(String::as_str).collect::<Vec<_>>());
    let matchers = [LabelMatcher::regex("__name__", ".*")];

    let f = fixture(all());
    let read = f
        .storage
        .query_selector(&matchers, at(100), at(120), true, 2, 100)
        .await
        .unwrap();
    assert_eq!(read.unwrap_err(), SelectorLimitExceeded::Series);

    let f = fixture(all());
    for _ in 0..3 {
        f.script()
            .fetch
            .push_back(Ok(response(10, &[(110, 1.0), (120, 2.0)])));
    }
    let read = f
        .storage
        .query_selector(&matchers, at(100), at(120), true, 10, 5)
        .await
        .unwrap();
    assert_eq!(read.unwrap_err(), SelectorLimitExceeded::Samples);
}

#[tokio::test]
async fn an_aggregated_selector_aggregates_after_reading_and_leaves_out_empty_series() {
    use crate::storage::Aggregation;

    let names: Vec<String> = [1u128, 2]
        .iter()
        .map(|n| format!("{}.rrd", id(*n)))
        .collect();
    let f = fixture(listing(
        &names.iter().map(String::as_str).collect::<Vec<_>>(),
    ));
    f.script().fetch.push_back(Ok(response(
        10,
        &[(110, 1.0), (120, 3.0), (130, 5.0), (140, 7.0)],
    )));
    f.script()
        .fetch
        .push_back(Ok(response(10, &[(110, f64::NAN)])));
    let options = SensorDataQueryOptions {
        start_time: at(100),
        end_time: at(140),
        limit: None,
        step_ms: Some(20_000),
        aggregation: Some(Aggregation::Avg),
        simplify: None,
    };
    let read = f
        .storage
        .query_selector_aggregated(&[LabelMatcher::regex("__name__", ".*")], &options, 10, 100)
        .await
        .unwrap()
        .unwrap();

    assert_eq!(read.len(), 1);
    assert_eq!(read[0].sensor.uuid, id(1));
    assert_eq!(values(&read[0]), vec![(100, 1.0), (120, 4.0), (140, 7.0)]);
}

#[tokio::test]
async fn the_limit_of_an_aggregated_read_counts_the_buckets() {
    use crate::storage::Aggregation;

    let f = fixture(Script {
        fetch: VecDeque::from([Ok(response(
            10,
            &[(110, 1.0), (120, 3.0), (130, 5.0), (140, 7.0)],
        ))]),
        ..Script::default()
    });
    let options = SensorDataQueryOptions {
        start_time: at(100),
        end_time: at(140),
        limit: Some(1),
        step_ms: Some(20_000),
        aggregation: Some(Aggregation::Avg),
        simplify: None,
    };
    let data = f
        .storage
        .query_sensor_data_advanced(&id(1).to_string(), &options)
        .await
        .unwrap()
        .unwrap();
    // Three buckets of 20 seconds, the first one is kept: not one raw row cut before aggregating
    assert_eq!(values(&data), vec![(100, 1.0)]);
}
