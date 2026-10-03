//! Storage in RRDtool files, through the RRDCached daemon. See `docs/RRDCACHED.md` for what this
//! backend does differently from the others.
//!
//! One file per series, named after its UUID, with one gauge data source. Nothing else is stored,
//! so everything else about a series (name, labels, unit, type) is lost, and what is read back is
//! consolidated by the archives of the file.

mod connection;
mod preset;
mod updates;

use crate::datamodel::{
    Sample, Sensor, SensorData, SensorType, TypedSamples,
    sensapp_datetime::{SensAppDateTime, SensAppDateTimeExt},
    sensapp_vec::SensAppVec,
};
use crate::storage::{
    LabelMatcher, MatcherType, SelectorLimitExceeded, SelectorRead, SensorDataQueryOptions,
    StorageError, StorageInstance,
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use futures::FutureExt;
use regex::Regex;
use rrdcached_client::{
    consolidation_function::ConsolidationFunction,
    create::{CreateArguments, CreateDataSource, CreateDataSourceType},
    errors::RRDCachedClientError,
    fetch::FetchResponse,
};
use smallvec::SmallVec;
use std::{collections::HashSet, sync::Arc};
use tokio::sync::RwLock;
use tracing::{debug, warn};
use url::Url;
use uuid::Uuid;

use connection::{Connection, RRDCachedConnectionTarget, is_connection_error};
pub use preset::Preset;
use preset::{DEFAULT_HEARTBEAT_SECONDS, STEP_SECONDS};
use updates::{SeriesPoints, batch_updates, prepare_series, stale_updates};

/// The name of the one data source of the files.
const DATA_SOURCE_NAME: &str = "sensapp";

/// The freshest rows of a long window are not in the archive that serves the window (the row that
/// holds the last update is not complete yet): they are read from finer archives. This is how many
/// are tried, at most one per archive of a preset.
const MAX_FRESH_TAIL_FETCHES: usize = 4;

#[derive(Debug)]
pub struct RrdCachedStorage {
    connection: Connection,
    /// The series whose file is known to exist: a cache, to not ask the daemon at every write.
    known_files: RwLock<HashSet<Uuid>>,
    preset: Preset,
    heartbeat_seconds: i64,
}

/// Settings of a connection string: `rrdcached://host:port?preset=hoarder&heartbeat=3600`.
#[derive(Debug, PartialEq)]
struct Settings {
    preset: Preset,
    heartbeat_seconds: i64,
}

impl Settings {
    fn from_url(url: &Url) -> Result<Self> {
        let mut preset = Preset::Hoarder;
        let mut heartbeat_seconds = DEFAULT_HEARTBEAT_SECONDS;
        for (key, value) in url.query_pairs() {
            match key.as_ref() {
                "preset" => preset = value.parse()?,
                "heartbeat" => {
                    heartbeat_seconds = value.parse().with_context(|| {
                        format!("RRDCached heartbeat must be a number of seconds, got '{value}'")
                    })?;
                    if heartbeat_seconds < STEP_SECONDS as i64 {
                        bail!(
                            "RRDCached heartbeat must be at least {STEP_SECONDS} seconds, got {heartbeat_seconds}"
                        );
                    }
                }
                other => bail!(
                    "Unknown RRDCached connection parameter '{other}', expected 'preset' or 'heartbeat'"
                ),
            }
        }
        Ok(Self {
            preset,
            heartbeat_seconds,
        })
    }
}

/// The error of an operation, as the HTTP layer sorts them: a daemon that cannot be reached is
/// "unavailable", anything else is a failure of the operation.
fn storage_error(operation: &str, error: RRDCachedClientError) -> anyhow::Error {
    if is_connection_error(&error) {
        StorageError::Unavailable(format!("RRDCached {operation}: {error}")).into()
    } else {
        StorageError::OperationFailed {
            operation: format!("RRDCached {operation}"),
            details: error.to_string(),
        }
        .into()
    }
}

/// The daemon says a file does not exist.
fn is_missing_file(error: &RRDCachedClientError) -> bool {
    matches!(error, RRDCachedClientError::UnexpectedResponse(_, message) if message.contains("No such file"))
}

/// The daemon refuses to create a file that exists (it was started with `-O`).
fn is_existing_file(error: &RRDCachedClientError) -> bool {
    matches!(error, RRDCachedClientError::UnexpectedResponse(_, message) if message.contains("File exists"))
}

fn unix_seconds(datetime: &SensAppDateTime) -> i64 {
    datetime.to_unix_seconds().floor() as i64
}

fn now_seconds() -> i64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs() as i64)
}

/// What is known of a series that only exists as a file: its UUID.
fn sensor_from_uuid(uuid: Uuid) -> Sensor {
    Sensor {
        uuid,
        name: uuid.to_string(),
        sensor_type: SensorType::Float,
        unit: None,
        labels: SmallVec::new(),
    }
}

/// The rows of a fetch that belong to the window `[start, end]`, `NaN` rows included.
///
/// A row is stamped with the end of the interval it consolidates, `(time - step, time]`: it is
/// part of the window if it ends at or after `start`, and begins before `end`.
fn rows_in_window(response: &FetchResponse, start: i64, end: i64) -> Vec<(i64, f64)> {
    let step = response.step as i64;
    response
        .data
        .iter()
        .filter_map(|(time, values)| {
            let time = *time as i64;
            let value = *values.first()?;
            (time >= start && time - step < end).then_some((time, value))
        })
        .collect()
}

impl RrdCachedStorage {
    pub async fn connect(connection_string: &str) -> Result<Self> {
        let url = Url::parse(connection_string)?;
        let settings = Settings::from_url(&url)?;

        let target = match url.scheme() {
            "rrdcached" | "rrdcached+tcp" => {
                let host = url.host_str().ok_or_else(|| {
                    anyhow::Error::from(StorageError::Configuration(
                        "RRDCached connection URL missing host".to_string(),
                    ))
                })?;
                let port = url.port().ok_or_else(|| {
                    anyhow::Error::from(StorageError::Configuration(
                        "RRDCached connection URL missing port".to_string(),
                    ))
                })?;
                RRDCachedConnectionTarget::Tcp {
                    address: format!("{host}:{port}"),
                }
            }
            "rrdcached+unix" => {
                let socket_path = url.path();
                if socket_path.is_empty() {
                    bail!("RRDCached Unix socket connection URL missing socket path");
                }
                RRDCachedConnectionTarget::Unix {
                    socket_path: socket_path.to_string(),
                }
            }
            scheme => bail!("Invalid scheme in connection string: {}", scheme),
        };

        let connection = Connection::connect(Arc::new(target))
            .await
            .context("Failed to connect to RRDCached")?;
        Ok(Self::new(connection, settings))
    }

    fn new(connection: Connection, settings: Settings) -> Self {
        Self {
            connection,
            known_files: RwLock::new(HashSet::new()),
            preset: settings.preset,
            heartbeat_seconds: settings.heartbeat_seconds,
        }
    }

    #[cfg(test)]
    fn new_for_test(
        client: Box<dyn connection::RRDCachedClientTrait>,
        connector: Arc<dyn connection::RRDCachedConnectorTrait>,
    ) -> Self {
        Self::new(
            Connection::with_client(connector, client),
            Settings {
                preset: Preset::Hoarder,
                heartbeat_seconds: DEFAULT_HEARTBEAT_SECONDS,
            },
        )
    }

    /// Make sure the files of the series exist, creating the ones that do not.
    async fn ensure_files(&self, series: &[SeriesPoints]) -> Result<()> {
        let missing: Vec<&SeriesPoints> = {
            let known = self.known_files.read().await;
            series
                .iter()
                .filter(|series| !known.contains(&series.uuid))
                .collect()
        };
        for series in missing {
            self.create_file(series).await?;
            self.known_files.write().await.insert(series.uuid);
        }
        Ok(())
    }

    /// Create the file of a series, unless it exists. Without `no_overwrite` the daemon replaces
    /// a file that exists, with all its history: a restart, another instance or a concurrent
    /// request must find it refused, and that is fine.
    async fn create_file(&self, series: &SeriesPoints) -> Result<()> {
        let path = series.uuid.to_string();

        // The file starts before its first sample: updates must be after the start
        let start_timestamp = series
            .first_time()
            .unwrap_or_default()
            .saturating_sub(STEP_SECONDS);
        let heartbeat = self.heartbeat_seconds;
        let created = self
            .connection
            .run("create", |client| {
                let arguments = CreateArguments {
                    path: path.clone(),
                    data_sources: vec![CreateDataSource {
                        name: DATA_SOURCE_NAME.to_string(),
                        minimum: None,
                        maximum: None,
                        heartbeat,
                        serie_type: CreateDataSourceType::Gauge,
                    }],
                    round_robin_archives: self.preset.get_round_robin_archives(),
                    start_timestamp,
                    step_seconds: STEP_SECONDS,
                    no_overwrite: true,
                };
                async move { client.create(arguments).await }.boxed()
            })
            .await;
        match created {
            Ok(()) => Ok(()),
            // It is there already: stored by an earlier run, another instance or request
            Err(error) if is_existing_file(&error) => Ok(()),
            Err(error) => Err(storage_error("create", error)),
        }
    }

    /// Send the updates of a batch. The daemon applies the lines it accepts and reports the others.
    async fn update(&self, series: &[SeriesPoints]) -> Result<()> {
        let result = self
            .connection
            .run("batch update", |client| match batch_updates(series) {
                Ok(updates) => async move { client.batch(updates).await }.boxed(),
                Err(error) => async move { Err(error) }.boxed(),
            })
            .await;

        let Err(error) = result else {
            return Ok(());
        };
        if let Some(count) = stale_updates(&error) {
            // A request that is retried, or samples older than what the file has. An RRD cannot
            // take them, and the request has stored everything else.
            warn!(
                "RRDCached: {count} samples are not after the last update of their series and were not stored"
            );
            return Ok(());
        }
        if let RRDCachedClientError::BatchUpdateErrorResponse(_, lines) = &error {
            // A file may have been removed behind our back: look for the files again next time
            self.known_files.write().await.clear();
            for line in lines.iter().take(5) {
                warn!("RRDCached refused an update: {}", line.trim());
            }
        }
        Err(storage_error("batch update", error))
    }

    /// The UUIDs of the series that have a file, sorted.
    async fn list_uuids(&self) -> Result<Vec<Uuid>> {
        let files = self
            .connection
            .run("list", |client| {
                async move { client.list(true, None).await }.boxed()
            })
            .await
            .map_err(|error| storage_error("list", error))?;

        let mut uuids: Vec<Uuid> = files
            .iter()
            .filter_map(|file| {
                // The names may come with a path and a newline. Only the files named by SensApp
                // (the canonical UUID) are series.
                let name = file.trim().rsplit('/').next()?.strip_suffix(".rrd")?;
                let uuid = Uuid::parse_str(name).ok()?;
                (uuid.to_string() == name).then_some(uuid)
            })
            .collect();
        uuids.sort_unstable();
        uuids.dedup();
        Ok(uuids)
    }

    /// The time of the last update of a series, in seconds; `None` when it has no file.
    async fn last_update(&self, uuid: Uuid) -> Result<Option<i64>> {
        let path = uuid.to_string();
        let result = self
            .connection
            .run("check", |client| {
                let path = path.clone();
                async move { client.last(&path).await }.boxed()
            })
            .await;
        match result {
            Ok(time) => Ok(Some(time as i64)),
            Err(error) if is_missing_file(&error) => Ok(None),
            Err(error) => Err(storage_error("check", error)),
        }
    }

    /// One `FETCH`; `None` when the series has no file.
    async fn fetch_rows(&self, uuid: Uuid, start: i64, end: i64) -> Result<Option<FetchResponse>> {
        let path = uuid.to_string();
        let result = self
            .connection
            .run("fetch", |client| {
                let path = path.clone();
                async move {
                    client
                        .fetch(
                            &path,
                            ConsolidationFunction::Average,
                            Some(start),
                            Some(end),
                        )
                        .await
                }
                .boxed()
            })
            .await;
        match result {
            Ok(response) => Ok(Some(response)),
            Err(error) if is_missing_file(&error) => Ok(None),
            Err(error) => Err(storage_error("fetch", error)),
        }
    }

    /// The consolidated values of a series in `[start, end]` (seconds), oldest first, without the
    /// unknown ones. `None` when the series has no file.
    ///
    /// Without `end` the window ends now, without `start` it begins where the longest archive of
    /// the preset begins. RRDtool serves a window from the archive with the best resolution that
    /// covers all of it.
    async fn read_series(
        &self,
        uuid: Uuid,
        start: Option<i64>,
        end: Option<i64>,
    ) -> Result<Option<Vec<(i64, f64)>>> {
        let end = end.unwrap_or_else(now_seconds);
        let start = start.unwrap_or_else(|| end.saturating_sub(self.preset.retention_seconds()));
        // No RRD holds a time before 1970, and a negative time would be read as a relative one
        let (start, end) = (start.max(0), end.max(0));
        if start > end {
            // An empty window: the series is there, or it is not
            return Ok(self.last_update(uuid).await?.map(|_| Vec::new()));
        }

        // A row is stamped with the end of its interval, and a fetch returns the rows after its
        // start: one second earlier, so that a sample exactly at `start` is in the first row.
        let fetch_from = (start - 1).max(1);
        let Some(first) = self.fetch_rows(uuid, fetch_from, end).await? else {
            return Ok(None);
        };
        let step = first.step as i64;
        let mut rows = rows_in_window(&first, start, end);

        // The row of a coarse archive that holds the last update is only there once its interval
        // is over, so the end of a long window looks empty: read it from finer archives.
        if step > self.preset.finest_step_seconds()
            && rows.last().is_some_and(|(_, value)| value.is_nan())
        {
            self.add_fresh_rows(uuid, &mut rows, step, start, end)
                .await?;
        }

        rows.retain(|(_, value)| !value.is_nan());
        Ok(Some(rows))
    }

    /// Replace the rows of the interval that holds the last update, which a coarse archive does
    /// not have yet, by the rows of finer archives. The window of a fetch is served by one archive,
    /// the finest that covers its start: starting at the boundary of the interval, a short window
    /// that is capped, picks the finest archive that has the freshest data.
    async fn add_fresh_rows(
        &self,
        uuid: Uuid,
        rows: &mut Vec<(i64, f64)>,
        mut step: i64,
        start: i64,
        end: i64,
    ) -> Result<()> {
        let Some(last_update) = self.last_update(uuid).await? else {
            return Ok(());
        };
        for _ in 0..MAX_FRESH_TAIL_FETCHES {
            if step <= self.preset.finest_step_seconds() {
                break;
            }
            let boundary = last_update - last_update % step;
            if boundary >= end || boundary + step < start {
                break; // The rows of the window are all complete
            }
            let Some(tail) = self
                .fetch_rows(uuid, boundary, end.min(boundary + step))
                .await?
            else {
                break;
            };
            if tail.step as i64 >= step {
                break;
            }
            step = tail.step as i64;
            rows.retain(|(time, _)| *time <= boundary);
            rows.extend(
                rows_in_window(&tail, start, end)
                    .into_iter()
                    .filter(|(time, _)| *time > boundary),
            );
        }
        Ok(())
    }

    /// The series that a selector matches. A name that is a UUID is looked up directly, anything
    /// else needs the list of the files.
    async fn matching_sensors(&self, matchers: &[LabelMatcher]) -> Result<Vec<Sensor>> {
        // The series have no labels: an empty selector selects nothing
        if matchers.is_empty() {
            return Ok(Vec::new());
        }
        let compiled = CompiledMatchers::new(matchers);
        let candidates = match compiled.exact_uuid() {
            Some(uuid) => vec![uuid],
            None => self.list_uuids().await?,
        };
        Ok(candidates
            .into_iter()
            .map(sensor_from_uuid)
            .filter(|sensor| compiled.matches(sensor))
            .collect())
    }
}

/// The selector of a read: the series have no labels, only a name that is their UUID.
struct CompiledMatchers {
    matchers: Vec<CompiledMatcher>,
}

enum CompiledMatcher {
    /// A condition on `__name__`
    Name {
        kind: MatcherType,
        value: String,
        regex: Option<Regex>,
    },
    /// A condition on a label, which no series has: only a negated matcher is true
    Label { negated: bool },
}

impl CompiledMatchers {
    fn new(matchers: &[LabelMatcher]) -> Self {
        let matchers = matchers
            .iter()
            .map(|matcher| {
                if matcher.is_name_matcher() {
                    CompiledMatcher::Name {
                        kind: matcher.matcher_type,
                        value: matcher.value.clone(),
                        regex: matcher
                            .matcher_type
                            .is_regex()
                            .then(|| Regex::new(&matcher.value).ok())
                            .flatten(),
                    }
                } else {
                    CompiledMatcher::Label {
                        negated: matcher.matcher_type.is_negated(),
                    }
                }
            })
            .collect();
        Self { matchers }
    }

    /// The one series a `__name__="<uuid>"` matcher can match.
    fn exact_uuid(&self) -> Option<Uuid> {
        self.matchers.iter().find_map(|matcher| match matcher {
            CompiledMatcher::Name {
                kind: MatcherType::Equal,
                value,
                ..
            } => Uuid::parse_str(value)
                .ok()
                .filter(|uuid| uuid.to_string() == *value),
            _ => None,
        })
    }

    fn matches(&self, sensor: &Sensor) -> bool {
        self.matchers.iter().all(|matcher| match matcher {
            CompiledMatcher::Name { kind, value, regex } => match kind {
                MatcherType::Equal => sensor.name == *value,
                MatcherType::NotEqual => sensor.name != *value,
                // A pattern that does not compile matches nothing, and its negation everything
                MatcherType::RegexMatch => {
                    regex.as_ref().is_some_and(|re| re.is_match(&sensor.name))
                }
                MatcherType::RegexNotMatch => {
                    regex.as_ref().is_none_or(|re| !re.is_match(&sensor.name))
                }
            },
            CompiledMatcher::Label { negated } => *negated,
        })
    }
}

fn truncate_samples(samples: &mut TypedSamples, limit: usize) {
    match samples {
        TypedSamples::Integer(samples) => samples.truncate(limit),
        TypedSamples::Numeric(samples) => samples.truncate(limit),
        TypedSamples::Float(samples) => samples.truncate(limit),
        _ => {}
    }
}

fn sample_count(samples: &TypedSamples) -> usize {
    match samples {
        TypedSamples::Integer(samples) => samples.len(),
        TypedSamples::Numeric(samples) => samples.len(),
        TypedSamples::Float(samples) => samples.len(),
        _ => 0,
    }
}

#[async_trait]
impl StorageInstance for RrdCachedStorage {
    async fn create_or_migrate(&self) -> Result<()> {
        Ok(())
    }

    async fn publish(&self, batch: Arc<crate::datamodel::batch::Batch>) -> Result<()> {
        use rust_decimal::prelude::ToPrimitive;

        let mut series = Vec::with_capacity(batch.sensors.len());
        for single_sensor_batch in batch.sensors.as_ref() {
            let samples = single_sensor_batch.samples.read().await;
            let points: Vec<(i64, f64)> = match &*samples {
                TypedSamples::Float(samples) => samples
                    .iter()
                    .map(|sample| (unix_seconds(&sample.datetime), sample.value))
                    .collect(),
                TypedSamples::Integer(samples) => samples
                    .iter()
                    .map(|sample| (unix_seconds(&sample.datetime), sample.value as f64))
                    .collect(),
                TypedSamples::Numeric(samples) => samples
                    .iter()
                    .map(|sample| {
                        (
                            unix_seconds(&sample.datetime),
                            sample.value.to_f64().unwrap_or(f64::NAN),
                        )
                    })
                    .collect(),
                TypedSamples::Boolean(samples) => samples
                    .iter()
                    .map(|sample| {
                        (
                            unix_seconds(&sample.datetime),
                            if sample.value { 1.0 } else { 0.0 },
                        )
                    })
                    .collect(),
                _ => {
                    warn!(
                        "RRDCached only stores numbers and booleans: the {:?} samples of {} were not stored",
                        single_sensor_batch.sensor.sensor_type, single_sensor_batch.sensor.name
                    );
                    continue;
                }
            };
            series.push((single_sensor_batch.sensor.uuid, points));
        }

        let (series, skipped) = prepare_series(series);
        if skipped.before_epoch > 0 {
            warn!(
                "RRDCached cannot store samples before 1970: {} were not stored",
                skipped.before_epoch
            );
        }
        if skipped.same_second > 0 {
            debug!(
                "RRDCached keeps one sample per second: {} samples were replaced by a later one of the same second",
                skipped.same_second
            );
        }
        if series.is_empty() {
            return Ok(());
        }

        self.ensure_files(&series).await?;
        self.update(&series).await
    }

    async fn vacuum(&self) -> Result<()> {
        Ok(())
    }

    async fn delete_series(&self, _sensor_uuid: &str) -> Result<bool> {
        Err(StorageError::Unsupported(
            "deleting series is not implemented for RRDcached".to_string(),
        )
        .into())
    }

    async fn delete_series_samples(
        &self,
        _sensor_uuid: &str,
        _start_time: SensAppDateTime,
        _end_time: SensAppDateTime,
    ) -> Result<Option<u64>> {
        Err(StorageError::Unsupported(
            "deleting samples is not implemented for RRDcached".to_string(),
        )
        .into())
    }

    async fn list_series(
        &self,
        metric_filter: Option<&str>,
        limit: Option<usize>,
        bookmark: Option<&str>,
    ) -> Result<crate::storage::ListSeriesResult> {
        let limit = limit
            .unwrap_or(crate::storage::DEFAULT_LIST_SERIES_LIMIT)
            .min(crate::storage::MAX_LIST_SERIES_LIMIT);

        // The list of the daemon has no order, the bookmark is a position in the sorted UUIDs
        let mut page: Vec<Uuid> = self
            .list_uuids()
            .await?
            .into_iter()
            .filter(|uuid| {
                let name = uuid.to_string();
                metric_filter.is_none_or(|filter| name.contains(filter))
                    && bookmark.is_none_or(|bookmark| name.as_str() > bookmark)
            })
            .take(limit + 1)
            .collect();
        let has_more = page.len() > limit;
        page.truncate(limit);

        let bookmark = if has_more {
            page.last().map(|uuid| uuid.to_string())
        } else {
            None
        };
        Ok(crate::storage::ListSeriesResult {
            series: page.into_iter().map(sensor_from_uuid).collect(),
            bookmark,
        })
    }

    async fn list_metrics(&self) -> Result<Vec<crate::datamodel::Metric>> {
        // No names are stored, so there are no metrics
        Ok(vec![])
    }

    async fn query_sensor_data(
        &self,
        sensor_uuid: &str,
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
        limit: Option<usize>,
    ) -> Result<Option<SensorData>> {
        let uuid = Uuid::parse_str(sensor_uuid).context("Failed to parse sensor UUID")?;
        let rows = self
            .read_series(
                uuid,
                start_time.as_ref().map(unix_seconds),
                end_time.as_ref().map(unix_seconds),
            )
            .await?;
        let Some(rows) = rows else {
            return Ok(None);
        };

        let mut samples: SensAppVec<Sample<f64>> = rows
            .into_iter()
            .map(|(time, value)| Sample {
                datetime: SensAppDateTime::from_unix_seconds_i64(time),
                value,
            })
            .collect();
        if let Some(limit) = limit {
            samples.truncate(limit);
        }
        Ok(Some(SensorData::new(
            sensor_from_uuid(uuid),
            TypedSamples::Float(samples),
        )))
    }

    /// The limit applies to what is returned, after the aggregation, as in the other backends.
    async fn query_sensor_data_advanced(
        &self,
        sensor_uuid: &str,
        options: &SensorDataQueryOptions,
    ) -> Result<Option<SensorData>> {
        options.validate()?;
        let Some(raw) = self
            .query_sensor_data(sensor_uuid, options.start_time, options.end_time, None)
            .await?
        else {
            return Ok(None);
        };
        let mut data = crate::storage::common::apply_query_options(raw, options)?;
        if let Some(limit) = options.limit {
            truncate_samples(&mut data.samples, limit);
        }
        Ok(Some(data))
    }

    async fn query_sensors_by_labels(
        &self,
        matchers: &[LabelMatcher],
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
        limit: Option<usize>,
        _numeric_only: bool,
    ) -> Result<Vec<SensorData>> {
        let mut results = Vec::new();
        for sensor in self.matching_sensors(matchers).await? {
            if let Some(data) = self
                .query_sensor_data(&sensor.uuid.to_string(), start_time, end_time, limit)
                .await?
            {
                results.push(data);
            }
        }
        Ok(results)
    }

    /// The default reads every matching series twice (once to find the series, once to read
    /// them): here the series are found from their names alone.
    async fn query_selector(
        &self,
        matchers: &[LabelMatcher],
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
        _numeric_only: bool,
        max_series: usize,
        max_samples: usize,
    ) -> Result<SelectorRead> {
        let sensors = self.matching_sensors(matchers).await?;
        if sensors.len() > max_series {
            return Ok(Err(SelectorLimitExceeded::Series));
        }

        let mut remaining = max_samples;
        let mut result = Vec::with_capacity(sensors.len());
        for sensor in sensors {
            let data = self
                .query_sensor_data(
                    &sensor.uuid.to_string(),
                    start_time,
                    end_time,
                    Some(remaining + 1),
                )
                .await?;
            if let Some(data) = data {
                let count = sample_count(&data.samples);
                if count > remaining {
                    return Ok(Err(SelectorLimitExceeded::Samples));
                }
                remaining -= count;
                result.push(data);
            }
        }
        Ok(Ok(result))
    }

    async fn query_selector_aggregated(
        &self,
        matchers: &[LabelMatcher],
        options: &SensorDataQueryOptions,
        max_series: usize,
        max_samples: usize,
    ) -> Result<SelectorRead> {
        options.validate()?;
        let sensors = self.matching_sensors(matchers).await?;
        if sensors.len() > max_series {
            return Ok(Err(SelectorLimitExceeded::Series));
        }

        let mut remaining = max_samples;
        let mut result = Vec::with_capacity(sensors.len());
        for sensor in sensors {
            let mut options = options.clone();
            options.limit = Some(remaining + 1);
            let data = self
                .query_sensor_data_advanced(&sensor.uuid.to_string(), &options)
                .await?;
            if let Some(data) = data {
                let count = sample_count(&data.samples);
                if count == 0 {
                    continue;
                }
                if count > remaining {
                    return Ok(Err(SelectorLimitExceeded::Samples));
                }
                remaining -= count;
                result.push(data);
            }
        }
        Ok(Ok(result))
    }

    /// `PING`: the check is on the connection, it does not make the daemon write anything.
    async fn health_check(&self) -> Result<()> {
        self.connection
            .run("health check", |client| {
                async move { client.ping().await }.boxed()
            })
            .await
            .map_err(|error| storage_error("health check", error))
            .context("RRDCached health check failed")
    }

    #[cfg(any(test, feature = "test-utils"))]
    async fn cleanup_test_data(&self) -> Result<()> {
        // The files stay: tests use series of their own. Only what is remembered is cleared.
        self.known_files.write().await.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests;
