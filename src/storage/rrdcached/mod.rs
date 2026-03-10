use crate::storage::StorageError;
use crate::{
    datamodel::{Sensor, SensorType, TypedSamples},
    storage::StorageInstance,
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use futures::future::BoxFuture;
use rrdcached_client::{
    RRDCachedClient,
    batch_update::BatchUpdate,
    consolidation_function::ConsolidationFunction,
    create::{CreateArguments, CreateDataSource, CreateDataSourceType, CreateRoundRobinArchive},
    errors::RRDCachedClientError,
};
use smallvec::SmallVec;
use std::{collections::HashSet, sync::Arc};
use tokio::sync::RwLock;
use tracing::{error, warn};
use url::Url;
use uuid::Uuid;

#[derive(Debug, Clone, PartialEq)]
pub enum Preset {
    Munin,
    Hoarder,
}

impl Preset {
    pub fn get_round_robin_archives(&self) -> Vec<CreateRoundRobinArchive> {
        match self {
            Preset::Munin => vec![
                // Every 5 minutes for 600 entries
                CreateRoundRobinArchive {
                    consolidation_function: ConsolidationFunction::Average,
                    xfiles_factor: 0.5,
                    steps: 30,
                    rows: 600,
                },
                // Every 30 minutes for 700 entries
                CreateRoundRobinArchive {
                    consolidation_function: ConsolidationFunction::Average,
                    xfiles_factor: 0.5,
                    steps: 180,
                    rows: 700,
                },
                // Every 2 hours for 775 entries
                CreateRoundRobinArchive {
                    consolidation_function: ConsolidationFunction::Average,
                    xfiles_factor: 0.5,
                    steps: 720,
                    rows: 775,
                },
                // Every day for 797 entries
                CreateRoundRobinArchive {
                    consolidation_function: ConsolidationFunction::Average,
                    xfiles_factor: 0.5,
                    steps: 8640,
                    rows: 797,
                },
            ],
            Preset::Hoarder => vec![
                // Every 10 seconds for 1 day
                CreateRoundRobinArchive {
                    consolidation_function: ConsolidationFunction::Average,
                    xfiles_factor: 0.5,
                    steps: 1,
                    rows: 8640,
                },
                // Every minute for 2 days
                CreateRoundRobinArchive {
                    consolidation_function: ConsolidationFunction::Average,
                    xfiles_factor: 0.5,
                    steps: 6,
                    rows: 2880,
                },
                // Every 10 minutes for 7 days
                CreateRoundRobinArchive {
                    consolidation_function: ConsolidationFunction::Average,
                    xfiles_factor: 0.5,
                    steps: 60,
                    rows: 1008,
                },
                // Every hour for 1 year
                CreateRoundRobinArchive {
                    consolidation_function: ConsolidationFunction::Average,
                    xfiles_factor: 0.5,
                    steps: 360,
                    rows: 8760,
                },
                // Every day for 10 years
                CreateRoundRobinArchive {
                    consolidation_function: ConsolidationFunction::Average,
                    xfiles_factor: 0.5,
                    steps: 8640,
                    rows: 3650,
                },
            ],
        }
    }
}

impl std::str::FromStr for Preset {
    type Err = anyhow::Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_lowercase().as_str() {
            "munin" => Ok(Preset::Munin),
            "hoarder" => Ok(Preset::Hoarder),
            _ => bail!("Invalid preset: {}", s),
        }
    }
}

#[derive(Debug)]
pub struct RrdCachedStorage {
    client: Arc<RwLock<Box<dyn RRDCachedClientTrait>>>,
    connector: Arc<dyn RRDCachedConnectorTrait>,
    created_sensors: Arc<RwLock<HashSet<Uuid>>>,
    preset: Preset,
}

#[derive(Debug, Clone)]
struct PreparedBatchUpdate {
    path: String,
    timestamp: Option<usize>,
    data: Vec<f64>,
}

impl PreparedBatchUpdate {
    fn into_batch_update(self) -> Result<BatchUpdate, RRDCachedClientError> {
        BatchUpdate::new(&self.path, self.timestamp, self.data)
    }
}

// Trait to abstract over TCP and Unix socket clients
#[async_trait::async_trait]
trait RRDCachedClientTrait: Send + Sync + std::fmt::Debug {
    async fn create(
        &mut self,
        args: rrdcached_client::create::CreateArguments,
    ) -> Result<(), rrdcached_client::errors::RRDCachedClientError>;
    async fn batch(
        &mut self,
        batch_updates: Vec<BatchUpdate>,
    ) -> Result<(), rrdcached_client::errors::RRDCachedClientError>;
    async fn flush_all(&mut self) -> Result<(), rrdcached_client::errors::RRDCachedClientError>;
    async fn list(
        &mut self,
        recursive: bool,
        path: Option<&str>,
    ) -> Result<Vec<String>, rrdcached_client::errors::RRDCachedClientError>;
    async fn fetch(
        &mut self,
        path: &str,
        consolidation_function: ConsolidationFunction,
        start: Option<i64>,
        end: Option<i64>,
        columns: Option<Vec<String>>,
    ) -> Result<
        rrdcached_client::fetch::FetchResponse,
        rrdcached_client::errors::RRDCachedClientError,
    >;
}

#[async_trait]
trait RRDCachedConnectorTrait: Send + Sync + std::fmt::Debug {
    async fn connect(&self) -> Result<Box<dyn RRDCachedClientTrait>, RRDCachedClientError>;
}

#[derive(Debug, Clone)]
enum RRDCachedConnectionTarget {
    Tcp { address: String },
    Unix { socket_path: String },
}

#[async_trait]
impl RRDCachedConnectorTrait for RRDCachedConnectionTarget {
    async fn connect(&self) -> Result<Box<dyn RRDCachedClientTrait>, RRDCachedClientError> {
        match self {
            RRDCachedConnectionTarget::Tcp { address } => {
                let client = RRDCachedClient::connect_tcp(address).await?;
                Ok(Box::new(client))
            }
            RRDCachedConnectionTarget::Unix { socket_path } => {
                let client = RRDCachedClient::connect_unix(socket_path).await?;
                Ok(Box::new(client))
            }
        }
    }
}

// Implement the trait for TCP client
#[async_trait::async_trait]
impl RRDCachedClientTrait for RRDCachedClient<tokio::net::TcpStream> {
    async fn create(
        &mut self,
        args: rrdcached_client::create::CreateArguments,
    ) -> Result<(), rrdcached_client::errors::RRDCachedClientError> {
        RRDCachedClient::create(self, args).await
    }

    async fn batch(
        &mut self,
        batch_updates: Vec<BatchUpdate>,
    ) -> Result<(), rrdcached_client::errors::RRDCachedClientError> {
        RRDCachedClient::batch(self, batch_updates).await
    }

    async fn flush_all(&mut self) -> Result<(), rrdcached_client::errors::RRDCachedClientError> {
        RRDCachedClient::flush_all(self).await
    }

    async fn list(
        &mut self,
        recursive: bool,
        path: Option<&str>,
    ) -> Result<Vec<String>, rrdcached_client::errors::RRDCachedClientError> {
        RRDCachedClient::list(self, recursive, path).await
    }

    async fn fetch(
        &mut self,
        path: &str,
        consolidation_function: ConsolidationFunction,
        start: Option<i64>,
        end: Option<i64>,
        columns: Option<Vec<String>>,
    ) -> Result<
        rrdcached_client::fetch::FetchResponse,
        rrdcached_client::errors::RRDCachedClientError,
    > {
        RRDCachedClient::fetch(self, path, consolidation_function, start, end, columns).await
    }
}

// Implement the trait for Unix socket client
#[async_trait::async_trait]
impl RRDCachedClientTrait for RRDCachedClient<tokio::net::UnixStream> {
    async fn create(
        &mut self,
        args: rrdcached_client::create::CreateArguments,
    ) -> Result<(), rrdcached_client::errors::RRDCachedClientError> {
        RRDCachedClient::create(self, args).await
    }

    async fn batch(
        &mut self,
        batch_updates: Vec<BatchUpdate>,
    ) -> Result<(), rrdcached_client::errors::RRDCachedClientError> {
        RRDCachedClient::batch(self, batch_updates).await
    }

    async fn flush_all(&mut self) -> Result<(), rrdcached_client::errors::RRDCachedClientError> {
        RRDCachedClient::flush_all(self).await
    }

    async fn list(
        &mut self,
        recursive: bool,
        path: Option<&str>,
    ) -> Result<Vec<String>, rrdcached_client::errors::RRDCachedClientError> {
        RRDCachedClient::list(self, recursive, path).await
    }

    async fn fetch(
        &mut self,
        path: &str,
        consolidation_function: ConsolidationFunction,
        start: Option<i64>,
        end: Option<i64>,
        columns: Option<Vec<String>>,
    ) -> Result<
        rrdcached_client::fetch::FetchResponse,
        rrdcached_client::errors::RRDCachedClientError,
    > {
        RRDCachedClient::fetch(self, path, consolidation_function, start, end, columns).await
    }
}

impl RrdCachedStorage {
    pub async fn connect(connection_string: &str) -> Result<Self> {
        let url = Url::parse(connection_string)?;
        let scheme = url.scheme();

        let preset = url
            .query_pairs()
            .find(|(key, _)| key == "preset")
            .map(|(_, value)| value.parse())
            .transpose()?
            .unwrap_or(Preset::Hoarder); // Default to Hoarder if not specified

        match scheme {
            "rrdcached" | "rrdcached+tcp" => {
                // extract host and port
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

                let connector = Arc::new(RRDCachedConnectionTarget::Tcp {
                    address: format!("{}:{}", host, port),
                });

                Self::connect_with_connector(connector, preset).await
            }
            "rrdcached+unix" => {
                // Extract Unix socket path from the URL
                let socket_path = url.path();
                if socket_path.is_empty() {
                    bail!("RRDCached Unix socket connection URL missing socket path");
                }

                let connector = Arc::new(RRDCachedConnectionTarget::Unix {
                    socket_path: socket_path.to_string(),
                });

                Self::connect_with_connector(connector, preset).await
            }
            _ => bail!("Invalid scheme in connection string: {}", scheme),
        }
    }

    async fn connect_with_connector(
        connector: Arc<dyn RRDCachedConnectorTrait>,
        preset: Preset,
    ) -> Result<Self> {
        let client = connector.connect().await?;
        Ok(Self::new_with_parts(client, connector, preset))
    }

    fn new_with_parts(
        client: Box<dyn RRDCachedClientTrait>,
        connector: Arc<dyn RRDCachedConnectorTrait>,
        preset: Preset,
    ) -> Self {
        Self {
            client: Arc::new(RwLock::new(client)),
            connector,
            created_sensors: Arc::new(RwLock::new(HashSet::new())),
            preset,
        }
    }

    #[cfg(test)]
    fn new_for_test(
        client: Box<dyn RRDCachedClientTrait>,
        connector: Arc<dyn RRDCachedConnectorTrait>,
    ) -> Self {
        Self::new_with_parts(client, connector, Preset::Hoarder)
    }

    fn is_reconnectable_error(error: &RRDCachedClientError) -> bool {
        matches!(
            error,
            RRDCachedClientError::Io(_) | RRDCachedClientError::Parsing(_)
        )
    }

    fn log_client_error(operation: &str, error: &RRDCachedClientError) {
        error!("RRDCached {} failed: {:?}", operation, error);
        if let RRDCachedClientError::BatchUpdateErrorResponse(message, errors) = error {
            error!("RRDCached batch update error response: {:?}", message);
            for item in errors {
                error!("RRDCached batch update error: {:?}", item);
            }
        }
    }

    async fn reconnect(&self, operation: &str) -> Result<()> {
        let client =
            self.connector.connect().await.with_context(|| {
                format!("Failed to reconnect RRDCached client after {}", operation)
            })?;
        let mut current_client = self.client.write().await;
        *current_client = client;
        Ok(())
    }

    async fn with_reconnect<T>(
        &self,
        operation: &str,
        mut action: impl for<'a> FnMut(
            &'a mut dyn RRDCachedClientTrait,
        ) -> BoxFuture<'a, Result<T, RRDCachedClientError>>,
    ) -> Result<T> {
        let mut retried = false;

        loop {
            let result = {
                let mut client = self.client.write().await;
                action(client.as_mut()).await
            };

            match result {
                Ok(value) => return Ok(value),
                Err(error) if !retried && Self::is_reconnectable_error(&error) => {
                    Self::log_client_error(operation, &error);
                    warn!(
                        "RRDCached {} hit a reconnectable error; reconnecting and retrying once",
                        operation
                    );
                    retried = true;
                    self.reconnect(operation).await?;
                }
                Err(error) => {
                    Self::log_client_error(operation, &error);
                    return Err(error).context(format!("RRDCached {} failed", operation));
                }
            }
        }
    }

    async fn create_sensors(&self, sensors: &[Arc<Sensor>], start_timestamp: u64) -> Result<()> {
        if sensors.is_empty() {
            return Ok(());
        }

        for sensor in sensors {
            let sensor = sensor.clone();
            let sensor_for_create = sensor.clone();
            let preset = self.preset.clone();

            self.with_reconnect("create", move |client| {
                let sensor = sensor_for_create.clone();
                let preset = preset.clone();
                Box::pin(async move {
                    client
                        .create(CreateArguments {
                            path: sensor.uuid.to_string(),
                            data_sources: vec![CreateDataSource {
                                name: "sensapp".to_string(),
                                minimum: None,
                                maximum: None,
                                heartbeat: 20,
                                serie_type: CreateDataSourceType::Gauge,
                            }],
                            round_robin_archives: preset.get_round_robin_archives(),
                            start_timestamp,
                            step_seconds: 10,
                        })
                        .await
                })
            })
            .await?;

            let mut created_sensors = self.created_sensors.write().await;
            created_sensors.insert(sensor.uuid);
        }
        Ok(())
    }
}

#[async_trait]
impl StorageInstance for RrdCachedStorage {
    async fn create_or_migrate(&self) -> Result<()> {
        Ok(())
    }
    async fn publish(&self, batch: std::sync::Arc<crate::datamodel::batch::Batch>) -> Result<()> {
        if batch.sensors.is_empty() {
            return Ok(());
        }

        let mut batch_updates = vec![];
        let mut min_timestamp = usize::MAX;

        for single_sensor_batch in batch.sensors.as_ref() {
            let samples_guard = single_sensor_batch.samples.read().await;
            let uuid = single_sensor_batch.sensor.uuid;
            let name = uuid.to_string();
            match &*samples_guard {
                TypedSamples::Float(samples) => {
                    for value in samples {
                        let timestamp = value.datetime.to_unix_seconds().floor() as usize;
                        if timestamp < min_timestamp {
                            min_timestamp = timestamp;
                        }
                        batch_updates.push(PreparedBatchUpdate {
                            path: name.clone(),
                            timestamp: Some(timestamp),
                            data: vec![value.value],
                        });
                    }
                }
                TypedSamples::Numeric(samples) => {
                    for value in samples {
                        let timestamp = value.datetime.to_unix_seconds().floor() as usize;
                        if timestamp < min_timestamp {
                            min_timestamp = timestamp;
                        }
                        use rust_decimal::prelude::ToPrimitive;
                        batch_updates.push(PreparedBatchUpdate {
                            path: name.clone(),
                            timestamp: Some(timestamp),
                            data: vec![value.value.to_f64().unwrap_or(f64::NAN)],
                        });
                    }
                }
                TypedSamples::Integer(samples) => {
                    for value in samples {
                        let timestamp = value.datetime.to_unix_seconds().floor() as usize;
                        if timestamp < min_timestamp {
                            min_timestamp = timestamp;
                        }
                        batch_updates.push(PreparedBatchUpdate {
                            path: name.clone(),
                            timestamp: Some(timestamp),
                            data: vec![value.value as f64],
                        });
                    }
                }
                TypedSamples::Boolean(samples) => {
                    for value in samples {
                        let timestamp = value.datetime.to_unix_seconds().floor() as usize;
                        if timestamp < min_timestamp {
                            min_timestamp = timestamp;
                        }
                        batch_updates.push(PreparedBatchUpdate {
                            path: name.clone(),
                            timestamp: Some(timestamp),
                            data: vec![if value.value { 1.0 } else { 0.0 }],
                        });
                    }
                }
                _ => {
                    tracing::warn!(
                        "RRDCached: unsupported sensor type {:?}, skipping",
                        single_sensor_batch.sensor.sensor_type
                    );
                }
            }
        }

        // Find the sensors that need to be created
        let sensors_to_create: Vec<Arc<Sensor>>;
        {
            let created_sensors = self.created_sensors.read().await;
            sensors_to_create = batch
                .sensors
                .iter()
                .filter(|single_sensor_batch| {
                    let sensor = &single_sensor_batch.sensor;
                    !created_sensors.contains(&sensor.uuid)
                        && (sensor.sensor_type == SensorType::Float
                            || sensor.sensor_type == SensorType::Numeric
                            || sensor.sensor_type == SensorType::Integer
                            || sensor.sensor_type == SensorType::Boolean)
                })
                .map(|single_sensor_batch| single_sensor_batch.sensor.clone())
                .collect::<Vec<_>>();
        }
        if !sensors_to_create.is_empty() {
            self.create_sensors(&sensors_to_create, min_timestamp as u64 - 10)
                .await?;
        }

        if batch_updates.is_empty() {
            return Ok(());
        }

        self.with_reconnect("batch update", move |client| {
            let batch_updates = batch_updates.clone();
            Box::pin(async move {
                let batch_updates = batch_updates
                    .into_iter()
                    .map(PreparedBatchUpdate::into_batch_update)
                    .collect::<Result<Vec<_>, _>>()?;
                client.batch(batch_updates).await
            })
        })
        .await?;

        self.with_reconnect("flush_all", |client| {
            Box::pin(async move { client.flush_all().await })
        })
        .await?;

        Ok(())
    }

    async fn vacuum(&self) -> Result<()> {
        Ok(())
    }

    async fn list_series(
        &self,
        metric_filter: Option<&str>,
        limit: Option<usize>,
        bookmark: Option<&str>,
    ) -> Result<crate::storage::ListSeriesResult> {
        // Validate and cap the limit
        let effective_limit = limit
            .unwrap_or(crate::storage::DEFAULT_LIST_SERIES_LIMIT)
            .min(crate::storage::MAX_LIST_SERIES_LIMIT);

        match self
            .with_reconnect("list", |client| {
                Box::pin(async move { client.list(true, None).await })
            })
            .await
        {
            Ok(rrd_files) => {
                let mut sensors = Vec::new();
                let mut last_uuid: Option<String> = None;

                for rrd_file in rrd_files {
                    // Trim whitespace from filename (RRDcached LIST returns names with trailing newlines)
                    let rrd_file = rrd_file.trim();

                    // Filter by metric name if provided
                    if let Some(filter) = metric_filter
                        && !rrd_file.contains(filter)
                    {
                        continue;
                    }

                    // Extract UUID from filename (assuming format: "<uuid>.rrd")
                    let filename = rrd_file.rsplit('/').next().unwrap_or(rrd_file);
                    let uuid_str = filename.strip_suffix(".rrd").unwrap_or(filename);

                    // Apply bookmark-based pagination: skip entries <= bookmark
                    if let Some(bm) = bookmark
                        && uuid_str <= bm
                    {
                        continue;
                    }

                    if let Ok(uuid) = uuid_str.parse::<Uuid>() {
                        let sensor = crate::datamodel::Sensor {
                            uuid,
                            name: uuid.to_string(),
                            sensor_type: crate::datamodel::SensorType::Float,
                            unit: None,
                            labels: SmallVec::new(),
                        };
                        last_uuid = Some(uuid_str.to_string());
                        sensors.push(sensor);
                    } else {
                        let sensor = crate::datamodel::Sensor {
                            uuid: Uuid::new_v4(),
                            name: filename.to_string(),
                            sensor_type: crate::datamodel::SensorType::Float,
                            unit: None,
                            labels: SmallVec::new(),
                        };
                        last_uuid = Some(uuid_str.to_string());
                        sensors.push(sensor);
                    }

                    if sensors.len() >= effective_limit {
                        break;
                    }
                }

                // Return bookmark for next page if we hit the limit
                let next_bookmark = if sensors.len() == effective_limit {
                    last_uuid
                } else {
                    None
                };

                Ok(crate::storage::ListSeriesResult {
                    series: sensors,
                    bookmark: next_bookmark,
                })
            }
            Err(e) => {
                error!("Failed to list RRD files: {:?}", e);

                // Fallback to tracking created sensors in this session
                let created_sensors = self.created_sensors.read().await;
                let mut sensors = Vec::new();

                for uuid in created_sensors.iter() {
                    sensors.push(crate::datamodel::Sensor {
                        uuid: *uuid,
                        name: uuid.to_string(),
                        sensor_type: crate::datamodel::SensorType::Float,
                        unit: None,
                        labels: SmallVec::new(),
                    });
                }

                Ok(crate::storage::ListSeriesResult {
                    series: sensors,
                    bookmark: None,
                })
            }
        }
    }

    async fn list_metrics(&self) -> Result<Vec<crate::datamodel::Metric>> {
        // RRDcached doesn't support metric-level operations like PostgreSQL
        // Return empty list for now, as RRDcached focuses on individual series
        Ok(vec![])
    }

    async fn query_sensor_data(
        &self,
        sensor_uuid: &str,
        start_time: Option<crate::datamodel::SensAppDateTime>,
        end_time: Option<crate::datamodel::SensAppDateTime>,
        _limit: Option<usize>, // RRD doesn't support limiting results directly
    ) -> Result<Option<crate::datamodel::SensorData>> {
        use crate::datamodel::{
            Sample, SensorData, SensorType, sensapp_datetime::SensAppDateTimeExt,
        };
        use smallvec::SmallVec;

        // Check if sensor exists in our created_sensors set
        let created_sensors = self.created_sensors.read().await;
        let sensor_uuid_obj = sensor_uuid
            .parse::<Uuid>()
            .map_err(|e| anyhow::anyhow!("Invalid sensor UUID: {}", e))?;

        drop(created_sensors);

        // Convert time parameters to Unix timestamps
        let start_timestamp = start_time.map(|t| t.to_unix_seconds().floor() as i64);
        let end_timestamp = end_time.map(|t| t.to_unix_seconds().floor() as i64);

        // Fetch data from RRDcached - first flush to ensure data is written
        if let Err(e) = self
            .with_reconnect("flush before query", |client| {
                Box::pin(async move { client.flush_all().await })
            })
            .await
        {
            tracing::warn!("Failed to flush before query: {:?}", e);
        }
        let rrd_path = sensor_uuid.to_string();

        let fetch_response = match self
            .with_reconnect("fetch", |client| {
                let rrd_path = rrd_path.clone();
                Box::pin(async move {
                    client
                        .fetch(
                            &rrd_path,
                            ConsolidationFunction::Average,
                            start_timestamp,
                            end_timestamp,
                            None,
                        )
                        .await
                })
            })
            .await
        {
            Ok(response) => {
                tracing::debug!(
                    "Fetch successful for sensor {}: {} data points",
                    sensor_uuid,
                    response.data.len()
                );
                response
            }
            Err(e) => {
                // If fetch fails, it might be because no data exists yet
                tracing::debug!("Failed to fetch data for sensor {}: {:?}", sensor_uuid, e);
                return Ok(None);
            }
        };

        // Convert RRD data to SensApp samples
        let mut samples = SmallVec::new();

        tracing::debug!("Processing {} RRD data points", fetch_response.data.len());

        // RRD returns data as Vec<(timestamp, Vec<f64>)>
        // We use the first data source (index 0) since we create RRDs with one DS
        for (timestamp, values) in fetch_response.data {
            tracing::debug!(
                "RRD data point: timestamp={}, values={:?}",
                timestamp,
                values
            );
            if let Some(&value) = values.first() {
                // Skip NaN values (RRD uses NaN for missing data)
                if !value.is_nan() {
                    let datetime =
                        crate::datamodel::SensAppDateTime::from_unix_seconds_i64(timestamp as i64);
                    samples.push(Sample { datetime, value });
                    tracing::trace!("Added sample: time={:?}, value={}", datetime, value);
                } else {
                    tracing::trace!("Skipped NaN value at timestamp {}", timestamp);
                }
            }
        }

        tracing::debug!("Converted {} valid samples from RRD data", samples.len());

        if samples.is_empty() {
            tracing::debug!("No valid samples found, returning None");
            return Ok(None);
        }

        // Create sensor metadata (we have to reconstruct it since RRD doesn't store it)
        let sensor = crate::datamodel::Sensor {
            uuid: sensor_uuid_obj,
            name: sensor_uuid.to_string(), // Use UUID as name since we don't have the original
            sensor_type: SensorType::Float, // Assume Float since RRD stores f64 values
            unit: None,                    // We don't have unit information
            labels: SmallVec::new(),       // We don't have labels information
        };

        let typed_samples = TypedSamples::Float(samples);
        let sensor_data = SensorData::new(sensor, typed_samples);

        Ok(Some(sensor_data))
    }

    async fn query_sensors_by_labels(
        &self,
        matchers: &[super::LabelMatcher],
        start_time: Option<crate::datamodel::SensAppDateTime>,
        end_time: Option<crate::datamodel::SensAppDateTime>,
        limit: Option<usize>,
        _numeric_only: bool,
    ) -> Result<Vec<crate::datamodel::SensorData>> {
        // RRDcached doesn't store label metadata, so we can only match on __name__
        // (which corresponds to sensor name / UUID).
        // All RRD data is numeric, so numeric_only is always satisfied.
        if matchers.is_empty() {
            return Ok(Vec::new());
        }

        // Get all available series
        let result = self.list_series(None, None, None).await?;

        // Filter sensors by matchers (only __name__ is meaningful for RRDcached)
        let filtered_sensors: Vec<_> = result
            .series
            .into_iter()
            .filter(|sensor| {
                matchers.iter().all(|matcher| {
                    if matcher.is_name_matcher() {
                        match matcher.matcher_type {
                            super::MatcherType::Equal => sensor.name == matcher.value,
                            super::MatcherType::NotEqual => sensor.name != matcher.value,
                            super::MatcherType::RegexMatch => regex::Regex::new(&matcher.value)
                                .map(|re| re.is_match(&sensor.name))
                                .unwrap_or(false),
                            super::MatcherType::RegexNotMatch => regex::Regex::new(&matcher.value)
                                .map(|re| !re.is_match(&sensor.name))
                                .unwrap_or(true),
                        }
                    } else {
                        // RRDcached has no labels, so non-name matchers
                        // match nothing for Equal/RegexMatch, everything for NotEqual/RegexNotMatch
                        matcher.matcher_type.is_negated()
                    }
                })
            })
            .collect();

        // Fetch data for each matching sensor
        let mut results = Vec::with_capacity(filtered_sensors.len());
        for sensor in filtered_sensors {
            let uuid_str = sensor.uuid.to_string();
            if let Some(sensor_data) = self
                .query_sensor_data(&uuid_str, start_time, end_time, limit)
                .await?
            {
                results.push(sensor_data);
            }
        }

        Ok(results)
    }

    /// Health check for RRDCached storage
    /// Verifies the connection to RRDCached by checking if client can respond
    async fn health_check(&self) -> Result<()> {
        self.with_reconnect("health check", |client| {
            Box::pin(async move { client.flush_all().await })
        })
        .await
        .context("RRDCached health check failed")?;
        Ok(())
    }

    /// Clean up all test data from the database (RRDCached implementation)
    #[cfg(any(test, feature = "test-utils"))]
    async fn cleanup_test_data(&self) -> Result<()> {
        // RRDCached doesn't have a traditional database cleanup mechanism
        // For tests, we would typically use separate RRD files or instances
        // Clear the created sensors set as a minimal cleanup
        let mut created_sensors = self.created_sensors.write().await;
        created_sensors.clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::{
        collections::VecDeque,
        io,
        sync::{
            Mutex,
            atomic::{AtomicUsize, Ordering},
        },
    };

    #[derive(Debug)]
    struct MockClient {
        list_results: Mutex<VecDeque<Result<Vec<String>, RRDCachedClientError>>>,
        flush_results: Mutex<VecDeque<Result<(), RRDCachedClientError>>>,
    }

    impl MockClient {
        fn with_list_results(results: Vec<Result<Vec<String>, RRDCachedClientError>>) -> Self {
            Self {
                list_results: Mutex::new(results.into()),
                flush_results: Mutex::new(VecDeque::new()),
            }
        }

        fn with_flush_results(results: Vec<Result<(), RRDCachedClientError>>) -> Self {
            Self {
                list_results: Mutex::new(VecDeque::new()),
                flush_results: Mutex::new(results.into()),
            }
        }
    }

    #[async_trait]
    impl RRDCachedClientTrait for MockClient {
        async fn create(&mut self, _args: CreateArguments) -> Result<(), RRDCachedClientError> {
            Ok(())
        }

        async fn batch(
            &mut self,
            _batch_updates: Vec<BatchUpdate>,
        ) -> Result<(), RRDCachedClientError> {
            Ok(())
        }

        async fn flush_all(&mut self) -> Result<(), RRDCachedClientError> {
            self.flush_results
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Ok(()))
        }

        async fn list(
            &mut self,
            _recursive: bool,
            _path: Option<&str>,
        ) -> Result<Vec<String>, RRDCachedClientError> {
            self.list_results
                .lock()
                .unwrap()
                .pop_front()
                .unwrap_or(Ok(Vec::new()))
        }

        async fn fetch(
            &mut self,
            _path: &str,
            _consolidation_function: ConsolidationFunction,
            _start: Option<i64>,
            _end: Option<i64>,
            _columns: Option<Vec<String>>,
        ) -> Result<rrdcached_client::fetch::FetchResponse, RRDCachedClientError> {
            Ok(rrdcached_client::fetch::FetchResponse {
                flush_version: 0,
                start: 0,
                end: 0,
                step: 1,
                ds_count: 0,
                ds_names: Vec::new(),
                data: Vec::new(),
            })
        }
    }

    #[derive(Debug)]
    struct MockConnector {
        connect_calls: AtomicUsize,
        clients: Mutex<VecDeque<Box<dyn RRDCachedClientTrait>>>,
    }

    impl MockConnector {
        fn new(clients: Vec<Box<dyn RRDCachedClientTrait>>) -> Self {
            Self {
                connect_calls: AtomicUsize::new(0),
                clients: Mutex::new(clients.into()),
            }
        }

        fn connect_calls(&self) -> usize {
            self.connect_calls.load(Ordering::SeqCst)
        }
    }

    #[async_trait]
    impl RRDCachedConnectorTrait for MockConnector {
        async fn connect(&self) -> Result<Box<dyn RRDCachedClientTrait>, RRDCachedClientError> {
            self.connect_calls.fetch_add(1, Ordering::SeqCst);
            self.clients
                .lock()
                .unwrap()
                .pop_front()
                .ok_or_else(|| RRDCachedClientError::Parsing("missing mock client".to_string()))
        }
    }

    #[tokio::test]
    async fn list_series_reconnects_after_io_error() {
        let initial_client = Box::new(MockClient::with_list_results(vec![Err(
            RRDCachedClientError::Io(io::Error::new(io::ErrorKind::BrokenPipe, "boom")),
        )]));
        let connector = Arc::new(MockConnector::new(vec![Box::new(
            MockClient::with_list_results(vec![Ok(vec![format!("{}.rrd\n", Uuid::nil())])]),
        )]));
        let storage = RrdCachedStorage::new_for_test(initial_client, connector.clone());

        let result = storage.list_series(None, Some(10), None).await.unwrap();

        assert_eq!(result.series.len(), 1);
        assert_eq!(result.series[0].uuid, Uuid::nil());
        assert_eq!(connector.connect_calls(), 1);
    }

    #[tokio::test]
    async fn health_check_does_not_retry_non_reconnectable_errors() {
        let initial_client = Box::new(MockClient::with_flush_results(vec![Err(
            RRDCachedClientError::UnexpectedResponse(-1, "protocol failure".to_string()),
        )]));
        let connector = Arc::new(MockConnector::new(Vec::new()));
        let storage = RrdCachedStorage::new_for_test(initial_client, connector.clone());

        let error = storage.health_check().await.unwrap_err().to_string();

        assert!(error.contains("RRDCached health check failed"));
        assert_eq!(connector.connect_calls(), 0);
    }
}
