use crate::storage::StorageError;
use crate::{
    config,
    datamodel::{Sensor, SensorType, TypedSamples},
    storage::{StorageInstance, common::sync_with_timeout},
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use rrdcached_client::{
    RRDCachedClient,
    batch_update::BatchUpdate,
    consolidation_function::ConsolidationFunction,
    create::{CreateArguments, CreateDataSource, CreateDataSourceType, CreateRoundRobinArchive},
    errors::RRDCachedClientError,
};
use std::{collections::HashSet, sync::Arc};
use smallvec::SmallVec;
use tokio::sync::RwLock;
use tracing::error;
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
    created_sensors: Arc<RwLock<HashSet<Uuid>>>,
    preset: Preset,
}

// Trait to abstract over TCP and Unix socket clients
#[async_trait::async_trait]
trait RRDCachedClientTrait: Send + Sync + std::fmt::Debug {
    async fn create(&mut self, args: rrdcached_client::create::CreateArguments) -> Result<(), rrdcached_client::errors::RRDCachedClientError>;
    async fn batch(&mut self, batch_updates: Vec<BatchUpdate>) -> Result<(), rrdcached_client::errors::RRDCachedClientError>;
    async fn flush_all(&mut self) -> Result<(), rrdcached_client::errors::RRDCachedClientError>;
    async fn list(&mut self, recursive: bool, path: Option<&str>) -> Result<Vec<String>, rrdcached_client::errors::RRDCachedClientError>;
    async fn fetch(
        &mut self,
        path: &str,
        consolidation_function: ConsolidationFunction,
        start: Option<i64>,
        end: Option<i64>,
        columns: Option<Vec<String>>,
    ) -> Result<rrdcached_client::fetch::FetchResponse, rrdcached_client::errors::RRDCachedClientError>;
}

// Implement the trait for TCP client
#[async_trait::async_trait]
impl RRDCachedClientTrait for RRDCachedClient<tokio::net::TcpStream> {
    async fn create(&mut self, args: rrdcached_client::create::CreateArguments) -> Result<(), rrdcached_client::errors::RRDCachedClientError> {
        RRDCachedClient::create(self, args).await
    }

    async fn batch(&mut self, batch_updates: Vec<BatchUpdate>) -> Result<(), rrdcached_client::errors::RRDCachedClientError> {
        RRDCachedClient::batch(self, batch_updates).await
    }

    async fn flush_all(&mut self) -> Result<(), rrdcached_client::errors::RRDCachedClientError> {
        RRDCachedClient::flush_all(self).await
    }

    async fn list(&mut self, recursive: bool, path: Option<&str>) -> Result<Vec<String>, rrdcached_client::errors::RRDCachedClientError> {
        RRDCachedClient::list(self, recursive, path).await
    }

    async fn fetch(
        &mut self,
        path: &str,
        consolidation_function: ConsolidationFunction,
        start: Option<i64>,
        end: Option<i64>,
        columns: Option<Vec<String>>,
    ) -> Result<rrdcached_client::fetch::FetchResponse, rrdcached_client::errors::RRDCachedClientError> {
        RRDCachedClient::fetch(self, path, consolidation_function, start, end, columns).await
    }
}

// Implement the trait for Unix socket client
#[async_trait::async_trait]
impl RRDCachedClientTrait for RRDCachedClient<tokio::net::UnixStream> {
    async fn create(&mut self, args: rrdcached_client::create::CreateArguments) -> Result<(), rrdcached_client::errors::RRDCachedClientError> {
        RRDCachedClient::create(self, args).await
    }

    async fn batch(&mut self, batch_updates: Vec<BatchUpdate>) -> Result<(), rrdcached_client::errors::RRDCachedClientError> {
        RRDCachedClient::batch(self, batch_updates).await
    }

    async fn flush_all(&mut self) -> Result<(), rrdcached_client::errors::RRDCachedClientError> {
        RRDCachedClient::flush_all(self).await
    }

    async fn list(&mut self, recursive: bool, path: Option<&str>) -> Result<Vec<String>, rrdcached_client::errors::RRDCachedClientError> {
        RRDCachedClient::list(self, recursive, path).await
    }

    async fn fetch(
        &mut self,
        path: &str,
        consolidation_function: ConsolidationFunction,
        start: Option<i64>,
        end: Option<i64>,
        columns: Option<Vec<String>>,
    ) -> Result<rrdcached_client::fetch::FetchResponse, rrdcached_client::errors::RRDCachedClientError> {
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

                let client = RRDCachedClient::connect_tcp(&format!("{}:{}", host, port)).await?;
                Ok(Self {
                    client: Arc::new(RwLock::new(Box::new(client))),
                    created_sensors: Arc::new(RwLock::new(HashSet::new())),
                    preset,
                })
            }
            "rrdcached+unix" => {
                // Extract Unix socket path from the URL
                let socket_path = url.path();
                if socket_path.is_empty() {
                    bail!("RRDCached Unix socket connection URL missing socket path");
                }

                let client = RRDCachedClient::connect_unix(socket_path).await?;
                Ok(Self {
                    client: Arc::new(RwLock::new(Box::new(client))),
                    created_sensors: Arc::new(RwLock::new(HashSet::new())),
                    preset,
                })
            }
            _ => bail!("Invalid scheme in connection string: {}", scheme),
        }
    }

    async fn create_sensors(&self, sensors: &[Arc<Sensor>], start_timestamp: u64) -> Result<()> {
        if sensors.is_empty() {
            return Ok(());
        }
        let mut client = self.client.write().await;
        let mut created_sensors = self.created_sensors.write().await;
        for sensor in sensors {
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
                    round_robin_archives: self.preset.get_round_robin_archives(),
                    start_timestamp,
                    step_seconds: 10,
                })
                .await?;
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
    async fn publish(
        &self,
        batch: std::sync::Arc<crate::datamodel::batch::Batch>,
        sync_sender: async_broadcast::Sender<()>,
    ) -> Result<()> {
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
                        batch_updates.push(BatchUpdate::new(
                            &name,
                            Some(timestamp),
                            vec![value.value],
                        )?);
                    }
                }
                TypedSamples::Numeric(samples) => {
                    for value in samples {
                        let timestamp = value.datetime.to_unix_seconds().floor() as usize;
                        if timestamp < min_timestamp {
                            min_timestamp = timestamp;
                        }
                        use rust_decimal::prelude::ToPrimitive;
                        batch_updates.push(BatchUpdate::new(
                            &name,
                            Some(timestamp),
                            vec![value.value.to_f64().unwrap_or(f64::NAN)],
                        )?);
                    }
                }
                TypedSamples::Integer(samples) => {
                    for value in samples {
                        let timestamp = value.datetime.to_unix_seconds().floor() as usize;
                        if timestamp < min_timestamp {
                            min_timestamp = timestamp;
                        }
                        batch_updates.push(BatchUpdate::new(
                            &name,
                            Some(timestamp),
                            vec![value.value as f64],
                        )?);
                    }
                }
                TypedSamples::Boolean(samples) => {
                    for value in samples {
                        let timestamp = value.datetime.to_unix_seconds().floor() as usize;
                        if timestamp < min_timestamp {
                            min_timestamp = timestamp;
                        }
                        batch_updates.push(BatchUpdate::new(
                            &name,
                            Some(timestamp),
                            vec![if value.value { 1.0 } else { 0.0 }],
                        )?);
                    }
                }
                _ => {
                    print!("Unsupported type");
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

        {
            let mut client = self.client.write().await;
            match client.batch(batch_updates).await {
                Ok(_) => {}
                Err(e) => {
                    error!("RRDCached: Failed to batch update: {:?}", e);
                    match e {
                        RRDCachedClientError::BatchUpdateErrorResponse(string, errors) => {
                            error!("RRDCached: Batch update error response: {:?}", string);
                            for error in errors {
                                error!("RRDCached: Batch update error: {:?}", error);
                            }
                        }
                        _ => {}
                    }
                }
            }
        }

        self.sync(sync_sender).await?;

        Ok(())
    }

    async fn sync(&self, sync_sender: async_broadcast::Sender<()>) -> Result<()> {
        // Flush !
        {
            let mut client = self.client.write().await;
            client.flush_all().await?;
        }

        let config = config::get().context("Failed to get configuration")?;
        sync_with_timeout(&sync_sender, config.storage_sync_timeout_seconds).await?;

        Ok(())
    }

    async fn vacuum(&self) -> Result<()> {
        Ok(())
    }

    async fn list_series(
        &self,
        metric_filter: Option<&str>,
    ) -> Result<Vec<crate::datamodel::Sensor>> {
        // Use RRDcached's native LIST command to get available RRD files
        let mut client = self.client.write().await;
        
        match client.list(true, None).await {
            Ok(rrd_files) => {
                let mut sensors = Vec::new();
                
                for rrd_file in rrd_files {
                    // Trim whitespace from filename (RRDcached LIST returns names with trailing newlines)
                    let rrd_file = rrd_file.trim();
                    
                    // Filter by metric name if provided
                    if let Some(filter) = metric_filter {
                        if !rrd_file.contains(filter) {
                            continue;
                        }
                    }
                    
                    // Extract UUID from filename (assuming format: "<uuid>.rrd")
                    let filename = rrd_file.rsplit('/').next().unwrap_or(&rrd_file);
                    let uuid_str = filename.strip_suffix(".rrd").unwrap_or(filename);
                    
                    if let Ok(uuid) = uuid_str.parse::<Uuid>() {
                        // Create a basic sensor object from RRD file info
                        // Note: RRDcached doesn't store full sensor metadata,
                        // so we create minimal sensor objects with defaults
                        let sensor = crate::datamodel::Sensor {
                            uuid,
                            name: uuid.to_string(), // Use UUID as name
                            sensor_type: crate::datamodel::SensorType::Float, // Default type
                            unit: None,
                            labels: SmallVec::new(),
                        };
                        sensors.push(sensor);
                    } else {
                        // If filename is not a valid UUID, create a sensor with a new UUID
                        let sensor = crate::datamodel::Sensor {
                            uuid: Uuid::new_v4(),
                            name: filename.to_string(),
                            sensor_type: crate::datamodel::SensorType::Float,
                            unit: None,
                            labels: SmallVec::new(),
                        };
                        sensors.push(sensor);
                    }
                }
                
                Ok(sensors)
            }
            Err(e) => {
                error!("Failed to list RRD files: {:?}", e);
                
                // Fallback to tracking created sensors in this session
                let created_sensors = self.created_sensors.read().await;
                let mut sensors = Vec::new();
                
                for uuid in created_sensors.iter() {
                    let sensor = crate::datamodel::Sensor {
                        uuid: *uuid,
                        name: uuid.to_string(),
                        sensor_type: crate::datamodel::SensorType::Float,
                        unit: None,
                        labels: SmallVec::new(),
                    };
                    sensors.push(sensor);
                }
                
                Ok(sensors)
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
        use crate::datamodel::{SensorData, SensorType, Sample, sensapp_datetime::SensAppDateTimeExt};
        use smallvec::SmallVec;

        // Check if sensor exists in our created_sensors set
        let created_sensors = self.created_sensors.read().await;
        let sensor_uuid_obj = sensor_uuid.parse::<Uuid>()
            .map_err(|e| anyhow::anyhow!("Invalid sensor UUID: {}", e))?;
        
        drop(created_sensors);

        // Convert time parameters to Unix timestamps
        let start_timestamp = start_time.map(|t| t.to_unix_seconds().floor() as i64);
        let end_timestamp = end_time.map(|t| t.to_unix_seconds().floor() as i64);

        // Fetch data from RRDcached - first flush to ensure data is written
        let mut client = self.client.write().await;
        
        // Force flush to ensure all pending data is written to RRD files
        if let Err(e) = client.flush_all().await {
            tracing::warn!("Failed to flush before query: {:?}", e);
        }
        let rrd_path = sensor_uuid; // RRD file path is just the UUID
        
        let fetch_response = match client.fetch(
            &rrd_path,
            ConsolidationFunction::Average, // Use AVERAGE consolidation by default
            start_timestamp,
            end_timestamp,
            None, // columns - use default
        ).await {
            Ok(response) => {
                tracing::info!("Fetch successful for sensor {}: {} data points", 
                              sensor_uuid, response.data.len());
                response
            },
            Err(e) => {
                // If fetch fails, it might be because no data exists yet
                tracing::info!("Failed to fetch data for sensor {}: {:?}", sensor_uuid, e);
                return Ok(None);
            }
        };

        // Convert RRD data to SensApp samples
        let mut samples = SmallVec::new();
        
        tracing::info!("Processing {} RRD data points", fetch_response.data.len());
        
        // RRD returns data as Vec<(timestamp, Vec<f64>)>
        // We use the first data source (index 0) since we create RRDs with one DS
        for (timestamp, values) in fetch_response.data {
            tracing::debug!("RRD data point: timestamp={}, values={:?}", timestamp, values);
            if let Some(&value) = values.get(0) {
                // Skip NaN values (RRD uses NaN for missing data)
                if !value.is_nan() {
                    let datetime = crate::datamodel::SensAppDateTime::from_unix_seconds_i64(timestamp as i64);
                    samples.push(Sample { datetime, value });
                    tracing::debug!("Added sample: time={:?}, value={}", datetime, value);
                } else {
                    tracing::debug!("Skipped NaN value at timestamp {}", timestamp);
                }
            }
        }

        tracing::info!("Converted {} valid samples from RRD data", samples.len());

        if samples.is_empty() {
            tracing::info!("No valid samples found, returning None");
            return Ok(None);
        }

        // Create sensor metadata (we have to reconstruct it since RRD doesn't store it)
        let sensor = crate::datamodel::Sensor {
            uuid: sensor_uuid_obj,
            name: sensor_uuid.to_string(), // Use UUID as name since we don't have the original
            sensor_type: SensorType::Float, // Assume Float since RRD stores f64 values
            unit: None, // We don't have unit information
            labels: SmallVec::new(), // We don't have labels information
        };

        let typed_samples = TypedSamples::Float(samples);
        let sensor_data = SensorData::new(sensor, typed_samples);

        Ok(Some(sensor_data))
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
