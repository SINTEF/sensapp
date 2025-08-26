use crate::storage::StorageError;
use crate::{
    datamodel::{Sensor, SensorType, TypedSamples},
    storage::StorageInstance,
};
use anyhow::{Context, Result, anyhow, bail};
use async_trait::async_trait;
use rrdcached_client::{
    RRDCachedClient,
    batch_update::BatchUpdate,
    consolidation_function::ConsolidationFunction,
    create::{CreateArguments, CreateDataSource, CreateDataSourceType, CreateRoundRobinArchive},
    errors::RRDCachedClientError,
};
use std::{collections::HashSet, sync::Arc};
use tokio::sync::RwLock;
use tracing::{debug, error};
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
    client: Arc<RwLock<RRDCachedClient>>,

    created_sensors: Arc<RwLock<HashSet<Uuid>>>,

    preset: Preset,
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
                    anyhow::Error::from(StorageError::configuration(
                        "RRDCached connection URL missing host",
                    ))
                })?;
                let port = url.port().ok_or_else(|| {
                    anyhow::Error::from(StorageError::configuration(
                        "RRDCached connection URL missing port",
                    ))
                })?;

                let client = RRDCachedClient::connect_tcp(&format!("{}:{}", host, port)).await?;
                Ok(Self {
                    client: Arc::new(RwLock::new(client)),
                    created_sensors: Arc::new(RwLock::new(HashSet::new())),
                    preset,
                })
            }
            "rrdcached+unix" => {
                unimplemented!()
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

        // Flush the RRD cached client
        {
            let mut client = self.client.write().await;
            client.flush_all().await?;
        }

        Ok(())
    }

    async fn vacuum(&self) -> Result<()> {
        Ok(())
    }

    async fn list_series(&self) -> Result<Vec<String>> {
        unimplemented!();
    }

    async fn query_sensor_data(
        &self,
        _sensor_uuid: &str,
        _start_time: Option<crate::datamodel::SensAppDateTime>,
        _end_time: Option<crate::datamodel::SensAppDateTime>,
        _limit: Option<usize>,
    ) -> Result<Option<crate::datamodel::SensorData>> {
        unimplemented!("RRDCached sensor data querying not yet implemented");
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
