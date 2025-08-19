use crate::storage::storage::StorageInstance;
use anyhow::{Result, bail};
use async_trait::async_trait;
use bigquery_publishers::{
    publish_blob_values, publish_boolean_values, publish_float_values, publish_integer_values,
    publish_json_values, publish_location_values, publish_numeric_values, publish_string_values,
};
use bigquery_sensors_utilities::get_sensor_ids_or_create_sensors;
use futures::future::try_join_all;
use gcp_bigquery_client::{
    error::BQError,
    model::{dataset::Dataset, query_request::QueryRequest},
    storage::StreamName,
};
use once_cell::sync::Lazy;
use regex::Regex;
use std::{future::Future, pin::Pin, sync::Arc, time::Duration};
use tokio::{sync::RwLock, time::timeout};
use url::Url;

mod bigquery_labels_utilities;
mod bigquery_prost_structs;
mod bigquery_publishers;
mod bigquery_sensors_utilities;
mod bigquery_string_values_utilities;
mod bigquery_table_descriptors;
mod bigquery_units_utilities;
mod bigquery_utilities;

pub struct BigQueryStorage {
    client: Arc<RwLock<gcp_bigquery_client::Client>>,

    project_id: String,

    dataset_id: String,
}

impl std::fmt::Debug for BigQueryStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BigQueryStorage")
            .field("project_id", &self.project_id)
            .field("dataset_id", &self.dataset_id)
            .finish()
    }
}

fn parse_connection_string(connection_string: &str) -> Result<(String, String, String)> {
    let url = Url::parse(connection_string)?;
    if url.scheme() != "bigquery" {
        bail!("Invalid scheme in connection string: {}", url.scheme());
    }

    static URL_PARSE_REX: Lazy<Regex> =
        Lazy::new(|| Regex::new(r"^bigquery://?(.*?)(\?|$)").expect("Failed to compile regex"));

    let gcp_sa_key = URL_PARSE_REX
        .captures(connection_string)
        .map(|caps| caps.get(1).expect("Failed to get capture").as_str())
        .expect("Failed to get capture")
        .to_string();

    let mut project_id = String::new();
    let mut dataset_id = String::new();

    for (key, value) in url.query_pairs() {
        match key.as_ref() {
            "project_id" => project_id = value.into_owned(),
            "dataset_id" => dataset_id = value.into_owned(),
            _ => {} // Ignore unknown parameters
        }
    }

    if project_id.is_empty() {
        bail!("project_id is required in connection string");
    }
    if dataset_id.is_empty() {
        bail!("dataset_id is required in connection string");
    }

    Ok((gcp_sa_key, project_id, dataset_id))
}

impl BigQueryStorage {
    pub async fn connect(connection_string: &str) -> Result<Self> {
        let (gcp_sa_key, project_id, dataset_id) = parse_connection_string(connection_string)?;

        println!(
            "Connecting to BigQuery with project_id: {}, dataset_id: {}",
            project_id, dataset_id
        );
        println!("File: {}", gcp_sa_key);
        let client = Arc::new(RwLock::new(
            gcp_bigquery_client::Client::from_service_account_key_file(&gcp_sa_key).await?,
        ));

        Ok(Self {
            client,
            project_id,
            dataset_id,
        })
    }

    pub fn client(&self) -> Arc<RwLock<gcp_bigquery_client::Client>> {
        self.client.clone()
    }

    pub fn project_id(&self) -> &str {
        &self.project_id
    }

    pub fn dataset_id(&self) -> &str {
        &self.dataset_id
    }

    pub fn new_stream_name(&self, table: String) -> StreamName {
        StreamName::new_default(self.project_id.clone(), self.dataset_id.clone(), table)
    }
}

#[async_trait]
impl StorageInstance for BigQueryStorage {
    async fn create_or_migrate(&self) -> Result<()> {
        match self
            .client
            .read()
            .await
            .dataset()
            .get(&self.project_id, &self.dataset_id)
            .await
        {
            Ok(_) => {
                println!("Dataset already exists");
            }
            Err(BQError::ResponseError { error }) if error.error.code == 404 => {
                println!("Dataset does not exist, creating it");
                let dataset =
                    Dataset::new(&self.project_id, &self.dataset_id).location("europe-north1");
                self.client.read().await.dataset().create(dataset).await?;
            }
            Err(e) => {
                return Err(e.into());
            }
        }
        // client.dataset().create(dataset).await.unwrap();

        const INIT_SQL: &str = include_str!("./migrations/20240223133248_init.sql");

        let parametrized_init_sql = INIT_SQL
            .replace("{project_id}", &self.project_id)
            .replace("{dataset_id}", &self.dataset_id);

        let rs = self
            .client
            .read()
            .await
            .job()
            .query(&self.project_id, QueryRequest::new(parametrized_init_sql))
            .await?;

        if let Some(total_rows) = rs.total_rows() {
            if total_rows > 0 {
                bail!("BigQuery should not return any rows on the schema creation query");
            }
        }

        Ok(())
    }
    async fn publish(
        &self,
        batch: Arc<crate::datamodel::batch::Batch>,
        sync_sender: async_broadcast::Sender<()>,
    ) -> Result<()> {
        let sensors = batch
            .sensors
            .iter()
            .map(|sensor_batch| sensor_batch.sensor.clone())
            .collect::<Vec<_>>();
        println!("Publishing batch with {} sensors", sensors.len());
        let sensor_ids = Arc::new(get_sensor_ids_or_create_sensors(self, &sensors).await?);

        let futures: Vec<Pin<Box<dyn Future<Output = Result<(), _>> + Send>>> = vec![
            Box::pin(publish_integer_values(
                self,
                batch.clone(),
                sensor_ids.clone(),
            )),
            Box::pin(publish_numeric_values(
                self,
                batch.clone(),
                sensor_ids.clone(),
            )),
            Box::pin(publish_float_values(
                self,
                batch.clone(),
                sensor_ids.clone(),
            )),
            Box::pin(publish_string_values(
                self,
                batch.clone(),
                sensor_ids.clone(),
            )),
            Box::pin(publish_boolean_values(
                self,
                batch.clone(),
                sensor_ids.clone(),
            )),
            Box::pin(publish_location_values(
                self,
                batch.clone(),
                sensor_ids.clone(),
            )),
            Box::pin(publish_json_values(self, batch.clone(), sensor_ids.clone())),
            Box::pin(publish_blob_values(self, batch.clone(), sensor_ids.clone())),
        ];

        println!("Waiting for all publishers to finish");
        try_join_all(futures).await?;
        println!("All publishers finished, syncing");
        self.sync(sync_sender).await?;
        println!("Sync finished, yo yo yo");
        Ok(())
    }

    async fn sync(&self, sync_sender: async_broadcast::Sender<()>) -> Result<()> {
        // SQLite doesn't need to do anything special for sync
        // As we use transactions and the WAL mode.
        if sync_sender.receiver_count() > 0 && !sync_sender.is_closed() {
            let _ = timeout(Duration::from_secs(15), sync_sender.broadcast(())).await?;
        }
        Ok(())
    }

    async fn vacuum(&self) -> Result<()> {
        // Implement vacuum logic here
        Ok(())
    }

    async fn list_sensors(&self) -> Result<Vec<String>> {
        unimplemented!();
    }

    async fn query_sensor_data(
        &self,
        _sensor_name: &str,
        _start_time: Option<i64>,
        _end_time: Option<i64>,
        _limit: Option<usize>,
    ) -> Result<Option<crate::datamodel::SensorData>> {
        unimplemented!("BigQuery sensor data querying not yet implemented");
    }
}
