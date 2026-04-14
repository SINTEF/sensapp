use crate::{
    datamodel::{SensAppDateTime, SensorData},
    storage::StorageInstance,
};
use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use bigquery_publishers::{
    publish_blob_values, publish_boolean_values, publish_float_values, publish_integer_values,
    publish_json_values, publish_location_values, publish_numeric_values, publish_string_values,
};
use bigquery_sensors_utilities::get_sensor_ids_or_create_sensors;
use futures::future::try_join_all;
use gcp_bigquery_client::{
    error::BQError,
    model::{dataset::Dataset, query_request::QueryRequest, query_response::ResultSet},
    storage::StreamName,
};
use once_cell::sync::Lazy;
use regex::Regex;
use std::{future::Future, pin::Pin, str::FromStr, sync::Arc};
use tokio::sync::RwLock;
use tracing::{debug, info};
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

        info!(
            "Connecting to BigQuery with project_id: {}, dataset_id: {}",
            project_id, dataset_id
        );
        debug!("Using service account key file: {}", gcp_sa_key);
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

    fn parse_sensor_type(sensor_type: &str) -> Result<crate::datamodel::SensorType> {
        crate::datamodel::SensorType::from_str(sensor_type).map_err(anyhow::Error::msg)
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
                debug!("BigQuery dataset already exists");
            }
            Err(BQError::ResponseError { error }) if error.error.code == 404 => {
                info!("BigQuery dataset does not exist, creating it");
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

        if let Some(total_rows) = rs
            .total_rows
            .as_deref()
            .and_then(|value| value.parse::<u64>().ok())
            && total_rows > 0
        {
            bail!("BigQuery should not return any rows on the schema creation query");
        }

        Ok(())
    }
    async fn publish(&self, batch: Arc<crate::datamodel::batch::Batch>) -> Result<()> {
        let sensors = batch
            .sensors
            .iter()
            .map(|sensor_batch| sensor_batch.sensor.clone())
            .collect::<Vec<_>>();
        debug!("BigQuery: Publishing batch with {} sensors", sensors.len());
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

        debug!("BigQuery: Waiting for all publishers to finish");
        try_join_all(futures).await?;
        Ok(())
    }

    async fn vacuum(&self) -> Result<()> {
        // Implement vacuum logic here
        Ok(())
    }

    async fn list_series(
        &self,
        _metric_filter: Option<&str>,
        _limit: Option<usize>,
        _bookmark: Option<&str>,
    ) -> Result<crate::storage::ListSeriesResult> {
        // TODO: Implement pagination for BigQuery backend
        // TODO: Implement metric_filter support for BigQuery backend
        // For now, ignore limit, bookmark, and metric_filter parameters and return all results
        use crate::datamodel::{Sensor, sensapp_vec::SensAppLabels, unit::Unit};
        use gcp_bigquery_client::model::query_request::QueryRequest;
        use smallvec::smallvec;
        use std::str::FromStr;
        use uuid::Uuid;

        let query = format!(
            r#"
            SELECT s.sensor_id, s.uuid AS sensor_uuid, s.name AS sensor_name, s.type AS sensor_type, u.name AS unit_name, u.description AS unit_description
            FROM `{}.{}.sensors` s
            LEFT JOIN `{}.{}.units` u ON s.unit = u.id
            ORDER BY s.uuid ASC
            "#,
            self.project_id, self.dataset_id, self.project_id, self.dataset_id
        );

        let rs = self
            .client
            .read()
            .await
            .job()
            .query(&self.project_id, QueryRequest::new(query))
            .await?;
        let mut rs = ResultSet::new_from_query_response(rs);

        let mut sensors = Vec::new();

        while rs.next_row() {
            let sensor_id = rs
                .get_i64_by_name("sensor_id")?
                .context("BigQuery row missing sensor_id")?;
            let sensor_uuid = Uuid::from_str(
                &rs.get_string_by_name("sensor_uuid")?
                    .context("BigQuery row missing sensor_uuid")?,
            )?;
            let sensor_name = rs
                .get_string_by_name("sensor_name")?
                .context("BigQuery row missing sensor_name")?;
            let sensor_type = Self::parse_sensor_type(
                &rs.get_string_by_name("sensor_type")?
                    .context("BigQuery row missing sensor_type")?,
            )?;
            let unit_name = rs.get_string_by_name("unit_name")?;
            let unit_description = rs.get_string_by_name("unit_description")?;
            let unit = unit_name.map(|name| Unit::new(name, unit_description));

            // Query labels for this sensor
            let labels_query = format!(
                r#"
                SELECT lnd.name as label_name, ldd.description as label_value
                FROM `{}.{}.labels` l
                JOIN `{}.{}.labels_name_dictionary` lnd ON l.name = lnd.id
                JOIN `{}.{}.labels_description_dictionary` ldd ON l.description = ldd.id
                WHERE l.sensor_id = {}
                "#,
                self.project_id,
                self.dataset_id,
                self.project_id,
                self.dataset_id,
                self.project_id,
                self.dataset_id,
                sensor_id
            );

            let labels_rs = self
                .client
                .read()
                .await
                .job()
                .query(&self.project_id, QueryRequest::new(labels_query))
                .await?;
            let mut labels_rs = ResultSet::new_from_query_response(labels_rs);

            let mut labels: SensAppLabels = smallvec![];
            while labels_rs.next_row() {
                let label_name = labels_rs
                    .get_string_by_name("label_name")?
                    .context("BigQuery row missing label_name")?;
                let label_value = labels_rs
                    .get_string_by_name("label_value")?
                    .context("BigQuery row missing label_value")?;
                labels.push((label_name, label_value));
            }

            let sensor = Sensor::new(sensor_uuid, sensor_name, sensor_type, unit, Some(labels));

            sensors.push(sensor);
        }

        Ok(crate::storage::ListSeriesResult {
            series: sensors,
            bookmark: None,
        })
    }

    async fn list_metrics(&self) -> Result<Vec<crate::datamodel::Metric>> {
        use crate::datamodel::{Metric, unit::Unit};

        let query = format!(
            r#"
            SELECT s.name AS metric_name, s.type AS sensor_type, u.name AS unit_name, u.description AS unit_description, COUNT(*) AS series_count
            FROM `{}.{}.sensors` s
            LEFT JOIN `{}.{}.units` u ON s.unit = u.id
            GROUP BY s.name, s.type, u.name, u.description
            ORDER BY s.name ASC
            "#,
            self.project_id, self.dataset_id, self.project_id, self.dataset_id
        );

        let rs = self
            .client
            .read()
            .await
            .job()
            .query(&self.project_id, QueryRequest::new(query))
            .await?;
        let mut rs = ResultSet::new_from_query_response(rs);
        let mut metrics = Vec::new();

        while rs.next_row() {
            let metric_name = rs
                .get_string_by_name("metric_name")?
                .context("BigQuery row missing metric_name")?;
            let sensor_type = Self::parse_sensor_type(
                &rs.get_string_by_name("sensor_type")?
                    .context("BigQuery row missing sensor_type")?,
            )?;
            let unit_name = rs.get_string_by_name("unit_name")?;
            let unit_description = rs.get_string_by_name("unit_description")?;
            let unit = unit_name.map(|name| Unit::new(name, unit_description));
            let series_count = rs
                .get_i64_by_name("series_count")?
                .context("BigQuery row missing series_count")?;

            metrics.push(Metric::new(
                metric_name,
                sensor_type,
                unit,
                series_count,
                Vec::new(),
            ));
        }

        Ok(metrics)
    }

    async fn query_sensor_data(
        &self,
        sensor_uuid: &str,
        _start_time: Option<crate::datamodel::SensAppDateTime>,
        _end_time: Option<crate::datamodel::SensAppDateTime>,
        _limit: Option<usize>,
    ) -> Result<Option<crate::datamodel::SensorData>> {
        use crate::datamodel::{Sensor, SensorData, sensapp_vec::SensAppLabels, unit::Unit};
        use gcp_bigquery_client::model::query_request::QueryRequest;
        use smallvec::smallvec;

        // Query sensor metadata by UUID
        let sensor_query = format!(
            r#"
            SELECT s.sensor_id, s.uuid AS sensor_uuid, s.name AS sensor_name, s.type AS sensor_type, u.name AS unit_name, u.description AS unit_description
            FROM `{}.{}.sensors` s
            LEFT JOIN `{}.{}.units` u ON s.unit = u.id
            WHERE s.uuid = '{}'
            "#,
            self.project_id, self.dataset_id, self.project_id, self.dataset_id, sensor_uuid
        );

        let sensor_rs = self
            .client
            .read()
            .await
            .job()
            .query(&self.project_id, QueryRequest::new(sensor_query))
            .await?;
        let mut sensor_rs = ResultSet::new_from_query_response(sensor_rs);
        if !sensor_rs.next_row() {
            return Ok(None);
        }

        let sensor_id = sensor_rs
            .get_i64_by_name("sensor_id")?
            .context("BigQuery row missing sensor_id")?;
        let sensor_uuid = uuid::Uuid::parse_str(
            &sensor_rs
                .get_string_by_name("sensor_uuid")?
                .context("BigQuery row missing sensor_uuid")?,
        )?;
        let sensor_name = sensor_rs
            .get_string_by_name("sensor_name")?
            .context("BigQuery row missing sensor_name")?;
        let sensor_type = Self::parse_sensor_type(
            &sensor_rs
                .get_string_by_name("sensor_type")?
                .context("BigQuery row missing sensor_type")?,
        )?;
        let unit_name = sensor_rs.get_string_by_name("unit_name")?;
        let unit_description = sensor_rs.get_string_by_name("unit_description")?;
        let unit = unit_name.map(|name| Unit::new(name, unit_description));

        // Query labels
        let labels_query = format!(
            r#"
            SELECT lnd.name as label_name, ldd.description as label_value
            FROM `{}.{}.labels` l
            JOIN `{}.{}.labels_name_dictionary` lnd ON l.name = lnd.id
            JOIN `{}.{}.labels_description_dictionary` ldd ON l.description = ldd.id
            WHERE l.sensor_id = {}
            "#,
            self.project_id,
            self.dataset_id,
            self.project_id,
            self.dataset_id,
            self.project_id,
            self.dataset_id,
            sensor_id
        );

        let labels_rs = self
            .client
            .read()
            .await
            .job()
            .query(&self.project_id, QueryRequest::new(labels_query))
            .await?;
        let mut labels_rs = ResultSet::new_from_query_response(labels_rs);

        let mut labels: SensAppLabels = smallvec![];
        while labels_rs.next_row() {
            let label_name = labels_rs
                .get_string_by_name("label_name")?
                .context("BigQuery row missing label_name")?;
            let label_value = labels_rs
                .get_string_by_name("label_value")?
                .context("BigQuery row missing label_value")?;
            labels.push((label_name, label_value));
        }

        let sensor = Sensor::new(
            sensor_uuid,
            sensor_name.to_string(),
            sensor_type,
            unit,
            Some(labels),
        );

        // For BigQuery, we'll return sensor metadata only for now
        // Sample querying would require complex BigQuery-specific logic
        let samples = crate::datamodel::TypedSamples::Integer(smallvec![]);

        Ok(Some(SensorData::new(sensor, samples)))
    }

    async fn query_sensors_by_labels(
        &self,
        _matchers: &[super::LabelMatcher],
        _start_time: Option<SensAppDateTime>,
        _end_time: Option<SensAppDateTime>,
        _limit: Option<usize>,
        _numeric_only: bool,
    ) -> Result<Vec<SensorData>> {
        // TODO: Implement label-based query for BigQuery
        anyhow::bail!("query_sensors_by_labels not yet implemented for BigQuery")
    }

    /// Health check for BigQuery storage
    /// Executes a simple SELECT 1 query to verify BigQuery connectivity
    async fn health_check(&self) -> Result<()> {
        let query = "SELECT 1".to_string();
        self.client
            .read()
            .await
            .job()
            .query(&self.project_id, QueryRequest::new(query))
            .await
            .context("BigQuery health check failed")?;
        Ok(())
    }

    /// Clean up all test data from the database (BigQuery implementation)
    #[cfg(any(test, feature = "test-utils"))]
    async fn cleanup_test_data(&self) -> Result<()> {
        // BigQuery doesn't support traditional TRUNCATE/DELETE operations well
        // For now, this is a no-op since tests typically use separate datasets
        // In a real implementation, you might recreate the dataset or use partitioned tables
        Ok(())
    }
}
