//! BigQuery: an optional backend for research and development. See `README.md` for what it does
//! differently, and `docs/BIGQUERY.md` for the limits and the setup.

use crate::{
    datamodel::{Metric, SensAppDateTime, SensorData, SensorType, batch::Batch, unit::Unit},
    storage::{
        DEFAULT_LIST_SERIES_LIMIT, DEFAULT_QUERY_LIMIT, LabelMatcher, ListSeriesResult,
        MAX_LIST_SERIES_LIMIT, SensorDataQueryOptions, StorageError, StorageInstance,
        selector::SelectorRead,
    },
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use client::{int_param, required, string_param};
use futures::{StreamExt, stream};
use gcp_bigquery_client::{Client, error::BQError, model::dataset::Dataset as BqDataset};
use reads::{Dataset, sample_table};
use std::{
    collections::HashMap,
    str::FromStr,
    sync::{Arc, Mutex},
    time::{Duration, Instant},
};
use tracing::{debug, info};

mod client;
mod connection;
mod matchers;
mod publishers;
mod reads;
mod rows;
mod selector;

/// Where a dataset is created when the connection string does not say
const DEFAULT_LOCATION: &str = "europe-north1";
/// A series that was registered is not looked up again for this long
const REGISTERED_LIFESPAN: Duration = Duration::from_secs(120);
const REGISTERED_CACHE_SIZE: usize = 65_536;
const HEALTH_CHECK_TIMEOUT: Duration = Duration::from_secs(15);
/// Series read at the same time by `query_sensors_by_labels`
const READ_CONCURRENCY: usize = 8;

/// Tables of the schema before the dictionaries were dropped
const LEGACY_TABLES: [&str; 4] = [
    "labels_name_dictionary",
    "labels_description_dictionary",
    "strings_values_dictionary",
    "sensor_labels_view",
];

/// Ids with the time they were seen, forgotten after `lifespan` and when there are too many.
struct RegisteredIds {
    seen: HashMap<i64, Instant>,
    lifespan: Duration,
    capacity: usize,
}

impl RegisteredIds {
    fn new(lifespan: Duration, capacity: usize) -> Self {
        Self {
            seen: HashMap::new(),
            lifespan,
            capacity,
        }
    }

    fn contains(&self, id: i64) -> bool {
        self.seen
            .get(&id)
            .is_some_and(|seen| seen.elapsed() < self.lifespan)
    }

    fn insert(&mut self, id: i64) {
        if self.seen.len() >= self.capacity {
            let lifespan = self.lifespan;
            self.seen.retain(|_, seen| seen.elapsed() < lifespan);
            if self.seen.len() >= self.capacity {
                self.seen.clear();
            }
        }
        self.seen.insert(id, Instant::now());
    }

    fn remove(&mut self, id: i64) {
        self.seen.remove(&id);
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn clear(&mut self) {
        self.seen.clear();
    }
}

pub struct BigQueryStorage {
    client: Client,
    dataset: Dataset,
    /// Where the dataset is created, and where the statements run when it is given
    location: Option<String>,
    max_bytes_billed: Option<i64>,
    /// Ids of the series known to be stored, so that a write does not look them up: a series
    /// deleted by another instance is written to for up to `REGISTERED_LIFESPAN` after the delete.
    registered: Mutex<RegisteredIds>,
}

impl std::fmt::Debug for BigQueryStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("BigQueryStorage")
            .field("project_id", &self.dataset.project_id)
            .field("dataset_id", &self.dataset.dataset_id)
            .finish()
    }
}

impl BigQueryStorage {
    pub async fn connect(connection_string: &str) -> Result<Self> {
        let info = connection::parse_connection_string(connection_string)?;
        info!(
            "Connecting to BigQuery with project_id: {}, dataset_id: {}",
            info.project_id, info.dataset_id
        );
        let client = match &info.credentials_file {
            Some(file) => Client::from_service_account_key_file(file)
                .await
                .with_context(|| format!("Failed to use the service account key {file}"))?,
            None => Client::from_application_default_credentials()
                .await
                .context("Failed to find Application Default Credentials")?,
        };
        Ok(Self {
            client,
            dataset: Dataset {
                project_id: info.project_id,
                dataset_id: info.dataset_id,
            },
            location: info.location,
            max_bytes_billed: info.max_bytes_billed,
            registered: Mutex::new(RegisteredIds::new(
                REGISTERED_LIFESPAN,
                REGISTERED_CACHE_SIZE,
            )),
        })
    }

    fn registered(&self) -> std::sync::MutexGuard<'_, RegisteredIds> {
        self.registered.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn is_registered(&self, sensor_id: i64) -> bool {
        self.registered().contains(sensor_id)
    }

    fn remember_registered(&self, sensor_id: i64) {
        self.registered().insert(sensor_id);
    }

    fn forget_registered(&self, sensor_id: i64) {
        self.registered().remove(sensor_id);
    }

    async fn reject_legacy_schema(&self) -> Result<()> {
        let sql = format!(
            "SELECT table_name FROM `{}.{}.INFORMATION_SCHEMA.TABLES` \
             WHERE table_name IN UNNEST(@names)",
            self.dataset.project_id, self.dataset.dataset_id
        );
        let params = vec![client::string_array_param("names", &LEGACY_TABLES)];
        let found = self
            .query_rows("look for the legacy schema", sql, params, |row| {
                required(row.get_string(0), "table_name")
            })
            .await?;
        if !found.is_empty() {
            return Err(StorageError::Configuration(format!(
                "the BigQuery dataset {} has the tables of the previous SensApp schema ({}); \
                 it cannot be migrated, use a new dataset or drop the old one",
                self.dataset.dataset_id,
                found.join(", ")
            ))
            .into());
        }
        Ok(())
    }
}

#[async_trait]
impl StorageInstance for BigQueryStorage {
    async fn create_or_migrate(&self) -> Result<()> {
        let dataset = &self.dataset;
        match self
            .client
            .dataset()
            .get(&dataset.project_id, &dataset.dataset_id)
            .await
        {
            Ok(_) => debug!("BigQuery dataset already exists"),
            Err(BQError::ResponseError { error }) if error.error.code == 404 => {
                info!("BigQuery dataset does not exist, creating it");
                let location = self.location.as_deref().unwrap_or(DEFAULT_LOCATION);
                self.client
                    .dataset()
                    .create(
                        BqDataset::new(&dataset.project_id, &dataset.dataset_id).location(location),
                    )
                    .await
                    .map_err(|e| client::map_error("create the dataset", e))?;
            }
            Err(e) => return Err(client::map_error("look for the dataset", e)),
        }

        self.reject_legacy_schema().await?;
        let sql = include_str!("migrations/init.sql").replace(
            "{dataset}",
            &format!("{}.{}", dataset.project_id, dataset.dataset_id),
        );
        self.execute("create the tables", sql, Vec::new()).await?;
        Ok(())
    }

    async fn publish(&self, batch: Arc<Batch>) -> Result<()> {
        self.publish_batch(&batch).await
    }

    async fn vacuum(&self) -> Result<()> {
        // Nothing to do: BigQuery compacts its storage by itself
        Ok(())
    }

    async fn delete_series(&self, sensor_uuid: &str) -> Result<bool> {
        let Some((id, sensor)) = self.get_sensor_metadata(sensor_uuid).await? else {
            return Ok(false);
        };
        // One script, the sensor last: a failure in the middle leaves a series that can be
        // deleted again. The id is a parameter of the whole script.
        let sql: String = [sample_table(sensor.sensor_type), "labels", "sensors"]
            .iter()
            .map(|table| format!("DELETE FROM {} WHERE sensor_id = @id;\n", self.table(table)))
            .collect();
        self.execute("delete a series", sql, vec![int_param("id", id)])
            .await?;
        self.forget_registered(id);
        Ok(true)
    }

    async fn delete_series_samples(
        &self,
        sensor_uuid: &str,
        start_time: SensAppDateTime,
        end_time: SensAppDateTime,
    ) -> Result<Option<u64>> {
        let Some((id, sensor)) = self.get_sensor_metadata(sensor_uuid).await? else {
            return Ok(None);
        };
        let micros = |time: &SensAppDateTime| {
            reads::clamp_micros(crate::storage::common::datetime_to_micros(time))
        };
        let sql = format!(
            "DELETE FROM {} WHERE sensor_id = @id \
             AND timestamp BETWEEN TIMESTAMP_MICROS(@start) AND TIMESTAMP_MICROS(@end)",
            self.table(sample_table(sensor.sensor_type))
        );
        let params = vec![
            int_param("id", id),
            int_param("start", micros(&start_time)),
            int_param("end", micros(&end_time)),
        ];
        let deleted = self.execute("delete samples", sql, params).await?;
        Ok(Some(deleted))
    }

    async fn list_series(
        &self,
        metric_filter: Option<&str>,
        limit: Option<usize>,
        bookmark: Option<&str>,
    ) -> Result<ListSeriesResult> {
        let after: Option<i64> = bookmark
            .map(|bookmark| {
                bookmark.parse().map_err(|error| {
                    anyhow::Error::from(StorageError::invalid_data_format(
                        &format!("Invalid bookmark format: {error}"),
                        None,
                        None,
                    ))
                })
            })
            .transpose()?;
        let limit = limit
            .unwrap_or(DEFAULT_LIST_SERIES_LIMIT)
            .min(MAX_LIST_SERIES_LIMIT);

        let mut conditions = Vec::new();
        let mut params = Vec::new();
        if let Some(metric) = metric_filter {
            conditions.push("s.name = @metric".to_string());
            params.push(string_param("metric", metric));
        }
        if let Some(after) = after {
            conditions.push("s.sensor_id > @bookmark".to_string());
            params.push(int_param("bookmark", after));
        }
        // One more than the page, to know whether there is a next one
        let tail = format!("ORDER BY s.sensor_id LIMIT {}", limit.saturating_add(1));
        let mut found = self
            .read_sensors(self.dataset.sensors_sql(&conditions, &tail), params)
            .await?;

        let has_more = found.len() > limit;
        found.truncate(limit);
        let bookmark = has_more
            .then(|| found.last().map(|(id, _)| id.to_string()))
            .flatten();
        Ok(ListSeriesResult {
            series: found.into_iter().map(|(_, sensor)| sensor).collect(),
            bookmark,
        })
    }

    async fn list_metrics(&self) -> Result<Vec<Metric>> {
        let sql = format!(
            "SELECT s.name AS metric_name, s.type AS sensor_type, \
             u.name AS unit_name, u.description AS unit_description, COUNT(*) AS series_count \
             FROM {} s LEFT JOIN {} u ON s.unit = u.id \
             GROUP BY s.name, s.type, u.name, u.description \
             ORDER BY s.name, s.type, u.name",
            self.dataset.sensors_set(),
            self.dataset.units_set()
        );
        self.query_rows("list metrics", sql, Vec::new(), |row| {
            let name = required(row.get_string_by_name("metric_name"), "metric_name")?;
            let sensor_type = required(row.get_string_by_name("sensor_type"), "sensor_type")?;
            let sensor_type = SensorType::from_str(&sensor_type).map_err(|error| {
                StorageError::invalid_data_format(
                    &format!("Failed to parse sensor type '{sensor_type}': {error}"),
                    None,
                    Some(&name),
                )
            })?;
            let unit = row
                .get_string_by_name("unit_name")?
                .map(|unit_name| -> Result<Unit> {
                    Ok(Unit::new(
                        unit_name,
                        row.get_string_by_name("unit_description")?,
                    ))
                })
                .transpose()?;
            let series_count = required(row.get_i64_by_name("series_count"), "series_count")?;
            Ok(Metric::new(
                name,
                sensor_type,
                unit,
                series_count,
                Vec::new(),
            ))
        })
        .await
    }

    async fn query_sensor_data(
        &self,
        sensor_uuid: &str,
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
        limit: Option<usize>,
    ) -> Result<Option<SensorData>> {
        let Some((id, sensor)) = self.get_sensor_metadata(sensor_uuid).await? else {
            return Ok(None);
        };
        let samples = self
            .query_samples_by_type(
                id,
                sensor.sensor_type,
                start_time,
                end_time,
                limit.unwrap_or(DEFAULT_QUERY_LIMIT),
                false,
            )
            .await?;
        Ok(Some(SensorData::new(sensor, samples)))
    }

    /// Aggregations run here, on the raw samples of the window: the `limit` counts the buckets
    /// returned, so it cannot cut the raw samples first.
    async fn query_sensor_data_advanced(
        &self,
        sensor_uuid: &str,
        options: &SensorDataQueryOptions,
    ) -> Result<Option<SensorData>> {
        options.validate()?;
        let aggregating = options.step_ms.is_some();
        let raw_limit = if aggregating { None } else { options.limit };
        let Some(raw) = self
            .query_sensor_data(sensor_uuid, options.start_time, options.end_time, raw_limit)
            .await?
        else {
            return Ok(None);
        };
        let mut data = crate::storage::common::apply_query_options(raw, options)?;
        if aggregating && let Some(limit) = options.limit {
            data.samples.truncate(limit);
        }
        Ok(Some(data))
    }

    async fn query_sensor_data_latest(
        &self,
        sensor_uuid: &str,
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
    ) -> Result<Option<SensorData>> {
        let Some((id, sensor)) = self.get_sensor_metadata(sensor_uuid).await? else {
            return Ok(None);
        };
        let samples = self
            .query_samples_by_type(id, sensor.sensor_type, start_time, end_time, 1, true)
            .await?;
        if samples.is_empty() {
            return Ok(None);
        }
        Ok(Some(SensorData::new(sensor, samples)))
    }

    async fn query_sensors_by_labels(
        &self,
        matchers: &[LabelMatcher],
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
        limit: Option<usize>,
        numeric_only: bool,
    ) -> Result<Vec<SensorData>> {
        if matchers.is_empty() {
            return Ok(Vec::new());
        }
        let sensors = self
            .find_sensors_by_matchers(matchers, numeric_only, None)
            .await?;
        let limit = limit.unwrap_or(DEFAULT_QUERY_LIMIT);
        // In the order of the sensors, a few at a time: every read is a round trip
        stream::iter(sensors)
            .map(|(id, sensor)| async move {
                let samples = self
                    .query_samples_by_type(
                        id,
                        sensor.sensor_type,
                        start_time,
                        end_time,
                        limit,
                        false,
                    )
                    .await?;
                Ok(SensorData::new(sensor, samples))
            })
            .buffered(READ_CONCURRENCY)
            .collect::<Vec<Result<SensorData>>>()
            .await
            .into_iter()
            .collect()
    }

    async fn query_selector(
        &self,
        matchers: &[LabelMatcher],
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
        numeric_only: bool,
        max_series: usize,
        max_samples: usize,
    ) -> Result<SelectorRead> {
        crate::storage::selector::read_selector_in_bulk(
            self,
            matchers,
            start_time,
            end_time,
            numeric_only,
            max_series,
            max_samples,
        )
        .await
    }

    async fn health_check(&self) -> Result<()> {
        tokio::time::timeout(
            HEALTH_CHECK_TIMEOUT,
            self.run_query("health check", "SELECT 1".to_string(), Vec::new()),
        )
        .await
        .map_err(|_| StorageError::Unavailable("health check timed out".to_string()))?
        .context("BigQuery health check failed")?;
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    async fn cleanup_test_data(&self) -> Result<()> {
        // DELETE, not TRUNCATE: recent rows of the Storage Write API can be deleted, and a delete
        // that matches whole partitions only touches metadata.
        let tables = crate::storage::common::VALUE_TABLES
            .into_iter()
            .chain(["labels", "sensors", "units"]);
        futures::future::try_join_all(tables.map(|table| {
            self.execute(
                "clean the tables",
                format!("DELETE FROM {} WHERE TRUE", self.table(table)),
                Vec::new(),
            )
        }))
        .await?;
        self.registered().clear();
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn registered_ids_expire_and_are_bounded() {
        let mut ids = RegisteredIds::new(Duration::from_secs(60), 2);
        ids.insert(1);
        assert!(ids.contains(1) && !ids.contains(2));
        ids.remove(1);
        assert!(!ids.contains(1));

        // Too many: forgotten all at once, never a growing map
        ids.insert(1);
        ids.insert(2);
        ids.insert(3);
        assert!(ids.contains(3) && ids.seen.len() <= 2);
        ids.clear();
        assert!(!ids.contains(3));

        let mut short = RegisteredIds::new(Duration::ZERO, 10);
        short.insert(1);
        assert!(!short.contains(1), "expired at once");
    }

    #[test]
    fn the_legacy_tables_are_not_in_the_schema() {
        let schema = include_str!("migrations/init.sql");
        for table in LEGACY_TABLES {
            assert!(!schema.contains(table), "{table}");
        }
    }
}
