//! BigQuery: an optional backend for research and development. See `README.md` for what it does
//! differently, and `docs/BIGQUERY.md` for the limits and the setup.

use crate::{
    datamodel::{
        Metric, SensAppDateTime, Sensor, SensorData, SensorType, batch::Batch, unit::Unit,
    },
    storage::{
        DEFAULT_LIST_SERIES_LIMIT, DEFAULT_QUERY_LIMIT, LabelMatcher, ListSeriesResult,
        MAX_LIST_SERIES_LIMIT, SensorDataQueryOptions, StorageError, StorageInstance,
        selector::{AggregatedRead, SelectorRead, aggregated_kind, empty_samples},
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

mod aggregation;
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

/// Ids with the time they were seen and the identity (name and type) they were stored with,
/// forgotten after `lifespan` and when there are too many. A write with another identity is not a
/// hit: it is looked up, and refused when the stored identity differs.
struct RegisteredIds {
    seen: HashMap<i64, (Instant, u64)>,
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

    fn contains(&self, id: i64, identity: u64) -> bool {
        self.seen
            .get(&id)
            .is_some_and(|(seen, stored)| *stored == identity && seen.elapsed() < self.lifespan)
    }

    fn insert(&mut self, id: i64, identity: u64) {
        if self.seen.len() >= self.capacity {
            let lifespan = self.lifespan;
            self.seen.retain(|_, (seen, _)| seen.elapsed() < lifespan);
            if self.seen.len() >= self.capacity {
                self.seen.clear();
            }
        }
        self.seen.insert(id, (Instant::now(), identity));
    }

    fn remove(&mut self, id: i64) {
        self.seen.remove(&id);
    }

    #[cfg(any(test, feature = "test-utils"))]
    fn clear(&mut self) {
        self.seen.clear();
    }
}

/// The UUID and the type of a sensor, hashed, for `RegisteredIds`.
fn identity_fingerprint(sensor: &Sensor) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    // Another UUID may have the same `sensor_id`
    sensor.uuid.hash(&mut hasher);
    sensor.sensor_type.to_string().hash(&mut hasher);
    hasher.finish()
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
        // The gRPC client of the Storage Write API builds its TLS configuration from the default
        // provider of rustls, and refuses to guess when both `ring` and `aws-lc-rs` are compiled in
        // (the REST client does not care). The server installs it at startup; a library user or a
        // test did not. `Err` only says that one is installed already.
        let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();
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

    fn is_registered(&self, sensor_id: i64, sensor: &Sensor) -> bool {
        self.registered()
            .contains(sensor_id, identity_fingerprint(sensor))
    }

    fn remember_registered(&self, sensor_id: i64, sensor: &Sensor) {
        self.registered()
            .insert(sensor_id, identity_fingerprint(sensor));
    }

    fn forget_registered(&self, sensor_id: i64) {
        self.registered().remove(sensor_id);
    }

    /// Refuse a dataset whose sensors were keyed another way, see `check_sensor_keys`. The keys are
    /// stored as signed `INT64`: the same bits.
    async fn check_stored_sensor_keys(&self) -> Result<()> {
        let sql = format!(
            "SELECT uuid, sensor_id FROM `{}.{}.sensors` LIMIT {}",
            self.dataset.project_id,
            self.dataset.dataset_id,
            crate::storage::common::SENSOR_KEY_GUARD_ROWS
        );
        let keys = self
            .query_rows(
                "read the keys of the stored sensors",
                sql,
                Vec::new(),
                |row| {
                    let uuid = required(row.get_string(0), "uuid")?;
                    let uuid = uuid::Uuid::parse_str(&uuid)
                        .map_err(|e| anyhow::anyhow!("Invalid stored UUID '{uuid}': {e}"))?;
                    let sensor_id = required(row.get_i64(1), "sensor_id")?;
                    Ok((uuid, sensor_id as u64))
                },
            )
            .await?;
        crate::storage::common::check_stored_sensor_keys(&keys)
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
        self.check_stored_sensor_keys().await?;
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
        let sql = list_metrics_sql(&self.dataset);
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
            let label_keys = required(row.get_string_by_name("label_keys"), "label_keys")?;
            let label_keys: Vec<String> = serde_json::from_str(&label_keys).map_err(|error| {
                StorageError::invalid_data_format(
                    &format!("Failed to read the label keys '{label_keys}': {error}"),
                    None,
                    Some(&name),
                )
            })?;
            Ok(Metric::new(
                name,
                sensor_type,
                unit,
                series_count,
                label_keys,
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

    /// Aggregations run in BigQuery for the numeric series; the `limit` counts the buckets. The
    /// other types have no aggregation, as on the other backends.
    async fn query_sensor_data_advanced(
        &self,
        sensor_uuid: &str,
        options: &SensorDataQueryOptions,
    ) -> Result<Option<SensorData>> {
        options.validate()?;
        let (Some(step_ms), Some(aggregation)) = (options.step_ms, options.aggregation) else {
            let raw = self
                .query_sensor_data(
                    sensor_uuid,
                    options.start_time,
                    options.end_time,
                    options.limit,
                )
                .await?;
            return raw
                .map(|data| crate::storage::common::apply_query_options(data, options))
                .transpose();
        };

        let Some((id, mut sensor)) = self.get_sensor_metadata(sensor_uuid).await? else {
            return Ok(None);
        };
        if !matches!(
            sensor.sensor_type,
            SensorType::Integer | SensorType::Numeric | SensorType::Float
        ) {
            anyhow::bail!("aggregation is only supported for numeric series");
        }
        let micros = |time: &SensAppDateTime| crate::storage::common::datetime_to_micros(time);
        let read = AggregatedRead {
            start_us: options.start_time.as_ref().map(micros),
            end_us: options.end_time.as_ref().map(micros),
            step_ms,
            aggregation,
        };
        let limit = options.limit.unwrap_or(DEFAULT_QUERY_LIMIT);
        let kind = aggregation::kind_type(aggregated_kind(sensor.sensor_type, aggregation));
        let samples = self
            .query_aggregated_of_many(sensor.sensor_type, &[id], &read, limit)
            .await?
            .remove(&id)
            .unwrap_or_else(|| empty_samples(kind));
        sensor.sensor_type = kind;
        if aggregation.output_is_count() {
            sensor.unit = None;
        }

        // What is left to do is the simplification, on the buckets
        let simplify_only = SensorDataQueryOptions {
            start_time: options.start_time,
            end_time: options.end_time,
            limit: options.limit,
            step_ms: None,
            aggregation: None,
            simplify: options.simplify,
        };
        crate::storage::common::apply_query_options(
            SensorData::new(sensor, samples),
            &simplify_only,
        )
        .map(Some)
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

    async fn query_sensor_data_availability(
        &self,
        sensor_uuid: &str,
        start_time: SensAppDateTime,
        end_time: SensAppDateTime,
        step_ms: Option<i64>,
    ) -> Result<Option<crate::storage::SensorAvailabilitySummary>> {
        let Some((id, sensor)) = self.get_sensor_metadata(sensor_uuid).await? else {
            return Ok(None);
        };
        self.query_availability(
            id,
            sensor,
            crate::storage::common::datetime_to_micros(&start_time),
            crate::storage::common::datetime_to_micros(&end_time),
            step_ms,
        )
        .await
        .map(Some)
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

    async fn query_selector_aggregated(
        &self,
        matchers: &[LabelMatcher],
        options: &SensorDataQueryOptions,
        max_series: usize,
        max_samples: usize,
    ) -> Result<SelectorRead> {
        crate::storage::selector::read_aggregated_selector_in_bulk(
            self,
            matchers,
            options,
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

/// The metrics, one per name and type as on the other backends, with their label keys as a JSON
/// array: a key may hold any character, a comma too.
fn list_metrics_sql(dataset: &Dataset) -> String {
    format!(
        "SELECT s.name AS metric_name, s.type AS sensor_type, \
         MIN(u.name) AS unit_name, MIN(u.description) AS unit_description, \
         COUNT(DISTINCT s.sensor_id) AS series_count, \
         TO_JSON_STRING(IFNULL(ARRAY_AGG(DISTINCT l.name IGNORE NULLS ORDER BY l.name), [])) \
         AS label_keys \
         FROM {} s LEFT JOIN {} u ON s.unit = u.id LEFT JOIN {} l ON l.sensor_id = s.sensor_id \
         GROUP BY s.name, s.type \
         ORDER BY s.name, s.type",
        dataset.sensors_set(),
        dataset.units_set(),
        dataset.table("labels")
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn metrics_list_their_label_keys_as_json() {
        let sql = list_metrics_sql(&Dataset {
            project_id: "p".to_string(),
            dataset_id: "d".to_string(),
        });
        assert!(sql.contains("TO_JSON_STRING(IFNULL(ARRAY_AGG(DISTINCT l.name IGNORE NULLS"));
        assert!(sql.contains("COUNT(DISTINCT s.sensor_id)"));
        assert!(sql.contains("GROUP BY s.name, s.type "));
    }

    #[test]
    fn registered_ids_expire_and_are_bounded() {
        let mut ids = RegisteredIds::new(Duration::from_secs(60), 2);
        ids.insert(1, 10);
        assert!(ids.contains(1, 10) && !ids.contains(2, 10));
        assert!(!ids.contains(1, 11), "another identity is not a hit");
        ids.remove(1);
        assert!(!ids.contains(1, 10));

        // Too many: forgotten all at once, never a growing map
        ids.insert(1, 10);
        ids.insert(2, 10);
        ids.insert(3, 10);
        assert!(ids.contains(3, 10) && ids.seen.len() <= 2);
        ids.clear();
        assert!(!ids.contains(3, 10));

        let mut short = RegisteredIds::new(Duration::ZERO, 10);
        short.insert(1, 10);
        assert!(!short.contains(1, 10), "expired at once");
    }

    #[test]
    fn the_legacy_tables_are_not_in_the_schema() {
        let schema = include_str!("migrations/init.sql");
        for table in LEGACY_TABLES {
            assert!(!schema.contains(table), "{table}");
        }
    }
}
