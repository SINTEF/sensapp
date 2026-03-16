use super::{
    Aggregation, DEFAULT_QUERY_LIMIT, SensorAvailabilitySummary, SensorDataQueryOptions,
    StorageError, StorageInstance,
};
use crate::datamodel::sensapp_vec::SensAppLabels;
use crate::datamodel::{
    Metric, Sample, SensAppDateTime, Sensor, SensorData, SensorType, TypedSamples, batch::Batch,
    unit::Unit,
};
use anyhow::{Context, Result};
use async_trait::async_trait;
use base64::prelude::*;
use clickhouse::Client;
use geo::Point;
use serde_json::Value as JsonValue;
use std::{str::FromStr, sync::Arc};
use uuid::Uuid;

pub mod clickhouse_publishers;
pub mod clickhouse_utilities;
mod matchers;

use crate::storage::{DEFAULT_LIST_SERIES_LIMIT, MAX_LIST_SERIES_LIMIT};
use clickhouse_publishers::ClickHousePublisher;
use clickhouse_utilities::{
    datetime_to_micros, decimal_from_clickhouse_raw, map_clickhouse_error, micros_to_datetime,
    uuid_to_sensor_id,
};

pub struct ClickHouseStorage {
    #[allow(dead_code)]
    client: Client,
    database: Option<String>,
    host: String,
    port: u16,
    user: String,
    password: Option<String>,
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct LatestTimestampRow {
    timestamp_us: i64,
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct AvailabilitySummaryWithStepRow {
    sample_count: u64,
    first_sample_at: Option<i64>,
    last_sample_at: Option<i64>,
    covered_buckets: u64,
}

#[derive(clickhouse::Row, serde::Deserialize)]
struct AvailabilitySummaryRow {
    sample_count: u64,
    first_sample_at: Option<i64>,
    last_sample_at: Option<i64>,
}

impl std::fmt::Debug for ClickHouseStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ClickHouseStorage")
            .field("client", &"<ClickHouse Client>")
            .finish()
    }
}

impl ClickHouseStorage {
    pub async fn connect(connection_string: &str) -> Result<Self> {
        // Parse ClickHouse connection string
        // Format: clickhouse://user:password@host:port/database
        let url = connection_string
            .strip_prefix("clickhouse://")
            .context("ClickHouse connection string must start with 'clickhouse://'")?;

        let (auth, rest) = if let Some(at_pos) = url.find('@') {
            let (auth_part, rest) = url.split_at(at_pos);
            (Some(auth_part), &rest[1..]) // Skip the '@'
        } else {
            (None, url)
        };

        let (host_port, database) = if let Some(slash_pos) = rest.find('/') {
            let (host_part, db_part) = rest.split_at(slash_pos);
            (host_part, Some(&db_part[1..])) // Skip the '/'
        } else {
            (rest, None)
        };

        let (host, port) = if let Some(colon_pos) = host_port.rfind(':') {
            let (host, port_str) = host_port.split_at(colon_pos);
            let port = port_str[1..]
                .parse::<u16>()
                .context("Invalid port number in ClickHouse connection string")?;
            (host, port)
        } else {
            (host_port, 8123) // Default ClickHouse HTTP port
        };

        let (user, password) = if let Some(auth) = auth {
            if let Some(colon_pos) = auth.find(':') {
                let (user, pass) = auth.split_at(colon_pos);
                (user, Some(&pass[1..])) // Skip the ':'
            } else {
                (auth, None)
            }
        } else {
            ("default", None)
        };

        // Build ClickHouse HTTP URL
        let http_url = format!("http://{}:{}", host, port);

        let mut client = Client::default().with_url(&http_url).with_user(user);

        if let Some(password) = password {
            client = client.with_password(password);
        }

        if let Some(database) = database {
            client = client.with_database(database);
        }

        Ok(Self {
            client,
            database: database.map(|s| s.to_string()),
            host: host.to_string(),
            port,
            user: user.to_string(),
            password: password.map(|s| s.to_string()),
        })
    }

    /// Run migrations by executing SQL files
    async fn run_migrations(&self) -> Result<()> {
        // First, create the database if it doesn't exist
        if let Some(database) = &self.database {
            // Create a client without database specification to create the database
            let http_url = format!("http://{}:{}", self.host, self.port);
            let mut create_db_client = Client::default().with_url(&http_url).with_user(&self.user);

            if let Some(password) = &self.password {
                create_db_client = create_db_client.with_password(password);
            }

            let create_db_query = format!("CREATE DATABASE IF NOT EXISTS {}", database);

            create_db_client
                .query(&create_db_query)
                .execute()
                .await
                .context("Failed to create database")?;
        }

        // Read and execute the migration SQL
        let migration_sql = include_str!("migrations/20240223133248_init.sql");

        // Split by semicolon and execute each statement
        let statements: Vec<&str> = migration_sql.split(';').collect();

        for (i, statement) in statements.iter().enumerate() {
            let statement = statement.trim();

            // Remove inline comments and clean up the statement
            let clean_statement = statement
                .lines()
                .filter(|line| !line.trim().starts_with("--") && !line.trim().is_empty())
                .collect::<Vec<_>>()
                .join("\n")
                .trim()
                .to_string();

            // Skip if no SQL content after removing comments
            if clean_statement.is_empty() {
                continue;
            }

            self.client
                .query(&clean_statement)
                .execute()
                .await
                .with_context(|| {
                    format!(
                        "Failed to execute migration statement {}: {}",
                        i + 1,
                        clean_statement
                    )
                })?;
        }

        Ok(())
    }

    async fn get_sensor_metadata(&self, sensor_uuid: &str) -> Result<Option<(u64, Sensor)>> {
        let uuid = Uuid::from_str(sensor_uuid).map_err(|e| {
            StorageError::invalid_data_format(
                &format!("Invalid UUID '{}': {}", sensor_uuid, e),
                None,
                None,
            )
        })?;

        let sensor_id = uuid_to_sensor_id(&uuid);

        let sensor_query = r#"
            SELECT
                s.uuid,
                s.name,
                s.type,
                u.name AS unit_name,
                u.description AS unit_description
            FROM sensors s
            LEFT JOIN units u ON s.unit = u.id
            WHERE s.sensor_id = ?
            LIMIT 1
        "#;

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct SensorMetadataRow {
            #[serde(with = "clickhouse::serde::uuid")]
            uuid: Uuid,
            name: String,
            r#type: String,
            unit_name: String,
            unit_description: Option<String>,
        }

        let mut sensor_cursor = self
            .client
            .query(sensor_query)
            .bind(sensor_id)
            .fetch::<SensorMetadataRow>()
            .map_err(|e| map_clickhouse_error(e, Some(uuid), None))?;

        let Some(row) = sensor_cursor.next().await? else {
            return Ok(None);
        };

        let sensor_type = SensorType::from_str(&row.r#type).map_err(|e| {
            StorageError::invalid_data_format(
                &format!("Failed to parse sensor type '{}': {}", row.r#type, e),
                Some(row.uuid),
                Some(&row.name),
            )
        })?;

        let unit = if !row.unit_name.is_empty() {
            Some(Unit {
                name: row.unit_name,
                description: row.unit_description,
            })
        } else {
            None
        };

        let labels_query = "SELECT name, COALESCE(description, '') FROM labels WHERE sensor_id = ?";
        let mut labels_cursor = self
            .client
            .query(labels_query)
            .bind(sensor_id)
            .fetch::<(String, String)>()
            .map_err(|e| map_clickhouse_error(e, Some(row.uuid), Some(&row.name)))?;

        let mut labels = Vec::new();
        while let Some((label_name, label_description)) = labels_cursor.next().await? {
            labels.push((label_name, label_description));
        }

        Ok(Some((
            sensor_id,
            Sensor {
                uuid: row.uuid,
                name: row.name,
                sensor_type,
                unit,
                labels: SensAppLabels::from(labels),
            },
        )))
    }

    fn sensor_table_name(sensor_type: SensorType) -> &'static str {
        match sensor_type {
            SensorType::Integer => "integer_values",
            SensorType::Numeric => "numeric_values",
            SensorType::Float => "float_values",
            SensorType::String => "string_values",
            SensorType::Boolean => "boolean_values",
            SensorType::Location => "location_values",
            SensorType::Json => "json_values",
            SensorType::Blob => "blob_values",
        }
    }

    async fn query_latest_timestamp_us(
        &self,
        table_name: &str,
        sensor_id: u64,
        start_time_us: Option<i64>,
        end_time_us: Option<i64>,
    ) -> Result<Option<i64>> {
        let query = format!(
            "SELECT timestamp_us FROM {table_name} WHERE sensor_id = ?{} ORDER BY timestamp_us DESC LIMIT 1",
            clickhouse_time_where(start_time_us, end_time_us)
        );

        let mut cursor = self.client.query(&query).bind(sensor_id);
        if let Some(start_time_us) = start_time_us {
            cursor = cursor.bind(start_time_us);
        }
        if let Some(end_time_us) = end_time_us {
            cursor = cursor.bind(end_time_us);
        }

        let mut rows = cursor
            .fetch::<LatestTimestampRow>()
            .map_err(|e| map_clickhouse_error(e, None, None))?;

        Ok(rows.next().await?.map(|row| row.timestamp_us))
    }

    async fn query_availability_summary_native(
        &self,
        table_name: &str,
        sensor_id: u64,
        sensor: Sensor,
        start_time_us: i64,
        end_time_us: i64,
        step_ms: Option<i64>,
    ) -> Result<SensorAvailabilitySummary> {
        if let Some(step_ms) = step_ms {
            let step_us = step_ms.checked_mul(1000).context("step is too large")?;
            let query = format!(
                r#"
                SELECT
                    count() AS sample_count,
                    minOrNull(timestamp_us) AS first_sample_at,
                    maxOrNull(timestamp_us) AS last_sample_at,
                    countDistinct(intDiv(timestamp_us - ?, ?)) AS covered_buckets
                FROM {table_name}
                WHERE sensor_id = ?
                  AND timestamp_us >= ?
                  AND timestamp_us <= ?
                "#
            );

            let mut rows = self.client
                .query(&query)
                .bind(start_time_us)
                .bind(step_us)
                .bind(sensor_id)
                .bind(start_time_us)
                .bind(end_time_us)
                .fetch::<AvailabilitySummaryWithStepRow>()
                .map_err(|e| map_clickhouse_error(e, Some(sensor.uuid), Some(&sensor.name)))?;

            let row = rows.next().await?.unwrap_or(AvailabilitySummaryWithStepRow {
                sample_count: 0,
                first_sample_at: None,
                last_sample_at: None,
                covered_buckets: 0,
            });

            return Ok(SensorAvailabilitySummary {
                sensor,
                sample_count: row.sample_count as usize,
                first_sample_at: row.first_sample_at.map(micros_to_datetime),
                last_sample_at: row.last_sample_at.map(micros_to_datetime),
                covered_buckets: Some(row.covered_buckets as usize),
            });
        }

        let query = format!(
            r#"
            SELECT
                count() AS sample_count,
                minOrNull(timestamp_us) AS first_sample_at,
                maxOrNull(timestamp_us) AS last_sample_at
            FROM {table_name}
            WHERE sensor_id = ?
              AND timestamp_us >= ?
              AND timestamp_us <= ?
            "#
        );

        let mut rows = self.client
            .query(&query)
            .bind(sensor_id)
            .bind(start_time_us)
            .bind(end_time_us)
            .fetch::<AvailabilitySummaryRow>()
            .map_err(|e| map_clickhouse_error(e, Some(sensor.uuid), Some(&sensor.name)))?;

        let row = rows.next().await?.unwrap_or(AvailabilitySummaryRow {
            sample_count: 0,
            first_sample_at: None,
            last_sample_at: None,
        });

        Ok(SensorAvailabilitySummary {
            sensor,
            sample_count: row.sample_count as usize,
            first_sample_at: row.first_sample_at.map(micros_to_datetime),
            last_sample_at: row.last_sample_at.map(micros_to_datetime),
            covered_buckets: None,
        })
    }
}

#[async_trait]
impl StorageInstance for ClickHouseStorage {
    async fn create_or_migrate(&self) -> Result<()> {
        self.run_migrations()
            .await
            .context("Failed to run ClickHouse migrations")?;
        Ok(())
    }

    async fn publish(&self, batch: Arc<Batch>) -> Result<()> {
        let mut publisher = ClickHousePublisher::new(&self.client);

        for single_sensor_batch in batch.sensors.as_ref() {
            publisher
                .publish_single_sensor_batch(single_sensor_batch)
                .await?;
        }

        // Commit all inserters in parallel
        publisher.commit_all().await?;

        Ok(())
    }

    async fn vacuum(&self) -> Result<()> {
        // ClickHouse doesn't have a traditional VACUUM operation
        // Instead, we can trigger OPTIMIZE for all tables to merge parts
        let tables = vec![
            "integer_values",
            "numeric_values",
            "float_values",
            "string_values",
            "boolean_values",
            "location_values",
            "json_values",
            "blob_values",
        ];

        for table in tables {
            let query = format!("OPTIMIZE TABLE {}", table);
            self.client
                .query(&query)
                .execute()
                .await
                .with_context(|| format!("Failed to optimize table {}", table))?;
        }

        Ok(())
    }

    async fn list_series(
        &self,
        metric_filter: Option<&str>,
        limit: Option<usize>,
        bookmark: Option<&str>,
    ) -> Result<crate::storage::ListSeriesResult> {
        let bookmark_id = if let Some(bookmark_str) = bookmark {
            Some(bookmark_str.parse::<u64>().map_err(|e| {
                anyhow::Error::from(StorageError::invalid_data_format(
                    &format!("Invalid bookmark format: {}", e),
                    None,
                    None,
                ))
            })?)
        } else {
            None
        };

        let effective_limit = limit
            .unwrap_or(DEFAULT_LIST_SERIES_LIMIT)
            .min(MAX_LIST_SERIES_LIMIT);
        let fetch_limit = effective_limit.saturating_add(1);

        let (query, use_filter, use_bookmark) =
            match (metric_filter.is_some(), bookmark_id.is_some()) {
                (true, true) => (
                    r#"
                    SELECT s.sensor_id, s.uuid, s.name, s.type,
                           COALESCE(u.name, '') as unit_name,
                           COALESCE(u.description, '') as unit_description
                    FROM sensors s
                    LEFT JOIN units u ON s.unit = u.id
                    WHERE s.name = ? AND s.sensor_id > ?
                    ORDER BY s.sensor_id ASC
                    LIMIT ?
                "#,
                    true,
                    true,
                ),
                (true, false) => (
                    r#"
                    SELECT s.sensor_id, s.uuid, s.name, s.type,
                           COALESCE(u.name, '') as unit_name,
                           COALESCE(u.description, '') as unit_description
                    FROM sensors s
                    LEFT JOIN units u ON s.unit = u.id
                    WHERE s.name = ?
                    ORDER BY s.sensor_id ASC
                    LIMIT ?
                "#,
                    true,
                    false,
                ),
                (false, true) => (
                    r#"
                    SELECT s.sensor_id, s.uuid, s.name, s.type,
                           COALESCE(u.name, '') as unit_name,
                           COALESCE(u.description, '') as unit_description
                    FROM sensors s
                    LEFT JOIN units u ON s.unit = u.id
                    WHERE s.sensor_id > ?
                    ORDER BY s.sensor_id ASC
                    LIMIT ?
                "#,
                    false,
                    true,
                ),
                (false, false) => (
                    r#"
                    SELECT s.sensor_id, s.uuid, s.name, s.type,
                           COALESCE(u.name, '') as unit_name,
                           COALESCE(u.description, '') as unit_description
                    FROM sensors s
                    LEFT JOIN units u ON s.unit = u.id
                    ORDER BY s.sensor_id ASC
                    LIMIT ?
                "#,
                    false,
                    false,
                ),
            };

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct SensorRow {
            sensor_id: u64,
            #[serde(with = "clickhouse::serde::uuid")]
            uuid: Uuid,
            name: String,
            r#type: String,
            unit_name: String,
            unit_description: String,
        }

        let mut query_builder = self.client.query(query);
        if use_filter {
            query_builder = query_builder.bind(metric_filter.unwrap());
        }
        if use_bookmark {
            query_builder = query_builder.bind(bookmark_id.unwrap());
        }
        let mut cursor = query_builder
            .bind(fetch_limit as u64)
            .fetch::<SensorRow>()
            .map_err(|e| map_clickhouse_error(e, None, None))?;

        // Process sensors directly from cursor
        let mut sensors = Vec::new();
        let mut last_sensor_id = None;
        let mut has_more = false;

        while let Some(row) = cursor.next().await? {
            if sensors.len() == effective_limit {
                has_more = true;
                break;
            }

            let sensor_id = row.sensor_id;
            last_sensor_id = Some(sensor_id);
            let uuid = row.uuid;
            let name = row.name;
            let sensor_type_str = row.r#type;

            let sensor_type = SensorType::from_str(&sensor_type_str).map_err(|e| {
                StorageError::invalid_data_format(
                    &format!("Failed to parse sensor type '{}': {}", sensor_type_str, e),
                    Some(uuid),
                    Some(&name),
                )
            })?;

            // Create unit from JOIN result if unit name exists (not empty string)
            let unit = if !row.unit_name.is_empty() {
                Some(Unit {
                    name: row.unit_name,
                    description: if row.unit_description.is_empty() {
                        None
                    } else {
                        Some(row.unit_description)
                    },
                })
            } else {
                None
            };

            // Query labels for this sensor
            let labels_query =
                "SELECT name, COALESCE(description, '') FROM labels WHERE sensor_id = ?";
            let mut labels_cursor = self
                .client
                .query(labels_query)
                .bind(sensor_id)
                .fetch::<(String, String)>()
                .map_err(|e| map_clickhouse_error(e, Some(uuid), Some(&name)))?;

            let mut labels = Vec::new();
            while let Some((label_name, label_description)) = labels_cursor.next().await? {
                labels.push((label_name, label_description));
            }

            let sensor = Sensor {
                uuid,
                name,
                sensor_type,
                unit,
                labels: SensAppLabels::from(labels),
            };

            sensors.push(sensor);
        }

        Ok(crate::storage::ListSeriesResult {
            series: sensors,
            bookmark: if has_more {
                last_sensor_id.map(|sensor_id| sensor_id.to_string())
            } else {
                None
            },
        })
    }

    async fn list_metrics(&self) -> Result<Vec<crate::datamodel::Metric>> {
        let query = r#"
            SELECT
                name,
                type,
                count(*) AS sensor_count
            FROM sensors
            GROUP BY name, type
            ORDER BY name ASC
        "#;

        let mut cursor = self
            .client
            .query(query)
            .fetch::<(String, String, u64)>()
            .map_err(|e| map_clickhouse_error(e, None, None))?;

        let mut metrics = Vec::new();

        while let Some((name, type_str, sensor_count)) = cursor.next().await? {
            let sensor_type = SensorType::from_str(&type_str).map_err(|e| {
                StorageError::invalid_data_format(
                    &format!("Failed to parse sensor type '{}': {}", type_str, e),
                    None,
                    Some(&name),
                )
            })?;

            let metric = Metric::new(
                name,
                sensor_type,
                None, // unit - we'll need to query this separately if needed
                sensor_count as i64,
                vec![], // label_keys - empty for now
            );

            metrics.push(metric);
        }

        Ok(metrics)
    }

    async fn query_sensor_data(
        &self,
        sensor_uuid: &str,
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
        limit: Option<usize>,
    ) -> Result<Option<crate::datamodel::SensorData>> {
        let uuid = Uuid::from_str(sensor_uuid).map_err(|e| {
            StorageError::invalid_data_format(
                &format!("Invalid UUID '{}': {}", sensor_uuid, e),
                None,
                None,
            )
        })?;

        let sensor_id = uuid_to_sensor_id(&uuid);

        // First, get sensor metadata
        let sensor_query = r#"
            SELECT
                s.uuid,
                s.name,
                s.type,
                u.name AS unit_name,
                u.description AS unit_description
            FROM sensors s
            LEFT JOIN units u ON s.unit = u.id
            WHERE s.sensor_id = ?
            LIMIT 1
        "#;

        #[derive(clickhouse::Row, serde::Deserialize)]
        struct SensorMetadataRow {
            #[serde(with = "clickhouse::serde::uuid")]
            uuid: Uuid,
            name: String,
            r#type: String,
            unit_name: String, // Changed from Option<String> to String
            unit_description: Option<String>,
        }

        let mut sensor_cursor = self
            .client
            .query(sensor_query)
            .bind(sensor_id)
            .fetch::<SensorMetadataRow>()
            .map_err(|e| map_clickhouse_error(e, Some(uuid), None))?;

        let row = if let Some(row) = sensor_cursor.next().await? {
            row
        } else {
            return Ok(None);
        };

        let uuid = row.uuid;
        let name = row.name;
        let sensor_type_str = row.r#type;

        let sensor_type = SensorType::from_str(&sensor_type_str).map_err(|e| {
            StorageError::invalid_data_format(
                &format!("Failed to parse sensor type '{}': {}", sensor_type_str, e),
                Some(uuid),
                Some(&name),
            )
        })?;

        // Create unit from query results
        let unit = if !row.unit_name.is_empty() {
            Some(Unit {
                name: row.unit_name,
                description: row.unit_description,
            })
        } else {
            None
        };

        // Get labels
        let labels_query = "SELECT name, COALESCE(description, '') FROM labels WHERE sensor_id = ?";
        let mut labels_cursor = self
            .client
            .query(labels_query)
            .bind(sensor_id)
            .fetch::<(String, String)>()
            .map_err(|e| map_clickhouse_error(e, Some(uuid), Some(&name)))?;

        let mut labels = Vec::new();
        while let Some((label_name, label_description)) = labels_cursor.next().await? {
            labels.push((label_name, label_description));
        }

        // Query samples based on sensor type and time range
        let samples = self
            .query_samples_by_type(
                sensor_id,
                &sensor_type,
                start_time,
                end_time,
                limit.unwrap_or(DEFAULT_QUERY_LIMIT),
            )
            .await?;

        let sensor_data = SensorData {
            sensor: Sensor {
                uuid,
                name,
                sensor_type,
                unit,
                labels: SensAppLabels::from(labels),
            },
            samples,
        };

        Ok(Some(sensor_data))
    }

    async fn query_sensor_data_advanced(
        &self,
        sensor_uuid: &str,
        options: &SensorDataQueryOptions,
    ) -> Result<Option<SensorData>> {
        options.validate()?;

        if let (Some(step_ms), Some(aggregation)) = (options.step_ms, options.aggregation) {
            if let Some((sensor_id, mut sensor)) = self.get_sensor_metadata(sensor_uuid).await? {
                let start_time_us = options.start_time.as_ref().map(datetime_to_micros);
                let end_time_us = options.end_time.as_ref().map(datetime_to_micros);
                let step_us = step_ms * 1000;
                let origin_us = start_time_us.unwrap_or(0);

                let samples = match sensor.sensor_type {
                    SensorType::Integer => {
                        self.query_integer_samples_aggregated(
                            sensor_id,
                            start_time_us,
                            end_time_us,
                            step_us,
                            origin_us,
                            aggregation,
                            options.limit,
                        )
                        .await?
                    }
                    SensorType::Numeric => {
                        self.query_numeric_samples_aggregated(
                            sensor_id,
                            start_time_us,
                            end_time_us,
                            step_us,
                            origin_us,
                            aggregation,
                            options.limit,
                        )
                        .await?
                    }
                    SensorType::Float => {
                        self.query_float_samples_aggregated(
                            sensor_id,
                            start_time_us,
                            end_time_us,
                            step_us,
                            origin_us,
                            aggregation,
                            options.limit,
                        )
                        .await?
                    }
                    _ => {
                        let raw = self
                            .query_sensor_data(
                                sensor_uuid,
                                options.start_time,
                                options.end_time,
                                options.limit,
                            )
                            .await?;
                        return raw
                            .map(|sensor_data| {
                                crate::storage::common::apply_query_options(sensor_data, options)
                            })
                            .transpose();
                    }
                };

                sensor.sensor_type = match &samples {
                    TypedSamples::Integer(_) => SensorType::Integer,
                    TypedSamples::Numeric(_) => SensorType::Numeric,
                    TypedSamples::Float(_) => SensorType::Float,
                    _ => sensor.sensor_type,
                };
                if aggregation.output_is_count() {
                    sensor.unit = None;
                }

                let post_query_options = SensorDataQueryOptions {
                    start_time: options.start_time,
                    end_time: options.end_time,
                    limit: options.limit,
                    step_ms: None,
                    aggregation: None,
                    simplify: options.simplify,
                };

                return crate::storage::common::apply_query_options(
                    SensorData::new(sensor, samples),
                    &post_query_options,
                )
                .map(Some);
            }

            return Ok(None);
        }

        let raw = self
            .query_sensor_data(
                sensor_uuid,
                options.start_time,
                options.end_time,
                options.limit,
            )
            .await?;

        raw.map(|sensor_data| crate::storage::common::apply_query_options(sensor_data, options))
            .transpose()
    }

    async fn query_sensor_data_latest(
        &self,
        sensor_uuid: &str,
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
    ) -> Result<Option<SensorData>> {
        let Some((sensor_id, sensor)) = self.get_sensor_metadata(sensor_uuid).await? else {
            return Ok(None);
        };

        let start_time_us = start_time.as_ref().map(datetime_to_micros);
        let end_time_us = end_time.as_ref().map(datetime_to_micros);
        let table_name = Self::sensor_table_name(sensor.sensor_type);

        let Some(latest_timestamp_us) = self
            .query_latest_timestamp_us(table_name, sensor_id, start_time_us, end_time_us)
            .await?
        else {
            return Ok(None);
        };

        let samples = self
            .query_samples_by_type(
                sensor_id,
                &sensor.sensor_type,
                Some(micros_to_datetime(latest_timestamp_us)),
                Some(micros_to_datetime(latest_timestamp_us)),
                1,
            )
            .await?;

        Ok(Some(SensorData::new(sensor, samples)))
    }

    async fn query_sensor_data_availability(
        &self,
        sensor_uuid: &str,
        start_time: SensAppDateTime,
        end_time: SensAppDateTime,
        step_ms: Option<i64>,
    ) -> Result<Option<SensorAvailabilitySummary>> {
        let Some((sensor_id, sensor)) = self.get_sensor_metadata(sensor_uuid).await? else {
            return Ok(None);
        };

        let summary = self
            .query_availability_summary_native(
                Self::sensor_table_name(sensor.sensor_type),
                sensor_id,
                sensor,
                datetime_to_micros(&start_time),
                datetime_to_micros(&end_time),
                step_ms,
            )
            .await?;

        Ok(Some(summary))
    }

    async fn query_sensors_by_labels(
        &self,
        matchers: &[super::LabelMatcher],
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
        limit: Option<usize>,
        numeric_only: bool,
    ) -> Result<Vec<SensorData>> {
        if matchers.is_empty() {
            return Ok(Vec::new());
        }

        let (name_matchers, label_matchers): (Vec<_>, Vec<_>) = matchers
            .iter()
            .partition(|matcher| matcher.is_name_matcher());

        let sensors = self
            .find_sensors_by_matchers(&name_matchers, &label_matchers, numeric_only)
            .await?;

        if sensors.is_empty() {
            return Ok(Vec::new());
        }

        let limit = limit.unwrap_or(DEFAULT_QUERY_LIMIT);
        let mut results = Vec::with_capacity(sensors.len());

        for (sensor_id, sensor) in sensors {
            let samples = self
                .query_samples_by_type(sensor_id, &sensor.sensor_type, start_time, end_time, limit)
                .await?;

            results.push(SensorData::new(sensor, samples));
        }

        Ok(results)
    }

    /// Health check for ClickHouse storage
    /// Executes a simple SELECT 1 query to verify database connectivity
    async fn health_check(&self) -> Result<()> {
        self.client
            .query("SELECT 1")
            .execute()
            .await
            .context("ClickHouse health check failed")?;
        Ok(())
    }

    #[cfg(any(test, feature = "test-utils"))]
    async fn cleanup_test_data(&self) -> Result<()> {
        clickhouse_utilities::test_utils::cleanup_test_data(&self.client).await
    }
}

impl ClickHouseStorage {
    async fn query_integer_samples_aggregated(
        &self,
        sensor_id: u64,
        start_time_us: Option<i64>,
        end_time_us: Option<i64>,
        step_us: i64,
        origin_us: i64,
        aggregation: Aggregation,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        let limit = limit.unwrap_or(DEFAULT_QUERY_LIMIT);
        let bucket_expr = format!(
            "{} + intDiv(timestamp_us - {}, {}) * {}",
            origin_us, origin_us, step_us, step_us
        );
        let where_clause = clickhouse_time_where(start_time_us, end_time_us);

        match aggregation {
            Aggregation::Avg => {
                #[derive(clickhouse::Row, serde::Deserialize)]
                struct Row {
                    timestamp_us: i64,
                    value: f64,
                }

                let query = format!(
                    "SELECT {bucket_expr} AS timestamp_us, avg(value) AS value FROM integer_values WHERE sensor_id = ?{where_clause} GROUP BY timestamp_us ORDER BY timestamp_us ASC LIMIT {limit}"
                );
                let mut cursor = self.client.query(&query).bind(sensor_id);
                if let Some(start_time_us) = start_time_us {
                    cursor = cursor.bind(start_time_us);
                }
                if let Some(end_time_us) = end_time_us {
                    cursor = cursor.bind(end_time_us);
                }
                let mut rows_cursor = cursor
                    .fetch::<Row>()
                    .map_err(|e| map_clickhouse_error(e, None, None))?;
                let mut samples = smallvec::smallvec![];
                while let Some(row) = rows_cursor.next().await? {
                    samples.push(Sample { datetime: micros_to_datetime(row.timestamp_us), value: row.value });
                }
                Ok(TypedSamples::Float(samples))
            }
            Aggregation::Count => {
                #[derive(clickhouse::Row, serde::Deserialize)]
                struct Row {
                    timestamp_us: i64,
                    value: i64,
                }

                let query = format!(
                    "SELECT {bucket_expr} AS timestamp_us, toInt64(count()) AS value FROM integer_values WHERE sensor_id = ?{where_clause} GROUP BY timestamp_us ORDER BY timestamp_us ASC LIMIT {limit}"
                );
                let mut cursor = self.client.query(&query).bind(sensor_id);
                if let Some(start_time_us) = start_time_us {
                    cursor = cursor.bind(start_time_us);
                }
                if let Some(end_time_us) = end_time_us {
                    cursor = cursor.bind(end_time_us);
                }
                let mut rows_cursor = cursor
                    .fetch::<Row>()
                    .map_err(|e| map_clickhouse_error(e, None, None))?;
                let mut samples = smallvec::smallvec![];
                while let Some(row) = rows_cursor.next().await? {
                    samples.push(Sample { datetime: micros_to_datetime(row.timestamp_us), value: row.value });
                }
                Ok(TypedSamples::Integer(samples))
            }
            _ => {
                #[derive(clickhouse::Row, serde::Deserialize)]
                struct Row {
                    timestamp_us: i64,
                    value: i64,
                }

                let expression = clickhouse_integer_expression(aggregation);
                let query = format!(
                    "SELECT {bucket_expr} AS timestamp_us, {expression} AS value FROM integer_values WHERE sensor_id = ?{where_clause} GROUP BY timestamp_us ORDER BY timestamp_us ASC LIMIT {limit}"
                );
                let mut cursor = self.client.query(&query).bind(sensor_id);
                if let Some(start_time_us) = start_time_us {
                    cursor = cursor.bind(start_time_us);
                }
                if let Some(end_time_us) = end_time_us {
                    cursor = cursor.bind(end_time_us);
                }
                let mut rows_cursor = cursor
                    .fetch::<Row>()
                    .map_err(|e| map_clickhouse_error(e, None, None))?;
                let mut samples = smallvec::smallvec![];
                while let Some(row) = rows_cursor.next().await? {
                    samples.push(Sample { datetime: micros_to_datetime(row.timestamp_us), value: row.value });
                }
                Ok(TypedSamples::Integer(samples))
            }
        }
    }

    async fn query_float_samples_aggregated(
        &self,
        sensor_id: u64,
        start_time_us: Option<i64>,
        end_time_us: Option<i64>,
        step_us: i64,
        origin_us: i64,
        aggregation: Aggregation,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        let limit = limit.unwrap_or(DEFAULT_QUERY_LIMIT);
        let bucket_expr = format!(
            "{} + intDiv(timestamp_us - {}, {}) * {}",
            origin_us, origin_us, step_us, step_us
        );
        let where_clause = clickhouse_time_where(start_time_us, end_time_us);

        match aggregation {
            Aggregation::Count => {
                #[derive(clickhouse::Row, serde::Deserialize)]
                struct Row {
                    timestamp_us: i64,
                    value: i64,
                }

                let query = format!(
                    "SELECT {bucket_expr} AS timestamp_us, toInt64(count()) AS value FROM float_values WHERE sensor_id = ?{where_clause} GROUP BY timestamp_us ORDER BY timestamp_us ASC LIMIT {limit}"
                );
                let mut cursor = self.client.query(&query).bind(sensor_id);
                if let Some(start_time_us) = start_time_us {
                    cursor = cursor.bind(start_time_us);
                }
                if let Some(end_time_us) = end_time_us {
                    cursor = cursor.bind(end_time_us);
                }
                let mut rows_cursor = cursor
                    .fetch::<Row>()
                    .map_err(|e| map_clickhouse_error(e, None, None))?;
                let mut samples = smallvec::smallvec![];
                while let Some(row) = rows_cursor.next().await? {
                    samples.push(Sample { datetime: micros_to_datetime(row.timestamp_us), value: row.value });
                }
                Ok(TypedSamples::Integer(samples))
            }
            _ => {
                #[derive(clickhouse::Row, serde::Deserialize)]
                struct Row {
                    timestamp_us: i64,
                    value: f64,
                }

                let expression = clickhouse_float_expression(aggregation);
                let query = format!(
                    "SELECT {bucket_expr} AS timestamp_us, {expression} AS value FROM float_values WHERE sensor_id = ?{where_clause} GROUP BY timestamp_us ORDER BY timestamp_us ASC LIMIT {limit}"
                );
                let mut cursor = self.client.query(&query).bind(sensor_id);
                if let Some(start_time_us) = start_time_us {
                    cursor = cursor.bind(start_time_us);
                }
                if let Some(end_time_us) = end_time_us {
                    cursor = cursor.bind(end_time_us);
                }
                let mut rows_cursor = cursor
                    .fetch::<Row>()
                    .map_err(|e| map_clickhouse_error(e, None, None))?;
                let mut samples = smallvec::smallvec![];
                while let Some(row) = rows_cursor.next().await? {
                    samples.push(Sample { datetime: micros_to_datetime(row.timestamp_us), value: row.value });
                }
                Ok(TypedSamples::Float(samples))
            }
        }
    }

    async fn query_numeric_samples_aggregated(
        &self,
        sensor_id: u64,
        start_time_us: Option<i64>,
        end_time_us: Option<i64>,
        step_us: i64,
        origin_us: i64,
        aggregation: Aggregation,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        let limit = limit.unwrap_or(DEFAULT_QUERY_LIMIT);
        let bucket_expr = format!(
            "{} + intDiv(timestamp_us - {}, {}) * {}",
            origin_us, origin_us, step_us, step_us
        );
        let where_clause = clickhouse_time_where(start_time_us, end_time_us);

        match aggregation {
            Aggregation::Count => {
                #[derive(clickhouse::Row, serde::Deserialize)]
                struct Row {
                    timestamp_us: i64,
                    value: i64,
                }

                let query = format!(
                    "SELECT {bucket_expr} AS timestamp_us, toInt64(count()) AS value FROM numeric_values WHERE sensor_id = ?{where_clause} GROUP BY timestamp_us ORDER BY timestamp_us ASC LIMIT {limit}"
                );
                let mut cursor = self.client.query(&query).bind(sensor_id);
                if let Some(start_time_us) = start_time_us {
                    cursor = cursor.bind(start_time_us);
                }
                if let Some(end_time_us) = end_time_us {
                    cursor = cursor.bind(end_time_us);
                }
                let mut rows_cursor = cursor
                    .fetch::<Row>()
                    .map_err(|e| map_clickhouse_error(e, None, None))?;
                let mut samples = smallvec::smallvec![];
                while let Some(row) = rows_cursor.next().await? {
                    samples.push(Sample { datetime: micros_to_datetime(row.timestamp_us), value: row.value });
                }
                Ok(TypedSamples::Integer(samples))
            }
            _ => {
                #[derive(clickhouse::Row, serde::Deserialize)]
                struct Row {
                    timestamp_us: i64,
                    value: i128,
                }

                let expression = clickhouse_numeric_expression(aggregation);
                let query = format!(
                    "SELECT {bucket_expr} AS timestamp_us, {expression} AS value FROM numeric_values WHERE sensor_id = ?{where_clause} GROUP BY timestamp_us ORDER BY timestamp_us ASC LIMIT {limit}"
                );
                let mut cursor = self.client.query(&query).bind(sensor_id);
                if let Some(start_time_us) = start_time_us {
                    cursor = cursor.bind(start_time_us);
                }
                if let Some(end_time_us) = end_time_us {
                    cursor = cursor.bind(end_time_us);
                }
                let mut rows_cursor = cursor
                    .fetch::<Row>()
                    .map_err(|e| map_clickhouse_error(e, None, None))?;
                let mut samples = smallvec::smallvec![];
                while let Some(row) = rows_cursor.next().await? {
                    samples.push(Sample { datetime: micros_to_datetime(row.timestamp_us), value: decimal_from_clickhouse_raw(row.value) });
                }
                Ok(TypedSamples::Numeric(samples))
            }
        }
    }

    /// Query samples by type with time range filtering
    async fn query_samples_by_type(
        &self,
        sensor_id: u64,
        sensor_type: &SensorType,
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
        limit: usize,
    ) -> Result<TypedSamples> {
        // Initialize typed_samples based on sensor_type
        let mut typed_samples = match sensor_type {
            SensorType::Integer => TypedSamples::Integer(smallvec::smallvec![]),
            SensorType::Numeric => TypedSamples::Numeric(smallvec::smallvec![]),
            SensorType::Float => TypedSamples::Float(smallvec::smallvec![]),
            SensorType::String => TypedSamples::String(smallvec::smallvec![]),
            SensorType::Boolean => TypedSamples::Boolean(smallvec::smallvec![]),
            SensorType::Location => TypedSamples::Location(smallvec::smallvec![]),
            SensorType::Json => TypedSamples::Json(smallvec::smallvec![]),
            SensorType::Blob => TypedSamples::Blob(smallvec::smallvec![]),
        };

        // Build time range conditions
        let mut time_conditions = Vec::new();
        let mut bind_values = Vec::new();

        bind_values.push(sensor_id.to_string());

        if let Some(start) = start_time {
            time_conditions.push("timestamp_us >= ?");
            bind_values.push(datetime_to_micros(&start).to_string());
        }

        if let Some(end) = end_time {
            time_conditions.push("timestamp_us <= ?");
            bind_values.push(datetime_to_micros(&end).to_string());
        }

        let time_where = if time_conditions.is_empty() {
            String::new()
        } else {
            format!(" AND {}", time_conditions.join(" AND "))
        };

        match sensor_type {
            SensorType::Integer => {
                let query = format!(
                    "SELECT timestamp_us, value FROM integer_values WHERE sensor_id = ?{} ORDER BY timestamp_us ASC LIMIT {}",
                    time_where, limit
                );

                let mut cursor = self.client.query(&query).bind(sensor_id);

                for bind_value in bind_values.into_iter().skip(1) {
                    cursor = cursor.bind(bind_value.parse::<i64>()?);
                }

                let mut result_cursor = cursor
                    .fetch::<(i64, i64)>()
                    .map_err(|e| map_clickhouse_error(e, None, None))?;

                if let TypedSamples::Integer(ref mut samples) = typed_samples {
                    while let Some((timestamp_us, value)) = result_cursor.next().await? {
                        samples.push(Sample {
                            datetime: micros_to_datetime(timestamp_us),
                            value,
                        });
                    }
                }
            }
            SensorType::Numeric => {
                let query = format!(
                    "SELECT timestamp_us, value FROM numeric_values WHERE sensor_id = ?{} ORDER BY timestamp_us ASC LIMIT {}",
                    time_where, limit
                );

                let mut cursor = self.client.query(&query).bind(sensor_id);

                for bind_value in bind_values.into_iter().skip(1) {
                    cursor = cursor.bind(bind_value.parse::<i64>()?);
                }

                let mut result_cursor = cursor
                    .fetch::<(i64, i128)>()
                    .map_err(|e| map_clickhouse_error(e, None, None))?;

                if let TypedSamples::Numeric(ref mut samples) = typed_samples {
                    while let Some((timestamp_us, value_raw)) = result_cursor.next().await? {
                        samples.push(Sample {
                            datetime: micros_to_datetime(timestamp_us),
                            value: decimal_from_clickhouse_raw(value_raw),
                        });
                    }
                }
            }
            SensorType::Float => {
                let query = format!(
                    "SELECT timestamp_us, value FROM float_values WHERE sensor_id = ?{} ORDER BY timestamp_us ASC LIMIT {}",
                    time_where, limit
                );

                let mut cursor = self.client.query(&query).bind(sensor_id);

                for bind_value in bind_values.into_iter().skip(1) {
                    cursor = cursor.bind(bind_value.parse::<i64>()?);
                }

                let mut result_cursor = cursor
                    .fetch::<(i64, f64)>()
                    .map_err(|e| map_clickhouse_error(e, None, None))?;

                if let TypedSamples::Float(ref mut samples) = typed_samples {
                    while let Some((timestamp_us, value)) = result_cursor.next().await? {
                        samples.push(Sample {
                            datetime: micros_to_datetime(timestamp_us),
                            value,
                        });
                    }
                }
            }
            SensorType::String => {
                let query = format!(
                    "SELECT timestamp_us, value FROM string_values WHERE sensor_id = ?{} ORDER BY timestamp_us ASC LIMIT {}",
                    time_where, limit
                );

                let mut cursor = self.client.query(&query).bind(sensor_id);

                for bind_value in bind_values.into_iter().skip(1) {
                    cursor = cursor.bind(bind_value.parse::<i64>()?);
                }

                let mut result_cursor = cursor
                    .fetch::<(i64, String)>()
                    .map_err(|e| map_clickhouse_error(e, None, None))?;

                if let TypedSamples::String(ref mut samples) = typed_samples {
                    while let Some((timestamp_us, value)) = result_cursor.next().await? {
                        samples.push(Sample {
                            datetime: micros_to_datetime(timestamp_us),
                            value,
                        });
                    }
                }
            }
            SensorType::Boolean => {
                let query = format!(
                    "SELECT timestamp_us, value FROM boolean_values WHERE sensor_id = ?{} ORDER BY timestamp_us ASC LIMIT {}",
                    time_where, limit
                );

                let mut cursor = self.client.query(&query).bind(sensor_id);

                for bind_value in bind_values.into_iter().skip(1) {
                    cursor = cursor.bind(bind_value.parse::<i64>()?);
                }

                let mut result_cursor = cursor
                    .fetch::<(i64, bool)>()
                    .map_err(|e| map_clickhouse_error(e, None, None))?;

                if let TypedSamples::Boolean(ref mut samples) = typed_samples {
                    while let Some((timestamp_us, value)) = result_cursor.next().await? {
                        samples.push(Sample {
                            datetime: micros_to_datetime(timestamp_us),
                            value,
                        });
                    }
                }
            }
            SensorType::Location => {
                let query = format!(
                    "SELECT timestamp_us, latitude, longitude FROM location_values WHERE sensor_id = ?{} ORDER BY timestamp_us ASC LIMIT {}",
                    time_where, limit
                );

                let mut cursor = self.client.query(&query).bind(sensor_id);

                for bind_value in bind_values.into_iter().skip(1) {
                    cursor = cursor.bind(bind_value.parse::<i64>()?);
                }

                let mut result_cursor = cursor
                    .fetch::<(i64, f64, f64)>()
                    .map_err(|e| map_clickhouse_error(e, None, None))?;

                if let TypedSamples::Location(ref mut samples) = typed_samples {
                    while let Some((timestamp_us, latitude, longitude)) =
                        result_cursor.next().await?
                    {
                        samples.push(Sample {
                            datetime: micros_to_datetime(timestamp_us),
                            value: Point::new(longitude, latitude),
                        });
                    }
                }
            }
            SensorType::Json => {
                let query = format!(
                    "SELECT timestamp_us, value FROM json_values WHERE sensor_id = ?{} ORDER BY timestamp_us ASC LIMIT {}",
                    time_where, limit
                );

                let mut cursor = self.client.query(&query).bind(sensor_id);

                for bind_value in bind_values.into_iter().skip(1) {
                    cursor = cursor.bind(bind_value.parse::<i64>()?);
                }

                let mut result_cursor = cursor
                    .fetch::<(i64, String)>()
                    .map_err(|e| map_clickhouse_error(e, None, None))?;

                if let TypedSamples::Json(ref mut samples) = typed_samples {
                    while let Some((timestamp_us, value_str)) = result_cursor.next().await? {
                        let value: JsonValue = serde_json::from_str(&value_str).map_err(|e| {
                            StorageError::invalid_data_format(
                                &format!("Failed to parse JSON value: {}", e),
                                None,
                                None,
                            )
                        })?;

                        samples.push(Sample {
                            datetime: micros_to_datetime(timestamp_us),
                            value,
                        });
                    }
                }
            }
            SensorType::Blob => {
                let query = format!(
                    "SELECT timestamp_us, value FROM blob_values WHERE sensor_id = ?{} ORDER BY timestamp_us ASC LIMIT {}",
                    time_where, limit
                );

                let mut cursor = self.client.query(&query).bind(sensor_id);

                for bind_value in bind_values.into_iter().skip(1) {
                    cursor = cursor.bind(bind_value.parse::<i64>()?);
                }

                let mut result_cursor = cursor
                    .fetch::<(i64, String)>()
                    .map_err(|e| map_clickhouse_error(e, None, None))?;

                if let TypedSamples::Blob(ref mut samples) = typed_samples {
                    while let Some((timestamp_us, value_str)) = result_cursor.next().await? {
                        let value = base64::prelude::BASE64_STANDARD
                            .decode(&value_str)
                            .map_err(|e| {
                                StorageError::invalid_data_format(
                                    &format!("Failed to decode base64 blob: {}", e),
                                    None,
                                    None,
                                )
                            })?;

                        samples.push(Sample {
                            datetime: micros_to_datetime(timestamp_us),
                            value,
                        });
                    }
                }
            }
        }

        Ok(typed_samples)
    }
}

fn clickhouse_time_where(start_time_us: Option<i64>, end_time_us: Option<i64>) -> String {
    let mut conditions = String::new();
    if start_time_us.is_some() {
        conditions.push_str(" AND timestamp_us >= ?");
    }
    if end_time_us.is_some() {
        conditions.push_str(" AND timestamp_us <= ?");
    }
    conditions
}

fn clickhouse_integer_expression(aggregation: Aggregation) -> &'static str {
    match aggregation {
        Aggregation::Min => "min(value)",
        Aggregation::Max => "max(value)",
        Aggregation::Sum => "sum(value)",
        Aggregation::First => "argMin(value, timestamp_us)",
        Aggregation::Last => "argMax(value, timestamp_us)",
        Aggregation::Avg | Aggregation::Count => unreachable!("handled separately"),
    }
}

fn clickhouse_float_expression(aggregation: Aggregation) -> &'static str {
    match aggregation {
        Aggregation::Avg => "avg(value)",
        Aggregation::Min => "min(value)",
        Aggregation::Max => "max(value)",
        Aggregation::Sum => "sum(value)",
        Aggregation::First => "argMin(value, timestamp_us)",
        Aggregation::Last => "argMax(value, timestamp_us)",
        Aggregation::Count => unreachable!("handled separately"),
    }
}

fn clickhouse_numeric_expression(aggregation: Aggregation) -> &'static str {
    match aggregation {
        Aggregation::Avg => "toDecimal128(avg(value), 8)",
        Aggregation::Min => "min(value)",
        Aggregation::Max => "max(value)",
        Aggregation::Sum => "sum(value)",
        Aggregation::First => "argMin(value, timestamp_us)",
        Aggregation::Last => "argMax(value, timestamp_us)",
        Aggregation::Count => unreachable!("handled separately"),
    }
}
