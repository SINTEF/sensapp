use super::{DEFAULT_QUERY_LIMIT, StorageError, StorageInstance, common::sync_with_timeout};
use crate::config;
use crate::datamodel::sensapp_vec::SensAppLabels;
use crate::datamodel::{
    Metric, Sample, SensAppDateTime, Sensor, SensorData, SensorType, TypedSamples, batch::Batch,
    unit::Unit,
};
use anyhow::{Context, Result};
use async_broadcast::Sender;
use async_trait::async_trait;
use base64::prelude::*;
use clickhouse::Client;
use geo::Point;
use serde_json::Value as JsonValue;
use std::{str::FromStr, sync::Arc};
use uuid::Uuid;

pub mod clickhouse_publishers;
pub mod clickhouse_utilities;

use clickhouse_publishers::ClickHousePublisher;
use clickhouse_utilities::{
    datetime_to_micros, map_clickhouse_error, micros_to_datetime, uuid_to_sensor_id,
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
}

#[async_trait]
impl StorageInstance for ClickHouseStorage {
    async fn create_or_migrate(&self) -> Result<()> {
        self.run_migrations()
            .await
            .context("Failed to run ClickHouse migrations")?;
        Ok(())
    }

    async fn publish(&self, batch: Arc<Batch>, sync_sender: Sender<()>) -> Result<()> {
        let publisher = ClickHousePublisher::new(&self.client);

        for single_sensor_batch in batch.sensors.as_ref() {
            publisher
                .publish_single_sensor_batch(single_sensor_batch)
                .await?;
        }

        self.sync(sync_sender).await?;
        Ok(())
    }

    async fn sync(&self, sync_sender: Sender<()>) -> Result<()> {
        // ClickHouse doesn't need explicit sync like some other databases
        // Just send the sync signal
        let config = config::get().context("Failed to get configuration")?;
        sync_with_timeout(&sync_sender, config.storage_sync_timeout_seconds).await
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
    ) -> Result<Vec<crate::datamodel::Sensor>> {
        let (query, use_filter) = match metric_filter {
            Some(_) => (
                r#"
                    SELECT s.sensor_id, s.uuid, s.name, s.type,
                           COALESCE(u.name, '') as unit_name,
                           COALESCE(u.description, '') as unit_description
                    FROM sensors s
                    LEFT JOIN units u ON s.unit = u.id
                    WHERE s.name = ? ORDER BY s.uuid ASC
                "#,
                true,
            ),
            None => (
                r#"
                    SELECT s.sensor_id, s.uuid, s.name, s.type,
                           COALESCE(u.name, '') as unit_name,
                           COALESCE(u.description, '') as unit_description
                    FROM sensors s
                    LEFT JOIN units u ON s.unit = u.id
                    ORDER BY s.uuid ASC
                "#,
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

        let mut cursor = if use_filter {
            self.client
                .query(query)
                .bind(metric_filter.unwrap())
                .fetch::<SensorRow>()
                .map_err(|e| map_clickhouse_error(e, None, None))?
        } else {
            self.client
                .query(query)
                .fetch::<SensorRow>()
                .map_err(|e| map_clickhouse_error(e, None, None))?
        };

        // Process sensors directly from cursor
        let mut sensors = Vec::new();

        while let Some(row) = cursor.next().await? {
            let sensor_id = row.sensor_id;
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

        Ok(sensors)
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

    #[cfg(any(test, feature = "test-utils"))]
    async fn cleanup_test_data(&self) -> Result<()> {
        clickhouse_utilities::test_utils::cleanup_test_data(&self.client).await
    }
}

impl ClickHouseStorage {
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
                    .fetch::<(i64, String)>()
                    .map_err(|e| map_clickhouse_error(e, None, None))?;

                if let TypedSamples::Numeric(ref mut samples) = typed_samples {
                    while let Some((timestamp_us, value_str)) = result_cursor.next().await? {
                        let value = rust_decimal::Decimal::from_str(&value_str).map_err(|e| {
                            StorageError::invalid_data_format(
                                &format!("Failed to parse decimal value: {}", e),
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
