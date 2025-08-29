use super::{DEFAULT_QUERY_LIMIT, StorageError, StorageInstance, common::datetime_to_micros};
use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
use crate::datamodel::{
    Metric, Sample, SensAppDateTime, Sensor, SensorData, SensorType, TypedSamples, batch::Batch,
};
use crate::datamodel::{sensapp_vec::SensAppLabels, unit::Unit};
use anyhow::{Context, Result};
use async_trait::async_trait;
use geo::Point;
use serde_json::Value as JsonValue;
use smallvec::smallvec;
use sqlx::{PgPool, postgres::PgConnectOptions};
use std::{str::FromStr, sync::Arc};
use tracing::debug;
use uuid::Uuid;

pub mod postgresql_publishers;
pub mod postgresql_utilities;
pub mod prometheus_matcher;

use postgresql_publishers::*;
use postgresql_utilities::get_sensor_id_or_create_sensor;

#[derive(Debug)]
pub struct PostgresStorage {
    pool: PgPool,
}

impl PostgresStorage {
    pub async fn connect(connection_string: &str) -> Result<Self> {
        let connect_options = PgConnectOptions::from_str(connection_string)
            .context("Failed to create postgres connection options")?;

        let pool = PgPool::connect_with(connect_options)
            .await
            .context("Failed to create postgres pool")?;

        Ok(Self { pool })
    }
}

#[async_trait]
impl StorageInstance for PostgresStorage {
    async fn create_or_migrate(&self) -> Result<()> {
        sqlx::migrate!("src/storage/postgresql/migrations")
            .run(&self.pool)
            .await
            .context("Failed to migrate database")?;

        Ok(())
    }
    async fn publish(&self, batch: Arc<Batch>) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        for single_sensor_batch in batch.sensors.as_ref() {
            println!(
                "debug: publishing batch for sensor {:?}",
                single_sensor_batch.sensor
            );
            self.publish_single_sensor_batch(&mut transaction, single_sensor_batch)
                .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn vacuum(&self) -> Result<()> {
        sqlx::query("VACUUM")
            .execute(&self.pool)
            .await
            .context("Failed to vacuum database")?;

        postgresql_utilities::clear_caches().await;

        Ok(())
    }

    async fn list_series(
        &self,
        metric_filter: Option<&str>,
    ) -> Result<Vec<crate::datamodel::Sensor>> {
        #[derive(sqlx::FromRow)]
        struct SensorRow {
            sensor_id: Option<i64>,
            uuid: Option<Uuid>,
            name: Option<String>,
            r#type: Option<String>,
            unit_name: Option<String>,
            unit_description: Option<String>,
        }

        // Query sensors with their metadata using the catalog view, optionally filtered by metric name
        let sensor_rows: Vec<SensorRow> = sqlx::query_as(
            r#"
            SELECT sensor_id, uuid, name, type, unit_name, unit_description
            FROM sensor_catalog_view
            WHERE ($1::TEXT IS NULL OR name = $1)
            ORDER BY uuid ASC
            "#,
        )
        .bind(metric_filter)
        .fetch_all(&self.pool)
        .await?;

        let mut sensors = Vec::new();

        for sensor_row in sensor_rows {
            // Parse sensor metadata with improved error handling
            let sensor_uuid = sensor_row
                .uuid
                .ok_or_else(|| {
                    StorageError::missing_field("UUID", None, sensor_row.name.as_deref())
                })
                .map_err(anyhow::Error::from)?;

            let sensor_name = sensor_row
                .name
                .ok_or_else(|| StorageError::missing_field("name", Some(sensor_uuid), None))
                .map_err(anyhow::Error::from)?;

            let sensor_type_str = sensor_row
                .r#type
                .ok_or_else(|| {
                    StorageError::missing_field("type", Some(sensor_uuid), Some(&sensor_name))
                })
                .map_err(anyhow::Error::from)?;

            let sensor_type = SensorType::from_str(&sensor_type_str).map_err(|e| {
                anyhow::Error::from(StorageError::invalid_data_format(
                    &format!("Failed to parse sensor type '{}': {}", sensor_type_str, e),
                    Some(sensor_uuid),
                    Some(&sensor_name),
                ))
            })?;

            let unit = match (sensor_row.unit_name, sensor_row.unit_description) {
                (Some(name), description) => Some(Unit::new(name, description)),
                _ => None,
            };

            // Query labels for this sensor with proper error context
            let sensor_id = sensor_row.sensor_id.ok_or_else(|| {
                anyhow::Error::from(StorageError::missing_field(
                    "sensor_id",
                    Some(sensor_uuid),
                    Some(&sensor_name),
                ))
            })?;

            #[derive(sqlx::FromRow)]
            struct LabelRow {
                label_name: String,
                label_value: String,
            }

            let labels_rows: Vec<LabelRow> = sqlx::query_as(
                r#"
                SELECT lnd.name as label_name, ldd.description as label_value
                FROM labels l
                JOIN labels_name_dictionary lnd ON l.name = lnd.id
                JOIN labels_description_dictionary ldd ON l.description = ldd.id
                WHERE l.sensor_id = $1
                "#,
            )
            .bind(sensor_id)
            .fetch_all(&self.pool)
            .await
            .with_context(|| {
                format!(
                    "Failed to query labels for sensor UUID={} name='{}'",
                    sensor_uuid, sensor_name
                )
            })?;

            let mut labels: SensAppLabels = smallvec![];
            for label_row in labels_rows {
                labels.push((label_row.label_name, label_row.label_value));
            }

            let sensor = Sensor::new(sensor_uuid, sensor_name, sensor_type, unit, Some(labels));

            sensors.push(sensor);
        }

        Ok(sensors)
    }

    async fn list_metrics(&self) -> Result<Vec<crate::datamodel::Metric>> {
        #[derive(sqlx::FromRow)]
        struct MetricsRow {
            metric_name: Option<String>,
            r#type: Option<String>,
            unit_name: Option<String>,
            unit_description: Option<String>,
            series_count: Option<i64>,
            label_keys: Option<Vec<String>>,
        }

        // Query metrics summary using the view
        let metrics_rows: Vec<MetricsRow> = sqlx::query_as(
            r#"
            SELECT metric_name, type, unit_name, unit_description, series_count, label_keys
            FROM metrics_summary
            ORDER BY metric_name ASC
            "#,
        )
        .fetch_all(&self.pool)
        .await?;

        let mut metrics = Vec::new();

        for metrics_row in metrics_rows {
            let metric_name = metrics_row.metric_name.ok_or_else(|| {
                anyhow::Error::from(StorageError::missing_field("metric_name", None, None))
            })?;

            let sensor_type_str = metrics_row.r#type.ok_or_else(|| {
                anyhow::Error::from(StorageError::missing_field(
                    "type",
                    None,
                    Some(&metric_name),
                ))
            })?;

            let sensor_type = SensorType::from_str(&sensor_type_str).map_err(|e| {
                anyhow::Error::from(StorageError::invalid_data_format(
                    &format!("Failed to parse sensor type '{}': {}", sensor_type_str, e),
                    None,
                    Some(&metric_name),
                ))
            })?;

            let unit = match (metrics_row.unit_name, metrics_row.unit_description) {
                (Some(name), description) => Some(Unit::new(name, description)),
                _ => None,
            };

            let series_count = metrics_row.series_count.ok_or_else(|| {
                anyhow::Error::from(StorageError::missing_field(
                    "series_count",
                    None,
                    Some(&metric_name),
                ))
            })?;

            // Handle optional label_keys array
            let label_keys = metrics_row.label_keys.unwrap_or_default();

            let metric = Metric::new(metric_name, sensor_type, unit, series_count, label_keys);

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
        // Parse UUID
        let parsed_uuid = Uuid::from_str(sensor_uuid).context("Failed to parse sensor UUID")?;

        #[derive(sqlx::FromRow)]
        struct SensorMetadataRow {
            sensor_id: Option<i64>,
            uuid: Option<Uuid>,
            name: Option<String>,
            r#type: Option<String>,
            unit_name: Option<String>,
            unit_description: Option<String>,
        }

        // Query sensor metadata by UUID using the catalog view
        let sensor_row: Option<SensorMetadataRow> = sqlx::query_as(
            r#"
            SELECT sensor_id, uuid, name, type, unit_name, unit_description
            FROM sensor_catalog_view
            WHERE uuid = $1
            "#,
        )
        .bind(parsed_uuid)
        .fetch_optional(&self.pool)
        .await?;

        let sensor_row = match sensor_row {
            Some(row) => row,
            None => {
                return Err(StorageError::SensorNotFound {
                    sensor_uuid: parsed_uuid,
                }
                .into());
            }
        };

        // Parse sensor metadata with improved error handling
        let sensor_uuid = sensor_row.uuid.ok_or_else(|| {
            anyhow::Error::from(StorageError::missing_field(
                "UUID",
                None,
                sensor_row.name.as_deref(),
            ))
        })?;

        let sensor_name = sensor_row.name.ok_or_else(|| {
            anyhow::Error::from(StorageError::missing_field("name", Some(sensor_uuid), None))
        })?;

        let sensor_type_str = sensor_row.r#type.ok_or_else(|| {
            anyhow::Error::from(StorageError::missing_field(
                "type",
                Some(sensor_uuid),
                Some(&sensor_name),
            ))
        })?;

        let sensor_type = SensorType::from_str(&sensor_type_str).map_err(|e| {
            anyhow::Error::from(StorageError::invalid_data_format(
                &format!("Failed to parse sensor type '{}': {}", sensor_type_str, e),
                Some(sensor_uuid),
                Some(&sensor_name),
            ))
        })?;

        let unit = match (sensor_row.unit_name, sensor_row.unit_description) {
            (Some(name), description) => Some(Unit::new(name, description)),
            _ => None,
        };

        // Query labels for this sensor with proper context
        let sensor_id = sensor_row.sensor_id.ok_or_else(|| {
            anyhow::Error::from(StorageError::missing_field(
                "sensor_id",
                Some(sensor_uuid),
                Some(&sensor_name),
            ))
        })?;
        #[derive(sqlx::FromRow)]
        struct LabelRow {
            label_name: String,
            label_value: String,
        }

        let labels_rows: Vec<LabelRow> = sqlx::query_as(
            r#"
            SELECT lnd.name as label_name, ldd.description as label_value
            FROM labels l
            JOIN labels_name_dictionary lnd ON l.name = lnd.id
            JOIN labels_description_dictionary ldd ON l.description = ldd.id
            WHERE l.sensor_id = $1
            "#,
        )
        .bind(sensor_id)
        .fetch_all(&self.pool)
        .await?;

        let mut labels: SensAppLabels = smallvec![];
        for label_row in labels_rows {
            labels.push((label_row.label_name, label_row.label_value));
        }

        let sensor = Sensor::new(sensor_uuid, sensor_name, sensor_type, unit, Some(labels));

        // Convert SensAppDateTime to microseconds for database queries using common utility
        let start_time_us = start_time.as_ref().map(datetime_to_micros);
        let end_time_us = end_time.as_ref().map(datetime_to_micros);

        // Query samples based on sensor type
        let samples = match sensor.sensor_type {
            SensorType::Integer => {
                self.query_integer_samples(sensor_id, start_time_us, end_time_us, limit)
                    .await?
            }
            SensorType::Numeric => {
                self.query_numeric_samples(sensor_id, start_time_us, end_time_us, limit)
                    .await?
            }
            SensorType::Float => {
                self.query_float_samples(sensor_id, start_time_us, end_time_us, limit)
                    .await?
            }
            SensorType::String => {
                self.query_string_samples(sensor_id, start_time_us, end_time_us, limit)
                    .await?
            }
            SensorType::Boolean => {
                self.query_boolean_samples(sensor_id, start_time_us, end_time_us, limit)
                    .await?
            }
            SensorType::Location => {
                self.query_location_samples(sensor_id, start_time_us, end_time_us, limit)
                    .await?
            }
            SensorType::Json => {
                self.query_json_samples(sensor_id, start_time_us, end_time_us, limit)
                    .await?
            }
            SensorType::Blob => {
                self.query_blob_samples(sensor_id, start_time_us, end_time_us, limit)
                    .await?
            }
        };

        Ok(Some(SensorData::new(sensor, samples)))
    }

    /// Clean up all test data from the database
    /// This removes all sensor data but keeps the schema intact
    /// Uses DELETE statements in dependency order to avoid foreign key conflicts
    #[cfg(feature = "test-utils")]
    async fn cleanup_test_data(&self) -> Result<()> {
        println!("Cleaning up test data from Postgres database...");
        // Use a transaction to ensure atomicity
        let mut tx = self.pool.begin().await?;

        for query in &[
            // Step 1: Delete all value tables (they reference sensors but nothing references them)
            "DELETE FROM blob_values",
            "DELETE FROM json_values",
            "DELETE FROM location_values",
            "DELETE FROM boolean_values",
            "DELETE FROM string_values",
            "DELETE FROM float_values",
            "DELETE FROM numeric_values",
            "DELETE FROM integer_values",
            // Step 2: Delete labels (references sensors and dictionaries)
            "DELETE FROM labels",
            // Step 3: Delete sensors (references units, but we'll preserve units for tests)
            "DELETE FROM sensors",
            // Step 4: Delete dictionaries that reference values/labels
            "DELETE FROM strings_values_dictionary",
            "DELETE FROM labels_name_dictionary",
            "DELETE FROM labels_description_dictionary",
            // Step 5: Reset sequences for clean test data
            "ALTER SEQUENCE sensors_sensor_id_seq RESTART WITH 1",
            "ALTER SEQUENCE strings_values_dictionary_id_seq RESTART WITH 1",
            "ALTER SEQUENCE labels_name_dictionary_id_seq RESTART WITH 1",
            "ALTER SEQUENCE labels_description_dictionary_id_seq RESTART WITH 1",
        ] {
            sqlx::query(query).execute(&mut *tx).await?;
        }

        tx.commit()
            .await
            .context("Failed to commit test data cleanup transaction")?;

        // Clear all PostgreSQL-related caches to ensure test isolation
        println!("Clearing cached data...");
        postgresql_utilities::clear_caches().await;

        Ok(())
    }

    /// Query time series data matching Prometheus label matchers within a time range.
    /// Returns (sensor_metadata, time_series_values) for each matching sensor.
    async fn query_prometheus_time_series(
        &self,
        matchers: &[crate::parsing::prometheus::remote_read_models::LabelMatcher],
        start_time_ms: i64,
        end_time_ms: i64,
    ) -> Result<Vec<(Sensor, Vec<Sample<f64>>)>> {
        use prometheus_matcher::PrometheusMatcher;

        // Convert milliseconds to microseconds for our internal storage
        let start_time_us = Some(start_time_ms * 1000);
        let end_time_us = Some(end_time_ms * 1000);

        // Find all sensors matching the label matchers
        let matcher = PrometheusMatcher::new(self.pool.clone());
        let sensor_ids = matcher
            .find_matching_sensors(matchers)
            .await
            .context("Failed to find matching sensors")?;

        debug!(
            "Found {} matching sensors for Prometheus query",
            sensor_ids.len()
        );

        let mut results = Vec::new();

        for sensor_id in sensor_ids {
            // Get sensor metadata and data by sensor_id directly
            if let Some(sensor_data) = self
                .query_sensor_data_by_id(
                    sensor_id,
                    start_time_us,
                    end_time_us,
                    None, // No limit for Prometheus queries
                )
                .await?
            {
                // Extract float samples (Prometheus only uses floats)
                match sensor_data.samples {
                    TypedSamples::Float(samples) => {
                        let float_samples: Vec<Sample<f64>> = samples
                            .into_iter()
                            .map(|s| Sample {
                                datetime: s.datetime,
                                value: s.value,
                            })
                            .collect();
                        results.push((sensor_data.sensor, float_samples));
                    }
                    TypedSamples::Integer(samples) => {
                        // Convert integers to floats for Prometheus compatibility
                        let float_samples: Vec<Sample<f64>> = samples
                            .into_iter()
                            .map(|s| Sample {
                                datetime: s.datetime,
                                value: s.value as f64,
                            })
                            .collect();
                        results.push((sensor_data.sensor, float_samples));
                    }
                    _ => {
                        // Skip non-numeric samples for Prometheus queries
                        debug!(
                            "Skipping non-numeric sensor {} for Prometheus query",
                            sensor_data.sensor.uuid
                        );
                    }
                }
            }
        }

        debug!(
            "Returning {} time series for Prometheus query",
            results.len()
        );
        Ok(results)
    }
}

impl PostgresStorage {
    async fn publish_single_sensor_batch(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        single_sensor_batch: &crate::datamodel::batch::SingleSensorBatch,
    ) -> Result<()> {
        let sensor_id =
            get_sensor_id_or_create_sensor(transaction, &single_sensor_batch.sensor).await?;
        println!("debug : got sensor_id {}", sensor_id);

        let samples_guard = single_sensor_batch.samples.read().await;
        match &*samples_guard {
            TypedSamples::Integer(values) => {
                publish_integer_values(transaction, sensor_id, values).await?;
            }
            TypedSamples::Numeric(values) => {
                publish_numeric_values(transaction, sensor_id, values).await?;
            }
            TypedSamples::Float(values) => {
                publish_float_values(transaction, sensor_id, values).await?;
            }
            TypedSamples::String(values) => {
                publish_string_values(transaction, sensor_id, values).await?;
            }
            TypedSamples::Boolean(values) => {
                publish_boolean_values(transaction, sensor_id, values).await?;
            }
            TypedSamples::Location(values) => {
                publish_location_values(transaction, sensor_id, values).await?;
            }
            TypedSamples::Blob(values) => {
                publish_blob_values(transaction, sensor_id, values).await?;
            }
            TypedSamples::Json(values) => {
                publish_json_values(transaction, sensor_id, values).await?;
            }
        }

        Ok(())
    }

    async fn query_integer_samples(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        #[derive(sqlx::FromRow)]
        struct IntegerValueRow {
            timestamp_us: i64,
            value: i64,
        }

        let rows: Vec<IntegerValueRow> = sqlx::query_as(
            r#"
            SELECT timestamp_us, value FROM integer_values
            WHERE sensor_id = $1
            AND ($2::BIGINT IS NULL OR timestamp_us >= $2)
            AND ($3::BIGINT IS NULL OR timestamp_us <= $3)
            ORDER BY timestamp_us ASC
            LIMIT $4
            "#,
        )
        .bind(sensor_id)
        .bind(start_time)
        .bind(end_time)
        .bind(limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us);
            let value = row.value;
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::Integer(samples))
    }

    async fn query_numeric_samples(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        #[derive(sqlx::FromRow)]
        struct NumericValueRow {
            timestamp_us: i64,
            value: rust_decimal::Decimal,
        }

        let rows: Vec<NumericValueRow> = sqlx::query_as(
            r#"
            SELECT timestamp_us, value FROM numeric_values
            WHERE sensor_id = $1
            AND ($2::BIGINT IS NULL OR timestamp_us >= $2)
            AND ($3::BIGINT IS NULL OR timestamp_us <= $3)
            ORDER BY timestamp_us ASC
            LIMIT $4
            "#,
        )
        .bind(sensor_id)
        .bind(start_time)
        .bind(end_time)
        .bind(limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us);
            let value = row.value;
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::Numeric(samples))
    }

    async fn query_float_samples(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        #[derive(sqlx::FromRow)]
        struct FloatValueRow {
            timestamp_us: i64,
            value: f64,
        }

        let rows: Vec<FloatValueRow> = sqlx::query_as(
            r#"
            SELECT timestamp_us, value FROM float_values
            WHERE sensor_id = $1
            AND ($2::BIGINT IS NULL OR timestamp_us >= $2)
            AND ($3::BIGINT IS NULL OR timestamp_us <= $3)
            ORDER BY timestamp_us ASC
            LIMIT $4
            "#,
        )
        .bind(sensor_id)
        .bind(start_time)
        .bind(end_time)
        .bind(limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us);
            let value = row.value;
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::Float(samples))
    }

    async fn query_string_samples(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        #[derive(sqlx::FromRow)]
        struct StringValueRow {
            timestamp_us: i64,
            string_value: String,
        }

        let rows: Vec<StringValueRow> = sqlx::query_as(
            r#"
            SELECT sv.timestamp_us, svd.value as string_value
            FROM string_values sv
            JOIN strings_values_dictionary svd ON sv.value = svd.id
            WHERE sv.sensor_id = $1
            AND ($2::BIGINT IS NULL OR sv.timestamp_us >= $2)
            AND ($3::BIGINT IS NULL OR sv.timestamp_us <= $3)
            ORDER BY sv.timestamp_us ASC
            LIMIT $4
            "#,
        )
        .bind(sensor_id)
        .bind(start_time)
        .bind(end_time)
        .bind(limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us);
            let value = row.string_value;
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::String(samples))
    }

    async fn query_boolean_samples(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        #[derive(sqlx::FromRow)]
        struct BooleanValueRow {
            timestamp_us: i64,
            value: bool,
        }

        let rows: Vec<BooleanValueRow> = sqlx::query_as(
            r#"
            SELECT timestamp_us, value FROM boolean_values
            WHERE sensor_id = $1
            AND ($2::BIGINT IS NULL OR timestamp_us >= $2)
            AND ($3::BIGINT IS NULL OR timestamp_us <= $3)
            ORDER BY timestamp_us ASC
            LIMIT $4
            "#,
        )
        .bind(sensor_id)
        .bind(start_time)
        .bind(end_time)
        .bind(limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us);
            let value = row.value;
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::Boolean(samples))
    }

    async fn query_location_samples(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        #[derive(sqlx::FromRow)]
        struct LocationValueRow {
            timestamp_us: i64,
            latitude: f64,
            longitude: f64,
        }

        let rows: Vec<LocationValueRow> = sqlx::query_as(
            r#"
            SELECT timestamp_us, latitude, longitude FROM location_values
            WHERE sensor_id = $1
            AND ($2::BIGINT IS NULL OR timestamp_us >= $2)
            AND ($3::BIGINT IS NULL OR timestamp_us <= $3)
            ORDER BY timestamp_us ASC
            LIMIT $4
            "#,
        )
        .bind(sensor_id)
        .bind(start_time)
        .bind(end_time)
        .bind(limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us);
            let value = Point::new(row.longitude, row.latitude);
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::Location(samples))
    }

    async fn query_json_samples(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        #[derive(sqlx::FromRow)]
        struct JsonValueRow {
            timestamp_us: i64,
            value: JsonValue,
        }

        let rows: Vec<JsonValueRow> = sqlx::query_as(
            r#"
            SELECT timestamp_us, value FROM json_values
            WHERE sensor_id = $1
            AND ($2::BIGINT IS NULL OR timestamp_us >= $2)
            AND ($3::BIGINT IS NULL OR timestamp_us <= $3)
            ORDER BY timestamp_us ASC
            LIMIT $4
            "#,
        )
        .bind(sensor_id)
        .bind(start_time)
        .bind(end_time)
        .bind(limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us);
            let value: JsonValue = row.value;
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::Json(samples))
    }

    async fn query_blob_samples(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        #[derive(sqlx::FromRow)]
        struct BlobValueRow {
            timestamp_us: i64,
            value: Vec<u8>,
        }

        let rows: Vec<BlobValueRow> = sqlx::query_as(
            r#"
            SELECT timestamp_us, value FROM blob_values
            WHERE sensor_id = $1
            AND ($2::BIGINT IS NULL OR timestamp_us >= $2)
            AND ($3::BIGINT IS NULL OR timestamp_us <= $3)
            ORDER BY timestamp_us ASC
            LIMIT $4
            "#,
        )
        .bind(sensor_id)
        .bind(start_time)
        .bind(end_time)
        .bind(limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us);
            let value = row.value;
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::Blob(samples))
    }

    /// Internal method to query sensor data by sensor_id (instead of UUID string)
    async fn query_sensor_data_by_id(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<Option<SensorData>> {
        // Get sensor metadata
        let sensor = self.get_sensor_by_id(sensor_id).await?;

        // Query samples based on sensor type (Prometheus only uses floats)
        let samples = self
            .query_float_samples(sensor_id, start_time, end_time, limit)
            .await?;

        Ok(Some(SensorData::new(sensor, samples)))
    }

    /// Get sensor metadata by sensor_id
    async fn get_sensor_by_id(&self, sensor_id: i64) -> Result<Sensor> {
        #[derive(sqlx::FromRow)]
        struct SensorRow {
            uuid: Uuid,
            name: String,
            r#type: String,
            unit_name: Option<String>,
            unit_description: Option<String>,
        }

        let sensor_row: SensorRow = sqlx::query_as(
            r#"
            SELECT s.uuid, s.name, s.type, u.name as unit_name, u.description as unit_description
            FROM sensors s
            LEFT JOIN units u ON s.unit = u.id
            WHERE s.sensor_id = $1
            "#,
        )
        .bind(sensor_id)
        .fetch_one(&self.pool)
        .await
        .context("Failed to fetch sensor by ID")?;

        let sensor_type = SensorType::from_str(&sensor_row.r#type)
            .map_err(|e| anyhow::anyhow!("Invalid sensor type in database: {}", e))?;

        let unit = match (sensor_row.unit_name, sensor_row.unit_description) {
            (Some(name), description) => Some(Unit::new(name, description)),
            _ => None,
        };

        // Query labels for this sensor
        let labels = self.get_sensor_labels(sensor_id).await?;

        Ok(Sensor {
            uuid: sensor_row.uuid,
            name: sensor_row.name,
            sensor_type,
            unit,
            labels: labels.unwrap_or_default(),
        })
    }

    /// Get labels for a sensor by sensor_id
    async fn get_sensor_labels(&self, sensor_id: i64) -> Result<Option<SensAppLabels>> {
        let labels_rows: Vec<(String, String)> = sqlx::query_as(
            r#"
            SELECT lnd.name as label_name, ldd.description as label_value
            FROM labels l
            JOIN labels_name_dictionary lnd ON l.name = lnd.id
            LEFT JOIN labels_description_dictionary ldd ON l.description = ldd.id
            WHERE l.sensor_id = $1
            "#,
        )
        .bind(sensor_id)
        .fetch_all(&self.pool)
        .await
        .context("Failed to fetch sensor labels")?;

        if labels_rows.is_empty() {
            Ok(None)
        } else {
            let mut labels = SensAppLabels::with_capacity(labels_rows.len());
            for (name, value) in labels_rows {
                labels.push((name, value));
            }
            Ok(Some(labels))
        }
    }
}

// Unit tests are covered by the integration tests in tests/crud_dcat_api.rs
// which test the full end-to-end functionality including the HTTP endpoints
