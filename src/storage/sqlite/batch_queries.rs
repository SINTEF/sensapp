//! Batch sample query methods for SQLite storage.
//!
//! This module contains optimized batch query methods that fetch samples
//! for multiple sensors. Unlike PostgreSQL, SQLite doesn't support LATERAL
//! joins or array parameters, so we use simpler query patterns with IN clauses.
//!
//! Used primarily by `query_sensors_by_labels` for efficient multi-sensor queries.

use super::SqliteStorage;
use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
use crate::datamodel::{Sample, SensAppDateTime, Sensor, SensorType, TypedSamples};
use crate::storage::DEFAULT_QUERY_LIMIT;
use anyhow::{Context, Result};
use geo::Point;
use serde_json::Value as JsonValue;
use smallvec::smallvec;
use std::collections::HashMap;

impl SqliteStorage {
    /// Batch query samples for multiple sensors, grouped by sensor type.
    ///
    /// This fetches samples for all provided sensors in optimized batch queries,
    /// one query per sensor type. Due to SQLite limitations, we use IN clauses
    /// with dynamic parameter binding.
    pub(super) async fn batch_query_samples(
        &self,
        sensors: &[(i64, Sensor)],
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<HashMap<i64, TypedSamples>> {
        let mut results: HashMap<i64, TypedSamples> = HashMap::new();
        let limit_val = limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64;

        // Group sensors by type
        let mut integer_sensors: Vec<i64> = Vec::new();
        let mut numeric_sensors: Vec<i64> = Vec::new();
        let mut float_sensors: Vec<i64> = Vec::new();
        let mut string_sensors: Vec<i64> = Vec::new();
        let mut boolean_sensors: Vec<i64> = Vec::new();
        let mut location_sensors: Vec<i64> = Vec::new();
        let mut json_sensors: Vec<i64> = Vec::new();
        let mut blob_sensors: Vec<i64> = Vec::new();

        for (sensor_id, sensor) in sensors {
            match sensor.sensor_type {
                SensorType::Integer => integer_sensors.push(*sensor_id),
                SensorType::Numeric => numeric_sensors.push(*sensor_id),
                SensorType::Float => float_sensors.push(*sensor_id),
                SensorType::String => string_sensors.push(*sensor_id),
                SensorType::Boolean => boolean_sensors.push(*sensor_id),
                SensorType::Location => location_sensors.push(*sensor_id),
                SensorType::Json => json_sensors.push(*sensor_id),
                SensorType::Blob => blob_sensors.push(*sensor_id),
            }
        }

        // Query each sensor type (sequentially for SQLite to avoid connection pool exhaustion)
        if !integer_sensors.is_empty() {
            let type_results = self
                .batch_query_integer_samples(&integer_sensors, start_time, end_time, limit_val)
                .await?;
            results.extend(type_results);
        }

        if !numeric_sensors.is_empty() {
            let type_results = self
                .batch_query_numeric_samples(&numeric_sensors, start_time, end_time, limit_val)
                .await?;
            results.extend(type_results);
        }

        if !float_sensors.is_empty() {
            let type_results = self
                .batch_query_float_samples(&float_sensors, start_time, end_time, limit_val)
                .await?;
            results.extend(type_results);
        }

        if !string_sensors.is_empty() {
            let type_results = self
                .batch_query_string_samples(&string_sensors, start_time, end_time, limit_val)
                .await?;
            results.extend(type_results);
        }

        if !boolean_sensors.is_empty() {
            let type_results = self
                .batch_query_boolean_samples(&boolean_sensors, start_time, end_time, limit_val)
                .await?;
            results.extend(type_results);
        }

        if !location_sensors.is_empty() {
            let type_results = self
                .batch_query_location_samples(&location_sensors, start_time, end_time, limit_val)
                .await?;
            results.extend(type_results);
        }

        if !json_sensors.is_empty() {
            let type_results = self
                .batch_query_json_samples(&json_sensors, start_time, end_time, limit_val)
                .await?;
            results.extend(type_results);
        }

        if !blob_sensors.is_empty() {
            let type_results = self
                .batch_query_blob_samples(&blob_sensors, start_time, end_time, limit_val)
                .await?;
            results.extend(type_results);
        }

        Ok(results)
    }

    /// Build IN clause placeholders for dynamic binding
    fn build_in_placeholders(count: usize, start_idx: usize) -> String {
        (start_idx..start_idx + count)
            .map(|i| format!("?{}", i))
            .collect::<Vec<_>>()
            .join(", ")
    }

    async fn batch_query_integer_samples(
        &self,
        sensor_ids: &[i64],
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: i64,
    ) -> Result<HashMap<i64, TypedSamples>> {
        #[derive(sqlx::FromRow)]
        struct Row {
            sensor_id: i64,
            timestamp_us: i64,
            value: i64,
        }

        let mut results: HashMap<i64, TypedSamples> = HashMap::new();

        // Initialize empty results for all sensors
        for sensor_id in sensor_ids {
            results.insert(*sensor_id, TypedSamples::Integer(smallvec![]));
        }

        // Build query with dynamic IN clause
        // We need to use a subquery with row_number to get per-sensor limits
        // SQLite approach: query all matching and filter/limit in Rust
        let placeholders = Self::build_in_placeholders(sensor_ids.len(), 1);
        let sql = format!(
            r#"
            SELECT sensor_id, timestamp_us, value
            FROM integer_values
            WHERE sensor_id IN ({})
            AND (?{} IS NULL OR timestamp_us >= ?{})
            AND (?{} IS NULL OR timestamp_us <= ?{})
            ORDER BY sensor_id, timestamp_us ASC
            "#,
            placeholders,
            sensor_ids.len() + 1,
            sensor_ids.len() + 2,
            sensor_ids.len() + 3,
            sensor_ids.len() + 4
        );

        let mut query = sqlx::query_as::<_, Row>(&sql);
        for sensor_id in sensor_ids {
            query = query.bind(sensor_id);
        }
        query = query
            .bind(start_time)
            .bind(start_time)
            .bind(end_time)
            .bind(end_time);

        let rows = query.fetch_all(&self.pool).await?;

        // Group results and apply per-sensor limit
        let mut counts: HashMap<i64, usize> = HashMap::new();
        for row in rows {
            let count = counts.entry(row.sensor_id).or_insert(0);
            if *count >= limit as usize {
                continue;
            }
            *count += 1;

            if let Some(TypedSamples::Integer(samples)) = results.get_mut(&row.sensor_id) {
                samples.push(Sample {
                    datetime: SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us),
                    value: row.value,
                });
            }
        }

        Ok(results)
    }

    async fn batch_query_numeric_samples(
        &self,
        sensor_ids: &[i64],
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: i64,
    ) -> Result<HashMap<i64, TypedSamples>> {
        #[derive(sqlx::FromRow)]
        struct Row {
            sensor_id: i64,
            timestamp_us: i64,
            value: String,
        }

        let mut results: HashMap<i64, TypedSamples> = HashMap::new();

        for sensor_id in sensor_ids {
            results.insert(*sensor_id, TypedSamples::Numeric(smallvec![]));
        }

        let placeholders = Self::build_in_placeholders(sensor_ids.len(), 1);
        let sql = format!(
            r#"
            SELECT sensor_id, timestamp_us, value
            FROM numeric_values
            WHERE sensor_id IN ({})
            AND (?{} IS NULL OR timestamp_us >= ?{})
            AND (?{} IS NULL OR timestamp_us <= ?{})
            ORDER BY sensor_id, timestamp_us ASC
            "#,
            placeholders,
            sensor_ids.len() + 1,
            sensor_ids.len() + 2,
            sensor_ids.len() + 3,
            sensor_ids.len() + 4
        );

        let mut query = sqlx::query_as::<_, Row>(&sql);
        for sensor_id in sensor_ids {
            query = query.bind(sensor_id);
        }
        query = query
            .bind(start_time)
            .bind(start_time)
            .bind(end_time)
            .bind(end_time);

        let rows = query.fetch_all(&self.pool).await?;

        let mut counts: HashMap<i64, usize> = HashMap::new();
        for row in rows {
            let count = counts.entry(row.sensor_id).or_insert(0);
            if *count >= limit as usize {
                continue;
            }
            *count += 1;

            if let Some(TypedSamples::Numeric(samples)) = results.get_mut(&row.sensor_id) {
                let value = rust_decimal::Decimal::from_str_exact(&row.value)
                    .context("Failed to parse decimal value")?;
                samples.push(Sample {
                    datetime: SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us),
                    value,
                });
            }
        }

        Ok(results)
    }

    async fn batch_query_float_samples(
        &self,
        sensor_ids: &[i64],
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: i64,
    ) -> Result<HashMap<i64, TypedSamples>> {
        #[derive(sqlx::FromRow)]
        struct Row {
            sensor_id: i64,
            timestamp_us: i64,
            value: f64,
        }

        let mut results: HashMap<i64, TypedSamples> = HashMap::new();

        for sensor_id in sensor_ids {
            results.insert(*sensor_id, TypedSamples::Float(smallvec![]));
        }

        let placeholders = Self::build_in_placeholders(sensor_ids.len(), 1);
        let sql = format!(
            r#"
            SELECT sensor_id, timestamp_us, value
            FROM float_values
            WHERE sensor_id IN ({})
            AND (?{} IS NULL OR timestamp_us >= ?{})
            AND (?{} IS NULL OR timestamp_us <= ?{})
            ORDER BY sensor_id, timestamp_us ASC
            "#,
            placeholders,
            sensor_ids.len() + 1,
            sensor_ids.len() + 2,
            sensor_ids.len() + 3,
            sensor_ids.len() + 4
        );

        let mut query = sqlx::query_as::<_, Row>(&sql);
        for sensor_id in sensor_ids {
            query = query.bind(sensor_id);
        }
        query = query
            .bind(start_time)
            .bind(start_time)
            .bind(end_time)
            .bind(end_time);

        let rows = query.fetch_all(&self.pool).await?;

        let mut counts: HashMap<i64, usize> = HashMap::new();
        for row in rows {
            let count = counts.entry(row.sensor_id).or_insert(0);
            if *count >= limit as usize {
                continue;
            }
            *count += 1;

            if let Some(TypedSamples::Float(samples)) = results.get_mut(&row.sensor_id) {
                samples.push(Sample {
                    datetime: SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us),
                    value: row.value,
                });
            }
        }

        Ok(results)
    }

    async fn batch_query_string_samples(
        &self,
        sensor_ids: &[i64],
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: i64,
    ) -> Result<HashMap<i64, TypedSamples>> {
        #[derive(sqlx::FromRow)]
        struct Row {
            sensor_id: i64,
            timestamp_us: i64,
            string_value: String,
        }

        let mut results: HashMap<i64, TypedSamples> = HashMap::new();

        for sensor_id in sensor_ids {
            results.insert(*sensor_id, TypedSamples::String(smallvec![]));
        }

        let placeholders = Self::build_in_placeholders(sensor_ids.len(), 1);
        let sql = format!(
            r#"
            SELECT sv.sensor_id, sv.timestamp_us, svd.value as string_value
            FROM string_values sv
            JOIN strings_values_dictionary svd ON sv.value = svd.id
            WHERE sv.sensor_id IN ({})
            AND (?{} IS NULL OR sv.timestamp_us >= ?{})
            AND (?{} IS NULL OR sv.timestamp_us <= ?{})
            ORDER BY sv.sensor_id, sv.timestamp_us ASC
            "#,
            placeholders,
            sensor_ids.len() + 1,
            sensor_ids.len() + 2,
            sensor_ids.len() + 3,
            sensor_ids.len() + 4
        );

        let mut query = sqlx::query_as::<_, Row>(&sql);
        for sensor_id in sensor_ids {
            query = query.bind(sensor_id);
        }
        query = query
            .bind(start_time)
            .bind(start_time)
            .bind(end_time)
            .bind(end_time);

        let rows = query.fetch_all(&self.pool).await?;

        let mut counts: HashMap<i64, usize> = HashMap::new();
        for row in rows {
            let count = counts.entry(row.sensor_id).or_insert(0);
            if *count >= limit as usize {
                continue;
            }
            *count += 1;

            if let Some(TypedSamples::String(samples)) = results.get_mut(&row.sensor_id) {
                samples.push(Sample {
                    datetime: SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us),
                    value: row.string_value,
                });
            }
        }

        Ok(results)
    }

    async fn batch_query_boolean_samples(
        &self,
        sensor_ids: &[i64],
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: i64,
    ) -> Result<HashMap<i64, TypedSamples>> {
        #[derive(sqlx::FromRow)]
        struct Row {
            sensor_id: i64,
            timestamp_us: i64,
            value: i64, // SQLite stores booleans as integers
        }

        let mut results: HashMap<i64, TypedSamples> = HashMap::new();

        for sensor_id in sensor_ids {
            results.insert(*sensor_id, TypedSamples::Boolean(smallvec![]));
        }

        let placeholders = Self::build_in_placeholders(sensor_ids.len(), 1);
        let sql = format!(
            r#"
            SELECT sensor_id, timestamp_us, value
            FROM boolean_values
            WHERE sensor_id IN ({})
            AND (?{} IS NULL OR timestamp_us >= ?{})
            AND (?{} IS NULL OR timestamp_us <= ?{})
            ORDER BY sensor_id, timestamp_us ASC
            "#,
            placeholders,
            sensor_ids.len() + 1,
            sensor_ids.len() + 2,
            sensor_ids.len() + 3,
            sensor_ids.len() + 4
        );

        let mut query = sqlx::query_as::<_, Row>(&sql);
        for sensor_id in sensor_ids {
            query = query.bind(sensor_id);
        }
        query = query
            .bind(start_time)
            .bind(start_time)
            .bind(end_time)
            .bind(end_time);

        let rows = query.fetch_all(&self.pool).await?;

        let mut counts: HashMap<i64, usize> = HashMap::new();
        for row in rows {
            let count = counts.entry(row.sensor_id).or_insert(0);
            if *count >= limit as usize {
                continue;
            }
            *count += 1;

            if let Some(TypedSamples::Boolean(samples)) = results.get_mut(&row.sensor_id) {
                samples.push(Sample {
                    datetime: SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us),
                    value: row.value != 0,
                });
            }
        }

        Ok(results)
    }

    async fn batch_query_location_samples(
        &self,
        sensor_ids: &[i64],
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: i64,
    ) -> Result<HashMap<i64, TypedSamples>> {
        #[derive(sqlx::FromRow)]
        struct Row {
            sensor_id: i64,
            timestamp_us: i64,
            latitude: f64,
            longitude: f64,
        }

        let mut results: HashMap<i64, TypedSamples> = HashMap::new();

        for sensor_id in sensor_ids {
            results.insert(*sensor_id, TypedSamples::Location(smallvec![]));
        }

        let placeholders = Self::build_in_placeholders(sensor_ids.len(), 1);
        let sql = format!(
            r#"
            SELECT sensor_id, timestamp_us, latitude, longitude
            FROM location_values
            WHERE sensor_id IN ({})
            AND (?{} IS NULL OR timestamp_us >= ?{})
            AND (?{} IS NULL OR timestamp_us <= ?{})
            ORDER BY sensor_id, timestamp_us ASC
            "#,
            placeholders,
            sensor_ids.len() + 1,
            sensor_ids.len() + 2,
            sensor_ids.len() + 3,
            sensor_ids.len() + 4
        );

        let mut query = sqlx::query_as::<_, Row>(&sql);
        for sensor_id in sensor_ids {
            query = query.bind(sensor_id);
        }
        query = query
            .bind(start_time)
            .bind(start_time)
            .bind(end_time)
            .bind(end_time);

        let rows = query.fetch_all(&self.pool).await?;

        let mut counts: HashMap<i64, usize> = HashMap::new();
        for row in rows {
            let count = counts.entry(row.sensor_id).or_insert(0);
            if *count >= limit as usize {
                continue;
            }
            *count += 1;

            if let Some(TypedSamples::Location(samples)) = results.get_mut(&row.sensor_id) {
                samples.push(Sample {
                    datetime: SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us),
                    value: Point::new(row.longitude, row.latitude),
                });
            }
        }

        Ok(results)
    }

    async fn batch_query_json_samples(
        &self,
        sensor_ids: &[i64],
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: i64,
    ) -> Result<HashMap<i64, TypedSamples>> {
        #[derive(sqlx::FromRow)]
        struct Row {
            sensor_id: i64,
            timestamp_us: i64,
            value: Vec<u8>,
        }

        let mut results: HashMap<i64, TypedSamples> = HashMap::new();

        for sensor_id in sensor_ids {
            results.insert(*sensor_id, TypedSamples::Json(smallvec![]));
        }

        let placeholders = Self::build_in_placeholders(sensor_ids.len(), 1);
        let sql = format!(
            r#"
            SELECT sensor_id, timestamp_us, value
            FROM json_values
            WHERE sensor_id IN ({})
            AND (?{} IS NULL OR timestamp_us >= ?{})
            AND (?{} IS NULL OR timestamp_us <= ?{})
            ORDER BY sensor_id, timestamp_us ASC
            "#,
            placeholders,
            sensor_ids.len() + 1,
            sensor_ids.len() + 2,
            sensor_ids.len() + 3,
            sensor_ids.len() + 4
        );

        let mut query = sqlx::query_as::<_, Row>(&sql);
        for sensor_id in sensor_ids {
            query = query.bind(sensor_id);
        }
        query = query
            .bind(start_time)
            .bind(start_time)
            .bind(end_time)
            .bind(end_time);

        let rows = query.fetch_all(&self.pool).await?;

        let mut counts: HashMap<i64, usize> = HashMap::new();
        for row in rows {
            let count = counts.entry(row.sensor_id).or_insert(0);
            if *count >= limit as usize {
                continue;
            }
            *count += 1;

            if let Some(TypedSamples::Json(samples)) = results.get_mut(&row.sensor_id) {
                let value: JsonValue =
                    serde_json::from_slice(&row.value).context("Failed to parse JSON value")?;
                samples.push(Sample {
                    datetime: SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us),
                    value,
                });
            }
        }

        Ok(results)
    }

    async fn batch_query_blob_samples(
        &self,
        sensor_ids: &[i64],
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: i64,
    ) -> Result<HashMap<i64, TypedSamples>> {
        #[derive(sqlx::FromRow)]
        struct Row {
            sensor_id: i64,
            timestamp_us: i64,
            value: Vec<u8>,
        }

        let mut results: HashMap<i64, TypedSamples> = HashMap::new();

        for sensor_id in sensor_ids {
            results.insert(*sensor_id, TypedSamples::Blob(smallvec![]));
        }

        let placeholders = Self::build_in_placeholders(sensor_ids.len(), 1);
        let sql = format!(
            r#"
            SELECT sensor_id, timestamp_us, value
            FROM blob_values
            WHERE sensor_id IN ({})
            AND (?{} IS NULL OR timestamp_us >= ?{})
            AND (?{} IS NULL OR timestamp_us <= ?{})
            ORDER BY sensor_id, timestamp_us ASC
            "#,
            placeholders,
            sensor_ids.len() + 1,
            sensor_ids.len() + 2,
            sensor_ids.len() + 3,
            sensor_ids.len() + 4
        );

        let mut query = sqlx::query_as::<_, Row>(&sql);
        for sensor_id in sensor_ids {
            query = query.bind(sensor_id);
        }
        query = query
            .bind(start_time)
            .bind(start_time)
            .bind(end_time)
            .bind(end_time);

        let rows = query.fetch_all(&self.pool).await?;

        let mut counts: HashMap<i64, usize> = HashMap::new();
        for row in rows {
            let count = counts.entry(row.sensor_id).or_insert(0);
            if *count >= limit as usize {
                continue;
            }
            *count += 1;

            if let Some(TypedSamples::Blob(samples)) = results.get_mut(&row.sensor_id) {
                samples.push(Sample {
                    datetime: SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us),
                    value: row.value,
                });
            }
        }

        Ok(results)
    }
}
