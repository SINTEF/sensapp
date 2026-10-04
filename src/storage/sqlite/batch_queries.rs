//! Batch sample query methods for SQLite storage.
//!
//! This module contains optimized batch query methods that fetch samples
//! for multiple sensors. Unlike PostgreSQL, SQLite doesn't support LATERAL
//! joins, so the per-sensor limit is a correlated subquery that reads the index of each sensor and
//! stops after `limit` rows.
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
    ///
    /// `limit` is a number of samples **per sensor**, the oldest first, as
    /// `query_sensors_by_labels` promises: it is applied row by row after the query, which has no
    /// SQL `LIMIT`. It is not the shared budget of the selector reads, which is global to all the
    /// sensors of a type (`selector.rs`, `read_numeric_samples`): the two must not be merged.
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

        // One statement, bounded by the index of each sensor: for every id, the subquery reads the
        // first `limit` rows of the sensor in the order of the `(sensor_id, timestamp_us)` index and
        // stops. Reading every sample of the window to keep a few per sensor costs far more.
        let sql = r#"
            SELECT v.sensor_id, v.timestamp_us, v.value
            FROM json_each(?1) ids
            JOIN integer_values v ON v.rowid IN (
                SELECT w.rowid FROM integer_values w
                WHERE w.sensor_id = ids.value
                AND w.timestamp_us >= COALESCE(?2, -9223372036854775807)
                AND w.timestamp_us <= COALESCE(?3, 9223372036854775807)
                ORDER BY w.timestamp_us ASC
                LIMIT ?4
            )
            ORDER BY v.sensor_id, v.timestamp_us ASC
            "#;
        let rows: Vec<Row> = sqlx::query_as::<_, Row>(sql)
            .bind(serde_json::to_string(sensor_ids)?)
            .bind(start_time)
            .bind(end_time)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;

        for row in rows {
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

        // One statement, bounded by the index of each sensor: for every id, the subquery reads the
        // first `limit` rows of the sensor in the order of the `(sensor_id, timestamp_us)` index and
        // stops. Reading every sample of the window to keep a few per sensor costs far more.
        let sql = r#"
            SELECT v.sensor_id, v.timestamp_us, v.value
            FROM json_each(?1) ids
            JOIN numeric_values v ON v.rowid IN (
                SELECT w.rowid FROM numeric_values w
                WHERE w.sensor_id = ids.value
                AND w.timestamp_us >= COALESCE(?2, -9223372036854775807)
                AND w.timestamp_us <= COALESCE(?3, 9223372036854775807)
                ORDER BY w.timestamp_us ASC
                LIMIT ?4
            )
            ORDER BY v.sensor_id, v.timestamp_us ASC
            "#;
        let rows: Vec<Row> = sqlx::query_as::<_, Row>(sql)
            .bind(serde_json::to_string(sensor_ids)?)
            .bind(start_time)
            .bind(end_time)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;

        for row in rows {
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

        // One statement, bounded by the index of each sensor: for every id, the subquery reads the
        // first `limit` rows of the sensor in the order of the `(sensor_id, timestamp_us)` index and
        // stops. Reading every sample of the window to keep a few per sensor costs far more.
        let sql = r#"
            SELECT v.sensor_id, v.timestamp_us, v.value
            FROM json_each(?1) ids
            JOIN float_values v ON v.rowid IN (
                SELECT w.rowid FROM float_values w
                WHERE w.sensor_id = ids.value
                AND w.timestamp_us >= COALESCE(?2, -9223372036854775807)
                AND w.timestamp_us <= COALESCE(?3, 9223372036854775807)
                ORDER BY w.timestamp_us ASC
                LIMIT ?4
            )
            ORDER BY v.sensor_id, v.timestamp_us ASC
            "#;
        let rows: Vec<Row> = sqlx::query_as::<_, Row>(sql)
            .bind(serde_json::to_string(sensor_ids)?)
            .bind(start_time)
            .bind(end_time)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;

        for row in rows {
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

        // One statement, bounded by the index of each sensor: for every id, the subquery reads the
        // first `limit` rows of the sensor in the order of the `(sensor_id, timestamp_us)` index and
        // stops. Reading every sample of the window to keep a few per sensor costs far more.
        let sql = r#"
            SELECT v.sensor_id, v.timestamp_us, svd.value as string_value
            FROM json_each(?1) ids
            JOIN string_values v ON v.rowid IN (
                SELECT w.rowid FROM string_values w
                WHERE w.sensor_id = ids.value
                AND w.timestamp_us >= COALESCE(?2, -9223372036854775807)
                AND w.timestamp_us <= COALESCE(?3, 9223372036854775807)
                ORDER BY w.timestamp_us ASC
                LIMIT ?4
            )
            JOIN strings_values_dictionary svd ON v.value = svd.id
            ORDER BY v.sensor_id, v.timestamp_us ASC
            "#;
        let rows: Vec<Row> = sqlx::query_as::<_, Row>(sql)
            .bind(serde_json::to_string(sensor_ids)?)
            .bind(start_time)
            .bind(end_time)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;

        for row in rows {
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

        // One statement, bounded by the index of each sensor: for every id, the subquery reads the
        // first `limit` rows of the sensor in the order of the `(sensor_id, timestamp_us)` index and
        // stops. Reading every sample of the window to keep a few per sensor costs far more.
        let sql = r#"
            SELECT v.sensor_id, v.timestamp_us, v.value
            FROM json_each(?1) ids
            JOIN boolean_values v ON v.rowid IN (
                SELECT w.rowid FROM boolean_values w
                WHERE w.sensor_id = ids.value
                AND w.timestamp_us >= COALESCE(?2, -9223372036854775807)
                AND w.timestamp_us <= COALESCE(?3, 9223372036854775807)
                ORDER BY w.timestamp_us ASC
                LIMIT ?4
            )
            ORDER BY v.sensor_id, v.timestamp_us ASC
            "#;
        let rows: Vec<Row> = sqlx::query_as::<_, Row>(sql)
            .bind(serde_json::to_string(sensor_ids)?)
            .bind(start_time)
            .bind(end_time)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;

        for row in rows {
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

        // One statement, bounded by the index of each sensor: for every id, the subquery reads the
        // first `limit` rows of the sensor in the order of the `(sensor_id, timestamp_us)` index and
        // stops. Reading every sample of the window to keep a few per sensor costs far more.
        let sql = r#"
            SELECT v.sensor_id, v.timestamp_us, v.latitude, v.longitude
            FROM json_each(?1) ids
            JOIN location_values v ON v.rowid IN (
                SELECT w.rowid FROM location_values w
                WHERE w.sensor_id = ids.value
                AND w.timestamp_us >= COALESCE(?2, -9223372036854775807)
                AND w.timestamp_us <= COALESCE(?3, 9223372036854775807)
                ORDER BY w.timestamp_us ASC
                LIMIT ?4
            )
            ORDER BY v.sensor_id, v.timestamp_us ASC
            "#;
        let rows: Vec<Row> = sqlx::query_as::<_, Row>(sql)
            .bind(serde_json::to_string(sensor_ids)?)
            .bind(start_time)
            .bind(end_time)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;

        for row in rows {
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

        // One statement, bounded by the index of each sensor: for every id, the subquery reads the
        // first `limit` rows of the sensor in the order of the `(sensor_id, timestamp_us)` index and
        // stops. Reading every sample of the window to keep a few per sensor costs far more.
        let sql = r#"
            SELECT v.sensor_id, v.timestamp_us, v.value
            FROM json_each(?1) ids
            JOIN json_values v ON v.rowid IN (
                SELECT w.rowid FROM json_values w
                WHERE w.sensor_id = ids.value
                AND w.timestamp_us >= COALESCE(?2, -9223372036854775807)
                AND w.timestamp_us <= COALESCE(?3, 9223372036854775807)
                ORDER BY w.timestamp_us ASC
                LIMIT ?4
            )
            ORDER BY v.sensor_id, v.timestamp_us ASC
            "#;
        let rows: Vec<Row> = sqlx::query_as::<_, Row>(sql)
            .bind(serde_json::to_string(sensor_ids)?)
            .bind(start_time)
            .bind(end_time)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;

        for row in rows {
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

        // One statement, bounded by the index of each sensor: for every id, the subquery reads the
        // first `limit` rows of the sensor in the order of the `(sensor_id, timestamp_us)` index and
        // stops. Reading every sample of the window to keep a few per sensor costs far more.
        let sql = r#"
            SELECT v.sensor_id, v.timestamp_us, v.value
            FROM json_each(?1) ids
            JOIN blob_values v ON v.rowid IN (
                SELECT w.rowid FROM blob_values w
                WHERE w.sensor_id = ids.value
                AND w.timestamp_us >= COALESCE(?2, -9223372036854775807)
                AND w.timestamp_us <= COALESCE(?3, 9223372036854775807)
                ORDER BY w.timestamp_us ASC
                LIMIT ?4
            )
            ORDER BY v.sensor_id, v.timestamp_us ASC
            "#;
        let rows: Vec<Row> = sqlx::query_as::<_, Row>(sql)
            .bind(serde_json::to_string(sensor_ids)?)
            .bind(start_time)
            .bind(end_time)
            .bind(limit)
            .fetch_all(&self.pool)
            .await?;

        for row in rows {
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
