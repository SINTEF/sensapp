//! Batch sample query methods for PostgreSQL storage.
//!
//! This module contains optimized batch query methods that fetch samples
//! for multiple sensors in a single query using lateral joins.
//! Used primarily by `query_sensors_by_labels` for efficient multi-sensor queries.

use super::{DEFAULT_QUERY_LIMIT, PostgresStorage};
use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
use crate::datamodel::{Sample, SensAppDateTime, Sensor, SensorType, TypedSamples};
use anyhow::Result;
use geo::Point;
use serde_json::Value as JsonValue;
use smallvec::smallvec;
use std::collections::HashMap;

impl PostgresStorage {
    /// Batch query samples for multiple sensors, grouped by sensor type.
    ///
    /// This fetches samples for all provided sensors in optimized batch queries,
    /// one query per sensor type, instead of N queries for N sensors.
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

        // Batch query for each type
        if !integer_sensors.is_empty() {
            self.batch_query_integer_samples(
                &integer_sensors,
                start_time,
                end_time,
                limit_val,
                &mut results,
            )
            .await?;
        }
        if !numeric_sensors.is_empty() {
            self.batch_query_numeric_samples(
                &numeric_sensors,
                start_time,
                end_time,
                limit_val,
                &mut results,
            )
            .await?;
        }
        if !float_sensors.is_empty() {
            self.batch_query_float_samples(
                &float_sensors,
                start_time,
                end_time,
                limit_val,
                &mut results,
            )
            .await?;
        }
        if !string_sensors.is_empty() {
            self.batch_query_string_samples(
                &string_sensors,
                start_time,
                end_time,
                limit_val,
                &mut results,
            )
            .await?;
        }
        if !boolean_sensors.is_empty() {
            self.batch_query_boolean_samples(
                &boolean_sensors,
                start_time,
                end_time,
                limit_val,
                &mut results,
            )
            .await?;
        }
        if !location_sensors.is_empty() {
            self.batch_query_location_samples(
                &location_sensors,
                start_time,
                end_time,
                limit_val,
                &mut results,
            )
            .await?;
        }
        if !json_sensors.is_empty() {
            self.batch_query_json_samples(
                &json_sensors,
                start_time,
                end_time,
                limit_val,
                &mut results,
            )
            .await?;
        }
        if !blob_sensors.is_empty() {
            self.batch_query_blob_samples(
                &blob_sensors,
                start_time,
                end_time,
                limit_val,
                &mut results,
            )
            .await?;
        }

        Ok(results)
    }

    async fn batch_query_integer_samples(
        &self,
        sensor_ids: &[i64],
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: i64,
        results: &mut HashMap<i64, TypedSamples>,
    ) -> Result<()> {
        #[derive(sqlx::FromRow)]
        struct Row {
            sensor_id: i64,
            timestamp_us: i64,
            value: i64,
        }

        // Use a lateral join to apply LIMIT per sensor_id
        let rows: Vec<Row> = sqlx::query_as(
            r#"
            SELECT sub.sensor_id, sub.timestamp_us, sub.value
            FROM unnest($1::BIGINT[]) AS s(id)
            CROSS JOIN LATERAL (
                SELECT sensor_id, timestamp_us, value
                FROM integer_values
                WHERE sensor_id = s.id
                AND ($2::BIGINT IS NULL OR timestamp_us >= $2)
                AND ($3::BIGINT IS NULL OR timestamp_us <= $3)
                ORDER BY timestamp_us ASC
                LIMIT $4
            ) sub
            ORDER BY sub.sensor_id, sub.timestamp_us
            "#,
        )
        .bind(sensor_ids)
        .bind(start_time)
        .bind(end_time)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        // Group by sensor_id
        for row in rows {
            let samples = results
                .entry(row.sensor_id)
                .or_insert_with(|| TypedSamples::Integer(smallvec![]));
            if let TypedSamples::Integer(vec) = samples {
                vec.push(Sample {
                    datetime: SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us),
                    value: row.value,
                });
            }
        }

        // Ensure all requested sensors have an entry (even if empty)
        for sensor_id in sensor_ids {
            results
                .entry(*sensor_id)
                .or_insert_with(|| TypedSamples::Integer(smallvec![]));
        }

        Ok(())
    }

    async fn batch_query_numeric_samples(
        &self,
        sensor_ids: &[i64],
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: i64,
        results: &mut HashMap<i64, TypedSamples>,
    ) -> Result<()> {
        #[derive(sqlx::FromRow)]
        struct Row {
            sensor_id: i64,
            timestamp_us: i64,
            value: rust_decimal::Decimal,
        }

        let rows: Vec<Row> = sqlx::query_as(
            r#"
            SELECT sub.sensor_id, sub.timestamp_us, sub.value
            FROM unnest($1::BIGINT[]) AS s(id)
            CROSS JOIN LATERAL (
                SELECT sensor_id, timestamp_us, value
                FROM numeric_values
                WHERE sensor_id = s.id
                AND ($2::BIGINT IS NULL OR timestamp_us >= $2)
                AND ($3::BIGINT IS NULL OR timestamp_us <= $3)
                ORDER BY timestamp_us ASC
                LIMIT $4
            ) sub
            ORDER BY sub.sensor_id, sub.timestamp_us
            "#,
        )
        .bind(sensor_ids)
        .bind(start_time)
        .bind(end_time)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        for row in rows {
            let samples = results
                .entry(row.sensor_id)
                .or_insert_with(|| TypedSamples::Numeric(smallvec![]));
            if let TypedSamples::Numeric(vec) = samples {
                vec.push(Sample {
                    datetime: SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us),
                    value: row.value,
                });
            }
        }

        for sensor_id in sensor_ids {
            results
                .entry(*sensor_id)
                .or_insert_with(|| TypedSamples::Numeric(smallvec![]));
        }

        Ok(())
    }

    async fn batch_query_float_samples(
        &self,
        sensor_ids: &[i64],
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: i64,
        results: &mut HashMap<i64, TypedSamples>,
    ) -> Result<()> {
        #[derive(sqlx::FromRow)]
        struct Row {
            sensor_id: i64,
            timestamp_us: i64,
            value: f64,
        }

        let rows: Vec<Row> = sqlx::query_as(
            r#"
            SELECT sub.sensor_id, sub.timestamp_us, sub.value
            FROM unnest($1::BIGINT[]) AS s(id)
            CROSS JOIN LATERAL (
                SELECT sensor_id, timestamp_us, value
                FROM float_values
                WHERE sensor_id = s.id
                AND ($2::BIGINT IS NULL OR timestamp_us >= $2)
                AND ($3::BIGINT IS NULL OR timestamp_us <= $3)
                ORDER BY timestamp_us ASC
                LIMIT $4
            ) sub
            ORDER BY sub.sensor_id, sub.timestamp_us
            "#,
        )
        .bind(sensor_ids)
        .bind(start_time)
        .bind(end_time)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        for row in rows {
            let samples = results
                .entry(row.sensor_id)
                .or_insert_with(|| TypedSamples::Float(smallvec![]));
            if let TypedSamples::Float(vec) = samples {
                vec.push(Sample {
                    datetime: SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us),
                    value: row.value,
                });
            }
        }

        for sensor_id in sensor_ids {
            results
                .entry(*sensor_id)
                .or_insert_with(|| TypedSamples::Float(smallvec![]));
        }

        Ok(())
    }

    async fn batch_query_string_samples(
        &self,
        sensor_ids: &[i64],
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: i64,
        results: &mut HashMap<i64, TypedSamples>,
    ) -> Result<()> {
        #[derive(sqlx::FromRow)]
        struct Row {
            sensor_id: i64,
            timestamp_us: i64,
            string_value: String,
        }

        let rows: Vec<Row> = sqlx::query_as(
            r#"
            SELECT sub.sensor_id, sub.timestamp_us, sub.string_value
            FROM unnest($1::BIGINT[]) AS s(id)
            CROSS JOIN LATERAL (
                SELECT sv.sensor_id, sv.timestamp_us, svd.value as string_value
                FROM string_values sv
                JOIN strings_values_dictionary svd ON sv.value = svd.id
                WHERE sv.sensor_id = s.id
                AND ($2::BIGINT IS NULL OR sv.timestamp_us >= $2)
                AND ($3::BIGINT IS NULL OR sv.timestamp_us <= $3)
                ORDER BY sv.timestamp_us ASC
                LIMIT $4
            ) sub
            ORDER BY sub.sensor_id, sub.timestamp_us
            "#,
        )
        .bind(sensor_ids)
        .bind(start_time)
        .bind(end_time)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        for row in rows {
            let samples = results
                .entry(row.sensor_id)
                .or_insert_with(|| TypedSamples::String(smallvec![]));
            if let TypedSamples::String(vec) = samples {
                vec.push(Sample {
                    datetime: SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us),
                    value: row.string_value,
                });
            }
        }

        for sensor_id in sensor_ids {
            results
                .entry(*sensor_id)
                .or_insert_with(|| TypedSamples::String(smallvec![]));
        }

        Ok(())
    }

    async fn batch_query_boolean_samples(
        &self,
        sensor_ids: &[i64],
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: i64,
        results: &mut HashMap<i64, TypedSamples>,
    ) -> Result<()> {
        #[derive(sqlx::FromRow)]
        struct Row {
            sensor_id: i64,
            timestamp_us: i64,
            value: bool,
        }

        let rows: Vec<Row> = sqlx::query_as(
            r#"
            SELECT sub.sensor_id, sub.timestamp_us, sub.value
            FROM unnest($1::BIGINT[]) AS s(id)
            CROSS JOIN LATERAL (
                SELECT sensor_id, timestamp_us, value
                FROM boolean_values
                WHERE sensor_id = s.id
                AND ($2::BIGINT IS NULL OR timestamp_us >= $2)
                AND ($3::BIGINT IS NULL OR timestamp_us <= $3)
                ORDER BY timestamp_us ASC
                LIMIT $4
            ) sub
            ORDER BY sub.sensor_id, sub.timestamp_us
            "#,
        )
        .bind(sensor_ids)
        .bind(start_time)
        .bind(end_time)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        for row in rows {
            let samples = results
                .entry(row.sensor_id)
                .or_insert_with(|| TypedSamples::Boolean(smallvec![]));
            if let TypedSamples::Boolean(vec) = samples {
                vec.push(Sample {
                    datetime: SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us),
                    value: row.value,
                });
            }
        }

        for sensor_id in sensor_ids {
            results
                .entry(*sensor_id)
                .or_insert_with(|| TypedSamples::Boolean(smallvec![]));
        }

        Ok(())
    }

    async fn batch_query_location_samples(
        &self,
        sensor_ids: &[i64],
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: i64,
        results: &mut HashMap<i64, TypedSamples>,
    ) -> Result<()> {
        #[derive(sqlx::FromRow)]
        struct Row {
            sensor_id: i64,
            timestamp_us: i64,
            latitude: f64,
            longitude: f64,
        }

        let rows: Vec<Row> = sqlx::query_as(
            r#"
            SELECT sub.sensor_id, sub.timestamp_us, sub.latitude, sub.longitude
            FROM unnest($1::BIGINT[]) AS s(id)
            CROSS JOIN LATERAL (
                SELECT sensor_id, timestamp_us, latitude, longitude
                FROM location_values
                WHERE sensor_id = s.id
                AND ($2::BIGINT IS NULL OR timestamp_us >= $2)
                AND ($3::BIGINT IS NULL OR timestamp_us <= $3)
                ORDER BY timestamp_us ASC
                LIMIT $4
            ) sub
            ORDER BY sub.sensor_id, sub.timestamp_us
            "#,
        )
        .bind(sensor_ids)
        .bind(start_time)
        .bind(end_time)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        for row in rows {
            let samples = results
                .entry(row.sensor_id)
                .or_insert_with(|| TypedSamples::Location(smallvec![]));
            if let TypedSamples::Location(vec) = samples {
                vec.push(Sample {
                    datetime: SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us),
                    value: Point::new(row.longitude, row.latitude),
                });
            }
        }

        for sensor_id in sensor_ids {
            results
                .entry(*sensor_id)
                .or_insert_with(|| TypedSamples::Location(smallvec![]));
        }

        Ok(())
    }

    async fn batch_query_json_samples(
        &self,
        sensor_ids: &[i64],
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: i64,
        results: &mut HashMap<i64, TypedSamples>,
    ) -> Result<()> {
        #[derive(sqlx::FromRow)]
        struct Row {
            sensor_id: i64,
            timestamp_us: i64,
            value: JsonValue,
        }

        let rows: Vec<Row> = sqlx::query_as(
            r#"
            SELECT sub.sensor_id, sub.timestamp_us, sub.value
            FROM unnest($1::BIGINT[]) AS s(id)
            CROSS JOIN LATERAL (
                SELECT sensor_id, timestamp_us, value
                FROM json_values
                WHERE sensor_id = s.id
                AND ($2::BIGINT IS NULL OR timestamp_us >= $2)
                AND ($3::BIGINT IS NULL OR timestamp_us <= $3)
                ORDER BY timestamp_us ASC
                LIMIT $4
            ) sub
            ORDER BY sub.sensor_id, sub.timestamp_us
            "#,
        )
        .bind(sensor_ids)
        .bind(start_time)
        .bind(end_time)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        for row in rows {
            let samples = results
                .entry(row.sensor_id)
                .or_insert_with(|| TypedSamples::Json(smallvec![]));
            if let TypedSamples::Json(vec) = samples {
                vec.push(Sample {
                    datetime: SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us),
                    value: row.value,
                });
            }
        }

        for sensor_id in sensor_ids {
            results
                .entry(*sensor_id)
                .or_insert_with(|| TypedSamples::Json(smallvec![]));
        }

        Ok(())
    }

    async fn batch_query_blob_samples(
        &self,
        sensor_ids: &[i64],
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: i64,
        results: &mut HashMap<i64, TypedSamples>,
    ) -> Result<()> {
        #[derive(sqlx::FromRow)]
        struct Row {
            sensor_id: i64,
            timestamp_us: i64,
            value: Vec<u8>,
        }

        let rows: Vec<Row> = sqlx::query_as(
            r#"
            SELECT sub.sensor_id, sub.timestamp_us, sub.value
            FROM unnest($1::BIGINT[]) AS s(id)
            CROSS JOIN LATERAL (
                SELECT sensor_id, timestamp_us, value
                FROM blob_values
                WHERE sensor_id = s.id
                AND ($2::BIGINT IS NULL OR timestamp_us >= $2)
                AND ($3::BIGINT IS NULL OR timestamp_us <= $3)
                ORDER BY timestamp_us ASC
                LIMIT $4
            ) sub
            ORDER BY sub.sensor_id, sub.timestamp_us
            "#,
        )
        .bind(sensor_ids)
        .bind(start_time)
        .bind(end_time)
        .bind(limit)
        .fetch_all(&self.pool)
        .await?;

        for row in rows {
            let samples = results
                .entry(row.sensor_id)
                .or_insert_with(|| TypedSamples::Blob(smallvec![]));
            if let TypedSamples::Blob(vec) = samples {
                vec.push(Sample {
                    datetime: SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us),
                    value: row.value,
                });
            }
        }

        for sensor_id in sensor_ids {
            results
                .entry(*sensor_id)
                .or_insert_with(|| TypedSamples::Blob(smallvec![]));
        }

        Ok(())
    }
}
