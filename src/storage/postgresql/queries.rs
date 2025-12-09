//! Single-sensor sample query methods for PostgreSQL storage.
//!
//! This module contains the individual query methods for each sensor type,
//! used when querying samples for a single sensor by ID.

use super::{DEFAULT_QUERY_LIMIT, PostgresStorage};
use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
use crate::datamodel::{Sample, SensAppDateTime, TypedSamples};
use anyhow::Result;
use geo::Point;
use serde_json::Value as JsonValue;
use smallvec::smallvec;

impl PostgresStorage {
    pub(super) async fn query_integer_samples(
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

    pub(super) async fn query_numeric_samples(
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

    pub(super) async fn query_float_samples(
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

    pub(super) async fn query_string_samples(
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

    pub(super) async fn query_boolean_samples(
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

    pub(super) async fn query_location_samples(
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

    pub(super) async fn query_json_samples(
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

    pub(super) async fn query_blob_samples(
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
}
