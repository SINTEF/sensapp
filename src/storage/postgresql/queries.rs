//! Single-sensor sample query methods for PostgreSQL storage.
//!
//! This module contains the individual query methods for each sensor type,
//! used when querying samples for a single sensor by ID.

use super::{DEFAULT_QUERY_LIMIT, PostgresStorage};
use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
use crate::datamodel::{Sample, SensAppDateTime, TypedSamples};
use crate::storage::Aggregation;
use anyhow::Result;
use geo::Point;
use serde_json::Value as JsonValue;
use smallvec::smallvec;

impl PostgresStorage {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn query_integer_samples_aggregated(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        step_ms: i64,
        origin_us: i64,
        aggregation: Aggregation,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        let limit = limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64;

        match aggregation {
            Aggregation::Avg => {
                #[derive(sqlx::FromRow)]
                struct Row {
                    timestamp_us: i64,
                    value: f64,
                }

                let sql = format!(
                    "{} {} {}",
                    integer_bucketed_cte(),
                    "SELECT bucket_us AS timestamp_us, AVG(value)::double precision AS value",
                    integer_group_by_clause()
                );

                let rows: Vec<Row> = sqlx::query_as(sqlx::AssertSqlSafe(sql.as_str()))
                    .bind(sensor_id)
                    .bind(start_time)
                    .bind(end_time)
                    .bind(step_ms)
                    .bind(origin_us)
                    .bind(limit)
                    .fetch_all(&self.pool)
                    .await?;

                let mut samples = smallvec![];
                for row in rows {
                    samples.push(Sample {
                        datetime: SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us),
                        value: row.value,
                    });
                }
                Ok(TypedSamples::Float(samples))
            }
            Aggregation::Count => {
                #[derive(sqlx::FromRow)]
                struct Row {
                    timestamp_us: i64,
                    value: i64,
                }

                let sql = format!(
                    "{} {} {}",
                    integer_bucketed_cte(),
                    "SELECT bucket_us AS timestamp_us, COUNT(*)::bigint AS value",
                    integer_group_by_clause()
                );

                let rows: Vec<Row> = sqlx::query_as(sqlx::AssertSqlSafe(sql.as_str()))
                    .bind(sensor_id)
                    .bind(start_time)
                    .bind(end_time)
                    .bind(step_ms)
                    .bind(origin_us)
                    .bind(limit)
                    .fetch_all(&self.pool)
                    .await?;

                let mut samples = smallvec![];
                for row in rows {
                    samples.push(Sample {
                        datetime: SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us),
                        value: row.value,
                    });
                }
                Ok(TypedSamples::Integer(samples))
            }
            _ => {
                #[derive(sqlx::FromRow)]
                struct Row {
                    timestamp_us: i64,
                    value: i64,
                }

                let expression = integer_aggregate_expression(aggregation);
                let sql = format!(
                    "{} SELECT {} AS timestamp_us, {} AS value {}",
                    integer_bucketed_cte(),
                    bucket_timestamp_expression(aggregation),
                    expression,
                    integer_group_by_clause()
                );

                let rows: Vec<Row> = sqlx::query_as(sqlx::AssertSqlSafe(sql.as_str()))
                    .bind(sensor_id)
                    .bind(start_time)
                    .bind(end_time)
                    .bind(step_ms)
                    .bind(origin_us)
                    .bind(limit)
                    .fetch_all(&self.pool)
                    .await?;

                let mut samples = smallvec![];
                for row in rows {
                    samples.push(Sample {
                        datetime: SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us),
                        value: row.value,
                    });
                }
                Ok(TypedSamples::Integer(samples))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn query_float_samples_aggregated(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        step_ms: i64,
        origin_us: i64,
        aggregation: Aggregation,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        let limit = limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64;

        match aggregation {
            Aggregation::Count => {
                #[derive(sqlx::FromRow)]
                struct Row {
                    timestamp_us: i64,
                    value: i64,
                }

                let sql = format!(
                    "{} {} {}",
                    float_bucketed_cte(),
                    "SELECT bucket_us AS timestamp_us, COUNT(*)::bigint AS value",
                    integer_group_by_clause()
                );

                let rows: Vec<Row> = sqlx::query_as(sqlx::AssertSqlSafe(sql.as_str()))
                    .bind(sensor_id)
                    .bind(start_time)
                    .bind(end_time)
                    .bind(step_ms)
                    .bind(origin_us)
                    .bind(limit)
                    .fetch_all(&self.pool)
                    .await?;

                let mut samples = smallvec![];
                for row in rows {
                    samples.push(Sample {
                        datetime: SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us),
                        value: row.value,
                    });
                }
                Ok(TypedSamples::Integer(samples))
            }
            _ => {
                #[derive(sqlx::FromRow)]
                struct Row {
                    timestamp_us: i64,
                    value: f64,
                }

                let expression = float_aggregate_expression(aggregation);
                let sql = format!(
                    "{} SELECT {} AS timestamp_us, {} AS value {}",
                    float_bucketed_cte(),
                    bucket_timestamp_expression(aggregation),
                    expression,
                    integer_group_by_clause()
                );

                let rows: Vec<Row> = sqlx::query_as(sqlx::AssertSqlSafe(sql.as_str()))
                    .bind(sensor_id)
                    .bind(start_time)
                    .bind(end_time)
                    .bind(step_ms)
                    .bind(origin_us)
                    .bind(limit)
                    .fetch_all(&self.pool)
                    .await?;

                let mut samples = smallvec![];
                for row in rows {
                    samples.push(Sample {
                        datetime: SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us),
                        value: row.value,
                    });
                }
                Ok(TypedSamples::Float(samples))
            }
        }
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn query_numeric_samples_aggregated(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        step_ms: i64,
        origin_us: i64,
        aggregation: Aggregation,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        let limit = limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64;

        match aggregation {
            Aggregation::Count => {
                #[derive(sqlx::FromRow)]
                struct Row {
                    timestamp_us: i64,
                    value: i64,
                }

                let sql = format!(
                    "{} {} {}",
                    numeric_bucketed_cte(),
                    "SELECT bucket_us AS timestamp_us, COUNT(*)::bigint AS value",
                    integer_group_by_clause()
                );

                let rows: Vec<Row> = sqlx::query_as(sqlx::AssertSqlSafe(sql.as_str()))
                    .bind(sensor_id)
                    .bind(start_time)
                    .bind(end_time)
                    .bind(step_ms)
                    .bind(origin_us)
                    .bind(limit)
                    .fetch_all(&self.pool)
                    .await?;

                let mut samples = smallvec![];
                for row in rows {
                    samples.push(Sample {
                        datetime: SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us),
                        value: row.value,
                    });
                }
                Ok(TypedSamples::Integer(samples))
            }
            _ => {
                #[derive(sqlx::FromRow)]
                struct Row {
                    timestamp_us: i64,
                    value: rust_decimal::Decimal,
                }

                let expression = numeric_aggregate_expression(aggregation);
                let sql = format!(
                    "{} SELECT {} AS timestamp_us, {} AS value {}",
                    numeric_bucketed_cte(),
                    bucket_timestamp_expression(aggregation),
                    expression,
                    integer_group_by_clause()
                );

                let rows: Vec<Row> = sqlx::query_as(sqlx::AssertSqlSafe(sql.as_str()))
                    .bind(sensor_id)
                    .bind(start_time)
                    .bind(end_time)
                    .bind(step_ms)
                    .bind(origin_us)
                    .bind(limit)
                    .fetch_all(&self.pool)
                    .await?;

                let mut samples = smallvec![];
                for row in rows {
                    samples.push(Sample {
                        datetime: SensAppDateTime::from_unix_microseconds_i64(row.timestamp_us),
                        value: row.value,
                    });
                }
                Ok(TypedSamples::Numeric(samples))
            }
        }
    }

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
            AND timestamp_us >= COALESCE($2::BIGINT, -9223372036854775807)
            AND timestamp_us <= COALESCE($3::BIGINT, 9223372036854775807)
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
            AND timestamp_us >= COALESCE($2::BIGINT, -9223372036854775807)
            AND timestamp_us <= COALESCE($3::BIGINT, 9223372036854775807)
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
            AND timestamp_us >= COALESCE($2::BIGINT, -9223372036854775807)
            AND timestamp_us <= COALESCE($3::BIGINT, 9223372036854775807)
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
            AND sv.timestamp_us >= COALESCE($2::BIGINT, -9223372036854775807)
            AND sv.timestamp_us <= COALESCE($3::BIGINT, 9223372036854775807)
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
            AND timestamp_us >= COALESCE($2::BIGINT, -9223372036854775807)
            AND timestamp_us <= COALESCE($3::BIGINT, 9223372036854775807)
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
            AND timestamp_us >= COALESCE($2::BIGINT, -9223372036854775807)
            AND timestamp_us <= COALESCE($3::BIGINT, 9223372036854775807)
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
            AND timestamp_us >= COALESCE($2::BIGINT, -9223372036854775807)
            AND timestamp_us <= COALESCE($3::BIGINT, 9223372036854775807)
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
            AND timestamp_us >= COALESCE($2::BIGINT, -9223372036854775807)
            AND timestamp_us <= COALESCE($3::BIGINT, 9223372036854775807)
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

/// The buckets of the samples of one sensor (`WHERE sensor_id = $1`) or of many
/// (`WHERE sensor_id = ANY($1)`, with the sensor in the rows), $2 and $3 bounding the time, $4 the
/// step in milliseconds and $5 the origin of the buckets in microseconds.
///
/// The bucket is computed on the integer, as `date_bin` would: the start of the step that holds the
/// sample, steps counted from the origin. The `%` form floors, where an integer division truncates
/// toward zero, so a sample before the origin lands in its own step too. Converting every row to a
/// timestamp for `date_bin` took six times longer (1.25 s against 0.21 s for 1.33 M samples).
pub(super) fn bucketed_cte(table: &str, many_sensors: bool) -> String {
    let (sensor_column, sensor_filter) = if many_sensors {
        ("sensor_id,", "sensor_id = ANY($1)")
    } else {
        ("", "sensor_id = $1")
    };
    format!(
        r#"
    WITH bucketed AS (
        SELECT
            {sensor_column}
            timestamp_us - (((timestamp_us - $5::bigint) % ($4::bigint * 1000)) + ($4::bigint * 1000)) % ($4::bigint * 1000) AS bucket_us,
            timestamp_us,
            value
        FROM {table}
        WHERE {sensor_filter}
        AND timestamp_us >= COALESCE($2::BIGINT, -9223372036854775807)
        AND timestamp_us <= COALESCE($3::BIGINT, 9223372036854775807)
    )
    "#
    )
}

fn integer_bucketed_cte() -> String {
    bucketed_cte("integer_values", false)
}

fn float_bucketed_cte() -> String {
    bucketed_cte("float_values", false)
}

fn numeric_bucketed_cte() -> String {
    bucketed_cte("numeric_values", false)
}

fn integer_group_by_clause() -> &'static str {
    group_by_clause(false)
}

/// Group by bucket (and sensor), ordered, $6 limiting the number of buckets in total.
pub(super) fn group_by_clause(many_sensors: bool) -> &'static str {
    if many_sensors {
        "FROM bucketed GROUP BY sensor_id, bucket_us ORDER BY sensor_id, bucket_us ASC LIMIT $6"
    } else {
        "FROM bucketed GROUP BY bucket_us ORDER BY bucket_us ASC LIMIT $6"
    }
}

/// The timestamp of a bucket in the select list of a grouped query: its start, or for `Latest`
/// the timestamp of the last sample it holds.
pub(super) fn bucket_timestamp_expression(aggregation: Aggregation) -> &'static str {
    match aggregation {
        Aggregation::Latest => "MAX(timestamp_us)",
        _ => "bucket_us",
    }
}

pub(super) fn integer_aggregate_expression(aggregation: Aggregation) -> &'static str {
    match aggregation {
        Aggregation::Min => "MIN(value)",
        Aggregation::Max => "MAX(value)",
        Aggregation::Sum => "SUM(value)::bigint",
        Aggregation::First => "(array_agg(value ORDER BY timestamp_us ASC))[1]",
        Aggregation::Last | Aggregation::Latest => {
            "(array_agg(value ORDER BY timestamp_us DESC))[1]"
        }
        Aggregation::Avg | Aggregation::Count => unreachable!("handled separately"),
    }
}

pub(super) fn float_aggregate_expression(aggregation: Aggregation) -> &'static str {
    match aggregation {
        Aggregation::Avg => "AVG(value)",
        Aggregation::Min => "MIN(value)",
        Aggregation::Max => "MAX(value)",
        Aggregation::Sum => "SUM(value)",
        Aggregation::First => "(array_agg(value ORDER BY timestamp_us ASC))[1]",
        Aggregation::Last | Aggregation::Latest => {
            "(array_agg(value ORDER BY timestamp_us DESC))[1]"
        }
        Aggregation::Count => unreachable!("handled separately"),
    }
}

pub(super) fn numeric_aggregate_expression(aggregation: Aggregation) -> &'static str {
    match aggregation {
        Aggregation::Avg => "AVG(value)",
        Aggregation::Min => "MIN(value)",
        Aggregation::Max => "MAX(value)",
        Aggregation::Sum => "SUM(value)",
        Aggregation::First => "(array_agg(value ORDER BY timestamp_us ASC))[1]",
        Aggregation::Last | Aggregation::Latest => {
            "(array_agg(value ORDER BY timestamp_us DESC))[1]"
        }
        Aggregation::Count => unreachable!("handled separately"),
    }
}
