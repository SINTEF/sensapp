pub mod timescaledb_publishers;
pub mod timescaledb_utilities;

use self::timescaledb_publishers::*;
use self::timescaledb_utilities::get_sensor_id_or_create_sensor;
use super::{
    Aggregation, DEFAULT_LIST_SERIES_LIMIT, DEFAULT_QUERY_LIMIT, MAX_LIST_SERIES_LIMIT,
    SensorAvailabilitySummary, SensorDataQueryOptions, StorageError, StorageInstance,
    common::datetime_to_micros,
};
use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
use crate::datamodel::{
    Sample, SensAppDateTime, Sensor, SensorData, SensorType, TypedSamples, batch::Batch,
};
use crate::datamodel::{sensapp_vec::SensAppLabels, unit::Unit};
use anyhow::{Context, Result};
use async_trait::async_trait;
use smallvec::smallvec;
use sqlx::{
    PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::{collections::HashMap, str::FromStr, sync::Arc};
use uuid::Uuid;

#[derive(Debug)]
pub struct TimeScaleDBStorage {
    pool: PgPool,
}

fn micros_to_offset_datetime(timestamp_us: i64) -> sqlx::types::time::OffsetDateTime {
    sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((timestamp_us as i128) * 1000)
        .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
}

fn offset_datetime_to_sensapp(datetime: sqlx::types::time::OffsetDateTime) -> SensAppDateTime {
    SensAppDateTime::from_unix_microseconds_i64((datetime.unix_timestamp_nanos() / 1000) as i64)
}

impl TimeScaleDBStorage {
    pub async fn connect(connection_string: &str) -> Result<Self> {
        // Convert timescaledb:// to postgres:// for sqlx compatibility
        let postgres_connection_string = if connection_string.starts_with("timescaledb://") {
            connection_string.replace("timescaledb://", "postgres://")
        } else {
            connection_string.to_string()
        };

        let connect_options = PgConnectOptions::from_str(&postgres_connection_string)
            .context("Failed to create timescaledb connection options")?;

        let max_connections = std::env::var("SENSAPP_PG_POOL_MAX_CONNECTIONS")
            .ok()
            .and_then(|value| value.parse::<u32>().ok())
            .filter(|value| *value > 0)
            .unwrap_or(10);

        let pool = PgPoolOptions::new()
            .max_connections(max_connections)
            .connect_with(connect_options)
            .await
            .context("Failed to create timescaledb pool")?;

        Ok(Self { pool })
    }

    async fn find_sensors_by_matchers(
        &self,
        name_matchers: &[&super::LabelMatcher],
        label_matchers: &[&super::LabelMatcher],
        numeric_only: bool,
    ) -> Result<Vec<(i64, Sensor)>> {
        let mut sql = String::from(
            r#"SELECT DISTINCT s.sensor_id, s.uuid, s.name, s.type,
                      u.name as unit_name, u.description as unit_description
               FROM sensors s
               LEFT JOIN units u ON s.unit = u.id"#,
        );
        let mut where_clauses: Vec<String> = Vec::new();
        let mut params: Vec<String> = Vec::new();
        let mut param_idx = 1;

        if numeric_only {
            where_clauses.push("s.type IN ('Integer', 'Numeric', 'Float')".to_string());
        }

        for matcher in name_matchers {
            let clause = match matcher.matcher_type {
                super::MatcherType::Equal => {
                    params.push(matcher.value.clone());
                    let clause = format!("s.name = ${}", param_idx);
                    param_idx += 1;
                    clause
                }
                super::MatcherType::NotEqual => {
                    params.push(matcher.value.clone());
                    let clause = format!("s.name != ${}", param_idx);
                    param_idx += 1;
                    clause
                }
                super::MatcherType::RegexMatch => {
                    params.push(matcher.value.clone());
                    let clause = format!("s.name ~ ${}", param_idx);
                    param_idx += 1;
                    clause
                }
                super::MatcherType::RegexNotMatch => {
                    params.push(matcher.value.clone());
                    let clause = format!("s.name !~ ${}", param_idx);
                    param_idx += 1;
                    clause
                }
            };
            where_clauses.push(clause);
        }

        for matcher in label_matchers {
            let subquery = match matcher.matcher_type {
                super::MatcherType::Equal => {
                    params.push(matcher.name.clone());
                    params.push(matcher.value.clone());
                    let subquery = format!(
                        r#"s.sensor_id IN (
                            SELECT l.sensor_id FROM labels l
                            JOIN labels_name_dictionary lnd ON l.name = lnd.id
                            JOIN labels_description_dictionary ldd ON l.description = ldd.id
                            WHERE lnd.name = ${} AND ldd.description = ${}
                        )"#,
                        param_idx,
                        param_idx + 1
                    );
                    param_idx += 2;
                    subquery
                }
                super::MatcherType::NotEqual => {
                    params.push(matcher.name.clone());
                    params.push(matcher.value.clone());
                    let subquery = format!(
                        r#"s.sensor_id NOT IN (
                            SELECT l.sensor_id FROM labels l
                            JOIN labels_name_dictionary lnd ON l.name = lnd.id
                            JOIN labels_description_dictionary ldd ON l.description = ldd.id
                            WHERE lnd.name = ${} AND ldd.description = ${}
                        )"#,
                        param_idx,
                        param_idx + 1
                    );
                    param_idx += 2;
                    subquery
                }
                super::MatcherType::RegexMatch => {
                    params.push(matcher.name.clone());
                    params.push(matcher.value.clone());
                    let subquery = format!(
                        r#"s.sensor_id IN (
                            SELECT l.sensor_id FROM labels l
                            JOIN labels_name_dictionary lnd ON l.name = lnd.id
                            JOIN labels_description_dictionary ldd ON l.description = ldd.id
                            WHERE lnd.name = ${} AND ldd.description ~ ${}
                        )"#,
                        param_idx,
                        param_idx + 1
                    );
                    param_idx += 2;
                    subquery
                }
                super::MatcherType::RegexNotMatch => {
                    params.push(matcher.name.clone());
                    params.push(matcher.value.clone());
                    let subquery = format!(
                        r#"s.sensor_id NOT IN (
                            SELECT l.sensor_id FROM labels l
                            JOIN labels_name_dictionary lnd ON l.name = lnd.id
                            JOIN labels_description_dictionary ldd ON l.description = ldd.id
                            WHERE lnd.name = ${} AND ldd.description ~ ${}
                        )"#,
                        param_idx,
                        param_idx + 1
                    );
                    param_idx += 2;
                    subquery
                }
            };
            where_clauses.push(subquery);
        }

        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }
        sql.push_str(" ORDER BY s.sensor_id");

        #[derive(sqlx::FromRow)]
        struct SensorRow {
            sensor_id: i64,
            uuid: Uuid,
            name: String,
            r#type: String,
            unit_name: Option<String>,
            unit_description: Option<String>,
        }

        let mut query = sqlx::query_as::<_, SensorRow>(&sql);
        for param in &params {
            query = query.bind(param);
        }

        let sensor_rows = query.fetch_all(&self.pool).await?;

        if sensor_rows.is_empty() {
            return Ok(Vec::new());
        }

        let sensor_ids: Vec<i64> = sensor_rows.iter().map(|row| row.sensor_id).collect();

        #[derive(sqlx::FromRow)]
        struct LabelRow {
            sensor_id: i64,
            label_name: String,
            label_value: String,
        }

        let labels_rows: Vec<LabelRow> = sqlx::query_as(
            r#"
            SELECT l.sensor_id, lnd.name as label_name, ldd.description as label_value
            FROM labels l
            JOIN labels_name_dictionary lnd ON l.name = lnd.id
            JOIN labels_description_dictionary ldd ON l.description = ldd.id
            WHERE l.sensor_id = ANY($1)
            ORDER BY l.sensor_id
            "#,
        )
        .bind(&sensor_ids)
        .fetch_all(&self.pool)
        .await?;

        let mut labels_map: HashMap<i64, SensAppLabels> = HashMap::new();
        for label_row in labels_rows {
            labels_map
                .entry(label_row.sensor_id)
                .or_insert_with(|| smallvec![])
                .push((label_row.label_name, label_row.label_value));
        }

        let mut results = Vec::with_capacity(sensor_rows.len());
        for row in sensor_rows {
            let sensor_type = SensorType::from_str(&row.r#type).map_err(|e| {
                anyhow::Error::from(StorageError::invalid_data_format(
                    &format!("Failed to parse sensor type '{}': {}", row.r#type, e),
                    Some(row.uuid),
                    Some(&row.name),
                ))
            })?;

            let unit = match (row.unit_name, row.unit_description) {
                (Some(name), description) => Some(Unit::new(name, description)),
                _ => None,
            };

            let labels = labels_map.remove(&row.sensor_id).unwrap_or_default();
            let sensor = Sensor::new(row.uuid, row.name, sensor_type, unit, Some(labels));
            results.push((row.sensor_id, sensor));
        }

        Ok(results)
    }

    async fn get_sensor_metadata(&self, sensor_uuid: &str) -> Result<Option<(i64, Sensor)>> {
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

        let Some(sensor_row) = sensor_row else {
            return Ok(None);
        };

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

        Ok(Some((
            sensor_id,
            Sensor::new(sensor_uuid, sensor_name, sensor_type, unit, Some(labels)),
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
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
    ) -> Result<Option<i64>> {
        let sql = format!(
            r#"
            SELECT (EXTRACT(EPOCH FROM MAX(time)) * 1000000)::bigint AS timestamp_us
            FROM {table_name}
            WHERE sensor_id = $1
            AND ($2::TIMESTAMPTZ IS NULL OR time >= $2)
            AND ($3::TIMESTAMPTZ IS NULL OR time <= $3)
            "#
        );

        let timestamp_us: Option<i64> = sqlx::query_scalar(&sql)
            .bind(sensor_id)
            .bind(start_time.map(micros_to_offset_datetime))
            .bind(end_time.map(micros_to_offset_datetime))
            .fetch_one(&self.pool)
            .await?;

        Ok(timestamp_us)
    }

    async fn query_availability_summary_native(
        &self,
        table_name: &str,
        sensor_id: i64,
        sensor: Sensor,
        start_time: SensAppDateTime,
        end_time: SensAppDateTime,
        step_ms: Option<i64>,
    ) -> Result<SensorAvailabilitySummary> {
        #[derive(sqlx::FromRow)]
        struct Row {
            sample_count: i64,
            first_sample_at: Option<sqlx::types::time::OffsetDateTime>,
            last_sample_at: Option<sqlx::types::time::OffsetDateTime>,
            covered_buckets: Option<i64>,
        }

        let start_ts = micros_to_offset_datetime(datetime_to_micros(&start_time));
        let end_ts = micros_to_offset_datetime(datetime_to_micros(&end_time));

        let row: Row = if let Some(step_ms) = step_ms {
            let sql = format!(
                r#"
                SELECT
                    COUNT(*)::bigint AS sample_count,
                    MIN(time) AS first_sample_at,
                    MAX(time) AS last_sample_at,
                    COUNT(DISTINCT time_bucket($1::bigint * INTERVAL '1 millisecond', time, $2::timestamptz))::bigint AS covered_buckets
                FROM {table_name}
                WHERE sensor_id = $3
                AND time >= $4
                AND time <= $5
                "#
            );

            sqlx::query_as(&sql)
                .bind(step_ms)
                .bind(start_ts)
                .bind(sensor_id)
                .bind(start_ts)
                .bind(end_ts)
                .fetch_one(&self.pool)
                .await?
        } else {
            let sql = format!(
                r#"
                SELECT
                    COUNT(*)::bigint AS sample_count,
                    MIN(time) AS first_sample_at,
                    MAX(time) AS last_sample_at,
                    NULL::bigint AS covered_buckets
                FROM {table_name}
                WHERE sensor_id = $1
                AND time >= $2
                AND time <= $3
                "#
            );

            sqlx::query_as(&sql)
                .bind(sensor_id)
                .bind(start_ts)
                .bind(end_ts)
                .fetch_one(&self.pool)
                .await?
        };

        Ok(SensorAvailabilitySummary {
            sensor,
            sample_count: row.sample_count.max(0) as usize,
            first_sample_at: row.first_sample_at.map(offset_datetime_to_sensapp),
            last_sample_at: row.last_sample_at.map(offset_datetime_to_sensapp),
            covered_buckets: row.covered_buckets.map(|value| value.max(0) as usize),
        })
    }
}

fn timescaledb_bucketed_cte(table_name: &str) -> String {
    format!(
        r#"
        WITH bucketed AS (
            SELECT
                time_bucket(
                    $4::bigint * INTERVAL '1 millisecond',
                    time,
                    to_timestamp($5::double precision / 1000000.0)
                ) AS bucket_time,
                time,
                value
            FROM {table_name}
            WHERE sensor_id = $1
              AND ($2::TIMESTAMPTZ IS NULL OR time >= $2)
              AND ($3::TIMESTAMPTZ IS NULL OR time <= $3)
        )
        "#
    )
}

fn timescaledb_group_by_clause() -> &'static str {
    "FROM bucketed GROUP BY 1 ORDER BY 1 ASC LIMIT $6"
}

fn timescaledb_integer_expression(aggregation: Aggregation) -> &'static str {
    match aggregation {
        Aggregation::Min => "MIN(value)",
        Aggregation::Max => "MAX(value)",
        Aggregation::Sum => "SUM(value)",
        Aggregation::First => "first(value, time)",
        Aggregation::Last => "last(value, time)",
        Aggregation::Avg | Aggregation::Count => unreachable!("handled separately"),
    }
}

fn timescaledb_float_expression(aggregation: Aggregation) -> &'static str {
    match aggregation {
        Aggregation::Avg => "AVG(value)",
        Aggregation::Min => "MIN(value)",
        Aggregation::Max => "MAX(value)",
        Aggregation::Sum => "SUM(value)",
        Aggregation::First => "first(value, time)",
        Aggregation::Last => "last(value, time)",
        Aggregation::Count => unreachable!("handled separately"),
    }
}

fn timescaledb_numeric_expression(aggregation: Aggregation) -> &'static str {
    match aggregation {
        Aggregation::Avg => "AVG(value)",
        Aggregation::Min => "MIN(value)",
        Aggregation::Max => "MAX(value)",
        Aggregation::Sum => "SUM(value)",
        Aggregation::First => "first(value, time)",
        Aggregation::Last => "last(value, time)",
        Aggregation::Count => unreachable!("handled separately"),
    }
}

#[async_trait]
impl StorageInstance for TimeScaleDBStorage {
    async fn create_or_migrate(&self) -> Result<()> {
        sqlx::migrate!("src/storage/timescaledb/migrations")
            .run(&self.pool)
            .await
            .context("Failed to migrate database")?;

        Ok(())
    }
    async fn publish(&self, batch: Arc<Batch>) -> Result<()> {
        let mut transaction = self.pool.begin().await?;
        for single_sensor_batch in batch.sensors.as_ref() {
            self.publish_single_sensor_batch(&mut transaction, single_sensor_batch)
                .await?;
        }
        transaction.commit().await?;
        Ok(())
    }

    async fn vacuum(&self) -> Result<()> {
        self.vacuum().await?;
        Ok(())
    }

    async fn list_series(
        &self,
        metric_filter: Option<&str>,
        limit: Option<usize>,
        bookmark: Option<&str>,
    ) -> Result<crate::storage::ListSeriesResult> {
        #[derive(sqlx::FromRow)]
        struct SensorRow {
            sensor_id: Option<i64>,
            uuid: Option<uuid::Uuid>,
            name: Option<String>,
            r#type: Option<String>,
            unit_name: Option<String>,
            unit_description: Option<String>,
        }

        let bookmark_id = if let Some(bookmark_str) = bookmark {
            Some(bookmark_str.parse::<i64>().map_err(|e| {
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

        // Query sensors with their metadata using the catalog view, optionally filtered by metric name
        let sensor_rows: Vec<SensorRow> = sqlx::query_as(
            r#"
            SELECT sensor_id, uuid, name, type, unit_name, unit_description
            FROM sensor_catalog_view
            WHERE ($1::TEXT IS NULL OR name = $1)
              AND ($2::BIGINT IS NULL OR sensor_id > $2)
            ORDER BY sensor_id ASC
            LIMIT $3
            "#,
        )
        .bind(metric_filter)
        .bind(bookmark_id)
        .bind(fetch_limit as i64)
        .fetch_all(&self.pool)
        .await?;

        let has_more = sensor_rows.len() > effective_limit;
        let mut last_sensor_id = None;
        let mut sensors = Vec::with_capacity(sensor_rows.len().min(effective_limit));

        for sensor_row in sensor_rows.into_iter().take(effective_limit) {
            // Parse sensor metadata with improved error handling
            let sensor_uuid = sensor_row
                .uuid
                .ok_or_else(|| {
                    crate::storage::StorageError::missing_field(
                        "UUID",
                        None,
                        sensor_row.name.as_deref(),
                    )
                })
                .map_err(anyhow::Error::from)?;

            let sensor_name = sensor_row
                .name
                .ok_or_else(|| {
                    crate::storage::StorageError::missing_field("name", Some(sensor_uuid), None)
                })
                .map_err(anyhow::Error::from)?;

            let sensor_type_str = sensor_row
                .r#type
                .ok_or_else(|| {
                    crate::storage::StorageError::missing_field(
                        "type",
                        Some(sensor_uuid),
                        Some(&sensor_name),
                    )
                })
                .map_err(anyhow::Error::from)?;

            let sensor_type =
                crate::datamodel::SensorType::from_str(&sensor_type_str).map_err(|e| {
                    anyhow::Error::from(crate::storage::StorageError::invalid_data_format(
                        &format!("Failed to parse sensor type '{}': {}", sensor_type_str, e),
                        Some(sensor_uuid),
                        Some(&sensor_name),
                    ))
                })?;

            let unit = match (sensor_row.unit_name, sensor_row.unit_description) {
                (Some(name), description) => {
                    Some(crate::datamodel::unit::Unit::new(name, description))
                }
                _ => None,
            };

            // Query labels for this sensor with proper error context
            let sensor_id = sensor_row.sensor_id.ok_or_else(|| {
                anyhow::Error::from(crate::storage::StorageError::missing_field(
                    "sensor_id",
                    Some(sensor_uuid),
                    Some(&sensor_name),
                ))
            })?;
            last_sensor_id = Some(sensor_id);

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

            let mut labels: crate::datamodel::sensapp_vec::SensAppLabels = smallvec::smallvec![];
            for label_row in labels_rows {
                labels.push((label_row.label_name, label_row.label_value));
            }

            let sensor = crate::datamodel::Sensor::new(
                sensor_uuid,
                sensor_name,
                sensor_type,
                unit,
                Some(labels),
            );

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
                anyhow::Error::from(crate::storage::StorageError::missing_field(
                    "metric_name",
                    None,
                    None,
                ))
            })?;

            let sensor_type_str = metrics_row.r#type.ok_or_else(|| {
                anyhow::Error::from(crate::storage::StorageError::missing_field(
                    "type",
                    None,
                    Some(&metric_name),
                ))
            })?;

            let sensor_type =
                crate::datamodel::SensorType::from_str(&sensor_type_str).map_err(|e| {
                    anyhow::Error::from(crate::storage::StorageError::invalid_data_format(
                        &format!("Failed to parse sensor type '{}': {}", sensor_type_str, e),
                        None,
                        Some(&metric_name),
                    ))
                })?;

            let unit = match (metrics_row.unit_name, metrics_row.unit_description) {
                (Some(name), description) => {
                    Some(crate::datamodel::unit::Unit::new(name, description))
                }
                _ => None,
            };

            let series_count = metrics_row.series_count.ok_or_else(|| {
                anyhow::Error::from(crate::storage::StorageError::missing_field(
                    "series_count",
                    None,
                    Some(&metric_name),
                ))
            })?;

            // Handle optional label_keys array
            let label_keys = metrics_row.label_keys.unwrap_or_default();

            let metric = crate::datamodel::Metric::new(
                metric_name,
                sensor_type,
                unit,
                series_count,
                label_keys,
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
    ) -> Result<Option<SensorData>> {
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
            None => return Ok(None),
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
                let origin_us = start_time_us.unwrap_or(0);

                let samples = match sensor.sensor_type {
                    SensorType::Integer => {
                        self.query_integer_samples_aggregated(
                            sensor_id,
                            start_time_us,
                            end_time_us,
                            step_ms,
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
                            step_ms,
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
                            step_ms,
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

        let samples = match sensor.sensor_type {
            SensorType::Integer => {
                self.query_integer_samples(
                    sensor_id,
                    Some(latest_timestamp_us),
                    Some(latest_timestamp_us),
                    Some(1),
                )
                .await?
            }
            SensorType::Numeric => {
                self.query_numeric_samples(
                    sensor_id,
                    Some(latest_timestamp_us),
                    Some(latest_timestamp_us),
                    Some(1),
                )
                .await?
            }
            SensorType::Float => {
                self.query_float_samples(
                    sensor_id,
                    Some(latest_timestamp_us),
                    Some(latest_timestamp_us),
                    Some(1),
                )
                .await?
            }
            SensorType::String => {
                self.query_string_samples(
                    sensor_id,
                    Some(latest_timestamp_us),
                    Some(latest_timestamp_us),
                    Some(1),
                )
                .await?
            }
            SensorType::Boolean => {
                self.query_boolean_samples(
                    sensor_id,
                    Some(latest_timestamp_us),
                    Some(latest_timestamp_us),
                    Some(1),
                )
                .await?
            }
            SensorType::Location => {
                self.query_location_samples(
                    sensor_id,
                    Some(latest_timestamp_us),
                    Some(latest_timestamp_us),
                    Some(1),
                )
                .await?
            }
            SensorType::Json => {
                self.query_json_samples(
                    sensor_id,
                    Some(latest_timestamp_us),
                    Some(latest_timestamp_us),
                    Some(1),
                )
                .await?
            }
            SensorType::Blob => {
                self.query_blob_samples(
                    sensor_id,
                    Some(latest_timestamp_us),
                    Some(latest_timestamp_us),
                    Some(1),
                )
                .await?
            }
        };

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
                start_time,
                end_time,
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

        let start_time_us = start_time.as_ref().map(datetime_to_micros);
        let end_time_us = end_time.as_ref().map(datetime_to_micros);

        let mut results = Vec::with_capacity(sensors.len());
        for (sensor_id, sensor) in sensors {
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

            results.push(SensorData::new(sensor, samples));
        }

        Ok(results)
    }

    /// Health check for TimescaleDB storage
    /// Executes a simple SELECT 1 query to verify database connectivity
    async fn health_check(&self) -> Result<()> {
        sqlx::query("SELECT 1")
            .execute(&self.pool)
            .await
            .context("TimescaleDB health check failed")?;
        Ok(())
    }

    /// Clean up all test data from the database (TimescaleDB implementation)
    #[cfg(any(test, feature = "test-utils"))]
    async fn cleanup_test_data(&self) -> Result<()> {
        // TimescaleDB is PostgreSQL-based, so we use similar approach as PostgreSQL
        let mut tx = self.pool.begin().await?;

        // Delete all value tables in dependency order
        sqlx::query("DELETE FROM blob_values")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM json_values")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM location_values")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM boolean_values")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM string_values")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM float_values")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM numeric_values")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM integer_values")
            .execute(&mut *tx)
            .await?;

        // Delete metadata tables
        sqlx::query("DELETE FROM labels").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM sensors").execute(&mut *tx).await?;
        sqlx::query("DELETE FROM strings_values_dictionary")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM labels_description_dictionary")
            .execute(&mut *tx)
            .await?;
        sqlx::query("DELETE FROM labels_name_dictionary")
            .execute(&mut *tx)
            .await?;

        // Preserve units and ensure common test units exist
        sqlx::query("INSERT INTO units (name, description) VALUES ('°C', 'Celsius') ON CONFLICT (name) DO NOTHING")
            .execute(&mut *tx).await?;
        sqlx::query("INSERT INTO units (name, description) VALUES ('%', 'Percentage') ON CONFLICT (name) DO NOTHING")
            .execute(&mut *tx).await?;

        tx.commit()
            .await
            .context("Failed to commit test data cleanup transaction")?;

        // Step 5: Clear all cached function caches
        // The cached macro generates cache variables named after the function in uppercase
        use cached::Cached;
        timescaledb_utilities::GET_LABEL_NAME_ID_OR_CREATE
            .lock()
            .await
            .cache_clear();
        timescaledb_utilities::GET_LABEL_DESCRIPTION_ID_OR_CREATE
            .lock()
            .await
            .cache_clear();
        timescaledb_utilities::GET_UNIT_ID_OR_CREATE
            .lock()
            .await
            .cache_clear();
        timescaledb_utilities::GET_SENSOR_ID_OR_CREATE_SENSOR
            .lock()
            .await
            .cache_clear();
        timescaledb_utilities::GET_STRING_VALUE_ID_OR_CREATE
            .lock()
            .await
            .cache_clear();

        Ok(())
    }
}

impl TimeScaleDBStorage {
    async fn publish_single_sensor_batch(
        &self,
        transaction: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        single_sensor_batch: &crate::datamodel::batch::SingleSensorBatch,
    ) -> Result<()> {
        let sensor_id =
            get_sensor_id_or_create_sensor(transaction, &single_sensor_batch.sensor).await?;

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

    async fn vacuum(&self) -> Result<()> {
        sqlx::query("VACUUM")
            .execute(&self.pool)
            .await
            .context("Failed to vacuum database")?;

        Ok(())
    }

    async fn query_integer_samples_aggregated(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        step_ms: i64,
        origin_us: i64,
        aggregation: Aggregation,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        let start_time_ts = start_time.map(micros_to_offset_datetime);
        let end_time_ts = end_time.map(micros_to_offset_datetime);
        let limit = limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64;

        match aggregation {
            Aggregation::Avg => {
                #[derive(sqlx::FromRow)]
                struct Row {
                    bucket_time: sqlx::types::time::OffsetDateTime,
                    value: f64,
                }

                let rows: Vec<Row> = sqlx::query_as(&format!(
                    "{} SELECT bucket_time, AVG(value)::double precision AS value {}",
                    timescaledb_bucketed_cte("integer_values"),
                    timescaledb_group_by_clause()
                ))
                .bind(sensor_id)
                .bind(start_time_ts)
                .bind(end_time_ts)
                .bind(step_ms)
                .bind(origin_us as f64)
                .bind(limit)
                .fetch_all(&self.pool)
                .await?;

                let mut samples = smallvec![];
                for row in rows {
                    samples.push(Sample {
                        datetime: offset_datetime_to_sensapp(row.bucket_time),
                        value: row.value,
                    });
                }
                Ok(TypedSamples::Float(samples))
            }
            Aggregation::Count => {
                #[derive(sqlx::FromRow)]
                struct Row {
                    bucket_time: sqlx::types::time::OffsetDateTime,
                    value: i64,
                }

                let rows: Vec<Row> = sqlx::query_as(&format!(
                    "{} SELECT bucket_time, COUNT(*)::bigint AS value {}",
                    timescaledb_bucketed_cte("integer_values"),
                    timescaledb_group_by_clause()
                ))
                .bind(sensor_id)
                .bind(start_time_ts)
                .bind(end_time_ts)
                .bind(step_ms)
                .bind(origin_us as f64)
                .bind(limit)
                .fetch_all(&self.pool)
                .await?;

                let mut samples = smallvec![];
                for row in rows {
                    samples.push(Sample {
                        datetime: offset_datetime_to_sensapp(row.bucket_time),
                        value: row.value,
                    });
                }
                Ok(TypedSamples::Integer(samples))
            }
            _ => {
                #[derive(sqlx::FromRow)]
                struct Row {
                    bucket_time: sqlx::types::time::OffsetDateTime,
                    value: i64,
                }

                let rows: Vec<Row> = sqlx::query_as(&format!(
                    "{} SELECT bucket_time, {} AS value {}",
                    timescaledb_bucketed_cte("integer_values"),
                    timescaledb_integer_expression(aggregation),
                    timescaledb_group_by_clause()
                ))
                .bind(sensor_id)
                .bind(start_time_ts)
                .bind(end_time_ts)
                .bind(step_ms)
                .bind(origin_us as f64)
                .bind(limit)
                .fetch_all(&self.pool)
                .await?;

                let mut samples = smallvec![];
                for row in rows {
                    samples.push(Sample {
                        datetime: offset_datetime_to_sensapp(row.bucket_time),
                        value: row.value,
                    });
                }
                Ok(TypedSamples::Integer(samples))
            }
        }
    }

    async fn query_float_samples_aggregated(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        step_ms: i64,
        origin_us: i64,
        aggregation: Aggregation,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        let start_time_ts = start_time.map(micros_to_offset_datetime);
        let end_time_ts = end_time.map(micros_to_offset_datetime);
        let limit = limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64;

        match aggregation {
            Aggregation::Count => {
                #[derive(sqlx::FromRow)]
                struct Row {
                    bucket_time: sqlx::types::time::OffsetDateTime,
                    value: i64,
                }

                let rows: Vec<Row> = sqlx::query_as(&format!(
                    "{} SELECT bucket_time, COUNT(*)::bigint AS value {}",
                    timescaledb_bucketed_cte("float_values"),
                    timescaledb_group_by_clause()
                ))
                .bind(sensor_id)
                .bind(start_time_ts)
                .bind(end_time_ts)
                .bind(step_ms)
                .bind(origin_us as f64)
                .bind(limit)
                .fetch_all(&self.pool)
                .await?;

                let mut samples = smallvec![];
                for row in rows {
                    samples.push(Sample {
                        datetime: offset_datetime_to_sensapp(row.bucket_time),
                        value: row.value,
                    });
                }
                Ok(TypedSamples::Integer(samples))
            }
            _ => {
                #[derive(sqlx::FromRow)]
                struct Row {
                    bucket_time: sqlx::types::time::OffsetDateTime,
                    value: f64,
                }

                let rows: Vec<Row> = sqlx::query_as(&format!(
                    "{} SELECT bucket_time, {} AS value {}",
                    timescaledb_bucketed_cte("float_values"),
                    timescaledb_float_expression(aggregation),
                    timescaledb_group_by_clause()
                ))
                .bind(sensor_id)
                .bind(start_time_ts)
                .bind(end_time_ts)
                .bind(step_ms)
                .bind(origin_us as f64)
                .bind(limit)
                .fetch_all(&self.pool)
                .await?;

                let mut samples = smallvec![];
                for row in rows {
                    samples.push(Sample {
                        datetime: offset_datetime_to_sensapp(row.bucket_time),
                        value: row.value,
                    });
                }
                Ok(TypedSamples::Float(samples))
            }
        }
    }

    async fn query_numeric_samples_aggregated(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        step_ms: i64,
        origin_us: i64,
        aggregation: Aggregation,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        let start_time_ts = start_time.map(micros_to_offset_datetime);
        let end_time_ts = end_time.map(micros_to_offset_datetime);
        let limit = limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64;

        match aggregation {
            Aggregation::Count => {
                #[derive(sqlx::FromRow)]
                struct Row {
                    bucket_time: sqlx::types::time::OffsetDateTime,
                    value: i64,
                }

                let rows: Vec<Row> = sqlx::query_as(&format!(
                    "{} SELECT bucket_time, COUNT(*)::bigint AS value {}",
                    timescaledb_bucketed_cte("numeric_values"),
                    timescaledb_group_by_clause()
                ))
                .bind(sensor_id)
                .bind(start_time_ts)
                .bind(end_time_ts)
                .bind(step_ms)
                .bind(origin_us as f64)
                .bind(limit)
                .fetch_all(&self.pool)
                .await?;

                let mut samples = smallvec![];
                for row in rows {
                    samples.push(Sample {
                        datetime: offset_datetime_to_sensapp(row.bucket_time),
                        value: row.value,
                    });
                }
                Ok(TypedSamples::Integer(samples))
            }
            _ => {
                #[derive(sqlx::FromRow)]
                struct Row {
                    bucket_time: sqlx::types::time::OffsetDateTime,
                    value: rust_decimal::Decimal,
                }

                let rows: Vec<Row> = sqlx::query_as(&format!(
                    "{} SELECT bucket_time, {} AS value {}",
                    timescaledb_bucketed_cte("numeric_values"),
                    timescaledb_numeric_expression(aggregation),
                    timescaledb_group_by_clause()
                ))
                .bind(sensor_id)
                .bind(start_time_ts)
                .bind(end_time_ts)
                .bind(step_ms)
                .bind(origin_us as f64)
                .bind(limit)
                .fetch_all(&self.pool)
                .await?;

                let mut samples = smallvec![];
                for row in rows {
                    samples.push(Sample {
                        datetime: offset_datetime_to_sensapp(row.bucket_time),
                        value: row.value,
                    });
                }
                Ok(TypedSamples::Numeric(samples))
            }
        }
    }

    async fn query_integer_samples(
        &self,
        sensor_id: i64,
        start_time: Option<i64>,
        end_time: Option<i64>,
        limit: Option<usize>,
    ) -> Result<TypedSamples> {
        use crate::datamodel::Sample;
        use smallvec::smallvec;

        #[derive(sqlx::FromRow)]
        struct IntegerValueRow {
            time: sqlx::types::time::OffsetDateTime,
            value: i64,
        }

        // Convert microsecond timestamps to OffsetDateTime for TimescaleDB queries
        let start_time_ts = start_time.map(micros_to_offset_datetime);
        let end_time_ts = end_time.map(micros_to_offset_datetime);

        let rows: Vec<IntegerValueRow> = sqlx::query_as(
            r#"
            SELECT time, value FROM integer_values
            WHERE sensor_id = $1
            AND ($2::TIMESTAMPTZ IS NULL OR time >= $2)
            AND ($3::TIMESTAMPTZ IS NULL OR time <= $3)
            ORDER BY time ASC
            LIMIT $4
            "#,
        )
        .bind(sensor_id)
        .bind(start_time_ts)
        .bind(end_time_ts)
        .bind(limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = offset_datetime_to_sensapp(row.time);
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
        use crate::datamodel::{Sample, SensAppDateTime};
        use smallvec::smallvec;

        #[derive(sqlx::FromRow)]
        struct NumericValueRow {
            time: sqlx::types::time::OffsetDateTime,
            value: rust_decimal::Decimal,
        }

        // Convert microsecond timestamps to OffsetDateTime for TimescaleDB queries
        let start_time_ts = start_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });
        let end_time_ts = end_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });

        let rows: Vec<NumericValueRow> = sqlx::query_as(
            r#"
            SELECT time, value FROM numeric_values
            WHERE sensor_id = $1
            AND ($2::TIMESTAMPTZ IS NULL OR time >= $2)
            AND ($3::TIMESTAMPTZ IS NULL OR time <= $3)
            ORDER BY time ASC
            LIMIT $4
            "#,
        )
        .bind(sensor_id)
        .bind(start_time_ts)
        .bind(end_time_ts)
        .bind(limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            // Convert OffsetDateTime back to SensAppDateTime
            let unix_timestamp =
                row.time.unix_timestamp() as f64 + (row.time.nanosecond() as f64 / 1_000_000_000.0);
            let datetime = SensAppDateTime::from_unix_seconds(unix_timestamp);
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
        use crate::datamodel::Sample;
        use smallvec::smallvec;

        #[derive(sqlx::FromRow)]
        struct FloatValueRow {
            time: sqlx::types::time::OffsetDateTime,
            value: f64,
        }

        // Convert microsecond timestamps to OffsetDateTime for TimescaleDB queries
        let start_time_ts = start_time.map(micros_to_offset_datetime);
        let end_time_ts = end_time.map(micros_to_offset_datetime);

        let rows: Vec<FloatValueRow> = sqlx::query_as(
            r#"
            SELECT time, value FROM float_values
            WHERE sensor_id = $1
            AND ($2::TIMESTAMPTZ IS NULL OR time >= $2)
            AND ($3::TIMESTAMPTZ IS NULL OR time <= $3)
            ORDER BY time ASC
            LIMIT $4
            "#,
        )
        .bind(sensor_id)
        .bind(start_time_ts)
        .bind(end_time_ts)
        .bind(limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            let datetime = offset_datetime_to_sensapp(row.time);
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
        use crate::datamodel::{Sample, SensAppDateTime};
        use smallvec::smallvec;

        #[derive(sqlx::FromRow)]
        struct StringValueRow {
            time: sqlx::types::time::OffsetDateTime,
            string_value: String,
        }

        // Convert microsecond timestamps to OffsetDateTime for TimescaleDB queries
        let start_time_ts = start_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });
        let end_time_ts = end_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });

        let rows: Vec<StringValueRow> = sqlx::query_as(
            r#"
            SELECT sv.time, svd.value as string_value
            FROM string_values sv
            JOIN strings_values_dictionary svd ON sv.value = svd.id
            WHERE sv.sensor_id = $1
            AND ($2::TIMESTAMPTZ IS NULL OR sv.time >= $2)
            AND ($3::TIMESTAMPTZ IS NULL OR sv.time <= $3)
            ORDER BY sv.time ASC
            LIMIT $4
            "#,
        )
        .bind(sensor_id)
        .bind(start_time_ts)
        .bind(end_time_ts)
        .bind(limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            // Convert OffsetDateTime back to SensAppDateTime
            let unix_timestamp =
                row.time.unix_timestamp() as f64 + (row.time.nanosecond() as f64 / 1_000_000_000.0);
            let datetime = SensAppDateTime::from_unix_seconds(unix_timestamp);
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
        use crate::datamodel::{Sample, SensAppDateTime};
        use smallvec::smallvec;

        #[derive(sqlx::FromRow)]
        struct BooleanValueRow {
            time: sqlx::types::time::OffsetDateTime,
            value: bool,
        }

        // Convert microsecond timestamps to OffsetDateTime for TimescaleDB queries
        let start_time_ts = start_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });
        let end_time_ts = end_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });

        let rows: Vec<BooleanValueRow> = sqlx::query_as(
            r#"
            SELECT time, value FROM boolean_values
            WHERE sensor_id = $1
            AND ($2::TIMESTAMPTZ IS NULL OR time >= $2)
            AND ($3::TIMESTAMPTZ IS NULL OR time <= $3)
            ORDER BY time ASC
            LIMIT $4
            "#,
        )
        .bind(sensor_id)
        .bind(start_time_ts)
        .bind(end_time_ts)
        .bind(limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            // Convert OffsetDateTime back to SensAppDateTime
            let unix_timestamp =
                row.time.unix_timestamp() as f64 + (row.time.nanosecond() as f64 / 1_000_000_000.0);
            let datetime = SensAppDateTime::from_unix_seconds(unix_timestamp);
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
        use crate::datamodel::{Sample, SensAppDateTime};
        use smallvec::smallvec;

        #[derive(sqlx::FromRow)]
        struct LocationValueRow {
            time: sqlx::types::time::OffsetDateTime,
            latitude: f64,
            longitude: f64,
        }

        // Convert microsecond timestamps to OffsetDateTime for TimescaleDB queries
        let start_time_ts = start_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });
        let end_time_ts = end_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });

        let rows: Vec<LocationValueRow> = sqlx::query_as(
            r#"
            SELECT time, latitude, longitude FROM location_values
            WHERE sensor_id = $1
            AND ($2::TIMESTAMPTZ IS NULL OR time >= $2)
            AND ($3::TIMESTAMPTZ IS NULL OR time <= $3)
            ORDER BY time ASC
            LIMIT $4
            "#,
        )
        .bind(sensor_id)
        .bind(start_time_ts)
        .bind(end_time_ts)
        .bind(limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            // Convert OffsetDateTime back to SensAppDateTime
            let unix_timestamp =
                row.time.unix_timestamp() as f64 + (row.time.nanosecond() as f64 / 1_000_000_000.0);
            let datetime = SensAppDateTime::from_unix_seconds(unix_timestamp);
            let value = geo::Point::new(row.longitude, row.latitude);
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
        use crate::datamodel::{Sample, SensAppDateTime};
        use smallvec::smallvec;

        #[derive(sqlx::FromRow)]
        struct JsonValueRow {
            time: sqlx::types::time::OffsetDateTime,
            value: serde_json::Value,
        }

        // Convert microsecond timestamps to OffsetDateTime for TimescaleDB queries
        let start_time_ts = start_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });
        let end_time_ts = end_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });

        let rows: Vec<JsonValueRow> = sqlx::query_as(
            r#"
            SELECT time, value FROM json_values
            WHERE sensor_id = $1
            AND ($2::TIMESTAMPTZ IS NULL OR time >= $2)
            AND ($3::TIMESTAMPTZ IS NULL OR time <= $3)
            ORDER BY time ASC
            LIMIT $4
            "#,
        )
        .bind(sensor_id)
        .bind(start_time_ts)
        .bind(end_time_ts)
        .bind(limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            // Convert OffsetDateTime back to SensAppDateTime
            let unix_timestamp =
                row.time.unix_timestamp() as f64 + (row.time.nanosecond() as f64 / 1_000_000_000.0);
            let datetime = SensAppDateTime::from_unix_seconds(unix_timestamp);
            let value: serde_json::Value = row.value;
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
        use crate::datamodel::{Sample, SensAppDateTime};
        use smallvec::smallvec;

        #[derive(sqlx::FromRow)]
        struct BlobValueRow {
            time: sqlx::types::time::OffsetDateTime,
            value: Vec<u8>,
        }

        // Convert microsecond timestamps to OffsetDateTime for TimescaleDB queries
        let start_time_ts = start_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });
        let end_time_ts = end_time.map(|t| {
            sqlx::types::time::OffsetDateTime::from_unix_timestamp_nanos((t * 1000) as i128)
                .unwrap_or(sqlx::types::time::OffsetDateTime::UNIX_EPOCH)
        });

        let rows: Vec<BlobValueRow> = sqlx::query_as(
            r#"
            SELECT time, value FROM blob_values
            WHERE sensor_id = $1
            AND ($2::TIMESTAMPTZ IS NULL OR time >= $2)
            AND ($3::TIMESTAMPTZ IS NULL OR time <= $3)
            ORDER BY time ASC
            LIMIT $4
            "#,
        )
        .bind(sensor_id)
        .bind(start_time_ts)
        .bind(end_time_ts)
        .bind(limit.unwrap_or(DEFAULT_QUERY_LIMIT) as i64)
        .fetch_all(&self.pool)
        .await?;

        let mut samples = smallvec![];
        for row in rows {
            // Convert OffsetDateTime back to SensAppDateTime
            let unix_timestamp =
                row.time.unix_timestamp() as f64 + (row.time.nanosecond() as f64 / 1_000_000_000.0);
            let datetime = SensAppDateTime::from_unix_seconds(unix_timestamp);
            let value = row.value;
            samples.push(Sample { datetime, value });
        }

        Ok(TypedSamples::Blob(samples))
    }
}
