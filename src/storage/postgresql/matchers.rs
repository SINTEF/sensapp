//! Label matcher query building for PostgreSQL storage.
//!
//! This module contains the logic for finding sensors that match
//! Prometheus-style label matchers (equal, not-equal, regex, regex-not).

use super::{PostgresStorage, StorageError};
use crate::datamodel::sensapp_vec::SensAppLabels;
use crate::datamodel::unit::Unit;
use crate::datamodel::{Sensor, SensorType};
use crate::storage::{LabelMatcher, MatcherType};
use anyhow::Result;
use smallvec::smallvec;
use std::str::FromStr;
use uuid::Uuid;

impl PostgresStorage {
    /// Find sensors matching the given name and label matchers, returning full metadata.
    ///
    /// This builds a dynamic SQL query to find sensors based on:
    /// - Name matchers: filter on sensors.name column
    /// - Label matchers: filter on labels table with dictionary joins
    /// - numeric_only: if true, only return sensors with numeric types (Integer, Numeric, Float)
    ///
    /// Returns a vector of (sensor_id, Sensor) tuples with full metadata and labels.
    /// This is optimized to fetch all sensor metadata and labels in just two queries
    /// instead of N+1 queries.
    pub(super) async fn find_sensors_by_matchers(
        &self,
        name_matchers: &[&LabelMatcher],
        label_matchers: &[&LabelMatcher],
        numeric_only: bool,
    ) -> Result<Vec<(i64, Sensor)>> {
        // Build the base query to find matching sensor IDs with their metadata
        let mut sql = String::from(
            r#"SELECT DISTINCT s.sensor_id, s.uuid, s.name, s.type,
                      u.name as unit_name, u.description as unit_description
               FROM sensors s
               LEFT JOIN units u ON s.unit = u.id"#,
        );
        let mut where_clauses: Vec<String> = Vec::new();
        let mut params: Vec<String> = Vec::new();
        let mut param_idx = 1;

        // Filter by numeric types if requested (for Prometheus compatibility)
        if numeric_only {
            where_clauses.push("s.type IN ('Integer', 'Numeric', 'Float')".to_string());
        }

        // Handle name matchers (__name__ -> sensors.name)
        for matcher in name_matchers {
            let clause = match matcher.matcher_type {
                MatcherType::Equal => {
                    params.push(matcher.value.clone());
                    let clause = format!("s.name = ${}", param_idx);
                    param_idx += 1;
                    clause
                }
                MatcherType::NotEqual => {
                    params.push(matcher.value.clone());
                    let clause = format!("s.name != ${}", param_idx);
                    param_idx += 1;
                    clause
                }
                MatcherType::RegexMatch => {
                    params.push(matcher.value.clone());
                    let clause = format!("s.name ~ ${}", param_idx);
                    param_idx += 1;
                    clause
                }
                MatcherType::RegexNotMatch => {
                    params.push(matcher.value.clone());
                    let clause = format!("s.name !~ ${}", param_idx);
                    param_idx += 1;
                    clause
                }
            };
            where_clauses.push(clause);
        }

        // Handle label matchers using subqueries
        for matcher in label_matchers {
            let subquery = match matcher.matcher_type {
                MatcherType::Equal => {
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
                MatcherType::NotEqual => {
                    params.push(matcher.name.clone());
                    params.push(matcher.value.clone());
                    // NOT IN - matches sensors that don't have this label=value combo
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
                MatcherType::RegexMatch => {
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
                MatcherType::RegexNotMatch => {
                    params.push(matcher.name.clone());
                    params.push(matcher.value.clone());
                    // NOT IN with regex
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

        // Build the final query
        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }
        sql.push_str(" ORDER BY s.sensor_id");

        // Execute the dynamic query to get sensor metadata
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

        // Collect sensor IDs for the labels query
        let sensor_ids: Vec<i64> = sensor_rows.iter().map(|r| r.sensor_id).collect();

        // Fetch all labels for all matching sensors in a single query
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

        // Group labels by sensor_id
        let mut labels_map: std::collections::HashMap<i64, SensAppLabels> =
            std::collections::HashMap::new();
        for label_row in labels_rows {
            labels_map
                .entry(label_row.sensor_id)
                .or_insert_with(|| smallvec![])
                .push((label_row.label_name, label_row.label_value));
        }

        // Build the result with full Sensor objects
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
}
