use super::ClickHouseStorage;
use super::clickhouse_utilities::map_clickhouse_error;
use crate::datamodel::sensapp_vec::SensAppLabels;
use crate::datamodel::unit::Unit;
use crate::datamodel::{Sensor, SensorType};
use crate::storage::{LabelMatcher, MatcherType, StorageError};
use anyhow::Result;
use std::collections::HashMap;
use std::str::FromStr;
use uuid::Uuid;

/// Sensor ids sent to ClickHouse in one query: keeps the statement a reasonable size.
const LABEL_LOOKUP_CHUNK: usize = 2000;

impl ClickHouseStorage {
    /// The labels of many sensors, with one query per chunk of ids instead of one per sensor.
    pub(super) async fn labels_of_sensors(
        &self,
        sensor_ids: &[u64],
    ) -> Result<HashMap<u64, SensAppLabels>> {
        #[derive(clickhouse::Row, serde::Deserialize)]
        struct LabelRow {
            sensor_id: u64,
            name: String,
            description: String,
        }

        let mut labels: HashMap<u64, SensAppLabels> = HashMap::new();
        for chunk in sensor_ids.chunks(LABEL_LOOKUP_CHUNK) {
            let mut cursor = self
                .client
                .query(
                    "SELECT sensor_id, name, COALESCE(description, '') AS description \
                     FROM labels FINAL WHERE has(?, sensor_id) ORDER BY sensor_id, name",
                )
                .bind(chunk)
                .fetch::<LabelRow>()
                .map_err(|e| map_clickhouse_error(e, None, None))?;
            while let Some(row) = cursor.next().await? {
                labels
                    .entry(row.sensor_id)
                    .or_default()
                    .push((row.name, row.description));
            }
        }
        Ok(labels)
    }

    pub(super) async fn find_sensors_by_matchers(
        &self,
        name_matchers: &[&LabelMatcher],
        label_matchers: &[&LabelMatcher],
        numeric_only: bool,
        limit: Option<usize>,
    ) -> Result<Vec<(u64, Sensor)>> {
        let mut sql = String::from(
            r#"SELECT DISTINCT s.sensor_id, s.uuid, s.name, s.type,
                      COALESCE(u.name, '') AS unit_name,
                      COALESCE(u.description, '') AS unit_description
               FROM sensors s FINAL
               LEFT JOIN units u FINAL ON s.unit = u.id"#,
        );
        let mut where_clauses: Vec<String> = Vec::new();
        let mut params: Vec<String> = Vec::new();

        if numeric_only {
            where_clauses.push("s.type IN ('Integer', 'Numeric', 'Float')".to_string());
        }

        for matcher in name_matchers {
            let clause = match matcher.matcher_type {
                MatcherType::Equal => {
                    params.push(matcher.value.clone());
                    "s.name = ?".to_string()
                }
                MatcherType::NotEqual => {
                    params.push(matcher.value.clone());
                    "s.name != ?".to_string()
                }
                MatcherType::RegexMatch => {
                    params.push(matcher.value.clone());
                    "match(s.name, ?)".to_string()
                }
                MatcherType::RegexNotMatch => {
                    params.push(matcher.value.clone());
                    "NOT match(s.name, ?)".to_string()
                }
            };
            where_clauses.push(clause);
        }

        for matcher in label_matchers {
            let subquery = match matcher.matcher_type {
                MatcherType::Equal => {
                    params.push(matcher.name.clone());
                    params.push(matcher.value.clone());
                    r#"s.sensor_id IN (
                        SELECT l.sensor_id FROM labels l
                        WHERE l.name = ? AND COALESCE(l.description, '') = ?
                    )"#
                    .to_string()
                }
                MatcherType::NotEqual => {
                    params.push(matcher.name.clone());
                    params.push(matcher.value.clone());
                    r#"s.sensor_id NOT IN (
                        SELECT l.sensor_id FROM labels l
                        WHERE l.name = ? AND COALESCE(l.description, '') = ?
                    )"#
                    .to_string()
                }
                MatcherType::RegexMatch => {
                    params.push(matcher.name.clone());
                    params.push(matcher.value.clone());
                    r#"s.sensor_id IN (
                        SELECT l.sensor_id FROM labels l
                        WHERE l.name = ? AND match(COALESCE(l.description, ''), ?)
                    )"#
                    .to_string()
                }
                MatcherType::RegexNotMatch => {
                    params.push(matcher.name.clone());
                    params.push(matcher.value.clone());
                    r#"s.sensor_id NOT IN (
                        SELECT l.sensor_id FROM labels l
                        WHERE l.name = ? AND match(COALESCE(l.description, ''), ?)
                    )"#
                    .to_string()
                }
            };
            where_clauses.push(subquery);
        }

        if !where_clauses.is_empty() {
            sql.push_str(" WHERE ");
            sql.push_str(&where_clauses.join(" AND "));
        }
        sql.push_str(" ORDER BY s.sensor_id ASC");
        if let Some(limit) = limit {
            // A number, never text from a caller
            sql.push_str(&format!(" LIMIT {limit}"));
        }

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

        let mut query = self.client.query(&sql);
        for param in &params {
            query = query.bind(param.as_str());
        }

        let mut cursor = query
            .fetch::<SensorRow>()
            .map_err(|e| StorageError::invalid_data_format(&e.to_string(), None, None))?;

        let mut sensor_rows = Vec::new();
        while let Some(row) = cursor.next().await? {
            sensor_rows.push(row);
        }

        if sensor_rows.is_empty() {
            return Ok(Vec::new());
        }

        let sensor_ids: Vec<u64> = sensor_rows.iter().map(|row| row.sensor_id).collect();
        let mut labels_map = self.labels_of_sensors(&sensor_ids).await?;

        let mut results = Vec::with_capacity(sensor_rows.len());
        for row in sensor_rows {
            let sensor_type = SensorType::from_str(&row.r#type).map_err(|e| {
                anyhow::Error::from(StorageError::invalid_data_format(
                    &format!("Failed to parse sensor type '{}': {}", row.r#type, e),
                    Some(row.uuid),
                    Some(&row.name),
                ))
            })?;

            let unit = if row.unit_name.is_empty() {
                None
            } else {
                Some(Unit::new(
                    row.unit_name,
                    if row.unit_description.is_empty() {
                        None
                    } else {
                        Some(row.unit_description)
                    },
                ))
            };

            let labels = labels_map.remove(&row.sensor_id).unwrap_or_default();
            let sensor = Sensor::new(row.uuid, row.name, sensor_type, unit, Some(labels));
            results.push((row.sensor_id, sensor));
        }

        Ok(results)
    }
}
