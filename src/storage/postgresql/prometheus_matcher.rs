use crate::parsing::prometheus::remote_read_models::{LabelMatcher, label_matcher};
use anyhow::{Context, Result};
use sqlx::{PgPool, Row};
use std::collections::HashSet;
use tracing::debug;

/// Converts Prometheus label matchers to SQL queries for finding matching sensors.
/// All queries use parameterized statements to prevent SQL injection.
pub struct PrometheusMatcher {
    pool: PgPool,
}

impl PrometheusMatcher {
    pub fn new(pool: PgPool) -> Self {
        Self { pool }
    }

    /// Find all sensor IDs that match the given label matchers.
    /// Uses SQL parameters to prevent injection attacks.
    pub async fn find_matching_sensors(&self, matchers: &[LabelMatcher]) -> Result<Vec<i64>> {
        if matchers.is_empty() {
            // If no matchers, return all sensors
            let rows = sqlx::query("SELECT DISTINCT sensor_id FROM sensors")
                .fetch_all(&self.pool)
                .await
                .context("Failed to fetch all sensors")?;

            return Ok(rows
                .into_iter()
                .map(|row| row.get::<i64, _>("sensor_id"))
                .collect());
        }

        // Build the query for each matcher
        let mut sensor_ids: Option<HashSet<i64>> = None;

        for matcher in matchers {
            let matching_sensors =
                self.find_sensors_for_matcher(matcher)
                    .await
                    .with_context(|| {
                        format!(
                            "Failed to process matcher {}={}",
                            matcher.name, matcher.value
                        )
                    })?;

            match &mut sensor_ids {
                None => {
                    // First matcher - initialize the set
                    sensor_ids = Some(matching_sensors.into_iter().collect());
                }
                Some(existing) => {
                    // Subsequent matchers - intersect with existing results
                    let new_set: HashSet<i64> = matching_sensors.into_iter().collect();
                    existing.retain(|id| new_set.contains(id));
                }
            }

            // If no sensors match, we can short-circuit
            if sensor_ids.as_ref().is_some_and(|s| s.is_empty()) {
                return Ok(Vec::new());
            }
        }

        Ok(sensor_ids.unwrap_or_default().into_iter().collect())
    }

    /// Find sensors matching a single matcher.
    /// Uses parameterized queries to prevent SQL injection.
    async fn find_sensors_for_matcher(&self, matcher: &LabelMatcher) -> Result<Vec<i64>> {
        let matcher_type = label_matcher::Type::try_from(matcher.r#type)
            .map_err(|_| anyhow::anyhow!("Invalid matcher type: {}", matcher.r#type))?;

        debug!(
            "Processing matcher: name='{}', value='{}', type={:?}",
            matcher.name, matcher.value, matcher_type
        );

        match matcher_type {
            label_matcher::Type::Eq => self.find_sensors_eq(&matcher.name, &matcher.value).await,
            label_matcher::Type::Neq => self.find_sensors_neq(&matcher.name, &matcher.value).await,
            label_matcher::Type::Re => self.find_sensors_regex(&matcher.name, &matcher.value).await,
            label_matcher::Type::Nre => {
                self.find_sensors_not_regex(&matcher.name, &matcher.value)
                    .await
            }
        }
    }

    /// Find sensors with exact label match (EQ).
    /// Special handling for __name__ which maps to sensor.name.
    async fn find_sensors_eq(&self, label_name: &str, label_value: &str) -> Result<Vec<i64>> {
        if label_name == "__name__" {
            // Special case: __name__ matches sensor.name directly
            let rows = sqlx::query("SELECT sensor_id FROM sensors WHERE name = $1")
                .bind(label_value)
                .fetch_all(&self.pool)
                .await
                .context("Failed to query sensors by name")?;

            Ok(rows
                .into_iter()
                .map(|row| row.get::<i64, _>("sensor_id"))
                .collect())
        } else {
            // Regular label matching using the view
            let rows = sqlx::query(
                "SELECT DISTINCT sensor_id FROM sensor_labels_view 
                 WHERE label_name = $1 AND label_value = $2",
            )
            .bind(label_name)
            .bind(label_value)
            .fetch_all(&self.pool)
            .await
            .context("Failed to query sensors by label")?;

            Ok(rows
                .into_iter()
                .map(|row| row.get::<i64, _>("sensor_id"))
                .collect())
        }
    }

    /// Find sensors that do NOT have the exact label match (NEQ).
    async fn find_sensors_neq(&self, label_name: &str, label_value: &str) -> Result<Vec<i64>> {
        if label_name == "__name__" {
            // Special case: __name__ matches sensor.name directly
            let rows = sqlx::query("SELECT sensor_id FROM sensors WHERE name != $1")
                .bind(label_value)
                .fetch_all(&self.pool)
                .await
                .context("Failed to query sensors by name (neq)")?;

            Ok(rows
                .into_iter()
                .map(|row| row.get::<i64, _>("sensor_id"))
                .collect())
        } else {
            // Find all sensors that either don't have this label or have it with a different value
            let rows = sqlx::query(
                "SELECT DISTINCT s.sensor_id 
                 FROM sensors s
                 WHERE s.sensor_id NOT IN (
                     SELECT sensor_id FROM sensor_labels_view 
                     WHERE label_name = $1 AND label_value = $2
                 )",
            )
            .bind(label_name)
            .bind(label_value)
            .fetch_all(&self.pool)
            .await
            .context("Failed to query sensors by label (neq)")?;

            Ok(rows
                .into_iter()
                .map(|row| row.get::<i64, _>("sensor_id"))
                .collect())
        }
    }

    /// Find sensors with regex label match (RE).
    /// Uses PostgreSQL's ~ operator for regex matching.
    async fn find_sensors_regex(&self, label_name: &str, pattern: &str) -> Result<Vec<i64>> {
        if label_name == "__name__" {
            // Special case: __name__ matches sensor.name directly
            let rows = sqlx::query("SELECT sensor_id FROM sensors WHERE name ~ $1")
                .bind(pattern)
                .fetch_all(&self.pool)
                .await
                .context("Failed to query sensors by name regex")?;

            Ok(rows
                .into_iter()
                .map(|row| row.get::<i64, _>("sensor_id"))
                .collect())
        } else {
            // Regular label regex matching using the view
            let rows = sqlx::query(
                "SELECT DISTINCT sensor_id FROM sensor_labels_view 
                 WHERE label_name = $1 AND label_value ~ $2",
            )
            .bind(label_name)
            .bind(pattern)
            .fetch_all(&self.pool)
            .await
            .context("Failed to query sensors by label regex")?;

            Ok(rows
                .into_iter()
                .map(|row| row.get::<i64, _>("sensor_id"))
                .collect())
        }
    }

    /// Find sensors that do NOT match regex pattern (NRE).
    async fn find_sensors_not_regex(&self, label_name: &str, pattern: &str) -> Result<Vec<i64>> {
        if label_name == "__name__" {
            // Special case: __name__ matches sensor.name directly
            let rows = sqlx::query("SELECT sensor_id FROM sensors WHERE name !~ $1")
                .bind(pattern)
                .fetch_all(&self.pool)
                .await
                .context("Failed to query sensors by name not regex")?;

            Ok(rows
                .into_iter()
                .map(|row| row.get::<i64, _>("sensor_id"))
                .collect())
        } else {
            // Find all sensors that either don't have this label or have it not matching the pattern
            let rows = sqlx::query(
                "SELECT DISTINCT s.sensor_id 
                 FROM sensors s
                 WHERE s.sensor_id NOT IN (
                     SELECT sensor_id FROM sensor_labels_view 
                     WHERE label_name = $1 AND label_value ~ $2
                 )",
            )
            .bind(label_name)
            .bind(pattern)
            .fetch_all(&self.pool)
            .await
            .context("Failed to query sensors by label not regex")?;

            Ok(rows
                .into_iter()
                .map(|row| row.get::<i64, _>("sensor_id"))
                .collect())
        }
    }
}
