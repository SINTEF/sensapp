//! Label matchers as SQL conditions on the deduplicated sensors (`s`) and the `labels` table.
//!
//! The semantics are the ones of the ClickHouse backend: `!=` and `!~` also select the series that
//! do not have the label. Regex patterns are anchored by `LabelMatcher` and run by RE2, like
//! Prometheus' own.

use super::BigQueryStorage;
use super::client::string_param;
use crate::datamodel::Sensor;
use crate::storage::{LabelMatcher, MatcherType};
use anyhow::Result;
use gcp_bigquery_client::model::query_parameter::QueryParameter;

/// The `WHERE` conditions and parameters (`m0`, `m1`, ...) of a selector.
pub fn matcher_conditions(
    name_matchers: &[&LabelMatcher],
    label_matchers: &[&LabelMatcher],
    numeric_only: bool,
    labels_table: &str,
) -> (Vec<String>, Vec<QueryParameter>) {
    let mut conditions = Vec::new();
    let mut params = Vec::new();
    let mut parameter = |value: &str| {
        let name = format!("m{}", params.len());
        params.push(string_param(&name, value));
        format!("@{name}")
    };

    if numeric_only {
        conditions.push("s.type IN ('Integer', 'Numeric', 'Float')".to_string());
    }

    for matcher in name_matchers {
        let value = parameter(&matcher.value);
        conditions.push(match matcher.matcher_type {
            MatcherType::Equal => format!("s.name = {value}"),
            MatcherType::NotEqual => format!("s.name != {value}"),
            MatcherType::RegexMatch => format!("REGEXP_CONTAINS(s.name, {value})"),
            MatcherType::RegexNotMatch => format!("NOT REGEXP_CONTAINS(s.name, {value})"),
        });
    }

    for matcher in label_matchers {
        let name = parameter(&matcher.name);
        let value = parameter(&matcher.value);
        let (membership, test) = match matcher.matcher_type {
            MatcherType::Equal => ("IN", format!("l.description = {value}")),
            MatcherType::NotEqual => ("NOT IN", format!("l.description = {value}")),
            MatcherType::RegexMatch => ("IN", format!("REGEXP_CONTAINS(l.description, {value})")),
            MatcherType::RegexNotMatch => {
                ("NOT IN", format!("REGEXP_CONTAINS(l.description, {value})"))
            }
        };
        conditions.push(format!(
            "s.sensor_id {membership} (SELECT l.sensor_id FROM {labels_table} l \
             WHERE l.name = {name} AND {test})"
        ));
    }

    (conditions, params)
}

impl BigQueryStorage {
    /// The series matching all the matchers, by increasing id, with their labels and units.
    pub(super) async fn find_sensors_by_matchers(
        &self,
        matchers: &[LabelMatcher],
        numeric_only: bool,
        limit: Option<usize>,
    ) -> Result<Vec<(i64, Sensor)>> {
        let (name_matchers, label_matchers): (Vec<_>, Vec<_>) = matchers
            .iter()
            .partition(|matcher| matcher.is_name_matcher());
        let (conditions, params) = matcher_conditions(
            &name_matchers,
            &label_matchers,
            numeric_only,
            &self.table("labels"),
        );
        let mut tail = "ORDER BY s.sensor_id".to_string();
        if let Some(limit) = limit {
            // A number, never text from a caller
            tail.push_str(&format!(" LIMIT {limit}"));
        }
        self.read_sensors(self.dataset.sensors_sql(&conditions, &tail), params)
            .await
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn conditions(matchers: &[LabelMatcher], numeric_only: bool) -> (Vec<String>, usize) {
        let (names, labels): (Vec<_>, Vec<_>) = matchers
            .iter()
            .partition(|matcher| matcher.is_name_matcher());
        let (conditions, params) =
            matcher_conditions(&names, &labels, numeric_only, "`p.d.labels`");
        (conditions, params.len())
    }

    #[test]
    fn names_and_labels_become_parameters_never_text() {
        let (conditions, parameters) = conditions(
            &[
                LabelMatcher::eq("__name__", "cpu'; DROP TABLE x; --"),
                LabelMatcher::eq("site", "oslo"),
                LabelMatcher::neq("env", "prod"),
            ],
            false,
        );
        assert_eq!(parameters, 5);
        assert_eq!(conditions[0], "s.name = @m0");
        assert!(
            conditions[1].starts_with("s.sensor_id IN (SELECT l.sensor_id FROM `p.d.labels` l")
        );
        assert!(conditions[1].contains("l.name = @m1 AND l.description = @m2"));
        assert!(conditions[2].starts_with("s.sensor_id NOT IN"));
        assert!(conditions.iter().all(|c| !c.contains("DROP")));
    }

    #[test]
    fn regexes_use_re2_and_numeric_only_filters_the_type() {
        let (conditions, parameters) = conditions(
            &[
                LabelMatcher::regex("__name__", "cpu.*"),
                LabelMatcher::not_regex("site", "o.*"),
            ],
            true,
        );
        assert_eq!(parameters, 3);
        assert_eq!(conditions[0], "s.type IN ('Integer', 'Numeric', 'Float')");
        assert_eq!(conditions[1], "REGEXP_CONTAINS(s.name, @m0)");
        assert!(conditions[2].starts_with("s.sensor_id NOT IN"));
        assert!(conditions[2].contains("REGEXP_CONTAINS(l.description, @m2)"));
    }
}
