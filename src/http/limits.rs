use crate::datamodel::{SensAppDateTime, SensorData};
use crate::storage::cross_series::{CrossSeriesQuery, CrossSeriesStats, read_cross_series};
use crate::storage::{LabelMatcher, SelectorLimitExceeded, StorageInstance};
use std::sync::Arc;

use super::app_error::AppError;

/// Default of `SENSAPP_HTTP_MAX_QUERY_SAMPLES`: the most samples a read may return, for a series,
/// for the series of a selector in total, and for the buckets of a cross-series aggregation.
pub const DEFAULT_MAX_QUERY_SAMPLES: usize = 100_000;
pub const MAX_SELECTOR_SERIES: usize = 256;
/// Series a cross-series aggregation may combine. The raw samples are aggregated by the database,
/// so only buckets come back (at most the sample limit of them), and the number of
/// series is bounded by the labels and sensors held in memory, not by the volume of samples.
pub const MAX_AGGREGATED_SELECTOR_SERIES: usize = 10_000;

pub fn validate_direct_sample_count(count: usize, max_samples: usize) -> Result<(), AppError> {
    if count > max_samples {
        return Err(AppError::bad_request(anyhow::anyhow!(
            "Query exceeds {max_samples} samples; narrow the time range or use aggregation"
        )));
    }
    Ok(())
}

pub fn validate_selector_result(result: &[SensorData], max_samples: usize) -> Result<(), AppError> {
    if result.len() > MAX_SELECTOR_SERIES {
        return Err(AppError::bad_request(anyhow::anyhow!(
            "Query exceeds {MAX_SELECTOR_SERIES} series; narrow the selector"
        )));
    }
    let mut total = 0usize;
    for data in result {
        if data.samples.len() > max_samples {
            return Err(AppError::bad_request(anyhow::anyhow!(
                "Query exceeds {max_samples} samples per series; narrow the time range or use aggregation"
            )));
        }
        total = total.saturating_add(data.samples.len());
        if total > max_samples {
            return Err(AppError::bad_request(anyhow::anyhow!(
                "Query exceeds {max_samples} samples in total; narrow the selector or time range"
            )));
        }
    }
    Ok(())
}

/// Read the series of a selector within the series limit and the shared sample budget.
pub async fn query_selector_bounded(
    storage: &Arc<dyn StorageInstance>,
    matchers: &[LabelMatcher],
    start_time: Option<SensAppDateTime>,
    end_time: Option<SensAppDateTime>,
    numeric_only: bool,
    max_samples: usize,
) -> Result<Vec<SensorData>, AppError> {
    let result = storage
        .query_selector(
            matchers,
            start_time,
            end_time,
            numeric_only,
            MAX_SELECTOR_SERIES,
            max_samples,
        )
        .await?;
    let result = match result {
        Ok(result) => result,
        Err(SelectorLimitExceeded::Series) => {
            return Err(AppError::bad_request(anyhow::anyhow!(
                "Query exceeds {MAX_SELECTOR_SERIES} series; narrow the selector"
            )));
        }
        Err(SelectorLimitExceeded::Samples) => {
            return Err(AppError::bad_request(anyhow::anyhow!(
                "Query exceeds {max_samples} samples in total; narrow the selector or time range"
            )));
        }
    };
    validate_selector_result(&result, max_samples)?;
    Ok(result)
}

/// Aggregate the numeric series of a selector across series, within
/// `MAX_AGGREGATED_SELECTOR_SERIES` series and `max_buckets` buckets.
pub async fn query_cross_series_bounded(
    storage: &Arc<dyn StorageInstance>,
    matchers: &[LabelMatcher],
    start_time: Option<SensAppDateTime>,
    end_time: Option<SensAppDateTime>,
    query: &CrossSeriesQuery,
    max_buckets: usize,
) -> Result<(Vec<SensorData>, CrossSeriesStats), AppError> {
    query.validate().map_err(AppError::bad_request)?;
    let read = read_cross_series(
        storage.as_ref(),
        matchers,
        start_time,
        end_time,
        query,
        MAX_AGGREGATED_SELECTOR_SERIES,
        max_buckets,
    )
    .await?;
    match read {
        Ok(read) => Ok(read),
        Err(SelectorLimitExceeded::Series) => Err(AppError::bad_request(anyhow::anyhow!(
            "Query exceeds {MAX_AGGREGATED_SELECTOR_SERIES} series; narrow the selector"
        ))),
        Err(SelectorLimitExceeded::Samples) => Err(AppError::bad_request(anyhow::anyhow!(
            "Query exceeds {max_buckets} buckets in total; use a larger step or narrow the selector or time range"
        ))),
    }
}

/// Longest PromQL query or selector accepted, in bytes.
pub const MAX_PROMQL_LENGTH: usize = 4096;
/// Deepest PromQL nesting accepted: brackets, plus the unary signs within them.
pub const MAX_PROMQL_NESTING: usize = 64;

/// Refuse a PromQL query or selector that is too long or too deeply nested to parse. The parser is
/// a recursive descent without a depth limit: measured on a 2 MiB thread (a Tokio worker) in a
/// debug build, 150 nested parentheses or 700 unary minus signs overflow the stack, and a stack
/// overflow aborts the process. A release build goes about ten times deeper.
///
/// The nesting is counted conservatively: the depth of `(`, `[` and `{`, plus every `+` or `-` in
/// a unary position (after the start, an opening bracket, a comma or another operator) until its
/// bracket closes. Quoted strings are skipped, so label values may hold any character.
pub fn check_promql_size(query: &str) -> Result<(), AppError> {
    if query.len() > MAX_PROMQL_LENGTH {
        return Err(AppError::bad_request(anyhow::anyhow!(
            "The PromQL query is longer than {MAX_PROMQL_LENGTH} bytes"
        )));
    }
    // The unary signs of each open bracket, the outermost level first
    let mut levels: Vec<usize> = vec![0];
    let mut nesting = 0usize;
    let mut unary_position = true;
    let mut chars = query.chars();
    while let Some(c) = chars.next() {
        match c {
            '"' | '\'' | '`' => {
                // Skip the string, and the escaped characters of its content
                while let Some(inner) = chars.next() {
                    if inner == '\\' && c != '`' {
                        chars.next();
                    } else if inner == c {
                        break;
                    }
                }
                unary_position = false;
            }
            '(' | '[' | '{' => {
                levels.push(0);
                nesting += 1;
                unary_position = true;
            }
            ')' | ']' | '}' => {
                if levels.len() > 1 {
                    nesting -= 1 + levels.pop().unwrap_or(0);
                }
                unary_position = false;
            }
            '+' | '-' if unary_position => {
                if let Some(signs) = levels.last_mut() {
                    *signs += 1;
                }
                nesting += 1;
            }
            '+' | '-' | '*' | '/' | '%' | '^' | '=' | '!' | '<' | '>' | ',' => {
                unary_position = true;
            }
            c if c.is_whitespace() => {}
            _ => unary_position = false,
        }
        if nesting > MAX_PROMQL_NESTING {
            return Err(AppError::bad_request(anyhow::anyhow!(
                "The PromQL query is nested deeper than {MAX_PROMQL_NESTING} levels"
            )));
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datamodel::{Sensor, SensorType, TypedSamples};
    use uuid::Uuid;

    #[test]
    fn promql_nesting_counts_brackets_and_unary_signs_only() {
        let accepted = [
            r#"sum by (room) (rate(http_requests_total{job="api", path=~"/a-(b|c)"}[5m])) - -1"#,
            "a - b - c + d * -e",
            &format!("x{}", " - x".repeat(1_000)),
            &format!("{}x{}", "(".repeat(64), ")".repeat(64)),
            &format!("{}x", "-".repeat(64)),
            // The signs of a closed bracket no longer count
            &format!("{}x", "(--x) + ".repeat(200)),
            r#"{name="(((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((((("}"#,
        ];
        for query in accepted {
            assert!(check_promql_size(query).is_ok(), "{query}");
        }
        let refused = [
            format!("{}x{}", "(".repeat(65), ")".repeat(65)),
            format!("{}x", "-".repeat(65)),
            format!("{}x{}", "(-".repeat(33), ")".repeat(33)),
            format!("x{}", " + x".repeat(1_100)),
        ];
        for query in &refused {
            assert!(check_promql_size(query).is_err(), "{}", &query[..20]);
        }
    }

    #[test]
    fn direct_sample_limit_has_an_explicit_boundary() {
        let max = DEFAULT_MAX_QUERY_SAMPLES;
        assert!(validate_direct_sample_count(max, max).is_ok());
        assert!(validate_direct_sample_count(max + 1, max).is_err());
        // A lowered limit moves the boundary
        assert!(validate_direct_sample_count(50, 50).is_ok());
        assert!(validate_direct_sample_count(51, 50).is_err());
    }

    #[test]
    fn selector_rejects_too_many_series() {
        let series = (0..=MAX_SELECTOR_SERIES)
            .map(|_| {
                SensorData::new(
                    Sensor {
                        uuid: Uuid::nil(),
                        name: "temperature".into(),
                        sensor_type: SensorType::Integer,
                        unit: None,
                        labels: Default::default(),
                    },
                    TypedSamples::Integer(Default::default()),
                )
            })
            .collect::<Vec<_>>();
        assert!(validate_selector_result(&series, DEFAULT_MAX_QUERY_SAMPLES).is_err());
    }
}
