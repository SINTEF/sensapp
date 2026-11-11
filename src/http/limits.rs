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

/// Stack of the thread the PromQL parser runs on. The parser is a recursive descent without a depth
/// limit, and a stack overflow aborts the whole process: on the 2 MiB stack of a Tokio worker, 150
/// nested parentheses overflow it in a debug build. The length cap bounds the depth, since every
/// level takes at least one byte: 4,096 levels. Measured on the worst shapes at the cap (nested
/// parentheses, `^x^x…`, function calls, unary signs, behind a comment) the parse needs at most
/// 128 MiB in a debug build and 16 MiB in a release build, the stack is twice and four times that.
/// It is address space that the system only backs with memory when the parse reaches it.
#[cfg(debug_assertions)]
const PROMQL_PARSER_STACK_BYTES: usize = 256 * 1024 * 1024;
#[cfg(not(debug_assertions))]
const PROMQL_PARSER_STACK_BYTES: usize = 64 * 1024 * 1024;

/// Refuse a PromQL query or selector that is too long to parse.
pub fn check_promql_length(query: &str) -> Result<(), AppError> {
    if query.len() > MAX_PROMQL_LENGTH {
        return Err(AppError::bad_request(anyhow::anyhow!(
            "The PromQL query is longer than {MAX_PROMQL_LENGTH} bytes"
        )));
    }
    Ok(())
}

/// Run `work`, which parses a PromQL query and drops what it parsed (dropping the tree recurses
/// too), on a thread whose stack fits the deepest query that `check_promql_length` lets through.
/// The checks of an earlier version counted the nesting of the text instead, and the parser read it
/// differently: it skips comments, and recurses once per `^`.
pub fn on_promql_parser_stack<T: Send>(
    work: impl FnOnce() -> Result<T, AppError> + Send,
) -> Result<T, AppError> {
    std::thread::scope(|scope| {
        std::thread::Builder::new()
            .name("promql-parser".to_string())
            .stack_size(PROMQL_PARSER_STACK_BYTES)
            .spawn_scoped(scope, work)
            .map_err(AppError::internal_server_error)?
            .join()
            .map_err(|_| {
                AppError::internal_server_error(anyhow::anyhow!("The PromQL parser panicked"))
            })?
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datamodel::{Sensor, SensorType, TypedSamples};
    use uuid::Uuid;

    #[test]
    fn a_query_over_the_length_cap_is_refused() {
        assert!(check_promql_length(&"x".repeat(MAX_PROMQL_LENGTH)).is_ok());
        assert!(check_promql_length(&"x".repeat(MAX_PROMQL_LENGTH + 1)).is_err());
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

    /// Query shapes at the length cap that nest the parser's recursion as deep as it goes: the
    /// brackets, the signs, the right-associative `^`, the function calls, and a comment that
    /// hides what follows from a scanner that reads a quote in it as a string.
    pub(crate) fn hostile_queries() -> Vec<String> {
        let cap = MAX_PROMQL_LENGTH;
        vec![
            format!("#\"\n{}", "(".repeat(cap - 3)),
            format!("#\"\n{}x{}", "(".repeat(2_000), ")".repeat(2_000)),
            format!("x{}", "^x".repeat((cap - 1) / 2)),
            "(".repeat(cap),
            "-".repeat(cap),
            "[".repeat(cap),
            "{".repeat(cap),
            "f(".repeat(cap / 2),
            "-(".repeat(cap / 2),
            "sum(".repeat(cap / 4),
            format!("x{}", "[1m:1m]".repeat(cap / 7)),
            format!("x{}", " unless x".repeat((cap - 1) / 9)),
            format!("{}x", "- ".repeat(cap / 2 - 1)),
        ]
    }

    /// A Tokio worker has a 2 MiB stack and a stack overflow aborts the whole process: none of
    /// these may crash it, whatever the answer is. Run in a debug build, where frames are largest.
    /// The call sites take the parse to a thread of its own.
    #[test]
    fn hostile_queries_get_an_answer_on_a_small_stack() {
        for query in hostile_queries() {
            let shape = query.chars().take(24).collect::<String>();
            std::thread::Builder::new()
                .stack_size(2 * 1024 * 1024)
                .spawn(move || {
                    let _ = crate::http::simple_promql::parse_promql_query(&query);
                    let _ = crate::http::crud::parse_selector_to_matchers(&query);
                })
                .unwrap()
                .join()
                .unwrap_or_else(|_| panic!("{shape}"));
        }
    }

    /// Reasonable queries nest deeper than the old limit of 64 levels did allow.
    #[test]
    fn a_query_nested_a_hundred_levels_is_parsed() {
        let query = format!("{}x{}", "(".repeat(100), ")".repeat(100));
        assert!(crate::http::simple_promql::parse_promql_query(&query).is_ok());
        let selector = format!(r#"x{{a="{}"}}"#, "(".repeat(100));
        assert!(crate::http::crud::parse_selector_to_matchers(&selector).is_ok());
    }
}
