//! Simple PromQL query endpoint.
//!
//! This module provides a simplified PromQL query interface that allows users to query
//! data using familiar PromQL syntax (e.g., `my_metric{env="prod"}`) without needing
//! a full Prometheus setup.
//!
//! Supported expressions:
//! - `VectorSelector` (last hour): `my_metric{label="value"}`
//! - `MatrixSelector` (given window): `my_metric{label="value"}[5m]`
//! - A cross-series aggregation of either selector, optionally grouped:
//!   `avg by (room) (temperature[24h])`. Operators: `sum`, `avg`, `min`, `max`, `count`.
//!   The `step` query parameter sets the time bucket width; without it the whole window is a
//!   single bucket. Unlike strict PromQL, a matrix selector is accepted inside an aggregation,
//!   because it is the only way to choose the window.
//!
//! Other operations like `rate()`, arithmetic or `topk()` are rejected.

use crate::datamodel::SensAppDateTime;
use crate::exporters::{ArrowConverter, CsvConverter, JsonlConverter, SenMLConverter};
use crate::http::app_error::AppError;
use crate::http::crud::ExportFormat;
use crate::http::crud::promql_duration;
use crate::http::state::HttpServerState;
use crate::storage::Aggregation;
use crate::storage::common::datetime_to_micros;
use crate::storage::cross_series::{CrossSeriesQuery, Grouping};
use crate::storage::query::{LabelMatcher, MatcherType};
use axum::extract::{Query, State};
use axum::response::Response;
use rusty_promql_parser::{Expr, GroupingAction, expr};
use serde::Deserialize;
use std::time::Instant;

/// Default time range for instant queries (1 hour in milliseconds)
const DEFAULT_LOOKBACK_MS: i64 = 3600 * 1000;

/// Query parameters for the simple PromQL endpoint
#[derive(Debug, Deserialize)]
pub struct PromQLQuery {
    /// The PromQL query string
    pub query: String,
    /// Output format: senml, csv, jsonl, or arrow (default: senml)
    pub format: Option<String>,
    /// Time bucket width for aggregations, in Prometheus duration syntax (e.g. `5m`)
    pub step: Option<String>,
}

/// Convert PromQL LabelMatchOp to SensApp MatcherType
fn convert_label_match_op(op: rusty_promql_parser::LabelMatchOp) -> MatcherType {
    match op {
        rusty_promql_parser::LabelMatchOp::Equal => MatcherType::Equal,
        rusty_promql_parser::LabelMatchOp::NotEqual => MatcherType::NotEqual,
        rusty_promql_parser::LabelMatchOp::RegexMatch => MatcherType::RegexMatch,
        rusty_promql_parser::LabelMatchOp::RegexNotMatch => MatcherType::RegexNotMatch,
    }
}

/// Convert PromQL LabelMatcher to SensApp LabelMatcher
fn convert_label_matcher(m: &rusty_promql_parser::parser::selector::LabelMatcher) -> LabelMatcher {
    LabelMatcher::new(
        m.name.clone(),
        m.value.clone(),
        convert_label_match_op(m.op),
    )
}

/// Extract label matchers from a VectorSelector, including the metric name as __name__
fn extract_matchers_from_vector_selector(
    selector: &rusty_promql_parser::VectorSelector,
) -> Vec<LabelMatcher> {
    let mut matchers = Vec::new();

    // Add metric name as __name__ matcher if present
    if let Some(name) = &selector.name {
        matchers.push(LabelMatcher::eq("__name__", name.clone()));
    }

    // Add all explicit label matchers
    for m in &selector.matchers {
        matchers.push(convert_label_matcher(m));
    }

    matchers
}

/// Information extracted from a parsed PromQL query
#[derive(Debug)]
struct ParsedQuery {
    matchers: Vec<LabelMatcher>,
    start_time: Option<SensAppDateTime>,
    end_time: Option<SensAppDateTime>,
    aggregate: Option<AggregateSpec>,
}

/// A cross-series aggregation wrapped around the selector
#[derive(Debug)]
struct AggregateSpec {
    aggregation: Aggregation,
    grouping: Option<Grouping>,
}

const SIMPLE_QUERY_HINT: &str = "Only selectors like 'metric_name{label=\"value\"}' or 'metric_name[5m]', optionally wrapped in sum, avg, min, max or count, are supported.";

fn binary_operation_error_message(query: &str) -> String {
    let trimmed = query.trim();
    let looks_like_hyphenated_metric = trimmed.contains('-')
        && !trimmed.contains(" + ")
        && !trimmed.contains(" - ")
        && !trimmed.contains(" * ")
        && !trimmed.contains(" / ");

    if looks_like_hyphenated_metric {
        return "Binary operations (like +, -, *, /) are not supported. If you intended to query a sensor named like 'demo-temperature', PromQL parses '-' as subtraction. Query it as '{__name__=\"demo-temperature\"}[5m]' or use an underscore-friendly metric name for bare selectors.".to_string();
    }

    format!("Binary operations (like +, -, *, /) are not supported. {SIMPLE_QUERY_HINT}")
}

/// Selectors read the last `window_ms` milliseconds
fn selector_query(
    selector: &rusty_promql_parser::VectorSelector,
    window_ms: i64,
) -> Result<ParsedQuery, AppError> {
    let matchers = extract_matchers_from_vector_selector(selector);

    if matchers.is_empty() {
        return Err(AppError::bad_request(anyhow::anyhow!(
            "Query must have at least one matcher (metric name or label)"
        )));
    }

    let now = hifitime::Epoch::now().map_err(|e| {
        AppError::internal_server_error(anyhow::anyhow!("Failed to get current time: {}", e))
    })?;
    let start_time = now - hifitime::Duration::from_milliseconds(window_ms as f64);

    Ok(ParsedQuery {
        matchers,
        start_time: Some(start_time),
        end_time: Some(now),
        aggregate: None,
    })
}

fn aggregate_spec(agg: &rusty_promql_parser::Aggregation) -> Result<AggregateSpec, AppError> {
    let aggregation = match agg.op.to_ascii_lowercase().as_str() {
        "sum" => Aggregation::Sum,
        "avg" => Aggregation::Avg,
        "min" => Aggregation::Min,
        "max" => Aggregation::Max,
        "count" => Aggregation::Count,
        other => {
            return Err(AppError::bad_request(anyhow::anyhow!(
                "Aggregation '{other}' is not supported. Supported aggregations: sum, avg, min, max, count."
            )));
        }
    };

    let grouping = agg.grouping.as_ref().map(|grouping| match grouping.action {
        GroupingAction::By => Grouping::By(grouping.labels.clone()),
        GroupingAction::Without => Grouping::Without(grouping.labels.clone()),
    });

    Ok(AggregateSpec {
        aggregation,
        grouping,
    })
}

/// Validate a parsed expression and extract what the storage layer needs
fn parse_expr(ast: Expr, query: &str) -> Result<ParsedQuery, AppError> {
    match ast {
        // Instant queries use a default lookback period
        Expr::VectorSelector(selector) => selector_query(&selector, DEFAULT_LOOKBACK_MS),
        Expr::MatrixSelector(selector) => {
            selector_query(&selector.selector, selector.range_millis())
        }
        Expr::Paren(inner) => parse_expr(*inner, query),
        Expr::Aggregation(agg) => {
            let spec = aggregate_spec(&agg)?;
            if agg.param.is_some() {
                return Err(AppError::bad_request(anyhow::anyhow!(
                    "Parametric aggregations are not supported. {SIMPLE_QUERY_HINT}"
                )));
            }
            let mut parsed = parse_expr(agg.expr, query)?;
            if parsed.aggregate.is_some() {
                return Err(AppError::bad_request(anyhow::anyhow!(
                    "Nested aggregations are not supported."
                )));
            }
            parsed.aggregate = Some(spec);
            Ok(parsed)
        }
        // Reject all other complex expressions
        Expr::Call(_) => Err(AppError::bad_request(anyhow::anyhow!(
            "Function calls (like rate(), increase(), histogram_quantile()) are not supported. {SIMPLE_QUERY_HINT}"
        ))),
        Expr::Binary(_) => Err(AppError::bad_request(anyhow::anyhow!(
            binary_operation_error_message(query)
        ))),
        Expr::Unary(_) => Err(AppError::bad_request(anyhow::anyhow!(
            "Unary operations are not supported. {SIMPLE_QUERY_HINT}"
        ))),
        Expr::Subquery(_) => Err(AppError::bad_request(anyhow::anyhow!(
            "Subqueries are not supported. {SIMPLE_QUERY_HINT}"
        ))),
        Expr::Number(_) | Expr::String(_) => Err(AppError::bad_request(anyhow::anyhow!(
            "Literal values are not valid queries. Use a metric selector like 'metric_name{{label=\"value\"}}'."
        ))),
    }
}

/// Parse and validate a PromQL query, returning the extracted information
fn parse_promql_query(query: &str) -> Result<ParsedQuery, AppError> {
    let (rest, ast) = expr(query).map_err(|e| {
        AppError::bad_request(anyhow::anyhow!("Failed to parse PromQL query: {:?}", e))
    })?;

    // Ensure the entire query was consumed
    if !rest.trim().is_empty() {
        return Err(AppError::bad_request(anyhow::anyhow!(
            "Unexpected trailing content in query: '{}'",
            rest
        )));
    }

    parse_expr(ast, query)
}

/// Simple PromQL query endpoint.
///
/// Parses a PromQL expression and returns matching time series data.
/// Only simple selectors (VectorSelector and MatrixSelector) are supported.
///
/// # Examples
///
/// - Simple metric: `GET /api/v1/query?query=my_metric`
/// - With labels: `GET /api/v1/query?query=my_metric{env="prod"}`
/// - Range query: `GET /api/v1/query?query=my_metric[5m]`
/// - With format: `GET /api/v1/query?query=my_metric&format=csv`
#[utoipa::path(
    get,
    path = "/api/v1/query",
    security(("bearer" = ["read"])),
    tag = "SensApp",
    params(
        ("query" = String, Query, description = "PromQL query string (e.g., 'my_metric{label=\"value\"}' or 'my_metric[5m]')"),
        ("format" = Option<String>, Query, description = "Output format: senml (default), csv, jsonl, or arrow"),
        ("step" = Option<String>, Query, description = "Bucket width for aggregation queries, using Prometheus duration syntax (e.g., '5m'). Without it, the whole window is one bucket")
    ),
    responses(
        (status = 200, description = "Query results in requested format", body = Value),
        (status = 400, description = "Invalid or unsupported PromQL query"),
        (status = 500, description = "Internal server error"),
        (status = 503, description = "The storage backend is unavailable: retry later"),
        (status = 504, description = "The request took longer than SENSAPP_HTTP_SERVER_TIMEOUT_SECONDS, usually because the storage backend hangs")
    )
)]
pub async fn simple_promql_query(
    State(state): State<HttpServerState>,
    access: Option<axum::Extension<crate::http::auth::AccessContext>>,
    Query(query): Query<PromQLQuery>,
) -> Result<Response, AppError> {
    let state = state.with_access(access.map(|extension| extension.0));
    let metrics = state.metrics.clone();
    let started = Instant::now();

    let result = async move {
        let parsed = parse_promql_query(&query.query)?;

        let step_us =
            match query.step.as_deref() {
                Some(step) => {
                    if parsed.aggregate.is_none() {
                        return Err(AppError::bad_request(anyhow::anyhow!(
                            "'step' is only supported with an aggregation like avg(...)"
                        )));
                    }
                    let step_ms = promql_duration::parse_duration_millis(step).map_err(|e| {
                        AppError::bad_request(anyhow::anyhow!("Invalid step duration: {}", e))
                    })?;
                    Some(step_ms.checked_mul(1000).ok_or_else(|| {
                        AppError::bad_request(anyhow::anyhow!("'step' is too large"))
                    })?)
                }
                None => None,
            };

        let (results, series, samples) = match parsed.aggregate {
            // Prometheus series are numeric, so an aggregation only considers numeric sensors.
            // The database aggregates the raw samples, only the buckets come back.
            Some(spec) => {
                let query = CrossSeriesQuery {
                    aggregation: spec.aggregation,
                    grouping: spec.grouping,
                    step_us,
                    origin_us: parsed
                        .start_time
                        .as_ref()
                        .map(datetime_to_micros)
                        .unwrap_or(0),
                };
                let (results, stats) = crate::http::limits::query_cross_series_bounded(
                    &state.storage,
                    &parsed.matchers,
                    parsed.start_time,
                    parsed.end_time,
                    &query,
                    state.max_query_samples,
                )
                .await?;
                (results, stats.series, stats.buckets)
            }
            None => {
                let results = crate::http::limits::query_selector_bounded(
                    &state.storage,
                    &parsed.matchers,
                    parsed.start_time,
                    parsed.end_time,
                    false,
                    state.max_query_samples,
                )
                .await?;
                let series = results.len();
                let samples = results
                    .iter()
                    .map(|sensor_data| sensor_data.samples.len())
                    .sum();
                (results, series, samples)
            }
        };

        let format = match query.format.as_deref() {
            Some(format_str) => ExportFormat::from_extension(format_str).ok_or_else(|| {
                AppError::bad_request(anyhow::anyhow!(
                    "Unsupported export format '{}'. Supported formats: senml, csv, jsonl, arrow",
                    format_str
                ))
            })?,
            None => ExportFormat::Senml,
        };

        let response = match format {
            ExportFormat::Senml => {
                let json_value = SenMLConverter::to_senml_json_multi(&results)
                    .map_err(AppError::internal_server_error)?;
                axum::response::Response::builder()
                    .header("content-type", format.content_type())
                    .body(json_value.to_string().into())
            }
            ExportFormat::Csv => {
                let csv_content = CsvConverter::to_csv_multi(&results)
                    .map_err(AppError::internal_server_error)?;
                axum::response::Response::builder()
                    .header("content-type", format.content_type())
                    .body(csv_content.into())
            }
            ExportFormat::Jsonl => {
                let jsonl_content = JsonlConverter::to_jsonl_multi(&results)
                    .map_err(AppError::internal_server_error)?;
                axum::response::Response::builder()
                    .header("content-type", format.content_type())
                    .body(jsonl_content.into())
            }
            ExportFormat::Arrow => {
                let arrow_bytes = ArrowConverter::to_arrow_stream_multi(&results)
                    .map_err(AppError::internal_server_error)?;
                axum::response::Response::builder()
                    .header("content-type", format.content_type())
                    .body(arrow_bytes.into())
            }
        }
        .map_err(|e| {
            AppError::internal_server_error(anyhow::anyhow!("Failed to build response: {}", e))
        })?;

        Ok::<_, AppError>((response, series, samples))
    }
    .await;

    metrics.observe_operation_result(
        "read",
        "simple_promql_query",
        started.elapsed(),
        result.is_ok(),
    );
    if let Ok((_, series, samples)) = &result {
        metrics.observe_series("read", "simple_promql_query", *series);
        metrics.observe_samples("read", "simple_promql_query", *samples);
    }

    result.map(|(response, _, _)| response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_parse_simple_metric_name() {
        let result = parse_promql_query("my_metric");
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.matchers.len(), 1);
        assert_eq!(parsed.matchers[0].name, "__name__");
        assert_eq!(parsed.matchers[0].value, "my_metric");
        assert_eq!(parsed.matchers[0].matcher_type, MatcherType::Equal);
    }

    #[test]
    fn test_parse_metric_with_labels() {
        let result = parse_promql_query(r#"my_metric{env="prod",region="us"}"#);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.matchers.len(), 3); // __name__ + 2 labels
        assert_eq!(parsed.matchers[0].name, "__name__");
        assert_eq!(parsed.matchers[0].value, "my_metric");
    }

    #[test]
    fn test_parse_matrix_selector() {
        let result = parse_promql_query("my_metric[5m]");
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.matchers.len(), 1);
        // Matrix selectors should have a time range
        assert!(parsed.start_time.is_some());
        assert!(parsed.end_time.is_some());
    }

    #[test]
    fn test_parse_matrix_with_labels() {
        let result = parse_promql_query(r#"http_requests{method="GET"}[10m]"#);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        assert_eq!(parsed.matchers.len(), 2);
    }

    #[test]
    fn test_parse_aggregations() {
        let parsed = parse_promql_query("avg(my_metric)").unwrap();
        let spec = parsed.aggregate.unwrap();
        assert_eq!(spec.aggregation, Aggregation::Avg);
        assert_eq!(spec.grouping, None);
        assert_eq!(parsed.matchers.len(), 1);

        let parsed = parse_promql_query(r#"sum by (room, floor) (temp{env="prod"}[5m])"#).unwrap();
        let spec = parsed.aggregate.unwrap();
        assert_eq!(spec.aggregation, Aggregation::Sum);
        assert_eq!(
            spec.grouping,
            Some(Grouping::By(vec!["room".into(), "floor".into()]))
        );
        assert_eq!(parsed.matchers.len(), 2);
        assert!(parsed.start_time.is_some());

        let parsed = parse_promql_query("count without (id) ((temp))").unwrap();
        let spec = parsed.aggregate.unwrap();
        assert_eq!(spec.aggregation, Aggregation::Count);
        assert_eq!(spec.grouping, Some(Grouping::Without(vec!["id".into()])));

        for op in ["min", "max"] {
            assert!(parse_promql_query(&format!("{op}(temp)")).is_ok());
        }
        assert!(parse_promql_query("temp").unwrap().aggregate.is_none());
    }

    #[test]
    fn test_reject_unsupported_aggregations() {
        for query in [
            "stddev(my_metric)",
            "quantile(0.5, my_metric)",
            "topk(3, my_metric)",
            "count_values(\"v\", my_metric)",
            "sum(avg(my_metric))",
            "sum(rate(my_metric[5m]))",
            "sum(my_metric) + 1",
            "sum({})",
        ] {
            match parse_promql_query(query) {
                Err(AppError::BadRequest(_)) => {}
                other => panic!("expected BadRequest for {query}, got {other:?}"),
            }
        }
    }

    #[test]
    fn test_reject_function_call() {
        let result = parse_promql_query("rate(my_metric[5m])");
        assert!(result.is_err());
        match result {
            Err(AppError::BadRequest(err)) => {
                assert!(err.to_string().contains("Function"));
            }
            _ => panic!("Expected BadRequest error with Function message"),
        }
    }

    #[test]
    fn test_reject_binary_operation() {
        let result = parse_promql_query("my_metric + 1");
        assert!(result.is_err());
        match result {
            Err(AppError::BadRequest(err)) => {
                assert!(err.to_string().contains("Binary"));
            }
            _ => panic!("Expected BadRequest error with Binary message"),
        }
    }

    #[test]
    fn test_reject_literal() {
        let result = parse_promql_query("42");
        assert!(result.is_err());
    }

    #[test]
    fn test_parse_with_regex_matcher() {
        let result = parse_promql_query(r#"my_metric{env=~"prod.*"}"#);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        // Find the env matcher
        let env_matcher = parsed.matchers.iter().find(|m| m.name == "env");
        assert!(env_matcher.is_some());
        assert_eq!(env_matcher.unwrap().matcher_type, MatcherType::RegexMatch);
    }

    #[test]
    fn test_parse_with_not_equal_matcher() {
        let result = parse_promql_query(r#"my_metric{env!="test"}"#);
        assert!(result.is_ok());
        let parsed = result.unwrap();
        let env_matcher = parsed.matchers.iter().find(|m| m.name == "env");
        assert!(env_matcher.is_some());
        assert_eq!(env_matcher.unwrap().matcher_type, MatcherType::NotEqual);
    }

    #[test]
    fn test_convert_label_match_op() {
        assert_eq!(
            convert_label_match_op(rusty_promql_parser::LabelMatchOp::Equal),
            MatcherType::Equal
        );
        assert_eq!(
            convert_label_match_op(rusty_promql_parser::LabelMatchOp::NotEqual),
            MatcherType::NotEqual
        );
        assert_eq!(
            convert_label_match_op(rusty_promql_parser::LabelMatchOp::RegexMatch),
            MatcherType::RegexMatch
        );
        assert_eq!(
            convert_label_match_op(rusty_promql_parser::LabelMatchOp::RegexNotMatch),
            MatcherType::RegexNotMatch
        );
    }
}
