//! Simple PromQL query endpoint.
//!
//! This module provides a simplified PromQL query interface that allows users to query
//! data using familiar PromQL syntax (e.g., `my_metric{env="prod"}`) without needing
//! a full Prometheus setup.
//!
//! Only simple selectors are supported:
//! - `VectorSelector` (instant query): `my_metric{label="value"}`
//! - `MatrixSelector` (range query): `my_metric{label="value"}[5m]`
//!
//! Complex operations like `sum()`, `rate()`, or arithmetic are rejected.

use crate::datamodel::SensAppDateTime;
use crate::exporters::{ArrowConverter, CsvConverter, JsonlConverter, SenMLConverter};
use crate::ingestors::http::app_error::AppError;
use crate::ingestors::http::crud::ExportFormat;
use crate::ingestors::http::state::HttpServerState;
use crate::storage::query::{LabelMatcher, MatcherType};
use axum::extract::{Query, State};
use axum::response::Response;
use rusty_promql_parser::{Expr, expr};
use serde::Deserialize;

/// Default time range for instant queries (1 hour in milliseconds)
const DEFAULT_LOOKBACK_MS: i64 = 3600 * 1000;

/// Query parameters for the simple PromQL endpoint
#[derive(Debug, Deserialize)]
pub struct PromQLQuery {
    /// The PromQL query string
    pub query: String,
    /// Output format: senml, csv, jsonl, or arrow (default: senml)
    pub format: Option<String>,
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
}

/// Parse and validate a PromQL query, returning the extracted information
fn parse_promql_query(query: &str) -> Result<ParsedQuery, AppError> {
    // Parse the query
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

    // Extract information based on the AST type
    match ast {
        Expr::VectorSelector(selector) => {
            let matchers = extract_matchers_from_vector_selector(&selector);

            if matchers.is_empty() {
                return Err(AppError::bad_request(anyhow::anyhow!(
                    "Query must have at least one matcher (metric name or label)"
                )));
            }

            // For instant queries, use a default lookback period (1 hour)
            let now = hifitime::Epoch::now().map_err(|e| {
                AppError::internal_server_error(anyhow::anyhow!(
                    "Failed to get current time: {}",
                    e
                ))
            })?;
            let start_time =
                now - hifitime::Duration::from_milliseconds(DEFAULT_LOOKBACK_MS as f64);

            Ok(ParsedQuery {
                matchers,
                start_time: Some(start_time),
                end_time: Some(now),
            })
        }
        Expr::MatrixSelector(selector) => {
            let matchers = extract_matchers_from_vector_selector(&selector.selector);

            if matchers.is_empty() {
                return Err(AppError::bad_request(anyhow::anyhow!(
                    "Query must have at least one matcher (metric name or label)"
                )));
            }

            // Calculate time range from the matrix selector's range
            let range_ms = selector.range_millis();
            let now = hifitime::Epoch::now().map_err(|e| {
                AppError::internal_server_error(anyhow::anyhow!(
                    "Failed to get current time: {}",
                    e
                ))
            })?;
            let start_time = now - hifitime::Duration::from_milliseconds(range_ms as f64);

            Ok(ParsedQuery {
                matchers,
                start_time: Some(start_time),
                end_time: Some(now),
            })
        }
        // Reject all complex expressions
        Expr::Aggregation(_) => Err(AppError::bad_request(anyhow::anyhow!(
            "Aggregation expressions (like sum(), avg(), count()) are not supported. Only simple selectors like 'metric_name{{label=\"value\"}}' or 'metric_name[5m]' are supported."
        ))),
        Expr::Call(_) => Err(AppError::bad_request(anyhow::anyhow!(
            "Function calls (like rate(), increase(), histogram_quantile()) are not supported. Only simple selectors like 'metric_name{{label=\"value\"}}' or 'metric_name[5m]' are supported."
        ))),
        Expr::Binary(_) => Err(AppError::bad_request(anyhow::anyhow!(
            "Binary operations (like +, -, *, /) are not supported. Only simple selectors like 'metric_name{{label=\"value\"}}' or 'metric_name[5m]' are supported."
        ))),
        Expr::Unary(_) => Err(AppError::bad_request(anyhow::anyhow!(
            "Unary operations are not supported. Only simple selectors like 'metric_name{{label=\"value\"}}' or 'metric_name[5m]' are supported."
        ))),
        Expr::Paren(inner) => {
            // Unwrap parentheses and try again
            parse_promql_query_from_expr(*inner)
        }
        Expr::Subquery(_) => Err(AppError::bad_request(anyhow::anyhow!(
            "Subqueries are not supported. Only simple selectors like 'metric_name{{label=\"value\"}}' or 'metric_name[5m]' are supported."
        ))),
        Expr::Number(_) | Expr::String(_) => Err(AppError::bad_request(anyhow::anyhow!(
            "Literal values are not valid queries. Use a metric selector like 'metric_name{{label=\"value\"}}'."
        ))),
    }
}

/// Parse a PromQL query from an already-parsed Expr
fn parse_promql_query_from_expr(ast: Expr) -> Result<ParsedQuery, AppError> {
    match ast {
        Expr::VectorSelector(selector) => {
            let matchers = extract_matchers_from_vector_selector(&selector);

            if matchers.is_empty() {
                return Err(AppError::bad_request(anyhow::anyhow!(
                    "Query must have at least one matcher (metric name or label)"
                )));
            }

            let now = hifitime::Epoch::now().map_err(|e| {
                AppError::internal_server_error(anyhow::anyhow!(
                    "Failed to get current time: {}",
                    e
                ))
            })?;
            let start_time =
                now - hifitime::Duration::from_milliseconds(DEFAULT_LOOKBACK_MS as f64);

            Ok(ParsedQuery {
                matchers,
                start_time: Some(start_time),
                end_time: Some(now),
            })
        }
        Expr::MatrixSelector(selector) => {
            let matchers = extract_matchers_from_vector_selector(&selector.selector);

            if matchers.is_empty() {
                return Err(AppError::bad_request(anyhow::anyhow!(
                    "Query must have at least one matcher (metric name or label)"
                )));
            }

            let range_ms = selector.range_millis();
            let now = hifitime::Epoch::now().map_err(|e| {
                AppError::internal_server_error(anyhow::anyhow!(
                    "Failed to get current time: {}",
                    e
                ))
            })?;
            let start_time = now - hifitime::Duration::from_milliseconds(range_ms as f64);

            Ok(ParsedQuery {
                matchers,
                start_time: Some(start_time),
                end_time: Some(now),
            })
        }
        Expr::Paren(inner) => parse_promql_query_from_expr(*inner),
        _ => Err(AppError::bad_request(anyhow::anyhow!(
            "Only simple selectors are supported."
        ))),
    }
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
    tag = "SensApp",
    params(
        ("query" = String, Query, description = "PromQL query string (e.g., 'my_metric{label=\"value\"}' or 'my_metric[5m]')"),
        ("format" = Option<String>, Query, description = "Output format: senml (default), csv, jsonl, or arrow")
    ),
    responses(
        (status = 200, description = "Query results in requested format", body = Value),
        (status = 400, description = "Invalid or unsupported PromQL query"),
        (status = 500, description = "Internal server error")
    )
)]
pub async fn simple_promql_query(
    State(state): State<HttpServerState>,
    Query(query): Query<PromQLQuery>,
) -> Result<Response, AppError> {
    // Parse and validate the query
    let parsed = parse_promql_query(&query.query)?;

    // Execute the query
    let results = state
        .storage
        .query_sensors_by_labels(&parsed.matchers, parsed.start_time, parsed.end_time, None)
        .await?;

    // Parse format from query parameter, default to SenML/JSON
    let format = match query.format.as_deref() {
        Some(format_str) => ExportFormat::from_extension(format_str).ok_or_else(|| {
            AppError::bad_request(anyhow::anyhow!(
                "Unsupported export format '{}'. Supported formats: senml, csv, jsonl, arrow",
                format_str
            ))
        })?,
        None => ExportFormat::Senml, // Default to SenML/JSON format
    };

    // Convert based on requested format
    let response = match format {
        ExportFormat::Senml => {
            let json_value = SenMLConverter::to_senml_json_multi(&results)
                .map_err(AppError::internal_server_error)?;
            axum::response::Response::builder()
                .header("content-type", format.content_type())
                .body(json_value.to_string().into())
        }
        ExportFormat::Csv => {
            let csv_content =
                CsvConverter::to_csv_multi(&results).map_err(AppError::internal_server_error)?;
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
            let arrow_bytes = ArrowConverter::to_arrow_file_multi(&results)
                .map_err(AppError::internal_server_error)?;
            axum::response::Response::builder()
                .header("content-type", format.content_type())
                .body(arrow_bytes.into())
        }
    }
    .map_err(|e| {
        AppError::internal_server_error(anyhow::anyhow!("Failed to build response: {}", e))
    })?;

    Ok(response)
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
    fn test_reject_aggregation() {
        let result = parse_promql_query("sum(my_metric)");
        assert!(result.is_err());
        // Check that the error is about aggregation
        match result {
            Err(AppError::BadRequest(err)) => {
                assert!(err.to_string().contains("Aggregation"));
            }
            _ => panic!("Expected BadRequest error with Aggregation message"),
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
