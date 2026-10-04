//! Running statements: query parameters, jobs that outlast the first answer, result pages, DML counts
//! and the sorting of errors.
//!
//! `jobs.query` answers after 10 seconds even when the job is not done (`jobComplete: false`, no
//! rows), and `ResultSet::new_from_query_response` turns that answer into an empty result. Every
//! statement of SensApp goes through [`BigQueryStorage::run_query`], which waits for the job and
//! reads all the pages.

use super::BigQueryStorage;
use crate::storage::StorageError;
use anyhow::{Result, anyhow};
use gcp_bigquery_client::{
    error::BQError,
    model::{
        get_query_results_parameters::GetQueryResultsParameters, query_parameter::QueryParameter,
        query_parameter_type::QueryParameterType, query_parameter_value::QueryParameterValue,
        query_request::QueryRequest, query_response::ResultSet,
    },
};
use std::time::{Duration, Instant};

/// How long one request waits for a job (the service default is 10 seconds)
const WAIT_MS: i32 = 20_000;
/// How long a statement may run in total before the request gives up on it
const DEADLINE: Duration = Duration::from_secs(300);

fn parameter(
    name: &str,
    parameter_type: QueryParameterType,
    value: QueryParameterValue,
) -> QueryParameter {
    QueryParameter {
        name: Some(name.to_string()),
        parameter_type: Some(parameter_type),
        parameter_value: Some(value),
    }
}

fn scalar_type(name: &str) -> QueryParameterType {
    QueryParameterType {
        r#type: name.to_string(),
        struct_types: None,
        array_type: None,
    }
}

fn scalar_value(value: String) -> QueryParameterValue {
    QueryParameterValue {
        value: Some(value),
        struct_values: None,
        array_values: None,
    }
}

fn array_type(element: &str) -> QueryParameterType {
    QueryParameterType {
        r#type: "ARRAY".to_string(),
        struct_types: None,
        array_type: Some(Box::new(scalar_type(element))),
    }
}

fn array_value(values: impl Iterator<Item = String>) -> QueryParameterValue {
    QueryParameterValue {
        value: None,
        struct_values: None,
        array_values: Some(values.map(scalar_value).collect()),
    }
}

pub fn string_param(name: &str, value: &str) -> QueryParameter {
    parameter(name, scalar_type("STRING"), scalar_value(value.to_string()))
}

pub fn int_param(name: &str, value: i64) -> QueryParameter {
    parameter(name, scalar_type("INT64"), scalar_value(value.to_string()))
}

pub fn string_array_param(name: &str, values: &[&str]) -> QueryParameter {
    parameter(
        name,
        array_type("STRING"),
        array_value(values.iter().map(|value| value.to_string())),
    )
}

pub fn int_array_param(name: &str, values: &[i64]) -> QueryParameter {
    parameter(
        name,
        array_type("INT64"),
        array_value(values.iter().map(i64::to_string)),
    )
}

/// Whether a failure is worth retrying later: the service or the network, not the statement.
pub fn is_transient(error: &BQError) -> bool {
    match error {
        BQError::RequestError(error) => error.is_timeout() || error.is_connect(),
        BQError::ResponseError { error } => {
            let reason = error
                .error
                .errors
                .first()
                .and_then(|details| details.get("reason"))
                .map(String::as_str);
            matches!(error.error.code, 408 | 429 | 500 | 502 | 503 | 504)
                || (error.error.code == 403
                    && matches!(reason, Some("rateLimitExceeded" | "backendError")))
        }
        BQError::TonicStatusError(status) => grpc_code_is_transient(status.code() as i32),
        BQError::TonicTransportError(_) | BQError::ConnectionPoolError(_) => true,
        _ => false,
    }
}

/// `google.rpc.Code` numbers, shared by gRPC statuses and the `error` of an append response:
/// CANCELLED is left out, the others are DEADLINE_EXCEEDED, RESOURCE_EXHAUSTED, ABORTED, INTERNAL
/// and UNAVAILABLE.
pub fn grpc_code_is_transient(code: i32) -> bool {
    matches!(code, 4 | 8 | 10 | 13 | 14)
}

pub fn map_error(operation: &str, error: BQError) -> anyhow::Error {
    if is_transient(&error) {
        StorageError::Unavailable(format!("{operation}: {error}")).into()
    } else {
        StorageError::OperationFailed {
            operation: format!("BigQuery {operation}"),
            details: error.to_string(),
        }
        .into()
    }
}

pub struct QueryOutput {
    pub pages: Vec<ResultSet>,
    /// Rows changed by a DML statement
    pub affected_rows: u64,
}

impl BigQueryStorage {
    /// The fully qualified, quoted name of a table of the dataset.
    pub(super) fn table(&self, name: &str) -> String {
        self.dataset.table(name)
    }

    pub(super) async fn run_query(
        &self,
        operation: &str,
        sql: String,
        params: Vec<QueryParameter>,
    ) -> Result<QueryOutput> {
        let mut request = QueryRequest::new(sql);
        if !params.is_empty() {
            request.query_parameters = Some(params);
            request.parameter_mode = Some("NAMED".to_string());
        }
        request.location = self.location.clone();
        request.maximum_bytes_billed = self.max_bytes_billed.map(|bytes| bytes.to_string());
        request.timeout_ms = Some(WAIT_MS);
        // A result is never served from the cache of an earlier identical query: a read must see
        // what a write just stored
        request.use_query_cache = Some(false);

        let job = self.client.job();
        let started = Instant::now();
        let mut response = job
            .query(&self.dataset.project_id, request)
            .await
            .map_err(|error| map_error(operation, error))?;

        // The job outlasted the wait: poll it until it is done
        while !response.job_complete.unwrap_or(false) {
            if started.elapsed() > DEADLINE {
                return Err(StorageError::Unavailable(format!(
                    "{operation}: the statement did not finish in {} seconds",
                    DEADLINE.as_secs()
                ))
                .into());
            }
            let reference = response
                .job_reference
                .as_ref()
                .ok_or_else(|| anyhow!("BigQuery gave no job reference for {operation}"))?;
            let job_id = reference
                .job_id
                .as_ref()
                .ok_or_else(|| anyhow!("BigQuery gave no job id for {operation}"))?;
            let results = job
                .get_query_results(
                    &self.dataset.project_id,
                    job_id,
                    GetQueryResultsParameters {
                        location: reference.location.clone(),
                        timeout_ms: Some(WAIT_MS),
                        ..Default::default()
                    },
                )
                .await
                .map_err(|error| map_error(operation, error))?;
            response = results.into();
        }

        let affected_rows = response
            .num_dml_affected_rows
            .as_deref()
            .and_then(|rows| rows.parse().ok())
            .unwrap_or(0);
        let reference = response.job_reference.clone();
        let mut next_page = response.page_token.clone();
        let mut pages = vec![ResultSet::new_from_query_response(response)];

        while let Some(page_token) = next_page.take() {
            let reference = reference
                .as_ref()
                .ok_or_else(|| anyhow!("BigQuery gave no job reference for {operation}"))?;
            let job_id = reference
                .job_id
                .as_ref()
                .ok_or_else(|| anyhow!("BigQuery gave no job id for {operation}"))?;
            let results = job
                .get_query_results(
                    &self.dataset.project_id,
                    job_id,
                    GetQueryResultsParameters {
                        location: reference.location.clone(),
                        page_token: Some(page_token),
                        timeout_ms: Some(WAIT_MS),
                        ..Default::default()
                    },
                )
                .await
                .map_err(|error| map_error(operation, error))?;
            if !results.job_complete.unwrap_or(false) {
                return Err(StorageError::Unavailable(format!(
                    "{operation}: BigQuery lost the rest of the result"
                ))
                .into());
            }
            next_page = results.page_token.clone();
            pages.push(ResultSet::new_from_get_query_results_response(results));
        }

        Ok(QueryOutput {
            pages,
            affected_rows,
        })
    }

    /// Run a query and convert each row of each page.
    pub(super) async fn query_rows<T>(
        &self,
        operation: &str,
        sql: String,
        params: Vec<QueryParameter>,
        mut read: impl FnMut(&ResultSet) -> Result<T>,
    ) -> Result<Vec<T>> {
        let output = self.run_query(operation, sql, params).await?;
        let mut rows = Vec::new();
        for mut page in output.pages {
            while page.next_row() {
                rows.push(read(&page)?);
            }
        }
        Ok(rows)
    }

    /// Run a statement that returns no rows, and say how many rows it changed.
    pub(super) async fn execute(
        &self,
        operation: &str,
        sql: String,
        params: Vec<QueryParameter>,
    ) -> Result<u64> {
        Ok(self.run_query(operation, sql, params).await?.affected_rows)
    }
}

/// The value of a column that is never null in SensApp's tables.
pub fn required<T>(value: std::result::Result<Option<T>, BQError>, column: &str) -> Result<T> {
    value?.ok_or_else(|| StorageError::missing_field(column, None, None).into())
}

#[cfg(test)]
mod tests {
    use super::*;
    use gcp_bigquery_client::error::{NestedResponseError, ResponseError};
    use std::collections::HashMap;

    fn http_error(code: i64, reason: &str) -> BQError {
        BQError::ResponseError {
            error: ResponseError {
                error: NestedResponseError {
                    code,
                    errors: vec![HashMap::from([("reason".to_string(), reason.to_string())])],
                    message: "message".to_string(),
                    status: String::new(),
                },
            },
        }
    }

    #[test]
    fn the_parameters_have_the_json_shape_of_the_api() {
        let request = {
            let mut request = QueryRequest::new("SELECT 1");
            request.query_parameters = Some(vec![
                string_param("name", "°C"),
                int_param("id", -5),
                int_array_param("ids", &[1, 2]),
            ]);
            serde_json::to_value(request).unwrap()
        };
        let parameters = &request["queryParameters"];
        assert_eq!(parameters[0]["name"], "name");
        assert_eq!(parameters[0]["parameterType"]["type"], "STRING");
        assert_eq!(parameters[0]["parameterValue"]["value"], "°C");
        assert_eq!(parameters[1]["parameterType"]["type"], "INT64");
        assert_eq!(parameters[1]["parameterValue"]["value"], "-5");
        assert_eq!(parameters[2]["parameterType"]["type"], "ARRAY");
        assert_eq!(parameters[2]["parameterType"]["arrayType"]["type"], "INT64");
        assert_eq!(
            parameters[2]["parameterValue"]["arrayValues"][1]["value"],
            "2"
        );
        assert_eq!(request["useLegacySql"], false);
    }

    #[test]
    fn an_empty_array_is_still_an_array() {
        let value = serde_json::to_value(int_array_param("ids", &[])).unwrap();
        assert_eq!(
            value["parameterValue"]["arrayValues"],
            serde_json::json!([])
        );
    }

    #[test]
    fn overload_and_outages_are_transient_bad_statements_are_not() {
        for code in [408, 429, 500, 502, 503, 504] {
            assert!(is_transient(&http_error(code, "x")), "{code}");
        }
        assert!(is_transient(&http_error(403, "rateLimitExceeded")));
        assert!(!is_transient(&http_error(403, "accessDenied")));
        assert!(!is_transient(&http_error(400, "invalidQuery")));
        assert!(!is_transient(&http_error(404, "notFound")));
        assert!(!is_transient(&BQError::NoToken));
        assert!(is_transient(&BQError::ConnectionPoolError("down".into())));
    }

    #[test]
    fn grpc_codes() {
        for code in [4, 8, 10, 13, 14] {
            assert!(grpc_code_is_transient(code), "{code}");
        }
        // INVALID_ARGUMENT, NOT_FOUND, PERMISSION_DENIED
        for code in [3, 5, 7] {
            assert!(!grpc_code_is_transient(code), "{code}");
        }
    }

    #[test]
    fn errors_are_sorted_for_the_http_layer() {
        let unavailable = map_error("read", http_error(503, "backendError"));
        assert!(matches!(
            unavailable.downcast_ref::<StorageError>(),
            Some(StorageError::Unavailable(_))
        ));
        let failed = map_error("read", http_error(400, "invalidQuery"));
        assert!(matches!(
            failed.downcast_ref::<StorageError>(),
            Some(StorageError::OperationFailed { .. })
        ));
    }
}
