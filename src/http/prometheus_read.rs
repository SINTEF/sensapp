use crate::datamodel::sensapp_datetime::SensAppDateTimeExt;
use crate::datamodel::{Sample, SensAppDateTime, SensAppVec, SensorData, TypedSamples};
use crate::parsing::prometheus::chunk_encoder::ChunkEncoder;
use crate::parsing::prometheus::converter::{build_prometheus_labels, sensor_data_to_timeseries};
use crate::parsing::prometheus::remote_read_models::{
    Query, QueryResult, ReadHints, ReadResponse, read_request::ResponseType,
};
use crate::parsing::prometheus::remote_read_parser::{
    parse_remote_read_request, serialize_read_response,
};
use crate::parsing::prometheus::remote_write_models::{Label, Sample as PromSample};
use crate::parsing::prometheus::stream_writer::StreamWriter;
use crate::storage::query::LabelMatcher;
use crate::storage::{Aggregation, SelectorLimitExceeded, SensorDataQueryOptions};

use super::{app_error::AppError, state::HttpServerState};
use axum::{
    debug_handler,
    extract::State,
    http::{HeaderMap, StatusCode},
    response::Response,
};
use std::collections::HashMap;
use std::time::Instant;
use tokio_util::bytes::Bytes;
use tracing::debug;

/// Prometheus does not trust a remote read to have answered the function of the hint: it
/// evaluates the query again on the samples it gets back. A hint is therefore only answered with
/// buckets when the second evaluation gives the result of the raw samples, which is the case of
/// the `*_over_time` functions that can be merged (min, max, sum, first, last, and the average
/// of buckets, exact when the buckets hold the same number of samples). `count_over_time` would
/// count the buckets, and the aggregation operators (`sum`, `avg`, ...) ask for the value of each
/// series at the evaluation time, not for an aggregate of the hour that follows it.
fn aggregation_from_read_hints(hints: &ReadHints) -> Option<Aggregation> {
    match hints.func.trim() {
        "avg_over_time" => Some(Aggregation::Avg),
        "min_over_time" => Some(Aggregation::Min),
        "max_over_time" => Some(Aggregation::Max),
        "sum_over_time" => Some(Aggregation::Sum),
        "first_over_time" => Some(Aggregation::First),
        "last_over_time" => Some(Aggregation::Last),
        _ => None,
    }
}

/// How far before an evaluation time Prometheus looks for the sample of an instant selector
/// (`--query.lookback-delta`, 5 minutes by default). The hints do not say it.
const DEFAULT_LOOKBACK_MS: i64 = 5 * 60 * 1000;

/// What Prometheus evaluates on the value of each series at the evaluation time: no function (a
/// plain selector), and the aggregation operators. The functions of a range vector or of a
/// subquery (`rate`, `max_over_time(x[1d:1h])`) are not in the list: their selector is not
/// evaluated on the grid of the query.
fn is_evaluated_at_each_step(func: &str) -> bool {
    matches!(
        func.trim(),
        "" | "sum"
            | "avg"
            | "min"
            | "max"
            | "count"
            | "group"
            | "stddev"
            | "stdvar"
            | "quantile"
            | "topk"
            | "bottomk"
            | "count_values"
    )
}

/// The options to read the latest sample of each step of a grid that ends at `grid_end_ms`, from
/// the step that holds `start_ms` to `read_end_ms` at most. The buckets of the storage start 1 ms
/// after the end of the one before, so each step ends on a point of the grid.
fn latest_options(
    start_ms: i64,
    grid_end_ms: i64,
    read_end_ms: i64,
    step_ms: i64,
    max_samples: usize,
) -> Option<SensorDataQueryOptions> {
    let after_end_ms = grid_end_ms.checked_add(1)?;
    let steps = after_end_ms
        .checked_sub(start_ms)?
        .checked_add(step_ms - 1)?
        .checked_div(step_ms)?;
    let origin_ms = after_end_ms.checked_sub(steps.checked_mul(step_ms)?)?;
    let end_ms = grid_end_ms.min(read_end_ms);
    if steps <= 0 || end_ms < origin_ms {
        return None;
    }

    Some(SensorDataQueryOptions {
        start_time: Some(SensAppDateTime::from_unix_milliseconds_i64(origin_ms)),
        end_time: Some(SensAppDateTime::from_unix_milliseconds_i64(end_ms)),
        limit: Some(max_samples.saturating_add(1)),
        step_ms: Some(step_ms),
        aggregation: Some(Aggregation::Latest),
        simplify: None,
    })
}

/// The options to answer a query evaluated at each step on the latest sample of each series, such
/// as `my_metric` or `sum(my_metric)`: Prometheus looks, at each evaluation time `t`, for the last
/// sample in `(t - lookback, t]`, and ignores the others. One sample per step is enough, the
/// last sample of the step, with its own timestamp: Prometheus then keeps it or drops it for being
/// too old exactly as it does with the raw samples.
///
/// The steps must end on the evaluation times, which the hints do not give, and there are two
/// cases they cannot tell apart:
///
/// - A range query evaluates from its start, every step. Its last evaluation is at the end of the
///   hints when the range of the query is a whole number of steps, which Grafana makes sure of.
///   Prometheus asks from `t - lookback + 1 ms` for the first evaluation `t`, so with its default
///   lookback `end - start + 1 - lookback` is then a whole number of steps: when it is not, the
///   raw samples answer.
/// - A subquery, `max_over_time(sum(x)[1h:5m])`, evaluates on the multiples of its step, whatever
///   the time of the query, and the step of the hints is the one of the subquery.
///
/// The two grids are the same when the end is a multiple of the step. Otherwise both are
/// answered: the latest samples of the steps of both grids are raw samples, and Prometheus finds
/// among them the latest sample of every evaluation time of either grid.
fn latest_options_from_read_hints(
    hints: &ReadHints,
    read_end_ms: i64,
    max_samples: usize,
) -> Vec<SensorDataQueryOptions> {
    let (start_ms, end_ms, step_ms) = (hints.start_ms, hints.end_ms, hints.step_ms);
    if hints.range_ms != 0
        || step_ms <= 0
        || end_ms < start_ms
        || !is_evaluated_at_each_step(&hints.func)
    {
        return Vec::new();
    }
    let evaluated = end_ms
        .checked_sub(start_ms)
        .and_then(|span| span.checked_add(1 - DEFAULT_LOOKBACK_MS));
    if evaluated.is_none_or(|evaluated| evaluated.rem_euclid(step_ms) != 0) {
        return Vec::new();
    }

    let Some(grid_of_the_end) = latest_options(start_ms, end_ms, read_end_ms, step_ms, max_samples)
    else {
        return Vec::new();
    };
    let mut options = vec![grid_of_the_end];
    let off_the_step = end_ms.rem_euclid(step_ms);
    if off_the_step != 0 {
        // No multiple of the step in the hints, no evaluation of a subquery to answer
        options.extend(latest_options(
            start_ms,
            end_ms - off_the_step,
            read_end_ms,
            step_ms,
            max_samples,
        ));
    }
    options
}

/// `range_ms` is the width of the `[1h]` of the query, `step_ms` the distance between two
/// evaluations. Prometheus starts the window at the end of the first range, so buckets of one
/// step that start there fit the windows of the evaluations when the range is a whole number of
/// steps. A shorter range would be answered with a bucket wider than the window it asks for.
///
/// The buckets start with the hints of the selector: the query starts with the earliest selector
/// of the expression (`max_over_time(x[2m]) > y`). The first evaluation must be a multiple of
/// the step, as the selector of a subquery (`max_over_time(max_over_time(x[1h])[1d:1h])`) is
/// evaluated on the multiples of the step of the subquery, and the hints do not say which one
/// they are. Grafana aligns its queries on the step.
fn bucket_options_from_read_hints(
    hints: &ReadHints,
    aggregation: Aggregation,
    read_end_ms: i64,
    max_samples: usize,
) -> Option<SensorDataQueryOptions> {
    let (start_ms, end_ms, step_ms, range_ms) =
        (hints.start_ms, hints.end_ms, hints.step_ms, hints.range_ms);
    if step_ms <= 0 || range_ms <= 0 || range_ms % step_ms != 0 {
        return None;
    }
    let first_evaluation_ms = start_ms.checked_add(range_ms - 1)?;
    let end_ms = end_ms.min(read_end_ms);
    if first_evaluation_ms.rem_euclid(step_ms) != 0 || end_ms < start_ms {
        return None;
    }

    Some(SensorDataQueryOptions {
        start_time: Some(SensAppDateTime::from_unix_milliseconds_i64(start_ms)),
        end_time: Some(SensAppDateTime::from_unix_milliseconds_i64(end_ms)),
        limit: Some(max_samples.saturating_add(1)),
        step_ms: Some(step_ms),
        aggregation: Some(aggregation),
        simplify: None,
    })
}

/// The reads that answer the hints of a query, and their answers are merged. None: the raw
/// samples answer. The end of the query, not of the hints, is where Prometheus stops reading
/// from the remote storage when it has the end in its own.
fn read_options_from_hints(query: &Query, max_samples: usize) -> Vec<SensorDataQueryOptions> {
    let Some(hints) = &query.hints else {
        return Vec::new();
    };
    match aggregation_from_read_hints(hints) {
        Some(aggregation) => {
            bucket_options_from_read_hints(hints, aggregation, query.end_timestamp_ms, max_samples)
                .into_iter()
                .collect()
        }
        None => latest_options_from_read_hints(hints, query.end_timestamp_ms, max_samples),
    }
}

/// Add the samples of `other` to the series of `series`, in time order and once each.
/// Prometheus reads numbers only.
fn merge_series(series: &mut Vec<SensorData>, other: Vec<SensorData>) {
    fn merge<V>(samples: &mut SensAppVec<Sample<V>>, other: SensAppVec<Sample<V>>) {
        samples.extend(other);
        samples.sort_by_key(|sample| sample.datetime);
        samples.dedup_by(|a, b| a.datetime == b.datetime);
    }

    let mut index: HashMap<uuid::Uuid, usize> = series
        .iter()
        .enumerate()
        .map(|(i, data)| (data.sensor.uuid, i))
        .collect();
    for data in other {
        let Some(&i) = index.get(&data.sensor.uuid) else {
            index.insert(data.sensor.uuid, series.len());
            series.push(data);
            continue;
        };
        match (&mut series[i].samples, data.samples) {
            (TypedSamples::Integer(samples), TypedSamples::Integer(other)) => merge(samples, other),
            (TypedSamples::Numeric(samples), TypedSamples::Numeric(other)) => merge(samples, other),
            (TypedSamples::Float(samples), TypedSamples::Float(other)) => merge(samples, other),
            _ => {}
        }
    }
}

async fn query_aggregated_for_prometheus(
    state: &HttpServerState,
    matchers: &[LabelMatcher],
    options: &SensorDataQueryOptions,
) -> Result<Vec<SensorData>, AppError> {
    let aggregated = state
        .storage
        .query_selector_aggregated(
            matchers,
            options,
            crate::http::limits::MAX_SELECTOR_SERIES,
            state.max_query_samples,
        )
        .await?;
    match aggregated {
        Ok(aggregated) => Ok(aggregated),
        Err(SelectorLimitExceeded::Series) => Err(AppError::bad_request(anyhow::anyhow!(
            "Remote read exceeds {} series; narrow the selector",
            crate::http::limits::MAX_SELECTOR_SERIES
        ))),
        Err(SelectorLimitExceeded::Samples) => Err(AppError::bad_request(anyhow::anyhow!(
            "Remote read exceeds {} samples in total",
            state.max_query_samples
        ))),
    }
}

async fn query_sensor_data_for_prometheus(
    state: &HttpServerState,
    query: &Query,
) -> Result<Vec<SensorData>, AppError> {
    let matchers = query
        .matchers
        .iter()
        .map(LabelMatcher::try_from)
        .collect::<Result<Vec<_>, _>>()
        .map_err(AppError::bad_request)?;
    let start_time = SensAppDateTime::from_unix_milliseconds_i64(query.start_timestamp_ms);
    let end_time = SensAppDateTime::from_unix_milliseconds_i64(query.end_timestamp_ms);

    let options = read_options_from_hints(query, state.max_query_samples);
    if !options.is_empty() {
        if let Some(hints) = &query.hints {
            debug!(
                "Prometheus remote read: applying hints func='{}' step={}ms in {} reads",
                hints.func,
                hints.step_ms,
                options.len()
            );
        }

        let mut answer = Vec::new();
        for options in &options {
            let part = query_aggregated_for_prometheus(state, &matchers, options).await?;
            merge_series(&mut answer, part);
        }
        crate::http::limits::validate_selector_result(&answer, state.max_query_samples)?;
        return Ok(answer);
    }

    let raw = crate::http::limits::query_selector_bounded(
        &state.storage,
        &matchers,
        Some(start_time),
        Some(end_time),
        true,
        state.max_query_samples,
    )
    .await;

    // Say why the samples were not aggregated: the query cannot be shortened by the client
    match (raw, &query.hints) {
        (Err(AppError::BadRequest(error)), Some(hints)) if hints.step_ms > 0 => {
            Err(AppError::bad_request(anyhow::anyhow!(
                "{error}. This query (function '{}', step {} ms, range {} ms) could not be answered \
                 with one value per step, so its raw samples were counted",
                hints.func,
                hints.step_ms,
                hints.range_ms
            )))
        }
        (raw, _) => raw,
    }
}

fn verify_read_headers(headers: &HeaderMap) -> Result<(), AppError> {
    // Check that we have the right content encoding, that must be snappy
    match headers.get("content-encoding") {
        Some(content_encoding) => match content_encoding.to_str() {
            Ok("snappy") | Ok("SNAPPY") => {}
            _ => {
                return Err(AppError::bad_request(anyhow::anyhow!(
                    "Unsupported content-encoding, must be snappy"
                )));
            }
        },
        None => {
            return Err(AppError::bad_request(anyhow::anyhow!(
                "Missing content-encoding header"
            )));
        }
    }

    // Check that the content type is protocol buffer
    match headers.get("content-type") {
        Some(content_type) => match content_type.to_str() {
            Ok("application/x-protobuf") | Ok("APPLICATION/X-PROTOBUF") => {}
            _ => {
                return Err(AppError::bad_request(anyhow::anyhow!(
                    "Unsupported content-type, must be application/x-protobuf"
                )));
            }
        },
        None => {
            return Err(AppError::bad_request(anyhow::anyhow!(
                "Missing content-type header"
            )));
        }
    }

    // Check that the remote read version is supported
    match headers.get("x-prometheus-remote-read-version") {
        Some(version) => match version.to_str() {
            Ok("0.1.0") => {}
            _ => {
                return Err(AppError::bad_request(anyhow::anyhow!(
                    "Unsupported x-prometheus-remote-read-version, must be 0.1.0"
                )));
            }
        },
        None => {
            return Err(AppError::bad_request(anyhow::anyhow!(
                "Missing x-prometheus-remote-read-version header"
            )));
        }
    }

    Ok(())
}

/// Prometheus Remote Read API.
///
/// Allows you to read data from SensApp using Prometheus remote read protocol.
///
/// It follows the [Prometheus Remote Read specification](https://prometheus.io/docs/prometheus/latest/querying/remote_read_api/).
#[utoipa::path(
    post,
    path = "/api/v1/prometheus_remote_read",
    security(("bearer" = ["read"])),
    tag = "Prometheus",
    request_body(
        content_type = "application/x-protobuf",
        description = "Prometheus Remote Read endpoint. [Reference](https://prometheus.io/docs/prometheus/latest/querying/remote_read_api/)",
    ),
    params(
        ("content-encoding" = String, Header, format = "snappy", description = "Content encoding, must be snappy"),
        ("content-type" = String, Header, format = "application/x-protobuf", description = "Content type, must be application/x-protobuf"),
        ("x-prometheus-remote-read-version" = String, Header, format = "0.1.0", description = "Prometheus Remote Read version, must be 0.1.0"),
    ),
    responses(
        (status = 200, description = "Read Response", content_type = "application/x-protobuf"),
        (status = 400, description = "Bad Request", body = AppError),
        (status = 500, description = "Internal Server Error", body = AppError),
        (status = 503, description = "The storage backend is unavailable: retry later"),
        (status = 504, description = "The request took longer than SENSAPP_HTTP_SERVER_TIMEOUT_SECONDS, usually because the storage backend hangs")
    )
)]
#[debug_handler]
pub async fn prometheus_remote_read(
    State(state): State<HttpServerState>,
    access: Option<axum::Extension<crate::http::auth::AccessContext>>,
    headers: HeaderMap,
    bytes: Bytes,
) -> Result<Response<axum::body::Body>, AppError> {
    let state = state.with_access(access.map(|extension| extension.0));
    let metrics = state.metrics.clone();
    let started = Instant::now();

    let result = async move {
        debug!("Prometheus remote read: received {} bytes", bytes.len());

        verify_read_headers(&headers)?;

        let read_request = parse_remote_read_request(&bytes).map_err(|e| {
            AppError::bad_request(anyhow::anyhow!("Failed to parse read request: {}", e))
        })?;

        debug!(
            "Prometheus remote read: processing {} queries",
            read_request.queries.len()
        );
        for (i, query) in read_request.queries.iter().enumerate() {
            debug!(
                "Query {}: time range {}ms - {}ms, matchers {:?}, hints {:?}",
                i, query.start_timestamp_ms, query.end_timestamp_ms, query.matchers, query.hints
            );
        }

        debug!(
            "Accepted response types: {:?}",
            read_request.accepted_response_types
        );

        let use_streaming = read_request
            .accepted_response_types
            .contains(&(ResponseType::StreamedXorChunks as i32));

        if use_streaming {
            handle_streamed_response(&state, &read_request).await
        } else {
            handle_samples_response(&state, &read_request).await
        }
    }
    .await;

    metrics.observe_operation_result(
        "read",
        "prometheus_remote_read",
        started.elapsed(),
        result.is_ok(),
    );
    if let Ok((_, series, samples)) = &result {
        metrics.observe_series("read", "prometheus_remote_read", *series);
        metrics.observe_samples("read", "prometheus_remote_read", *samples);
    }

    result.map(|(response, _, _)| response)
}

/// Handle standard SAMPLES response type
async fn handle_samples_response(
    state: &HttpServerState,
    read_request: &crate::parsing::prometheus::remote_read_models::ReadRequest,
) -> Result<(Response<axum::body::Body>, usize, usize), AppError> {
    let mut results = Vec::with_capacity(read_request.queries.len());
    let mut series_count = 0usize;
    let mut sample_count = 0usize;

    for query in &read_request.queries {
        let sensor_data = query_sensor_data_for_prometheus(state, query).await?;
        if series_count.saturating_add(sensor_data.len()) > crate::http::limits::MAX_SELECTOR_SERIES
        {
            return Err(AppError::bad_request(anyhow::anyhow!(
                "Remote read exceeds {} series in total",
                crate::http::limits::MAX_SELECTOR_SERIES
            )));
        }
        sample_count += sensor_data
            .iter()
            .map(|series| series.samples.len())
            .sum::<usize>();
        if sample_count > state.max_query_samples {
            return Err(AppError::bad_request(anyhow::anyhow!(
                "Remote read exceeds {} samples in total",
                state.max_query_samples
            )));
        }

        debug!("Query returned {} sensors", sensor_data.len());

        // Convert to Prometheus TimeSeries
        let mut timeseries: Vec<_> = sensor_data
            .iter()
            .filter_map(sensor_data_to_timeseries)
            .collect();
        timeseries.sort_by(|a, b| compare_label_sets(&a.labels, &b.labels));
        series_count += timeseries.len();

        results.push(QueryResult { timeseries });
    }

    let response = ReadResponse { results };

    // Serialize and compress the response
    let response_bytes = serialize_read_response(&response).map_err(|e| {
        AppError::internal_server_error(anyhow::anyhow!("Failed to serialize response: {}", e))
    })?;

    debug!(
        "Prometheus remote read: Returning SAMPLES response with {} bytes",
        response_bytes.len()
    );

    // Build HTTP response with appropriate headers
    let response = Response::builder()
        .status(StatusCode::OK)
        .header("content-type", "application/x-protobuf")
        .header("content-encoding", "snappy")
        .body(axum::body::Body::from(response_bytes))
        .map_err(|e| {
            AppError::internal_server_error(anyhow::anyhow!("Failed to build response: {}", e))
        })?;

    Ok((response, series_count, sample_count))
}

/// Handle STREAMED_XOR_CHUNKS response type
async fn handle_streamed_response(
    state: &HttpServerState,
    read_request: &crate::parsing::prometheus::remote_read_models::ReadRequest,
) -> Result<(Response<axum::body::Body>, usize, usize), AppError> {
    // Prometheus expects ONE ChunkedReadResponse per series, not one per query
    // Each message contains exactly one series
    let mut chunked_responses = Vec::new();
    let mut series_count = 0usize;
    let mut sample_count = 0usize;

    for (query_index, query) in read_request.queries.iter().enumerate() {
        let sensor_data = query_sensor_data_for_prometheus(state, query).await?;
        if series_count.saturating_add(sensor_data.len()) > crate::http::limits::MAX_SELECTOR_SERIES
        {
            return Err(AppError::bad_request(anyhow::anyhow!(
                "Remote read exceeds {} series in total",
                crate::http::limits::MAX_SELECTOR_SERIES
            )));
        }
        let query_samples = sensor_data
            .iter()
            .map(|series| series.samples.len())
            .sum::<usize>();
        if sample_count.saturating_add(query_samples) > state.max_query_samples {
            return Err(AppError::bad_request(anyhow::anyhow!(
                "Remote read exceeds {} samples in total",
                state.max_query_samples
            )));
        }

        debug!(
            "Query {} returned {} sensors",
            query_index,
            sensor_data.len()
        );

        // One message per series, in the order of their label sets
        let mut series: Vec<_> = sensor_data
            .iter()
            .map(|sd| (build_prometheus_labels(&sd.sensor), sd))
            .collect();
        series.sort_by(|(a, _), (b, _)| compare_label_sets(a, b));
        for (labels, sd) in series {
            // Non-numeric types are not read by Prometheus
            let Some(samples) = extract_prom_samples_for_chunks(sd) else {
                continue;
            };
            sample_count += samples.len();

            let chunked_series = ChunkEncoder::encode_series(labels, samples).map_err(|error| {
                AppError::internal_server_error(anyhow::anyhow!(
                    "Failed to encode the remote read chunks of {}: {error}",
                    sd.sensor.name
                ))
            })?;
            let chunked_response =
                ChunkEncoder::create_response(query_index as i64, vec![chunked_series]);
            chunked_responses.push(chunked_response);
            series_count += 1;
        }
    }

    // Create the streaming response body
    let body = StreamWriter::create_stream_body(&chunked_responses).map_err(|e| {
        AppError::internal_server_error(anyhow::anyhow!("Failed to create stream body: {}", e))
    })?;

    debug!(
        "Prometheus remote read: Returning STREAMED_XOR_CHUNKS response with {} bytes",
        body.len()
    );

    // Build HTTP response with appropriate headers for streaming
    let response = Response::builder()
        .status(StatusCode::OK)
        .header(
            "content-type",
            "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse",
        )
        .body(axum::body::Body::from(body))
        .map_err(|e| {
            AppError::internal_server_error(anyhow::anyhow!("Failed to build response: {}", e))
        })?;

    Ok((response, series_count, sample_count))
}

/// The order of Prometheus between two label sets sorted by name: label by label, the name then
/// the value, in byte order.
fn compare_label_sets(a: &[Label], b: &[Label]) -> std::cmp::Ordering {
    a.iter()
        .map(|label| (&label.name, &label.value))
        .cmp(b.iter().map(|label| (&label.name, &label.value)))
}

/// Extract Prometheus samples from SensorData for chunk encoding.
/// Returns None for non-numeric types.
fn extract_prom_samples_for_chunks(
    sensor_data: &crate::datamodel::SensorData,
) -> Option<Vec<PromSample>> {
    use crate::datamodel::TypedSamples;

    match &sensor_data.samples {
        TypedSamples::Float(samples) => {
            let prom_samples = samples
                .iter()
                .map(|s| PromSample {
                    value: s.value,
                    timestamp: crate::parsing::prometheus::converter::datetime_to_millis(
                        &s.datetime,
                    ),
                })
                .collect();
            Some(prom_samples)
        }
        TypedSamples::Integer(samples) => {
            let prom_samples = samples
                .iter()
                .map(|s| PromSample {
                    value: s.value as f64,
                    timestamp: crate::parsing::prometheus::converter::datetime_to_millis(
                        &s.datetime,
                    ),
                })
                .collect();
            Some(prom_samples)
        }
        TypedSamples::Numeric(samples) => {
            use rust_decimal::prelude::ToPrimitive;
            let prom_samples = samples
                .iter()
                .filter_map(|s| {
                    s.value.to_f64().map(|value| PromSample {
                        value,
                        timestamp: crate::parsing::prometheus::converter::datetime_to_millis(
                            &s.datetime,
                        ),
                    })
                })
                .collect();
            Some(prom_samples)
        }
        // Non-numeric types cannot be represented in Prometheus format
        TypedSamples::String(_)
        | TypedSamples::Boolean(_)
        | TypedSamples::Location(_)
        | TypedSamples::Blob(_)
        | TypedSamples::Json(_) => None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parsing::prometheus::remote_read_models::ReadHints;
    use axum::http::HeaderValue;

    fn create_test_headers() -> HeaderMap {
        let mut headers = HeaderMap::new();
        headers.insert("content-encoding", HeaderValue::from_static("snappy"));
        headers.insert(
            "content-type",
            HeaderValue::from_static("application/x-protobuf"),
        );
        headers.insert(
            "x-prometheus-remote-read-version",
            HeaderValue::from_static("0.1.0"),
        );
        headers
    }

    #[test]
    fn test_verify_read_headers_valid() {
        let headers = create_test_headers();
        assert!(verify_read_headers(&headers).is_ok());
    }

    #[test]
    fn test_verify_read_headers_missing_content_encoding() {
        let mut headers = create_test_headers();
        headers.remove("content-encoding");
        assert!(verify_read_headers(&headers).is_err());
    }

    #[test]
    fn test_verify_read_headers_invalid_content_encoding() {
        let mut headers = create_test_headers();
        headers.insert("content-encoding", HeaderValue::from_static("gzip"));
        assert!(verify_read_headers(&headers).is_err());
    }

    #[test]
    fn test_verify_read_headers_missing_content_type() {
        let mut headers = create_test_headers();
        headers.remove("content-type");
        assert!(verify_read_headers(&headers).is_err());
    }

    #[test]
    fn test_verify_read_headers_invalid_version() {
        let mut headers = create_test_headers();
        headers.insert(
            "x-prometheus-remote-read-version",
            HeaderValue::from_static("2.0.0"),
        );
        assert!(verify_read_headers(&headers).is_err());
    }

    /// The hints of `func(x[range])` evaluated every `step_ms` from 0 to 4 steps.
    fn hints(func: &str, step_ms: i64, range_ms: i64) -> ReadHints {
        ReadHints {
            step_ms,
            func: func.to_string(),
            start_ms: 1i64.saturating_sub(range_ms),
            end_ms: step_ms.saturating_mul(4),
            grouping: vec![],
            by: false,
            range_ms,
        }
    }

    fn query_with(hints: ReadHints) -> Query {
        Query {
            start_timestamp_ms: hints.start_ms,
            end_timestamp_ms: hints.end_ms,
            matchers: vec![],
            hints: Some(hints),
        }
    }

    #[test]
    fn test_aggregation_from_read_hints() {
        for (func, expected) in [
            ("avg_over_time", Aggregation::Avg),
            ("min_over_time", Aggregation::Min),
            ("max_over_time", Aggregation::Max),
            ("sum_over_time", Aggregation::Sum),
            ("first_over_time", Aggregation::First),
            ("last_over_time", Aggregation::Last),
        ] {
            assert_eq!(
                aggregation_from_read_hints(&hints(func, 60_000, 60_000)),
                Some(expected),
                "{func}"
            );
        }
    }

    #[test]
    fn test_hints_that_buckets_would_answer_wrongly_are_not_aggregated() {
        // Prometheus counts the buckets, evaluates the aggregation operators at the evaluation
        // time and cannot use buckets for a rate.
        for func in [
            "count_over_time",
            "count",
            "sum",
            "avg",
            "min",
            "max",
            "rate",
            "",
        ] {
            assert_eq!(
                aggregation_from_read_hints(&hints(func, 60_000, 60_000)),
                None,
                "{func:?}"
            );
        }
    }

    fn options_of(query: &Query) -> Option<SensorDataQueryOptions> {
        let mut options = read_options_from_hints(query, 100_000);
        assert!(options.len() <= 1, "{options:?}");
        options.pop()
    }

    fn millis(ms: i64) -> Option<SensAppDateTime> {
        Some(SensAppDateTime::from_unix_milliseconds_i64(ms))
    }

    #[test]
    fn test_query_options_from_read_hints_follow_the_range_and_the_step() {
        let options = options_of(&query_with(hints("max_over_time", 2_000, 2_000))).unwrap();
        assert_eq!(options.step_ms, Some(2_000));
        assert_eq!(options.aggregation, Some(Aggregation::Max));

        // Several steps in a range keep the buckets inside the windows.
        assert!(options_of(&query_with(hints("max_over_time", 2_000, 6_000))).is_some());

        // A range shorter than the step, a range that is not a whole number of steps, an instant
        // selector (no range) or no step cannot be answered with buckets.
        for (step_ms, range_ms) in [(2_000, 1_000), (2_000, 3_000), (2_000, 0), (0, 2_000)] {
            assert!(
                options_of(&query_with(hints("max_over_time", step_ms, range_ms))).is_none(),
                "step {step_ms} range {range_ms}"
            );
        }

        assert!(options_of(&query_with(hints("rate", 2_000, 2_000))).is_none());
        assert!(
            options_of(&Query {
                hints: None,
                ..query_with(hints("max_over_time", 2_000, 2_000))
            })
            .is_none()
        );
    }

    const HOUR_MS: i64 = 3_600_000;
    const T0_MS: i64 = 1_704_067_200_000;

    /// The hints of Prometheus 3 for `func(x)` evaluated every `step_ms` from `T0 + first_ms`
    /// to `T0 + last_ms`: the first window starts a lookback of 5 minutes before the evaluation.
    fn instant_hints(func: &str, step_ms: i64, first_ms: i64, last_ms: i64) -> ReadHints {
        ReadHints {
            step_ms,
            func: func.to_string(),
            start_ms: T0_MS + first_ms - 300_000 + 1,
            end_ms: T0_MS + last_ms,
            grouping: vec![],
            by: false,
            range_ms: 0,
        }
    }

    #[test]
    fn test_a_query_evaluated_at_each_step_is_answered_with_the_latest_sample_of_each_step() {
        // A plain selector and the aggregation operators, over 10 hours at a step of 1 hour
        for func in ["", "sum", "avg", "min", "max", "count", "group", "topk"] {
            let options = options_of(&query_with(instant_hints(func, HOUR_MS, 0, 10 * HOUR_MS)))
                .unwrap_or_else(|| panic!("{func:?}"));
            assert_eq!(options.aggregation, Some(Aggregation::Latest), "{func:?}");
            assert_eq!(options.step_ms, Some(HOUR_MS));
            // The steps end on the evaluation times: the last one ends at the last evaluation
            // (to the millisecond, as the buckets start 1 ms after the end of the one before)
            // and the first one is the hour before the first evaluation, which has the lookback
            assert_eq!(
                options.start_time,
                Some(SensAppDateTime::from_unix_milliseconds_i64(
                    T0_MS - HOUR_MS + 1
                ))
            );
            assert_eq!(
                options.end_time,
                Some(SensAppDateTime::from_unix_milliseconds_i64(
                    T0_MS + 10 * HOUR_MS
                ))
            );
        }
    }

    #[test]
    fn test_the_steps_of_the_latest_sample_cover_the_start_of_the_hints() {
        // The lookback of the first evaluation is longer than a step: the first step starts
        // before it, on the grid
        let hints = instant_hints("", 60_000, 0, 600_000);
        let (hints_start_ms, hints_end_ms) = (hints.start_ms, hints.end_ms);
        let options = options_of(&query_with(hints)).unwrap();
        let start = options.start_time.unwrap();
        let start_ms = start.to_unix_milliseconds().round() as i64;
        assert!(start_ms <= hints_start_ms);
        assert!(hints_start_ms - start_ms < 60_000);
        assert_eq!((hints_end_ms + 1 - start_ms) % 60_000, 0);
    }

    #[test]
    fn test_a_query_that_is_not_evaluated_on_a_grid_that_ends_with_the_hints_gets_the_raw_samples()
    {
        // The range of the query is not a whole number of steps: the last evaluation is not at
        // the end
        let mut hints = instant_hints("", HOUR_MS, 0, 10 * HOUR_MS);
        hints.end_ms += 30 * 60_000;
        assert!(options_of(&query_with(hints)).is_none());

        // Another lookback, or Prometheus 2, which asks from the evaluation time less the lookback
        let mut hints = instant_hints("", HOUR_MS, 0, 10 * HOUR_MS);
        hints.start_ms -= 300_000;
        assert!(options_of(&query_with(hints)).is_none());
        let mut hints = instant_hints("", HOUR_MS, 0, 10 * HOUR_MS);
        hints.start_ms -= 1;
        assert!(options_of(&query_with(hints)).is_none());

        // An instant query has no step, and the hints may not say where they end
        assert!(options_of(&query_with(instant_hints("", 0, 0, 0))).is_none());
        let mut hints = instant_hints("", HOUR_MS, 0, 10 * HOUR_MS);
        hints.end_ms = 0;
        assert!(options_of(&query_with(hints)).is_none());
    }

    #[test]
    fn test_the_functions_of_a_range_or_of_a_subquery_are_not_answered_with_the_latest_sample() {
        // `rate(x[5m])` has a range; a subquery has none but its selector is not evaluated on
        // the grid of the query
        for func in ["rate", "increase", "max_over_time", "avg_over_time", "abs"] {
            assert!(
                options_of(&query_with(instant_hints(func, HOUR_MS, 0, 10 * HOUR_MS))).is_none(),
                "{func:?}"
            );
        }
        let mut hints = instant_hints("", HOUR_MS, 0, 10 * HOUR_MS);
        hints.range_ms = 300_000;
        assert!(options_of(&query_with(hints)).is_none());
    }

    #[test]
    fn test_buckets_follow_the_hints_of_their_selector() {
        // `max_over_time(x[1h]) > y` every hour from T0 to T0 + 5h: the query starts at the
        // lookback of `y`, before the range of `x`, and the buckets of `x` start with its range
        let hints = ReadHints {
            start_ms: T0_MS - HOUR_MS + 1,
            end_ms: T0_MS + 5 * HOUR_MS,
            ..hints("max_over_time", HOUR_MS, HOUR_MS)
        };
        let query = Query {
            start_timestamp_ms: T0_MS - 300_000 + 1 - 7 * 60_000,
            end_timestamp_ms: T0_MS + 5 * HOUR_MS,
            matchers: vec![],
            hints: Some(hints),
        };
        let options = options_of(&query).unwrap();
        assert_eq!(options.start_time, millis(T0_MS - HOUR_MS + 1));
        assert_eq!(options.end_time, millis(T0_MS + 5 * HOUR_MS));

        // Prometheus reads the end from its own storage: the buckets stop where the query does
        let query = Query {
            end_timestamp_ms: T0_MS + 3 * HOUR_MS,
            ..query
        };
        let options = options_of(&query).unwrap();
        assert_eq!(options.start_time, millis(T0_MS - HOUR_MS + 1));
        assert_eq!(options.end_time, millis(T0_MS + 3 * HOUR_MS));
    }

    #[test]
    fn test_buckets_need_evaluations_on_multiples_of_the_step() {
        // A subquery, `max_over_time(max_over_time(x[1h])[1d:1h])`, evaluates `x` on the
        // multiples of its step, whatever the time of the query: from evaluations at half past
        // the hour, the hints cannot tell a range query from a subquery
        let hints = ReadHints {
            start_ms: T0_MS + 30 * 60_000 - HOUR_MS + 1,
            end_ms: T0_MS + 30 * 60_000 + 5 * HOUR_MS,
            ..hints("max_over_time", HOUR_MS, HOUR_MS)
        };
        assert!(options_of(&query_with(hints)).is_none());
    }

    #[test]
    fn test_the_latest_samples_follow_the_grid_of_a_subquery_too() {
        // `max_over_time(sum(x)[1h:5m])` at T0 + 1 min: `sum(x)` is evaluated on the multiples
        // of 5 minutes, and a range query that ends at T0 + 1 min every 5 minutes from T0 - 59
        // min gives the same hints. Both grids are answered.
        let step = 5 * 60_000;
        let hints = instant_hints("sum", step, 60_000 - HOUR_MS, 60_000);
        let options = read_options_from_hints(&query_with(hints), 100_000);
        assert_eq!(options.len(), 2, "{options:?}");
        assert_eq!(options[0].end_time, millis(T0_MS + 60_000));
        assert_eq!(
            options[0].start_time,
            millis(T0_MS + 60_000 - HOUR_MS - step + 1)
        );
        assert_eq!(options[1].end_time, millis(T0_MS));
        assert_eq!(options[1].start_time, millis(T0_MS - HOUR_MS - step + 1));
        for options in options {
            assert_eq!(options.aggregation, Some(Aggregation::Latest));
            assert_eq!(options.step_ms, Some(step));
        }

        // On the grid of the step, one answer
        let hints = instant_hints("sum", step, -HOUR_MS, 0);
        assert_eq!(
            read_options_from_hints(&query_with(hints), 100_000).len(),
            1
        );
    }

    #[test]
    fn test_hints_out_of_range_get_the_raw_samples() {
        for (start_ms, end_ms, step_ms, range_ms) in [
            (i64::MIN, i64::MAX, 1, 0),
            (i64::MIN, i64::MAX, HOUR_MS, 0),
            (i64::MAX - 1, i64::MAX, 1, 0),
            (i64::MIN, i64::MIN + 1, 1, 0),
            (i64::MAX - 1, i64::MAX, 1, 1),
            (i64::MIN, i64::MAX, i64::MAX, i64::MAX),
        ] {
            for func in ["", "sum", "max_over_time"] {
                let hints = ReadHints {
                    start_ms,
                    end_ms,
                    ..hints(func, step_ms, range_ms)
                };
                // No panic, no overflow: an answer or the raw samples
                let _ = read_options_from_hints(&query_with(hints), 100_000);
            }
        }
    }
}
