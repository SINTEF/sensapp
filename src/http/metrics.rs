use super::app_error::AppError;
use super::crud::{parse_selector_to_matchers, sensor_matches_matchers};
use super::state::HttpServerState;
use axum::body::Body;
use axum::extract::{MatchedPath, Query, Request, State};
use axum::http::{Method, StatusCode, header};
use axum::middleware::Next;
use axum::response::Response;
use prometheus_client::encoding::EncodeLabelSet;
use prometheus_client::encoding::text::encode;
use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::family::Family;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::{Histogram, exponential_buckets};
use prometheus_client::registry::Registry;
use serde::Deserialize;
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use crate::parsing::prometheus::converter::datetime_to_millis;

const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";
const LATEST_SERIES_PAGE_SIZE: usize = crate::storage::MAX_LIST_SERIES_LIMIT;

#[derive(Clone, Debug, Default, Deserialize)]
pub struct PrometheusMetricsQuery {
    #[serde(default)]
    pub include_latest_samples: bool,
    pub metric: Option<String>,
    pub selector: Option<String>,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct HttpRequestLabels {
    method: String,
    path: String,
    status: String,
}

#[derive(Clone, Debug, Hash, PartialEq, Eq, EncodeLabelSet)]
struct HttpRequestDurationLabels {
    method: String,
    path: String,
}

pub struct HttpMetrics {
    registry: Registry,
    http_requests_total: Family<HttpRequestLabels, Counter>,
    http_request_duration_seconds: Family<HttpRequestDurationLabels, Histogram>,
    http_requests_in_flight: Gauge,
    uptime_seconds: Gauge,
    storage_ready: Gauge,
    start_time: Instant,
}

impl fmt::Debug for HttpMetrics {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter
            .debug_struct("HttpMetrics")
            .finish_non_exhaustive()
    }
}

impl Default for HttpMetrics {
    fn default() -> Self {
        Self::new()
    }
}

impl HttpMetrics {
    pub fn new() -> Self {
        let mut registry = Registry::default();

        let http_requests_total = Family::<HttpRequestLabels, Counter>::default();
        registry.register(
            "sensapp_http_requests",
            "Total number of handled HTTP requests",
            http_requests_total.clone(),
        );

        let http_request_duration_seconds =
            Family::<HttpRequestDurationLabels, Histogram>::new_with_constructor(|| {
                Histogram::new(exponential_buckets(0.005, 2.0, 12))
            });
        registry.register(
            "sensapp_http_request_duration_seconds",
            "HTTP request duration in seconds",
            http_request_duration_seconds.clone(),
        );

        let http_requests_in_flight = Gauge::default();
        registry.register(
            "sensapp_http_requests_in_flight",
            "Number of HTTP requests currently being handled",
            http_requests_in_flight.clone(),
        );

        let uptime_seconds = Gauge::default();
        registry.register(
            "sensapp_uptime_seconds",
            "Process uptime in seconds",
            uptime_seconds.clone(),
        );

        let storage_ready = Gauge::default();
        registry.register(
            "sensapp_storage_ready",
            "Storage health status reported during the latest metrics scrape (1=ready, 0=not ready)",
            storage_ready.clone(),
        );

        Self {
            registry,
            http_requests_total,
            http_request_duration_seconds,
            http_requests_in_flight,
            uptime_seconds,
            storage_ready,
            start_time: Instant::now(),
        }
    }

    pub fn observe_http_request(
        &self,
        method: &Method,
        path: &str,
        status: StatusCode,
        duration: Duration,
    ) {
        self.http_requests_total
            .get_or_create(&HttpRequestLabels {
                method: method.to_string(),
                path: path.to_string(),
                status: status.as_u16().to_string(),
            })
            .inc();

        self.http_request_duration_seconds
            .get_or_create(&HttpRequestDurationLabels {
                method: method.to_string(),
                path: path.to_string(),
            })
            .observe(duration.as_secs_f64());
    }

    pub fn increment_in_flight(&self) {
        self.http_requests_in_flight.inc();
    }

    pub fn decrement_in_flight(&self) {
        self.http_requests_in_flight.dec();
    }

    pub fn render(&self, storage_is_ready: bool) -> Result<String, fmt::Error> {
        self.uptime_seconds.set(
            self.start_time
                .elapsed()
                .as_secs()
                .try_into()
                .unwrap_or(i64::MAX),
        );
        self.storage_ready.set(if storage_is_ready { 1 } else { 0 });

        let mut encoded = String::new();
        encode(&mut encoded, &self.registry)?;
        Ok(encoded)
    }
}

#[utoipa::path(
    get,
    path = "/prometheus/metrics",
    tag = "Observability",
    params(
        ("include_latest_samples" = Option<bool>, Query, description = "Append the most recent sample for Prometheus-compatible series"),
        ("metric" = Option<String>, Query, description = "Optional metric name filter used when include_latest_samples=true"),
        ("selector" = Option<String>, Query, description = "Optional PromQL-style label selector used when include_latest_samples=true")
    ),
    responses(
        (status = 200, description = "Prometheus-compatible metrics", body = String)
    )
)]
pub async fn prometheus_metrics(
    State(state): State<HttpServerState>,
    Query(query): Query<PrometheusMetricsQuery>,
) -> Result<Response, AppError> {
    let mut body = state
        .metrics
        .render(state.storage.health_check().await.is_ok())
        .map_err(AppError::internal_server_error)?;

    let latest_metrics = render_latest_sample_metrics(&state, &query).await?;
    if !latest_metrics.is_empty() {
        if !body.ends_with('\n') {
            body.push('\n');
        }
        body.push_str(&latest_metrics);
    }

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)
        .body(Body::from(body))
        .map_err(AppError::internal_server_error)
}

async fn render_latest_sample_metrics(
    state: &HttpServerState,
    query: &PrometheusMetricsQuery,
) -> Result<String, AppError> {
    if !query.include_latest_samples {
        return Ok(String::new());
    }

    let label_matchers = match &query.selector {
        Some(selector) => Some(parse_selector_to_matchers(selector)?),
        None => None,
    };

    let mut bookmark = None;
    let mut rendered = String::new();

    loop {
        let result = state
            .storage
            .list_series(
                query.metric.as_deref(),
                Some(LATEST_SERIES_PAGE_SIZE),
                bookmark.as_deref(),
            )
            .await?;

        for sensor in result.series {
            if !is_prometheus_scrape_compatible_sensor(&sensor) {
                continue;
            }

            if let Some(matchers) = &label_matchers
                && !sensor_matches_matchers(&sensor, matchers)
            {
                continue;
            }

            let Some(sensor_data) = state
                .storage
                .query_sensor_data_latest(&sensor.uuid.to_string(), None, None)
                .await?
            else {
                continue;
            };

            if let Some(line) = sensor_data_to_latest_prometheus_line(&sensor_data) {
                rendered.push_str(&line);
                rendered.push('\n');
            }
        }

        match result.bookmark {
            Some(next) => bookmark = Some(next),
            None => break,
        }
    }

    Ok(rendered)
}

fn is_prometheus_scrape_compatible_sensor(sensor: &crate::datamodel::Sensor) -> bool {
    matches!(
        sensor.sensor_type,
        crate::datamodel::SensorType::Integer
            | crate::datamodel::SensorType::Numeric
            | crate::datamodel::SensorType::Float
    ) && is_valid_prometheus_metric_name(&sensor.name)
        && sensor
            .labels
            .iter()
            .all(|(name, _)| is_valid_prometheus_label_name(name))
}

fn is_valid_prometheus_metric_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };

    if !(first.is_ascii_alphabetic() || first == '_' || first == ':') {
        return false;
    }

    chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_' || ch == ':')
}

fn is_valid_prometheus_label_name(name: &str) -> bool {
    let mut chars = name.chars();
    let Some(first) = chars.next() else {
        return false;
    };

    if name.starts_with("__") || !(first.is_ascii_alphabetic() || first == '_') {
        return false;
    }

    chars.all(|ch| ch.is_ascii_alphanumeric() || ch == '_')
}

fn sensor_data_to_latest_prometheus_line(
    sensor_data: &crate::datamodel::SensorData,
) -> Option<String> {
    let labels =
        crate::parsing::prometheus::converter::build_prometheus_labels(&sensor_data.sensor);
    let metric_name = labels
        .iter()
        .find(|label| label.name == "__name__")?
        .value
        .clone();
    let (value, timestamp_ms) = latest_sample_value_and_timestamp(&sensor_data.samples)?;

    let rendered_labels = labels
        .into_iter()
        .filter(|label| label.name != "__name__")
        .map(|label| {
            format!(
                r#"{}="{}""#,
                label.name,
                escape_prometheus_label_value(&label.value)
            )
        })
        .collect::<Vec<_>>()
        .join(",");

    let sample_value = format_prometheus_sample_value(value);
    if rendered_labels.is_empty() {
        Some(format!("{} {} {}", metric_name, sample_value, timestamp_ms))
    } else {
        Some(format!(
            "{}{{{}}} {} {}",
            metric_name, rendered_labels, sample_value, timestamp_ms
        ))
    }
}

fn latest_sample_value_and_timestamp(
    samples: &crate::datamodel::TypedSamples,
) -> Option<(f64, i64)> {
    match samples {
        crate::datamodel::TypedSamples::Float(values) => values
            .last()
            .map(|sample| (sample.value, datetime_to_millis(&sample.datetime))),
        crate::datamodel::TypedSamples::Integer(values) => values
            .last()
            .map(|sample| (sample.value as f64, datetime_to_millis(&sample.datetime))),
        crate::datamodel::TypedSamples::Numeric(values) => values.last().and_then(|sample| {
            use rust_decimal::prelude::ToPrimitive;

            sample
                .value
                .to_f64()
                .map(|value| (value, datetime_to_millis(&sample.datetime)))
        }),
        crate::datamodel::TypedSamples::String(_)
        | crate::datamodel::TypedSamples::Boolean(_)
        | crate::datamodel::TypedSamples::Location(_)
        | crate::datamodel::TypedSamples::Blob(_)
        | crate::datamodel::TypedSamples::Json(_) => None,
    }
}

fn escape_prometheus_label_value(value: &str) -> String {
    value
        .replace('\\', r#"\\"#)
        .replace('\n', r#"\n"#)
        .replace('"', r#"\""#)
}

fn format_prometheus_sample_value(value: f64) -> String {
    if value.is_nan() {
        "NaN".to_string()
    } else if value == f64::INFINITY {
        "+Inf".to_string()
    } else if value == f64::NEG_INFINITY {
        "-Inf".to_string()
    } else {
        value.to_string()
    }
}

pub async fn track_http_metrics(
    State(metrics): State<Arc<HttpMetrics>>,
    request: Request,
    next: Next,
) -> Response {
    let method = request.method().clone();
    let path = request
        .extensions()
        .get::<MatchedPath>()
        .map(|matched_path| matched_path.as_str().to_string())
        .unwrap_or_else(|| request.uri().path().to_string());

    if path == "/prometheus/metrics" {
        return next.run(request).await;
    }

    metrics.increment_in_flight();
    let start = Instant::now();
    let response = next.run(request).await;
    metrics.decrement_in_flight();
    metrics.observe_http_request(&method, &path, response.status(), start.elapsed());
    response
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datamodel::{Sample, SensAppDateTime, Sensor, SensorType, TypedSamples};
    use smallvec::smallvec;

    fn create_sensor(name: &str, sensor_type: SensorType, labels: Vec<(&str, &str)>) -> Sensor {
        Sensor::new_without_uuid(
            name.to_string(),
            sensor_type,
            None,
            Some(
                labels
                    .into_iter()
                    .map(|(key, value)| (key.to_string(), value.to_string()))
                    .collect(),
            ),
        )
        .unwrap()
    }

    #[test]
    fn metrics_render_contains_registered_metrics() {
        let metrics = HttpMetrics::new();
        metrics.observe_http_request(
            &Method::GET,
            "/health/live",
            StatusCode::OK,
            Duration::from_millis(5),
        );
        let body = metrics.render(true).unwrap();

        assert!(body.contains("sensapp_http_requests_total"));
        assert!(body.contains("sensapp_uptime_seconds"));
        assert!(body.contains("sensapp_storage_ready 1"));
    }

    #[test]
    fn scrape_compatibility_requires_numeric_sensor_and_valid_names() {
        let compatible = create_sensor(
            "room_temperature_celsius",
            SensorType::Float,
            vec![("room", "lab")],
        );
        let invalid_metric = create_sensor("demo-temperature", SensorType::Float, vec![]);
        let invalid_label = create_sensor(
            "room_temperature_celsius",
            SensorType::Float,
            vec![("bad-label", "lab")],
        );
        let string_sensor = create_sensor("status_text", SensorType::String, vec![]);

        assert!(is_prometheus_scrape_compatible_sensor(&compatible));
        assert!(!is_prometheus_scrape_compatible_sensor(&invalid_metric));
        assert!(!is_prometheus_scrape_compatible_sensor(&invalid_label));
        assert!(!is_prometheus_scrape_compatible_sensor(&string_sensor));
    }

    #[test]
    fn latest_prometheus_line_uses_original_timestamp() {
        let sensor = create_sensor(
            "room_temperature_celsius",
            SensorType::Float,
            vec![("room", "lab")],
        );
        let sensor_data = crate::datamodel::SensorData {
            sensor,
            samples: TypedSamples::Float(smallvec![Sample {
                datetime: SensAppDateTime::from_unix_seconds(1_704_067_201.0),
                value: 42.5,
            }]),
        };

        let line = sensor_data_to_latest_prometheus_line(&sensor_data).unwrap();
        assert_eq!(
            line,
            r#"room_temperature_celsius{room="lab"} 42.5 1704067201000"#
        );
    }
}
