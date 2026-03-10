use super::app_error::AppError;
use super::state::HttpServerState;
use axum::body::Body;
use axum::extract::{MatchedPath, Request, State};
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
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

const PROMETHEUS_CONTENT_TYPE: &str = "text/plain; version=0.0.4; charset=utf-8";

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
    responses(
        (status = 200, description = "Prometheus-compatible metrics", body = String)
    )
)]
pub async fn prometheus_metrics(
    State(state): State<HttpServerState>,
) -> Result<Response, AppError> {
    let body = state
        .metrics
        .render(state.storage.health_check().await.is_ok())
        .map_err(AppError::internal_server_error)?;

    Response::builder()
        .status(StatusCode::OK)
        .header(header::CONTENT_TYPE, PROMETHEUS_CONTENT_TYPE)
        .body(Body::from(body))
        .map_err(AppError::internal_server_error)
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
}
