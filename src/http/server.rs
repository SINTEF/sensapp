use super::app_error::AppError;
use super::auth::{require_delete_auth, require_read_auth, require_write_auth};
use super::backpressure::{WriteLimiter, limit_concurrent_writes};
use super::crud::{
    delete_series, delete_series_samples, get_series_availability, get_series_data,
    get_series_last_sample, list_metrics, list_series,
};
use super::influxdb::publish_influxdb;
use super::metrics::{prometheus_metrics, track_http_metrics};
use super::prometheus_read::prometheus_remote_read;
use super::prometheus_write::publish_prometheus;
use super::simple_promql::simple_promql_query;
use super::state::HttpServerState;
use crate::config;
use crate::http::crud::{
    __path_delete_series, __path_delete_series_samples, __path_get_series_availability,
    __path_get_series_data, __path_get_series_last_sample, __path_list_metrics, __path_list_series,
};
use crate::http::health::{__path_liveness, __path_readiness, liveness, readiness};
use crate::http::influxdb::__path_publish_influxdb;
use crate::http::metrics::__path_prometheus_metrics;
use crate::http::prometheus_read::__path_prometheus_remote_read;
use crate::http::prometheus_write::__path_publish_prometheus;
use crate::http::simple_promql::__path_simple_promql_query;
use crate::importers::csv::publish_csv_async;
use crate::storage::StorageInstance;
use anyhow::Result;
use axum::Json;
use axum::Router;
use axum::body::Body;
use axum::extract::DefaultBodyLimit;
use axum::extract::Request;
use axum::extract::State;
use axum::http::HeaderMap;
use axum::http::StatusCode;
use axum::http::header;
use axum::middleware::Next;
use axum::response::{IntoResponse, Response};
use axum::routing::delete;
use axum::routing::get;
use axum::routing::post;
use futures::TryStreamExt;
use serde::Serialize;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tower::ServiceBuilder;
use tower_http::classify::ServerErrorsFailureClass;
use tower_http::request_id::MakeRequestUuid;
use tower_http::trace;
use tower_http::{ServiceBuilderExt, timeout::TimeoutLayer, trace::TraceLayer};
use tracing::Level;
use utoipa::{OpenApi, ToSchema};
use utoipa_scalar::{Scalar, Servable as ScalarServable};

#[derive(OpenApi)]
#[openapi(
    tags(
        (name = "SensApp", description = "SensApp API"),
        (name = "InfluxDB", description = "InfluxDB Write API"),
        (name = "Observability", description = "Prometheus-compatible service metrics"),
        (name = "Prometheus", description = "Prometheus Remote Write and Read API"),
        (name = "Admin", description = "Administrative operations"),
        (name = "Health", description = "Health check endpoints"),
    ),
    paths(frontpage, publish_sensors_data, prometheus_metrics, list_metrics, list_series, get_series_data, get_series_last_sample, get_series_availability, delete_series, delete_series_samples, publish_influxdb, publish_prometheus, prometheus_remote_read, simple_promql_query, vacuum_database, liveness, readiness),
)]
struct ApiDoc;

/// The settings of the layers that protect the server.
#[derive(Clone, Debug)]
pub struct RouterSettings {
    /// Largest accepted request body, in bytes
    pub max_body_bytes: usize,
    /// Time a request may take before it is answered with a 504
    pub request_timeout: Duration,
    /// Time the maintenance request (the vacuum) may take before it is answered with a 504
    pub maintenance_timeout: Duration,
    /// Write requests handled at the same time, 0 for no limit
    pub max_concurrent_writes: usize,
}

impl RouterSettings {
    pub fn from_config(config: &config::SensAppConfig) -> Result<Self> {
        Ok(Self {
            max_body_bytes: config.parse_http_body_limit()?,
            request_timeout: Duration::from_secs(config.http_server_timeout_seconds),
            maintenance_timeout: Duration::from_secs(config.http_maintenance_timeout_seconds),
            max_concurrent_writes: config.http_max_concurrent_writes,
        })
    }
}

/// Answer with a 504 when a request takes longer than `duration`. The server was too slow (a hung
/// database, usually), not the client: 5xx is what Prometheus, Telegraf and friends retry, 408 is
/// not.
fn timeout_layer(duration: Duration) -> TimeoutLayer {
    TimeoutLayer::with_status_code(StatusCode::GATEWAY_TIMEOUT, duration)
}

/// The routes and the layers of the HTTP server, without a socket: the real thing, that the
/// tests run as well.
pub fn build_router(state: HttpServerState, settings: &RouterSettings) -> Router {
    let max_body_bytes = settings.max_body_bytes;
    let max_body_layer =
        axum::middleware::from_fn_with_state(max_body_bytes, enforce_request_body_limit);
    let bytes_body_layer = DefaultBodyLimit::max(max_body_bytes);
    let write_limiter = WriteLimiter::new(settings.max_concurrent_writes)
        .with_shed_counter(state.metrics.writes_shed_counter());

    // Initialize tracing
    // Note: tracing subscriber is initialized in main.rs

    // List of headers that shouldn't be logged
    let sensitive_headers: Arc<[_]> = vec![header::AUTHORIZATION, header::COOKIE].into();

    // Middleware creation
    // Every request gets an `x-request-id` (kept when the client or a proxy already sent one).
    // It is part of the request's log span and is echoed in the response headers, errors included.
    let middleware = ServiceBuilder::new()
        .set_x_request_id(MakeRequestUuid)
        .propagate_x_request_id()
        .sensitive_request_headers(sensitive_headers.clone())
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(|request: &Request| {
                    let request_id = request
                        .headers()
                        .get("x-request-id")
                        .and_then(|value| value.to_str().ok())
                        .unwrap_or_default();
                    tracing::span!(
                        Level::INFO,
                        "request",
                        method = %request.method(),
                        uri = %request.uri(),
                        version = ?request.version(),
                        request_id = %request_id,
                    )
                })
                .on_response(trace::DefaultOnResponse::new().level(Level::INFO))
                .on_failure(log_failed_response),
        )
        .sensitive_response_headers(sensitive_headers)
        .compression()
        .into_inner();

    // Create our application with route groups split by auth requirements.
    //
    // Public routes — always accessible (health checks, docs, prometheus scrape):
    let public_routes = Router::new()
        .route("/", get(frontpage))
        .merge(Scalar::with_url("/docs", ApiDoc::openapi()))
        .route("/prometheus/metrics", get(prometheus_metrics))
        .route("/health/live", get(liveness))
        .route("/health/ready", get(readiness));

    // Read-protected routes — require a valid JWT with "read" scope when auth is enabled:
    let read_routes = Router::new()
        .route("/metrics", get(list_metrics))
        .route("/series", get(list_series))
        .route("/series/{series_uuid}", get(get_series_data))
        .route("/series/{series_uuid}/last", get(get_series_last_sample))
        .route(
            "/series/{series_uuid}/availability",
            get(get_series_availability),
        )
        .route("/api/v1/query", get(simple_promql_query))
        .route(
            "/api/v1/prometheus_remote_read",
            post(prometheus_remote_read)
                .layer(bytes_body_layer)
                .layer(max_body_layer.clone()),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            state.auth.clone(),
            require_read_auth,
        ));

    // Write-protected routes — require a valid JWT with "write" scope when auth is enabled:
    let write_routes = Router::new()
        .route(
            "/publish",
            post(publish_sensors_data).layer(max_body_layer.clone()),
        )
        .route(
            "/api/v2/write",
            post(publish_influxdb)
                .layer(bytes_body_layer)
                .layer(max_body_layer.clone()),
        )
        .route(
            "/api/v1/prometheus_remote_write",
            post(publish_prometheus)
                .layer(bytes_body_layer)
                .layer(max_body_layer),
        )
        // Layers added later run first: authenticate, then take a write slot, then (per route)
        // buffer the body. Only authenticated writers hold slots, and nothing is buffered
        // before a slot is free.
        .route_layer(axum::middleware::from_fn_with_state(
            write_limiter,
            limit_concurrent_writes,
        ))
        .route_layer(axum::middleware::from_fn_with_state(
            state.auth.clone(),
            require_write_auth,
        ));

    // Delete-protected routes — require a valid JWT with "delete" scope when auth is enabled.
    // The delete scope is never part of the default "read write" scope.
    let delete_routes = Router::new()
        .route("/series/{series_uuid}", delete(delete_series))
        .route(
            "/series/{series_uuid}/samples",
            delete(delete_series_samples),
        )
        .route_layer(axum::middleware::from_fn_with_state(
            state.auth.clone(),
            require_delete_auth,
        ));

    // The maintenance route is as protected as the delete routes, but it is expected to be slow:
    // removing duplicates scans every value table, so it has a timeout of its own.
    let maintenance_routes = Router::new()
        .route("/api/v1/admin/vacuum", post(vacuum_database))
        .route_layer(axum::middleware::from_fn_with_state(
            state.auth.clone(),
            require_delete_auth,
        ))
        .layer(timeout_layer(settings.maintenance_timeout));

    public_routes
        .merge(read_routes)
        .merge(write_routes)
        .merge(delete_routes)
        .layer(timeout_layer(settings.request_timeout))
        .merge(maintenance_routes)
        .layer(axum::middleware::from_fn_with_state(
            state.metrics.clone(),
            track_http_metrics,
        ))
        .layer(middleware)
        .with_state(state)
}

pub async fn run_http_server(state: HttpServerState, address: SocketAddr) -> Result<()> {
    let settings = RouterSettings::from_config(&*config::get()?)?;
    let app = build_router(state, &settings);

    // Bind to the address with improved error handling
    let listener = match tokio::net::TcpListener::bind(address).await {
        Ok(listener) => listener,
        Err(e) => {
            return Err(anyhow::anyhow!(
                "Cannot start HTTP server on {}: {} (port {} may already be in use)",
                address,
                e,
                address.port()
            ));
        }
    };

    axum::serve(listener, app)
        .with_graceful_shutdown(shutdown_signal())
        .await?;

    Ok(())
}

/// Log a 5xx response, at the level that fits it. The default logs every one of them as an
/// error: under overload that is one error line per rejected write.
fn log_failed_response(
    failure: ServerErrorsFailureClass,
    latency: Duration,
    _span: &tracing::Span,
) {
    let latency_ms = latency.as_millis();
    match failure {
        // Load shedding is counted in `sensapp_http_requests_total{status="503"}`, and a
        // storage outage is logged as an error where it is detected
        ServerErrorsFailureClass::StatusCode(StatusCode::SERVICE_UNAVAILABLE) => {
            tracing::debug!(latency_ms, "service unavailable");
        }
        ServerErrorsFailureClass::StatusCode(StatusCode::GATEWAY_TIMEOUT) => {
            tracing::warn!(latency_ms, "request timed out");
        }
        failure => {
            tracing::error!(classification = %failure, latency_ms, "response failed");
        }
    }
}

async fn shutdown_signal() {
    // Wait for the CTRL+C signal
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install shutdown CTRL+C signal handler");
}

async fn enforce_request_body_limit(
    State(max_bytes): State<usize>,
    request: Request,
    next: Next,
) -> Response {
    let (parts, body) = request.into_parts();
    match axum::body::to_bytes(body, max_bytes).await {
        Ok(bytes) => {
            next.run(Request::from_parts(parts, Body::from(bytes)))
                .await
        }
        Err(_) => (
            StatusCode::PAYLOAD_TOO_LARGE,
            format!("Request body exceeds {max_bytes} bytes"),
        )
            .into_response(),
    }
}

#[utoipa::path(
    get,
    path = "/",
    tag = "SensApp",
    responses(
        (status = 200, description = "SensApp Frontpage", body = String)
    )
)]
async fn frontpage(State(state): State<HttpServerState>) -> Result<Json<String>, AppError> {
    let name: String = (*state.name).clone();
    Ok(Json(name))
}

/// SensApp native data ingestion API supporting multiple formats.
///
/// Accepts sensor data in one of the following formats:
/// - **SenML JSON** (RFC 8428): `Content-Type: application/json`
/// - **CSV**: `Content-Type: text/csv` or `application/csv`
/// - **Apache Arrow IPC**: `Content-Type: application/vnd.apache.arrow.stream`
///
/// If no Content-Type header is provided, defaults to CSV format.
#[utoipa::path(
    post,
    path = "/publish",
    tag = "SensApp",
    request_body(
        content = String,
        description = "Sensor data in SenML JSON, CSV, or Apache Arrow format"
    ),
    responses(
        (status = 200, description = "Data ingested successfully", body = String),
        (status = 400, description = "Bad Request - invalid data format", body = AppError),
        (status = 500, description = "Internal Server Error", body = AppError),
        (
            status = 503,
            description = "The storage backend is unavailable, or SensApp is busy writing and sheds the write. A `Retry-After` header (seconds, randomised) means the request was not processed at all and can be sent again after that delay",
            headers(("Retry-After" = u32, description = "Seconds to wait before sending the write again"))
        ),
        (status = 504, description = "The request took longer than SENSAPP_HTTP_SERVER_TIMEOUT_SECONDS, usually because the storage backend hangs")
    )
)]
async fn publish_sensors_data(
    State(state): State<HttpServerState>,
    access: Option<axum::Extension<crate::http::auth::AccessContext>>,
    headers: HeaderMap,
    body: axum::body::Body,
) -> Result<String, AppError> {
    let state = state.with_access(access.map(|extension| extension.0));
    let metrics = state.metrics.clone();
    let started = Instant::now();

    let result = async move {
        let content_type = headers
            .get(header::CONTENT_TYPE)
            .and_then(|v| v.to_str().ok())
            .unwrap_or("text/csv");

        match content_type {
            ct if ct.contains("application/json") => {
                publish_json_format(body, state.storage.clone()).await
            }
            ct if ct.contains("application/vnd.apache.arrow.stream")
                || ct.contains("application/vnd.apache.arrow.file") =>
            {
                publish_arrow_format(body, state.storage.clone()).await
            }
            ct if ct.contains("text/csv") || ct.contains("application/csv") => {
                publish_csv_format(body, state.storage.clone()).await
            }
            _ => publish_csv_format(body, state.storage.clone()).await,
        }
    }
    .await;

    metrics.observe_operation_result("write", "native_publish", started.elapsed(), result.is_ok());
    if let Ok(stats) = &result {
        metrics.observe_series("write", "native_publish", stats.series);
        metrics.observe_samples("write", "native_publish", stats.samples);
    }

    result?;
    Ok("ok".to_string())
}

/// Handle JSON data ingestion (SenML format)
async fn publish_json_format(
    body: axum::body::Body,
    storage: Arc<dyn StorageInstance>,
) -> Result<crate::importers::IngestionStats, AppError> {
    let body_bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .map_err(|e| AppError::bad_request(anyhow::anyhow!("Failed to read JSON body: {}", e)))?;

    let json_str = String::from_utf8(body_bytes.to_vec())
        .map_err(|e| AppError::bad_request(anyhow::anyhow!("Invalid UTF-8 in JSON: {}", e)))?;

    publish_senml_data(&json_str, storage).await
}

/// Handle Arrow data ingestion
async fn publish_arrow_format(
    body: axum::body::Body,
    storage: Arc<dyn StorageInstance>,
) -> Result<crate::importers::IngestionStats, AppError> {
    let body_bytes = axum::body::to_bytes(body, usize::MAX)
        .await
        .map_err(|e| AppError::bad_request(anyhow::anyhow!("Failed to read Arrow body: {}", e)))?;

    // Errors from parsing are caused by the payload
    let sensor_data_maps =
        crate::importers::arrow::parse_arrow_sensors(&body_bytes).map_err(AppError::bad_request)?;

    crate::importers::arrow::publish_arrow_sensors(sensor_data_maps, storage)
        .await
        .map_err(AppError::internal_server_error)
}

/// Handle CSV data ingestion
async fn publish_csv_format(
    body: axum::body::Body,
    storage: Arc<dyn StorageInstance>,
) -> Result<crate::importers::IngestionStats, AppError> {
    let stream = body.into_data_stream();
    let stream = stream.map_err(io::Error::other);
    let reader = stream.into_async_read();

    let csv_reader = csv_async::AsyncReaderBuilder::new()
        .has_headers(true)
        .delimiter(b',') // Use comma for standard CSV
        .create_reader(reader);

    publish_csv_async(csv_reader, storage)
        .await
        .map_err(AppError::internal_server_error)
}

/// Handle SenML JSON data ingestion
/// Expected format: SenML JSON (RFC 8428)
pub async fn publish_senml_data(
    json_str: &str,
    storage: Arc<dyn StorageInstance>,
) -> Result<crate::importers::IngestionStats, AppError> {
    use crate::datamodel::batch_builder::BatchBuilder;
    use crate::importers::senml::SenMLImporter;

    // Parse SenML JSON
    let sensor_data_list =
        SenMLImporter::from_senml_json(json_str).map_err(AppError::bad_request)?;

    if sensor_data_list.is_empty() {
        return Err(AppError::bad_request(anyhow::anyhow!(
            "SenML JSON contains no valid sensor data"
        )));
    }

    // Convert to SensApp format and publish
    let mut batch_builder = BatchBuilder::new().map_err(AppError::internal_server_error)?;
    let mut series = 0usize;
    let mut samples = 0usize;

    for (_sensor_name, sensor_data) in sensor_data_list {
        series += 1;
        samples += sensor_data.samples.len();
        let sensor = std::sync::Arc::new(sensor_data.sensor);

        batch_builder
            .add(sensor, sensor_data.samples)
            .await
            .map_err(AppError::internal_server_error)?;
    }

    batch_builder
        .send_what_is_left(storage)
        .await
        .map_err(AppError::internal_server_error)?;

    Ok(crate::importers::IngestionStats::new(series, samples))
}

/// What a vacuum did.
#[derive(Debug, Serialize, ToSchema)]
struct VacuumResponse {
    /// `ok` when the maintenance completed
    status: &'static str,
    /// Number of duplicate samples removed (same series, same timestamp, same value, the first
    /// one written is kept), or `null` when the storage backend cannot remove duplicates
    duplicates_removed: Option<u64>,
}

/// Database Vacuuming
///
/// Removes the duplicate samples (a retried write, a client that sends twice, a crash in the middle
/// of a request leave some), then cleans up and optimizes the database and reclaims space, as far as
/// the storage backend supports it. Only exact duplicates go: two different values at the same
/// timestamp are both kept. Requires the `delete` scope, and a token without a sensor allow list.
#[utoipa::path(
    post,
    path = "/api/v1/admin/vacuum",
    tag = "Admin",
    responses(
        (status = 200, description = "Maintenance completed, with the number of duplicate samples removed", body = VacuumResponse),
        (status = 403, description = "The token does not have the delete scope, or has a sensor allow list"),
        (status = 500, description = "Failed to vacuum database", body = String),
        (status = 503, description = "The storage backend is unavailable: retry later"),
        (status = 504, description = "The request took longer than SENSAPP_HTTP_MAINTENANCE_TIMEOUT_SECONDS (one hour by default), and the storage backend may carry on after the answer")
    )
)]
async fn vacuum_database(
    State(state): State<HttpServerState>,
    access: Option<axum::Extension<crate::http::auth::AccessContext>>,
) -> Result<Json<VacuumResponse>, AppError> {
    if access.is_some_and(|extension| extension.0.sensor_allow_list.is_some()) {
        return Err(AppError::Forbidden(
            "Sensor-scoped tokens cannot run database-wide maintenance".into(),
        ));
    }
    let duplicates_removed = match state.storage.deduplicate_samples().await {
        Ok(removed) => Some(removed),
        Err(error)
            if matches!(
                error.downcast_ref::<crate::storage::StorageError>(),
                Some(crate::storage::StorageError::Unsupported(_))
            ) =>
        {
            None
        }
        Err(error) => return Err(error.into()),
    };
    state.storage.vacuum().await?;
    Ok(Json(VacuumResponse {
        status: "ok",
        duplicates_removed,
    }))
}

#[cfg(test)]
mod tests {
    use crate::http::metrics::HttpMetrics;
    use axum::{
        body::Body,
        http::{Request, StatusCode},
    };
    use serial_test::serial;
    use tower::ServiceExt;

    use super::*;

    #[test]
    fn openapi_document_includes_core_routes() {
        let document = serde_json::to_value(ApiDoc::openapi()).expect("serialize OpenAPI document");
        let paths = document["paths"].as_object().expect("OpenAPI paths");

        for path in [
            "/publish",
            "/metrics",
            "/series/{series_uuid}",
            "/series/{series_uuid}/samples",
            "/api/v1/query",
            "/health/live",
        ] {
            assert!(paths.contains_key(path), "missing OpenAPI path: {path}");
        }
    }

    #[test]
    fn data_endpoints_document_overload_and_timeout_answers() {
        let document = serde_json::to_value(ApiDoc::openapi()).expect("serialize OpenAPI document");
        let paths = document["paths"].as_object().expect("OpenAPI paths");
        let not_data = [
            "/",
            "/prometheus/metrics",
            "/health/live",
            "/health/ready",
            "/docs",
        ];
        let writes = [
            ("/publish", "post"),
            ("/api/v2/write", "post"),
            ("/api/v1/prometheus_remote_write", "post"),
        ];

        for (path, operations) in paths {
            if not_data.contains(&path.as_str()) {
                continue;
            }
            for (method, operation) in operations.as_object().expect("operations") {
                let responses = &operation["responses"];
                for status in ["503", "504"] {
                    assert!(
                        responses.get(status).is_some(),
                        "{method} {path} does not document {status}"
                    );
                }
                let is_write = writes.contains(&(path.as_str(), method.as_str()));
                let has_retry_after = responses["503"]["headers"].get("Retry-After").is_some();
                assert_eq!(
                    has_retry_after, is_write,
                    "{method} {path}: Retry-After is documented on writes only"
                );
            }
        }
        // The endpoints that are written to are among the documented ones
        for (path, _) in writes {
            assert!(paths.contains_key(path), "missing OpenAPI path: {path}");
        }
    }

    /// Helper to get test database URL - uses the centralized constant from test_utils
    fn get_test_database_url() -> String {
        sensapp::test_utils::get_test_database_url()
    }

    fn storage_backend_test_available() -> bool {
        cfg!(feature = "postgres") || std::env::var("TEST_DATABASE_URL").is_ok()
    }

    #[tokio::test]
    #[serial]
    async fn test_frontpage_handler() {
        if !storage_backend_test_available() {
            return;
        }

        use crate::storage::storage_factory::create_storage_from_connection_string;

        let connection_string = get_test_database_url();
        sensapp::test_utils::ensure_test_database_exists(&connection_string)
            .await
            .expect("Failed to create test database");
        let storage = create_storage_from_connection_string(&connection_string)
            .await
            .expect("Failed to create storage");

        // Ensure database is up to date
        storage
            .create_or_migrate()
            .await
            .expect("Failed to run migrations");

        let state = HttpServerState {
            name: Arc::new("hello world".to_string()),
            storage,
            metrics: Arc::new(HttpMetrics::new()),
            influxdb_with_numeric: false,
            auth: None,
        };
        let app = Router::new().route("/", get(frontpage)).with_state(state);
        let request = Request::builder().uri("/").body(Body::empty()).unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        use axum::body::to_bytes;
        let body_str =
            String::from_utf8(to_bytes(response.into_body(), 128).await.unwrap().to_vec()).unwrap();
        assert_eq!(body_str, "\"hello world\"");
    }

    #[tokio::test]
    async fn raw_body_is_limited_before_handler_reads_it() {
        use axum::body::Body;
        use axum::http::Request;
        use tower::ServiceExt;

        let app = Router::new().route(
            "/publish",
            post(|body: Body| async move { axum::body::to_bytes(body, usize::MAX).await.unwrap() })
                .layer(axum::middleware::from_fn_with_state(
                    4usize,
                    enforce_request_body_limit,
                )),
        );
        let response = app
            .oneshot(Request::post("/publish").body(Body::from("12345")).unwrap())
            .await
            .unwrap();
        assert_eq!(response.status(), StatusCode::PAYLOAD_TOO_LARGE);
    }
}
