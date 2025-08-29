use super::app_error::AppError;
use super::crud::{get_series_data, list_metrics, list_series};
use super::influxdb::publish_influxdb;
use super::prometheus_read::prometheus_remote_read;
use super::prometheus_write::publish_prometheus;
use super::state::HttpServerState;
use crate::config;
use crate::storage::StorageInstance;
use anyhow::Result;
use axum::extract::DefaultBodyLimit;
//use axum::extract::Multipart;
//use axum::extract::Path;
use crate::ingestors::http::crud::{
    __path_get_series_data, __path_list_metrics, __path_list_series,
};
use crate::ingestors::http::influxdb::__path_publish_influxdb;
use crate::ingestors::http::prometheus_read::__path_prometheus_remote_read;
use crate::ingestors::http::prometheus_write::__path_publish_prometheus;
use axum::Json;
use axum::Router;
use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::http::header;
use axum::routing::get;
use axum::routing::post;
use futures::TryStreamExt;
use serde::Deserialize;
use std::io;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tower::ServiceBuilder;
use tower_http::trace;
use tower_http::{ServiceBuilderExt, timeout::TimeoutLayer, trace::TraceLayer};
use tracing::Level;
use utoipa::OpenApi;
use utoipa_scalar::{Scalar, Servable as ScalarServable};

#[derive(Deserialize)]
struct PublishParams {
    #[serde(default = "default_mode")]
    mode: String,
}

fn default_mode() -> String {
    "strict".to_string()
}

#[derive(Debug, Clone, Copy)]
pub enum ParseMode {
    Strict,
    Infer,
}

#[derive(OpenApi)]
#[openapi(
    tags(
        (name = "SensApp", description = "SensApp API"),
        (name = "InfluxDB", description = "InfluxDB Write API"),
        (name = "Prometheus", description = "Prometheus Remote Write and Read API"),
        (name = "Admin", description = "Administrative operations"),
    ),
    paths(frontpage, list_metrics, list_series, get_series_data, publish_influxdb, publish_prometheus, prometheus_remote_read, vacuum_database),
)]
struct ApiDoc;

pub async fn run_http_server(state: HttpServerState, address: SocketAddr) -> Result<()> {
    let config = config::get()?;
    let max_body_layer = DefaultBodyLimit::max(config.parse_http_body_limit()?);
    let timeout_seconds = config.http_server_timeout_seconds;

    // Initialize tracing
    // Note: tracing subscriber is initialized in main.rs

    // List of headers that shouldn't be logged
    let sensitive_headers: Arc<[_]> = vec![header::AUTHORIZATION, header::COOKIE].into();

    // Middleware creation
    let middleware = ServiceBuilder::new()
        //.layer(NewSentryLayer::<Request>::new_from_top())
        .sensitive_request_headers(sensitive_headers.clone())
        .layer(
            TraceLayer::new_for_http()
                .make_span_with(trace::DefaultMakeSpan::new().level(Level::INFO))
                .on_response(trace::DefaultOnResponse::new().level(Level::INFO)),
        )
        .sensitive_response_headers(sensitive_headers)
        .layer(TimeoutLayer::new(Duration::from_secs(timeout_seconds)))
        .compression()
        .into_inner();

    // Create our application with a single route
    let app = Router::new()
        .route("/", get(frontpage))
        //.route("/api-docs/openapi.json", get(openapi))
        .merge(Scalar::with_url("/docs", ApiDoc::openapi()))
        .route(
            "/sensors/publish",
            post(publish_sensors_data).layer(max_body_layer),
        )
        // Metrics and Series CRUD
        .route("/metrics", get(list_metrics))
        .route("/series", get(list_series))
        .route("/series/{series_uuid}", get(get_series_data))
        // InfluxDB Write API
        .route(
            "/api/v2/write",
            post(publish_influxdb).layer(max_body_layer),
        )
        // Prometheus Remote Write API
        .route(
            "/api/v1/prometheus_remote_write",
            post(publish_prometheus).layer(max_body_layer),
        )
        // Prometheus Remote Read API
        .route(
            "/api/v1/prometheus_remote_read",
            post(prometheus_remote_read).layer(max_body_layer),
        )
        // Admin API
        .route("/api/v1/admin/vacuum", post(vacuum_database))
        .layer(middleware)
        .with_state(state);

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

async fn shutdown_signal() {
    // Wait for the CTRL+C signal
    tokio::signal::ctrl_c()
        .await
        .expect("failed to install shutdown CTRL+C signal handler");
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

// #[utoipa::path(
//     get,
//     path = "/api-docs/openapi.json",
//     responses(
//         (status = 200, description = "OpenAPI JSON", body = ApiDoc)
//     )
// )]
// async fn openapi() -> Json<utoipa::openapi::OpenApi> {
//     Json(ApiDoc::openapi())
// }

async fn publish_sensors_data(
    State(state): State<HttpServerState>,
    Query(params): Query<PublishParams>,
    headers: HeaderMap,
    body: axum::body::Body,
) -> Result<String, AppError> {
    // Determine content type from headers
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|v| v.to_str().ok())
        .unwrap_or("text/csv"); // Default to CSV

    // Determine parse mode
    let parse_mode = match params.mode.as_str() {
        "strict" => ParseMode::Strict,
        "infer" => ParseMode::Infer,
        _ => {
            return Err(AppError::bad_request(anyhow::anyhow!(
                "Invalid mode parameter. Supported values: 'strict', 'infer'"
            )));
        }
    };

    match content_type {
        ct if ct.contains("application/json") => {
            publish_json_format(body, state.storage.clone()).await?;
        }
        ct if ct.contains("application/vnd.apache.arrow.file") => {
            publish_arrow_format(body, parse_mode, state.storage.clone()).await?;
        }
        ct if ct.contains("text/csv") || ct.contains("application/csv") => {
            publish_csv_format(body, parse_mode, state.storage.clone()).await?;
        }
        _ => {
            // Default to CSV for unknown content types
            publish_csv_format(body, parse_mode, state.storage.clone()).await?;
        }
    }

    Ok("ok".to_string())
}

/// Handle JSON data ingestion (SenML format)
async fn publish_json_format(
    body: axum::body::Body,
    storage: Arc<dyn StorageInstance>,
) -> Result<(), AppError> {
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
    _mode: ParseMode,
    storage: Arc<dyn StorageInstance>,
) -> Result<(), AppError> {
    let stream = body.into_data_stream();
    let stream = stream.map_err(io::Error::other);
    let reader = stream.into_async_read();

    crate::importers::arrow::publish_arrow_async(reader, storage)
        .await
        .map_err(AppError::internal_server_error)
}

/// Handle CSV data ingestion
async fn publish_csv_format(
    body: axum::body::Body,
    mode: ParseMode,
    storage: Arc<dyn StorageInstance>,
) -> Result<(), AppError> {
    let stream = body.into_data_stream();
    let stream = stream.map_err(io::Error::other);
    let reader = stream.into_async_read();

    let csv_reader = csv_async::AsyncReaderBuilder::new()
        .has_headers(true)
        .delimiter(b',') // Use comma for standard CSV
        .create_reader(reader);

    crate::importers::csv::publish_csv(csv_reader, 8192, mode, storage)
        .await
        .map_err(AppError::internal_server_error)
}

/// Handle SenML JSON data ingestion
/// Expected format: SenML JSON (RFC 8428)
pub async fn publish_senml_data(
    json_str: &str,
    storage: Arc<dyn StorageInstance>,
) -> Result<(), AppError> {
    use crate::datamodel::batch_builder::BatchBuilder;
    use crate::exporters::SenMLConverter;

    // Parse SenML JSON
    let sensor_data_list =
        SenMLConverter::from_senml_json(json_str).map_err(AppError::bad_request)?;

    if sensor_data_list.is_empty() {
        return Err(AppError::bad_request(anyhow::anyhow!(
            "SenML JSON contains no valid sensor data"
        )));
    }

    // Convert to SensApp format and publish
    let mut batch_builder = BatchBuilder::new().map_err(AppError::internal_server_error)?;

    for (_sensor_name, sensor_data) in sensor_data_list {
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

    Ok(())
}

#[utoipa::path(
    post,
    path = "/api/v1/admin/vacuum",
    tag = "Admin",
    responses(
        (status = 200, description = "Database vacuum completed successfully", body = String),
        (status = 500, description = "Failed to vacuum database", body = String)
    )
)]
async fn vacuum_database(State(state): State<HttpServerState>) -> Result<Json<String>, AppError> {
    state.storage.vacuum().await?;
    Ok(Json("Database vacuum completed successfully".to_string()))
}

#[cfg(test)]
#[cfg(feature = "sqlite")]
mod tests {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    use super::*;
    use crate::storage::sqlite::SqliteStorage;

    #[tokio::test]
    async fn test_handler() {
        let state = HttpServerState {
            name: Arc::new("hello world".to_string()),
            storage: Arc::new(SqliteStorage::connect("sqlite::memory:").await.unwrap()),
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
}
