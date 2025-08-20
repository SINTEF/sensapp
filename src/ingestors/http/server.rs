use super::app_error::AppError;
use super::crud::{get_series_data, list_series, list_metrics};
use super::influxdb::publish_influxdb;
use super::prometheus::publish_prometheus;
use super::state::HttpServerState;
use crate::config;
use crate::importers::csv::publish_csv_async;
use anyhow::Result;
use axum::extract::DefaultBodyLimit;
//use axum::extract::Multipart;
//use axum::extract::Path;
use crate::ingestors::http::crud::{__path_get_series_data, __path_list_series, __path_list_metrics};
use crate::ingestors::http::influxdb::__path_publish_influxdb;
use crate::ingestors::http::prometheus::__path_publish_prometheus;
use axum::Json;
use axum::Router;
use axum::extract::State;
use axum::http::StatusCode;
use axum::http::header;
use axum::routing::get;
use axum::routing::post;
use futures::TryStreamExt;
use polars::prelude::*;
use std::io;
use std::io::Cursor;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio_util::bytes::Bytes;
use tower::ServiceBuilder;
use tower_http::trace;
use tower_http::{ServiceBuilderExt, timeout::TimeoutLayer, trace::TraceLayer};
use tracing::Level;
use utoipa::OpenApi;
use utoipa_scalar::{Scalar, Servable as ScalarServable};

#[derive(OpenApi)]
#[openapi(
    tags(
        (name = "SensApp", description = "SensApp API"),
        (name = "InfluxDB", description = "InfluxDB Write API"),
        (name = "Prometheus", description = "Prometheus Remote Write API"),
    ),
    paths(frontpage, list_metrics, list_series, get_series_data, publish_influxdb, publish_prometheus),
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
        .route("/publish", post(publish_handler).layer(max_body_layer))
        .route(
            "/sensors/{sensor_name_or_uuid}/publish_csv",
            post(publish_csv),
        )
        .route(
            "/sensors/{sensor_name_or_uuid}/publish_multipart",
            post(publish_multipart).layer(max_body_layer),
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

async fn publish_csv(
    State(state): State<HttpServerState>,
    //Path(sensor_name_or_uuid): Path<String>,
    body: axum::body::Body,
) -> Result<String, AppError> {
    // let uuid = name_to_uuid(sensor_name_or_uuid.as_str())?;
    // Convert the body in a stream
    let stream = body.into_data_stream();
    let stream = stream.map_err(io::Error::other);
    let reader = stream.into_async_read();
    //let reader = BufReader::new(stream.into_async_read());
    // csv_async already uses a BufReader internally
    let csv_reader = csv_async::AsyncReaderBuilder::new()
        .has_headers(true)
        .delimiter(b';')
        .create_reader(reader);

    //publish_csv_async(csv_reader, 100, state.storage.clone()).await?;
    publish_csv_async(csv_reader, 8192, state.storage.clone()).await?;

    Ok("ok".to_string())
}

async fn publish_handler(bytes: Bytes) -> Result<Json<String>, (StatusCode, String)> {
    let cursor = Cursor::new(bytes);
    let df_result = CsvReadOptions::default()
        .with_has_header(true)
        .with_parse_options(CsvParseOptions::default().with_separator(b';'))
        .into_reader_with_file_handle(cursor)
        .finish();

    // print the schema
    let schema = df_result.as_ref().unwrap().schema();
    println!("{:?}", schema);

    match df_result {
        Ok(df) => Ok(Json(format!("Number of rows: {}", df.height()))),
        Err(_) => Err((
            StatusCode::INTERNAL_SERVER_ERROR,
            "Error reading CSV".to_string(),
        )),
    }
}

async fn publish_multipart(/*mut multipart: Multipart*/)
 -> Result<Json<String>, (StatusCode, String)> {
    Ok(Json("ok".to_string()))
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
