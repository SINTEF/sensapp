use super::app_error::AppError;
use super::crud::list_sensors;
use super::influxdb::publish_influxdb;
use super::prometheus::{prometheus_remote_read, prometheus_remote_write};
use super::senml::publish_senml;
use super::state::HttpServerState;
use crate::config;
use crate::importers::csv::publish_csv_async;
use anyhow::Result;
use axum::extract::DefaultBodyLimit;
//use axum::extract::Multipart;
//use axum::extract::Path;
use crate::ingestors::http::crud::__path_list_sensors;
use crate::ingestors::http::influxdb::__path_publish_influxdb;
use crate::ingestors::http::prometheus::{
    __path_prometheus_remote_read, __path_prometheus_remote_write,
};
use crate::ingestors::http::senml::__path_publish_senml;
use axum::extract::State;
use axum::http::header;
use axum::http::StatusCode;
use axum::routing::get;
use axum::routing::post;
use axum::Json;
use axum::Router;
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
use tower_http::{timeout::TimeoutLayer, trace::TraceLayer, ServiceBuilderExt};
use tracing::Level;
use utoipa::OpenApi;
use utoipa_scalar::{Scalar, Servable as ScalarServable};

#[derive(OpenApi)]
#[openapi(
    tags(
        (name = "SensApp", description = "SensApp API"),
        (name = "InfluxDB", description = "InfluxDB Write API"),
        (name = "Prometheus", description = "Prometheus Remote Write API"),
        (name = "SenML", description = "SenML API"),
    ),
    paths(frontpage, list_sensors, vacuum,
        publish_influxdb,
        prometheus_remote_read, prometheus_remote_write, publish_senml),
)]
struct ApiDoc;

pub async fn run_http_server(state: HttpServerState, address: SocketAddr) -> Result<()> {
    let config = config::get()?;
    let max_body_layer = DefaultBodyLimit::max(config.parse_http_body_limit()?);
    let timeout_seconds = config.http_server_timeout_seconds;

    // Initialize tracing
    /*tracing_subscriber::fmt()
    .with_target(false)
    .compact()
    .init();*/

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
            "/publish",
            post(publish_handler).layer(max_body_layer.clone()),
        )
        .route(
            "/sensors/:sensor_name_or_uuid/publish_csv",
            post(publish_csv),
        )
        .route(
            "/sensors/:sensor_name_or_uuid/publish_multipart",
            post(publish_multipart).layer(max_body_layer.clone()),
        )
        // Boring Sensor CRUD
        .route("/api/v1/sensors", get(list_sensors))
        // Administration
        .route("/admin/vacuum", post(vacuum))
        // InfluxDB Write API
        .route(
            "/api/v2/write",
            post(publish_influxdb).layer(max_body_layer.clone()),
        )
        // Prometheus Remote Read/Write API
        .route(
            "/api/v1/prometheus_remote_read",
            post(prometheus_remote_read).layer(max_body_layer.clone()),
        )
        .route(
            "/api/v1/prometheus_remote_write",
            post(prometheus_remote_write).layer(max_body_layer.clone()),
        )
        // SenML legacy SensApp API
        .route(
            "/api/v1/senml",
            post(publish_senml).layer(max_body_layer.clone()),
        )
        .layer(middleware)
        .with_state(state);

    // Run our application
    let listener = tokio::net::TcpListener::bind(address).await?;
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
    let stream = stream.map_err(|err| io::Error::new(io::ErrorKind::Other, err));
    let reader = stream.into_async_read();
    //let reader = BufReader::new(stream.into_async_read());
    // csv_async already uses a BufReader internally
    let csv_reader = csv_async::AsyncReaderBuilder::new()
        .has_headers(true)
        .delimiter(b';')
        .create_reader(reader);

    //publish_csv_async(csv_reader, 100, state.event_bus.clone()).await?;
    publish_csv_async(csv_reader, 8192, state.event_bus.clone()).await?;

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

async fn publish_multipart(/*mut multipart: Multipart*/
) -> Result<Json<String>, (StatusCode, String)> {
    Ok(Json("ok".to_string()))
}

/// Vacuum the database.
///
/// This is a very expensive operation, so it should be used with caution.
#[utoipa::path(
    post,
    path = "/admin/vacuum",
    tag = "SensApp",
    responses(
        (status = 204, description = "No Content"),
        (status = 400, description = "Bad Request", body = AppError),
        (status = 500, description = "Internal Server Error", body = AppError),
    )
)]
pub async fn vacuum(State(state): State<HttpServerState>) -> Result<StatusCode, AppError> {
    state.storage.vacuum().await?;
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    use super::*;
    use crate::{bus::EventBus, storage::sqlite::SqliteStorage};

    #[tokio::test]
    async fn test_handler() {
        let state = HttpServerState {
            name: Arc::new("hello world".to_string()),
            event_bus: Arc::new(EventBus::new()),
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
