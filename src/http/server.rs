use super::app_error::AppError;
use super::influxdb::publish_influxdb;
use super::prometheus::publish_prometheus;
use super::state::HttpServerState;
use crate::config;
use crate::importers::csv::publish_csv_async;
use anyhow::Result;
use axum::extract::DefaultBodyLimit;
//use axum::extract::Multipart;
//use axum::extract::Path;
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

pub async fn run_http_server(state: HttpServerState, address: SocketAddr) -> Result<()> {
    let config = config::get()?;
    let max_body_layer = DefaultBodyLimit::max(config.parse_http_body_limit()?);
    let timeout_seconds = config.http_server_timeout_seconds;

    // Initialize tracing
    tracing_subscriber::fmt()
        .with_target(false)
        .compact()
        .init();

    // List of headers that shouldn't be logged
    let sensitive_headers: Arc<[_]> = vec![header::AUTHORIZATION, header::COOKIE].into();

    // Middleware creation
    let middleware = ServiceBuilder::new()
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
        .route("/", get(handler))
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
        // InfluxDB Write API
        .route(
            "/api/v2/write",
            post(publish_influxdb).layer(max_body_layer.clone()),
        )
        // Prometheus Remote Write API
        .route(
            "/api/v1/prometheus_remote_write",
            post(publish_prometheus).layer(max_body_layer.clone()),
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

async fn handler(State(state): State<HttpServerState>) -> Result<Json<String>, AppError> {
    let name: String = (*state.name).clone();
    Ok(Json(name))
}

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

    publish_csv_async(csv_reader, 100, state.event_bus.clone()).await?;

    Ok("ok".to_string())
}

async fn publish_handler(bytes: Bytes) -> Result<Json<String>, (StatusCode, String)> {
    let cursor = Cursor::new(bytes);
    let df_result = CsvReader::new(cursor)
        .with_separator(b';')
        //.infer_schema(Some(128))
        //.with_dtypes(
        .has_header(true)
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

#[cfg(test)]
mod tests {
    use axum::{body::Body, http::Request};
    use tower::ServiceExt;

    use super::*;
    use crate::bus::EventBus;

    #[tokio::test]
    async fn test_handler() {
        let state = HttpServerState {
            name: Arc::new("hello world".to_string()),
            event_bus: Arc::new(EventBus::init("test".to_string())),
        };
        let app = Router::new().route("/", get(handler)).with_state(state);
        let request = Request::builder().uri("/").body(Body::empty()).unwrap();

        let response = app.oneshot(request).await.unwrap();

        assert_eq!(response.status(), StatusCode::OK);
        use axum::body::to_bytes;
        let body_str =
            String::from_utf8(to_bytes(response.into_body(), 128).await.unwrap().to_vec()).unwrap();
        assert_eq!(body_str, "\"hello world\"");
    }
}
