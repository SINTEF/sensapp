use anyhow::Result;
use axum::extract::DefaultBodyLimit;
use axum::extract::State;
use axum::http::header;
use axum::http::StatusCode;
use axum::response::IntoResponse;
use axum::response::Response;
use axum::routing::get;
use axum::routing::post;
use axum::Json;
use axum::Router;
use futures::io::BufReader;
use futures::stream::StreamExt;
use futures::TryStreamExt;
use polars::prelude::*;
use sqlx::any;
use std::io;
use std::io::Cursor;
use std::net::IpAddr;
use std::net::Ipv4Addr;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;
use tokio_stream::wrappers::ReceiverStream;
use tokio_util::bytes::Bytes;
use tower::ServiceBuilder;
use tower_http::trace;
use tower_http::{timeout::TimeoutLayer, trace::TraceLayer, ServiceBuilderExt};
use tracing::Level;

use super::state::HttpServerState;
use crate::importers::csv::publish_csv_async;

// Anyhow error handling with axum
// https://github.com/tokio-rs/axum/blob/d3112a40d55f123bc5e65f995e2068e245f12055/examples/anyhow-error-response/src/main.rs
struct AppError(anyhow::Error);
impl IntoResponse for AppError {
    fn into_response(self) -> Response {
        (
            StatusCode::INTERNAL_SERVER_ERROR,
            format!("Something went wrong: {}", self.0),
        )
            .into_response()
    }
}
impl<E> From<E> for AppError
where
    E: Into<anyhow::Error>,
{
    fn from(err: E) -> Self {
        Self(err.into())
    }
}

pub async fn run_http_server(state: HttpServerState, address: SocketAddr) -> Result<()> {
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
        .layer(TimeoutLayer::new(Duration::from_secs(10)))
        .compression()
        .into_inner();

    // Create our application with a single route
    let app = Router::new()
        .route("/", get(handler))
        .route(
            "/publish",
            post(publish_handler).layer(DefaultBodyLimit::max(1024 * 1024 * 1024)),
        )
        //.route("/publish_stream", post(publish_stream_handler))
        .route("/publish_csv", post(publish_csv))
        .route("/fail", get(test_fail))
        .layer(middleware)
        .with_state(state);

    // Run our application
    let listener = tokio::net::TcpListener::bind(address).await?;
    axum::serve(listener, app).await?;

    Ok(())
}

async fn handler(State(state): State<HttpServerState>) -> Result<Json<String>, AppError> {
    //EVENT_BUS.get().unwrap().publish(42).await.unwrap();
    //state.event_bus.publish(42).await.unwrap();
    // let mut sync_receiver = state.event_bus.sync_request().await?;
    // sync_receiver.recv().await?;

    Ok(Json(state.name))
}

async fn publish_csv(
    State(state): State<HttpServerState>,
    body: axum::body::Body,
) -> Result<String, AppError> {
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

fn try_thing() -> Result<()> {
    anyhow::bail!("Test error");
}

async fn test_fail() -> Result<String, AppError> {
    // wait 3s
    tokio::time::sleep(Duration::from_secs(3)).await;
    try_thing()?;
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
