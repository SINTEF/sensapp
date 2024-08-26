use super::utils::get_potentiall_compressed_data;
use super::{app_error::AppError, state::HttpServerState};
use crate::parsing::senml::SenMLParser;
use crate::{datamodel::batch_builder::BatchBuilder, parsing::compressed::CompressedParser};
use anyhow::Result;
use axum::{
    debug_handler,
    extract::State,
    http::{HeaderMap, StatusCode},
};
use std::str;
use tokio_util::bytes::Bytes;

/// SenML JSON Write API.
///
/// Push SenML JSON data to SensApp.
///
/// SenML data can be compressed. Use the `Content-Encoding` header to specify the compression algorithm.
/// `snappy`, `gzip` and `zstd` are supported.
///
/// [SenML](https://www.rfc-editor.org/rfc/rfc8428) is a proposed standard for exchanging time-series data.
#[utoipa::path(
    post,
    path = "/api/v1/senml",
    tag = "SenML",
    request_body(
        content = String,
        content_type = "text/plain",
        description = "SenML data. [Reference](https://www.rfc-editor.org/rfc/rfc8428).",
        example = "[{\"n\":\"urn:dev:ow:10e2073a01080063\",\"u\":\"Cel\",\"v\":23.1}]"
    ),
    responses(
        (status = 204, description = "No Content"),
        (status = 400, description = "Bad Request", body = AppError),
        (status = 500, description = "Internal Server Error", body = AppError),
    )
)]
#[debug_handler]
pub async fn publish_senml(
    State(state): State<HttpServerState>,
    headers: HeaderMap,
    bytes: Bytes,
) -> Result<StatusCode, AppError> {
    let mut batch_builder = BatchBuilder::new()?;
    let parser = CompressedParser::new_if_needed(
        Box::new(SenMLParser),
        get_potentiall_compressed_data(&headers)?,
    );
    parser
        .parse_data(&bytes, None, &mut batch_builder)
        .await
        .map_err(|error| AppError::BadRequest(anyhow::anyhow!(error)))?;

    match batch_builder.send_what_is_left(state.event_bus).await {
        Ok(Some(mut receiver)) => {
            receiver.wait().await?;
        }
        Ok(None) => {}
        Err(error) => {
            return Err(AppError::InternalServerError(anyhow::anyhow!(error)));
        }
    }

    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use crate::bus::{self, message};
    use crate::config::load_configuration;
    use crate::storage::sqlite::SqliteStorage;

    use super::*;
    use std::io::Write;
    use std::sync::Arc;

    #[tokio::test]
    async fn test_publish_influxdb() {
        _ = load_configuration();
        let event_bus = Arc::new(bus::event_bus::EventBus::new());
        let mut wololo = event_bus.main_bus_receiver.activate_cloned();
        tokio::spawn(async move {
            while let Ok(message) = wololo.recv().await {
                match message {
                    message::Message::Publish(message::PublishMessage {
                        batch: _,
                        sync_receiver: _,
                        sync_sender,
                    }) => {
                        println!("Received publish message");
                        sync_sender.broadcast(()).await.unwrap();
                    }
                }
            }
        });
        let state = State(HttpServerState {
            name: Arc::new("influxdb test".to_string()),
            event_bus: event_bus.clone(),
            storage: Arc::new(SqliteStorage::connect("sqlite::memory:").await.unwrap()),
        });
        let headers = HeaderMap::new();
        let bytes =
            Bytes::from("[{\"n\":\"urn:dev:ow:10e2073a01080063\",\"u\":\"Cel\",\"v\":23.1}]");
        let result = publish_senml(state.clone(), headers, bytes).await.unwrap();
        assert_eq!(result, StatusCode::NO_CONTENT);

        // with good snappy encoding
        let mut headers = HeaderMap::new();
        headers.insert("content-encoding", "snappy".parse().unwrap());
        let bytes = "[{\"n\":\"urn:dev:ow:10e2073a01080063\",\"u\":\"Cel\",\"v\":23.1}]".as_bytes();
        let mut buffer = Vec::new();
        let mut encoder = snap::write::FrameEncoder::new(&mut buffer);
        encoder.write_all(bytes).unwrap();
        let data = encoder.into_inner().unwrap();
        let bytes = Bytes::from(data.to_vec());
        let result = publish_senml(state.clone(), headers, bytes).await.unwrap();
        assert_eq!(result, StatusCode::NO_CONTENT);

        // with wrong gzip encoding
        let mut headers = HeaderMap::new();
        headers.insert("content-encoding", "gzip".parse().unwrap());
        let bytes = Bytes::from("definetely not gzip");
        let result = publish_senml(state.clone(), headers, bytes).await;
        assert!(result.is_err());
        // Check it's an AppError::BadRequest
        assert!(matches!(result, Err(AppError::BadRequest(_))));

        // With wrong protocol
        let headers = HeaderMap::new();
        let bytes = Bytes::from("{\"notsenml\":true}");
        let result = publish_senml(state.clone(), headers, bytes).await;
        assert!(result.is_err());
        // Check it's an AppError::BadRequest
        assert!(matches!(result, Err(AppError::BadRequest(_))));
    }
}
