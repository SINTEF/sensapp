use super::{app_error::AppError, state::HttpServerState};
use crate::parsing::ParseData;
use crate::{
    datamodel::batch_builder::BatchBuilder,
    parsing::influx::{precision::Precision, InfluxLineProtocolCompression, InfluxParser},
};
use anyhow::Result;
use axum::{
    debug_handler,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
};
use hybridmap::HybridMap;
use serde::Deserialize;
use std::str;
use tokio_util::bytes::Bytes;

#[derive(Debug, Deserialize)]
pub struct InfluxDBQueryParams {
    pub bucket: String,
    pub org: Option<String>,
    #[serde(rename = "orgID")]
    pub org_id: Option<String>,
    pub precision: Option<String>,
}

/// InfluxDB Compatible Write API.
///
/// Allows you to write data from InfluxDB or Telegraf to SensApp.
/// [More information.](https://github.com/SINTEF/sensapp/blob/main/docs/INFLUX_DB.md)
#[utoipa::path(
    post,
    path = "/api/v2/write",
    tag = "InfluxDB",
    request_body(
        content = String,
        content_type = "text/plain",
        description = "InfluxDB Line Protocol endpoint. [Reference](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/).",
        example = "cpu,host=A,region=west usage_system=64.2 1590488773254420000"
    ),
    params(
        ("bucket" = String, Query, description = "Bucket name", example = "sensapp"),
        ("org" = Option<String>, Query, description = "Organization name", example = "sensapp"),
        ("org_id" = Option<String>, Query, description = "Organization ID"),
        ("precision" = Option<String>, Query, description = "Precision of the timestamps. One of ns, us, ms, s"),
    ),
    responses(
        (status = 204, description = "No Content"),
        (status = 400, description = "Bad Request", body = AppError),
        (status = 500, description = "Internal Server Error", body = AppError),
    )
)]
#[debug_handler]
pub async fn publish_influxdb(
    State(state): State<HttpServerState>,
    headers: HeaderMap,
    Query(InfluxDBQueryParams {
        bucket,
        org,
        org_id,
        precision,
    }): Query<InfluxDBQueryParams>,
    bytes: Bytes,
) -> Result<StatusCode, AppError> {
    // println!("InfluxDB publish");
    // println!("bucket: {}", bucket);
    // println!("org: {:?}", org);
    // println!("org_id: {:?}", org_id);
    // println!("precision: {:?}", precision);
    // println!("bytes: {:?}", bytes);
    // println!("headers: {:?}", headers);

    // Requires org or org_id
    if org.is_none() && org_id.is_none() {
        return Err(AppError::BadRequest(anyhow::anyhow!(
            "org or org_id must be specified"
        )));
    }

    // Org or org_id, this is the same for SensApp.
    let common_org_name = match org {
        Some(org) => org,
        None => org_id.unwrap_or_default(),
    };

    // Convert the precision string to a Precision enum
    let precision_enum = match precision {
        Some(precision) => match precision.parse() {
            Ok(precision) => precision,
            Err(_) => {
                return Err(AppError::BadRequest(anyhow::anyhow!(
                    "Invalid precision: {}",
                    precision
                )));
            }
        },
        None => Precision::default(),
    };

    let compression = match headers.get("content-encoding") {
        Some(value) => match value.to_str() {
            Ok("gzip") => InfluxLineProtocolCompression::Gzip,
            Ok("plain") => InfluxLineProtocolCompression::None,
            _ => {
                return Err(AppError::BadRequest(anyhow::anyhow!(
                    "Unsupported content-encoding: {:?}",
                    value
                )))
            }
        },
        // No content-encoding header
        None => InfluxLineProtocolCompression::Automatic,
    };

    let mut context_map = HybridMap::with_capacity(2);
    context_map.insert("influxdb_bucket".to_string(), bucket);
    context_map.insert("influxdb_org".to_string(), common_org_name);

    let parser = InfluxParser::new(compression, precision_enum, false);

    let mut batch_builder = BatchBuilder::new()?;

    parser
        .parse_data(&bytes, Some(context_map), &mut batch_builder)
        .await
        .map_err(|error| AppError::BadRequest(anyhow::anyhow!(error)))?;

    // TODO: Remove this println once debugged
    println!("INfluxDB: Sending to the event bus soon");

    match batch_builder.send_what_is_left(state.event_bus).await {
        Ok(Some(mut receiver)) => {
            println!("INfluxDB: Waiting for the receiver");
            receiver.wait().await?;
            println!("INfluxDB: Receiver done");
        }
        Ok(None) => {
            println!("INfluxDB: No receiver");
        }
        Err(error) => {
            println!("INfluxDB: Error: {:?}", error);
            return Err(AppError::InternalServerError(anyhow::anyhow!(error)));
        }
    }

    // OK no content
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use crate::bus::{self, message};
    use crate::config::load_configuration;
    use crate::storage::sqlite::SqliteStorage;

    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
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
        let query = Query(InfluxDBQueryParams {
            bucket: "test".to_string(),
            org: Some("test".to_string()),
            org_id: None,
            precision: None,
        });
        let bytes = Bytes::from("cpu,host=A,region=west usage_system=64i 1590488773254420000");
        let result = publish_influxdb(state.clone(), headers, query, bytes)
            .await
            .unwrap();
        assert_eq!(result, StatusCode::NO_CONTENT);

        // with good gzip encoding
        let mut headers = HeaderMap::new();
        headers.insert("content-encoding", "gzip".parse().unwrap());
        let query = Query(InfluxDBQueryParams {
            bucket: "test".to_string(),
            org: Some("test".to_string()),
            org_id: None,
            precision: None,
        });
        let bytes = "cpu,host=A,region=west usage_system=64i 1590488773254420000".as_bytes();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(bytes).unwrap();
        let bytes = Bytes::from(encoder.finish().unwrap());
        let result = publish_influxdb(state.clone(), headers, query, bytes)
            .await
            .unwrap();
        assert_eq!(result, StatusCode::NO_CONTENT);

        // with wrong gzip encoding
        let mut headers = HeaderMap::new();
        headers.insert("content-encoding", "gzip".parse().unwrap());
        let query = Query(InfluxDBQueryParams {
            bucket: "test".to_string(),
            org: None,
            org_id: Some("test".to_string()),
            precision: None,
        });
        let bytes = Bytes::from("definetely not gzip");
        let result = publish_influxdb(state.clone(), headers, query, bytes).await;
        assert!(result.is_err());
        // Check it's an AppError::BadRequest
        assert!(matches!(result, Err(AppError::BadRequest(_))));

        // With wrong line protocol
        let headers = HeaderMap::new();
        let query = Query(InfluxDBQueryParams {
            bucket: "test".to_string(),
            org: Some("test".to_string()),
            org_id: Some("test2".to_string()),
            precision: None,
        });
        let bytes = Bytes::from("wrong line protocol");
        let result = publish_influxdb(state.clone(), headers, query, bytes).await;
        assert!(result.is_err());
        // Check it's an AppError::BadRequest
        assert!(matches!(result, Err(AppError::BadRequest(_))));

        // With no org or org_id
        let headers = HeaderMap::new();
        let query = Query(InfluxDBQueryParams {
            bucket: "test".to_string(),
            org: None,
            org_id: None,
            precision: None,
        });
        let bytes = Bytes::from("cpu,host=A,region=west usage_system=64i 1590488773254420000");
        let result = publish_influxdb(state.clone(), headers, query, bytes).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(AppError::BadRequest(_))));

        // Without tags
        let headers = HeaderMap::new();
        let query = Query(InfluxDBQueryParams {
            bucket: "test".to_string(),
            org: Some("test".to_string()),
            org_id: None,
            precision: None,
        });
        let bytes = Bytes::from("cpu usage_system=64i 1590488773254420000");
        let result = publish_influxdb(state.clone(), headers, query, bytes)
            .await
            .unwrap();
        assert_eq!(result, StatusCode::NO_CONTENT);

        // Without datetime
        let headers = HeaderMap::new();
        let query = Query(InfluxDBQueryParams {
            bucket: "test".to_string(),
            org: Some("test".to_string()),
            org_id: None,
            precision: None,
        });
        let bytes = Bytes::from("cpu,host=A,region=west usage_system=64i");
        let result = publish_influxdb(state.clone(), headers, query, bytes)
            .await
            .unwrap();
        assert_eq!(result, StatusCode::NO_CONTENT);

        // Too high u64 value
        let headers = HeaderMap::new();
        let query = Query(InfluxDBQueryParams {
            bucket: "test".to_string(),
            org: Some("test".to_string()),
            org_id: None,
            precision: None,
        });
        let bytes = Bytes::from("cpu usage_system=9223372036854775808u");
        let result = publish_influxdb(state.clone(), headers, query, bytes).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(AppError::BadRequest(_))));

        // With various precisions
        let headers = HeaderMap::new();
        let query = Query(InfluxDBQueryParams {
            bucket: "test".to_string(),
            org: Some("test".to_string()),
            org_id: None,
            precision: Some("ns".to_string()),
        });
        let bytes = Bytes::from("cpu,host=A,region=west usage_system=64i 1590488773254420000");
        let result = publish_influxdb(state.clone(), headers, query, bytes)
            .await
            .unwrap();
        assert_eq!(result, StatusCode::NO_CONTENT);

        let headers = HeaderMap::new();
        let query = Query(InfluxDBQueryParams {
            bucket: "test".to_string(),
            org: Some("test".to_string()),
            org_id: None,
            precision: Some("us".to_string()),
        });
        let bytes = Bytes::from("cpu,host=A,region=west usage_system=64i 1590488773254420");
        let result = publish_influxdb(state.clone(), headers, query, bytes)
            .await
            .unwrap();
        assert_eq!(result, StatusCode::NO_CONTENT);

        let headers = HeaderMap::new();
        let query = Query(InfluxDBQueryParams {
            bucket: "test".to_string(),
            org: Some("test".to_string()),
            org_id: None,
            precision: Some("ms".to_string()),
        });
        let bytes = Bytes::from("cpu,host=A,region=west usage_system=64i 1590488773254");
        let result = publish_influxdb(state.clone(), headers, query, bytes)
            .await
            .unwrap();
        assert_eq!(result, StatusCode::NO_CONTENT);

        let headers = HeaderMap::new();
        let query = Query(InfluxDBQueryParams {
            bucket: "test".to_string(),
            org: Some("test".to_string()),
            org_id: None,
            precision: Some("s".to_string()),
        });
        let bytes = Bytes::from("cpu,host=A,region=west usage_system=64i 1590488773");
        let result = publish_influxdb(state.clone(), headers, query, bytes)
            .await
            .unwrap();
        assert_eq!(result, StatusCode::NO_CONTENT);

        // With wrong precision
        let headers = HeaderMap::new();
        let query = Query(InfluxDBQueryParams {
            bucket: "test".to_string(),
            org: Some("test".to_string()),
            org_id: None,
            precision: Some("wrong".to_string()),
        });
        let bytes = Bytes::from("cpu,host=A,region=west usage_system=64i 1590488773");
        let result = publish_influxdb(state.clone(), headers, query, bytes).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(AppError::BadRequest(_))));
    }
}
