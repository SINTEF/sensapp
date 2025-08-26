use anyhow::Result;
use axum::http::StatusCode;
use prost::Message;
use sensapp::parsing::prometheus::remote_read_models::{
    LabelMatcher, Query, ReadRequest, ReadResponse, label_matcher, read_request,
};
use snap::raw::Encoder;

mod common;
use common::TestDb;
use common::http::TestApp;

#[tokio::test]
async fn test_prometheus_remote_read_empty_request() -> Result<()> {
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage).await;

    // Create an empty ReadRequest
    let read_request = ReadRequest {
        queries: vec![],
        accepted_response_types: vec![read_request::ResponseType::Samples as i32],
    };

    // Encode and compress
    let encoded = read_request.encode_to_vec();
    let compressed = Encoder::new().compress_vec(&encoded)?;

    // Send the request
    let response = app
        .post_prometheus_read("/api/v1/prometheus_remote_read", &compressed)
        .await?;

    // Should return 200 OK with empty response
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/x-protobuf"
    );
    assert_eq!(
        response.headers().get("content-encoding").unwrap(),
        "snappy"
    );

    // Decompress and parse response
    let decompressed = snap::raw::Decoder::new().decompress_vec(response.body_bytes())?;
    let read_response = ReadResponse::decode(&mut std::io::Cursor::new(decompressed))?;

    // Should have empty results
    assert_eq!(read_response.results.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_prometheus_remote_read_with_query() -> Result<()> {
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage).await;

    // Create a ReadRequest with one query
    let read_request = ReadRequest {
        queries: vec![Query {
            start_timestamp_ms: 1000,
            end_timestamp_ms: 2000,
            matchers: vec![LabelMatcher {
                r#type: label_matcher::Type::Eq as i32,
                name: "__name__".to_string(),
                value: "test_metric".to_string(),
            }],
            hints: None,
        }],
        accepted_response_types: vec![read_request::ResponseType::Samples as i32],
    };

    // Encode and compress
    let encoded = read_request.encode_to_vec();
    let compressed = Encoder::new().compress_vec(&encoded)?;

    // Send the request
    let response = app
        .post_prometheus_read("/api/v1/prometheus_remote_read", &compressed)
        .await?;

    // Should return 200 OK
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/x-protobuf"
    );
    assert_eq!(
        response.headers().get("content-encoding").unwrap(),
        "snappy"
    );

    // Decompress and parse response
    let decompressed = snap::raw::Decoder::new().decompress_vec(response.body_bytes())?;
    let read_response = ReadResponse::decode(&mut std::io::Cursor::new(decompressed))?;

    // Should have one query result (empty for now since we're not fetching data yet)
    assert_eq!(read_response.results.len(), 1);
    assert_eq!(read_response.results[0].timeseries.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_prometheus_remote_read_multiple_queries() -> Result<()> {
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage).await;

    // Create a ReadRequest with multiple queries
    let read_request = ReadRequest {
        queries: vec![
            Query {
                start_timestamp_ms: 1000,
                end_timestamp_ms: 2000,
                matchers: vec![LabelMatcher {
                    r#type: label_matcher::Type::Eq as i32,
                    name: "__name__".to_string(),
                    value: "metric_1".to_string(),
                }],
                hints: None,
            },
            Query {
                start_timestamp_ms: 3000,
                end_timestamp_ms: 4000,
                matchers: vec![LabelMatcher {
                    r#type: label_matcher::Type::Eq as i32,
                    name: "__name__".to_string(),
                    value: "metric_2".to_string(),
                }],
                hints: None,
            },
        ],
        accepted_response_types: vec![read_request::ResponseType::Samples as i32],
    };

    // Encode and compress
    let encoded = read_request.encode_to_vec();
    let compressed = Encoder::new().compress_vec(&encoded)?;

    // Send the request
    let response = app
        .post_prometheus_read("/api/v1/prometheus_remote_read", &compressed)
        .await?;

    // Should return 200 OK
    assert_eq!(response.status(), StatusCode::OK);

    // Decompress and parse response
    let decompressed = snap::raw::Decoder::new().decompress_vec(response.body_bytes())?;
    let read_response = ReadResponse::decode(&mut std::io::Cursor::new(decompressed))?;

    // Should have two query results
    assert_eq!(read_response.results.len(), 2);
    assert_eq!(read_response.results[0].timeseries.len(), 0);
    assert_eq!(read_response.results[1].timeseries.len(), 0);

    Ok(())
}

#[tokio::test]
async fn test_prometheus_remote_read_invalid_headers() -> Result<()> {
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage).await;

    // Test missing content-encoding header
    let response = app
        .post_binary(
            "/api/v1/prometheus_remote_read",
            "application/x-protobuf",
            &[1, 2, 3, 4],
        )
        .await?;
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    Ok(())
}

#[tokio::test]
async fn test_prometheus_remote_read_invalid_data() -> Result<()> {
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage).await;

    // Send invalid protobuf data
    let invalid_data = b"this is not valid protobuf data";
    let response = app
        .post_prometheus_read("/api/v1/prometheus_remote_read", invalid_data)
        .await?;

    // Should return 400 Bad Request
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);

    Ok(())
}
