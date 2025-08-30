use anyhow::Result;
use axum::http::StatusCode;
use prost::Message;
use sensapp::parsing::prometheus::remote_read_models::{
    LabelMatcher, Query, ReadRequest, label_matcher, read_request, ChunkedReadResponse,
};
use snap::raw::Encoder;

use sensapp::test_utils::TestDb;
use sensapp::test_utils::http::TestApp;
use sensapp::test_utils::load_configuration_for_tests;
use serial_test::serial;
use std::sync::Once;
use std::io::Cursor;

// Ensure configuration is loaded once for all tests
static INIT: Once = Once::new();

fn ensure_config() {
    INIT.call_once(|| {
        load_configuration_for_tests().expect("Failed to load configuration for tests");
    });
}

// Helper functions for parsing chunked responses

/// Parse a varint-encoded integer from the buffer
fn parse_varint(cursor: &mut Cursor<&[u8]>) -> Result<u64> {
    use std::io::Read;
    
    let mut value = 0u64;
    let mut shift = 0;
    
    loop {
        let mut byte = [0u8; 1];
        cursor.read_exact(&mut byte)?;
        
        value |= ((byte[0] & 0x7F) as u64) << shift;
        
        if byte[0] & 0x80 == 0 {
            break;
        }
        
        shift += 7;
        if shift >= 64 {
            return Err(anyhow::anyhow!("Varint too large"));
        }
    }
    
    Ok(value)
}

/// Calculate CRC32C checksum (Castagnoli polynomial)
fn calculate_crc32c(data: &[u8]) -> u32 {
    const CASTAGNOLI_POLY: u32 = 0x82F63B78;
    
    let mut crc = !0u32;
    
    for &byte in data {
        crc ^= byte as u32;
        for _ in 0..8 {
            if crc & 1 == 1 {
                crc = (crc >> 1) ^ CASTAGNOLI_POLY;
            } else {
                crc >>= 1;
            }
        }
    }
    
    !crc
}

/// Parse a single ChunkedReadResponse from the stream
fn parse_chunked_response(cursor: &mut Cursor<&[u8]>) -> Result<ChunkedReadResponse> {
    use std::io::Read;
    
    // Read the varint length
    let msg_len = parse_varint(cursor)? as usize;
    
    // Read the message bytes
    let mut msg_bytes = vec![0u8; msg_len];
    cursor.read_exact(&mut msg_bytes)?;
    
    // Read the CRC32 checksum
    let mut crc_bytes = [0u8; 4];
    cursor.read_exact(&mut crc_bytes)?;
    let expected_crc = u32::from_be_bytes(crc_bytes);
    
    // Verify the CRC32
    let actual_crc = calculate_crc32c(&msg_bytes);
    if actual_crc != expected_crc {
        return Err(anyhow::anyhow!(
            "CRC32 mismatch: expected {:#x}, got {:#x}",
            expected_crc,
            actual_crc
        ));
    }
    
    // Decode the protobuf message
    let response = ChunkedReadResponse::decode(&mut Cursor::new(&msg_bytes))?;
    
    Ok(response)
}

/// Parse all ChunkedReadResponses from a response body
fn parse_all_chunked_responses(body: &[u8]) -> Result<Vec<ChunkedReadResponse>> {
    let mut cursor = Cursor::new(body);
    let mut responses = Vec::new();
    
    while cursor.position() < body.len() as u64 {
        responses.push(parse_chunked_response(&mut cursor)?);
    }
    
    Ok(responses)
}

/// Decode XOR chunk data to verify samples
/// Returns a vector of (timestamp_ms, value) pairs
fn decode_xor_chunk(chunk_data: &[u8]) -> Result<Vec<(i64, f64)>> {
    use rusty_chunkenc::chunk::{Chunk, read_chunk};
    
    // Parse the chunk from bytes using the nom parser
    let (_, chunk) = read_chunk(chunk_data)
        .map_err(|e| anyhow::anyhow!("Failed to parse chunk: {:?}", e))?;
    
    // Get samples from the chunk
    let mut samples = Vec::new();
    
    // Extract samples based on chunk type
    match chunk {
        Chunk::XOR(xor_chunk) => {
            // Get samples from the XOR chunk
            for sample in xor_chunk.samples() {
                samples.push((sample.timestamp, sample.value));
            }
        }
        _ => {
            return Err(anyhow::anyhow!("Expected XOR chunk"));
        }
    }
    
    Ok(samples)
}

#[tokio::test]
#[serial]
async fn test_prometheus_remote_read_empty_request() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage).await;

    // Create an empty ReadRequest
    // Note: Our server only supports StreamedXorChunks response type
    let read_request = ReadRequest {
        queries: vec![],
        accepted_response_types: vec![read_request::ResponseType::StreamedXorChunks as i32],
    };

    // Encode and compress
    let encoded = read_request.encode_to_vec();
    let compressed = Encoder::new().compress_vec(&encoded)?;

    // Send the request
    let response = app
        .post_prometheus_read("/api/v1/prometheus_remote_read", &compressed)
        .await?;

    // An empty request (no queries) should still return 200 OK with empty response
    // This is valid according to the Prometheus spec - empty queries means no data to return
    assert_eq!(response.status(), StatusCode::OK);
    // When using StreamedXorChunks, the content-type includes the proto type
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse"
    );
    // Check content-encoding - chunked responses may not be compressed
    if let Some(encoding) = response.headers().get("content-encoding") {
        assert_eq!(encoding, "snappy");
    }

    // Parse the chunked response
    let body = response.body_bytes();
    
    // For empty request (no queries), we expect an empty response body
    assert_eq!(body.len(), 0, "Expected empty response body for request with no queries");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_prometheus_remote_read_with_query() -> Result<()> {
    ensure_config();
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
        accepted_response_types: vec![read_request::ResponseType::StreamedXorChunks as i32],
    };

    // Encode and compress
    let encoded = read_request.encode_to_vec();
    let compressed = Encoder::new().compress_vec(&encoded)?;

    // Send the request
    let response = app
        .post_prometheus_read("/api/v1/prometheus_remote_read", &compressed)
        .await?;

    // Should return 200 OK with chunked response
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse"
    );
    // No content-encoding for chunked responses
    assert!(response.headers().get("content-encoding").is_none());

    // Parse the chunked response  
    let body = response.body_bytes();
    
    // The response should contain at least varint length + message + CRC32
    assert!(body.len() >= 5, "Body length is {}, expected >= 5", body.len());
    
    // Parse the ChunkedReadResponse
    let responses = parse_all_chunked_responses(body)?;
    
    // We should have exactly 1 response for our 1 query
    assert_eq!(responses.len(), 1, "Expected 1 ChunkedReadResponse");
    
    let response = &responses[0];
    
    // Verify the query index
    assert_eq!(response.query_index, 0, "Expected query_index to be 0");
    
    // Since no data was inserted, we should get 0 series back
    assert_eq!(response.chunked_series.len(), 0, "Expected 0 series (no data inserted)");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_prometheus_remote_read_multiple_queries() -> Result<()> {
    ensure_config();
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
        accepted_response_types: vec![read_request::ResponseType::StreamedXorChunks as i32],
    };

    // Encode and compress
    let encoded = read_request.encode_to_vec();
    let compressed = Encoder::new().compress_vec(&encoded)?;

    // Send the request
    let response = app
        .post_prometheus_read("/api/v1/prometheus_remote_read", &compressed)
        .await?;

    // Should return 200 OK with chunked response
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse"
    );
    // No content-encoding for chunked responses
    assert!(response.headers().get("content-encoding").is_none());

    // Parse the chunked response  
    let body = response.body_bytes();
    
    // The response should contain at least varint length + message + CRC32 for each query
    assert!(body.len() > 10, "Body length is {}, expected > 10 for 2 queries", body.len());
    
    // Parse all ChunkedReadResponses
    let responses = parse_all_chunked_responses(body)?;
    
    // We should have exactly 2 responses for our 2 queries
    assert_eq!(responses.len(), 2, "Expected 2 ChunkedReadResponses");
    
    // Verify the first response
    let response1 = &responses[0];
    assert_eq!(response1.query_index, 0, "Expected first query_index to be 0");
    assert_eq!(response1.chunked_series.len(), 0, "Expected 0 series for query 0 (no data)");
    
    // Verify the second response
    let response2 = &responses[1];
    assert_eq!(response2.query_index, 1, "Expected second query_index to be 1");
    assert_eq!(response2.chunked_series.len(), 0, "Expected 0 series for query 1 (no data)");

    Ok(())
}

#[tokio::test]
#[serial]
async fn test_prometheus_remote_read_invalid_headers() -> Result<()> {
    ensure_config();
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
#[serial]
async fn test_prometheus_remote_read_invalid_data() -> Result<()> {
    ensure_config();
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

#[tokio::test]
#[serial]
async fn test_prometheus_remote_read_chunked_response() -> Result<()> {
    ensure_config();
    
    // Initialize tracing for debug output
    let _ = tracing_subscriber::fmt()
        .with_env_filter("sensapp=debug")
        .try_init();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage.clone()).await;

    // First, insert some test data via Prometheus remote write
    let test_series = sensapp::parsing::prometheus::remote_write_models::TimeSeries {
        labels: vec![
            sensapp::parsing::prometheus::remote_write_models::Label {
                name: "__name__".to_string(),
                value: "test_metric".to_string(),
            },
            sensapp::parsing::prometheus::remote_write_models::Label {
                name: "job".to_string(),
                value: "test_job".to_string(),
            },
        ],
        samples: vec![
            sensapp::parsing::prometheus::remote_write_models::Sample {
                timestamp: 1500,
                value: 42.0,
            },
        ],
    };

    let write_request = sensapp::parsing::prometheus::remote_write_models::WriteRequest {
        timeseries: vec![test_series],
    };

    // Send the write request to insert data
    let encoded_write = write_request.encode_to_vec();
    let compressed_write = Encoder::new().compress_vec(&encoded_write)?;
    
    let write_response = app
        .post_prometheus_write("/api/v1/prometheus_remote_write", &compressed_write)
        .await?;
    if write_response.status() != StatusCode::NO_CONTENT {
        eprintln!("Write failed with status: {:?}", write_response.status());
        eprintln!("Body: {}", write_response.body());
    }
    assert_eq!(write_response.status(), StatusCode::NO_CONTENT);

    // Now create a ReadRequest with chunked response type accepted
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
        accepted_response_types: vec![read_request::ResponseType::StreamedXorChunks as i32],
    };

    // Encode and compress
    let encoded = read_request.encode_to_vec();
    let compressed = Encoder::new().compress_vec(&encoded)?;

    // Send the request
    let response = app
        .post_prometheus_read("/api/v1/prometheus_remote_read", &compressed)
        .await?;

    // Should return 200 OK with chunked response content-type
    assert_eq!(response.status(), StatusCode::OK);
    assert_eq!(
        response.headers().get("content-type").unwrap(),
        "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse"
    );
    // No content-encoding for chunked responses
    assert!(response.headers().get("content-encoding").is_none());

    // Parse the chunked response
    let body = response.body_bytes();
    
    // Debug: print body length
    eprintln!("Response body length: {}", body.len());
    eprintln!("Response status: {:?}", response.status());
    
    // The response should contain at least varint length + message + CRC32
    assert!(body.len() > 5, "Body length is {}, expected > 5", body.len());

    // Parse all ChunkedReadResponses from the body
    let responses = parse_all_chunked_responses(body)?;
    
    // We should have exactly 1 response for our 1 query
    assert_eq!(responses.len(), 1, "Expected 1 ChunkedReadResponse");
    
    let response = &responses[0];
    
    // Verify the query index
    assert_eq!(response.query_index, 0, "Expected query_index to be 0");
    
    // Verify we got exactly 1 series
    assert_eq!(response.chunked_series.len(), 1, "Expected 1 series");
    
    let series = &response.chunked_series[0];
    
    // Verify the labels
    assert_eq!(series.labels.len(), 2, "Expected 2 labels");
    
    // Find and verify __name__ label
    let name_label = series.labels.iter()
        .find(|l| l.name == "__name__")
        .expect("Missing __name__ label");
    assert_eq!(name_label.value, "test_metric");
    
    // Find and verify job label
    let job_label = series.labels.iter()
        .find(|l| l.name == "job")
        .expect("Missing job label");
    assert_eq!(job_label.value, "test_job");
    
    // Verify we got exactly 1 chunk
    assert_eq!(series.chunks.len(), 1, "Expected 1 chunk");
    
    let chunk = &series.chunks[0];
    
    // Verify chunk metadata
    assert_eq!(chunk.min_time_ms, 1500, "Expected min_time_ms to be 1500");
    assert_eq!(chunk.max_time_ms, 1500, "Expected max_time_ms to be 1500");
    assert_eq!(chunk.r#type, sensapp::parsing::prometheus::remote_read_models::chunk::Encoding::Xor as i32);
    
    // Decode the XOR chunk to verify the actual sample
    let samples = decode_xor_chunk(&chunk.data)?;
    
    // We should have exactly 1 sample
    assert_eq!(samples.len(), 1, "Expected 1 sample in the chunk");
    
    let (timestamp, value) = samples[0];
    assert_eq!(timestamp, 1500, "Expected sample timestamp to be 1500ms");
    assert_eq!(value, 42.0, "Expected sample value to be 42.0");
    
    eprintln!("✅ Successfully verified ChunkedReadResponse with correct data!");
    
    Ok(())
}

#[tokio::test]
#[serial]
async fn test_prometheus_remote_read_no_chunks_support() -> Result<()> {
    ensure_config();
    let test_db = TestDb::new().await?;
    let storage = test_db.storage();
    let app = TestApp::new(storage).await;

    // Create a ReadRequest with only SAMPLES response type (no chunks)
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

    // Should return 400 Bad Request since we only support chunks
    assert_eq!(response.status(), StatusCode::BAD_REQUEST);
    
    Ok(())
}
