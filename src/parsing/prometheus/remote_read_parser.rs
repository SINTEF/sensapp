use super::common::decompress_snappy;
use super::remote_read_models::{ReadRequest, ReadResponse};
use anyhow::Result;
use prost::Message;
use std::io::Cursor;
use tracing::debug;

fn parse_protobuf(input: &[u8]) -> Result<ReadRequest> {
    Ok(ReadRequest::decode(&mut Cursor::new(input))?)
}

pub fn parse_remote_read_request(input: &[u8]) -> Result<ReadRequest> {
    debug!("Parsing remote read request: {} bytes", input.len());
    let decompressed = decompress_snappy(input)?;
    debug!("Decompressed to {} bytes", decompressed.len());

    let request = parse_protobuf(&decompressed)?;
    debug!("Parsed ReadRequest with {} queries", request.queries.len());

    // Log query details for debugging
    for (i, query) in request.queries.iter().enumerate() {
        debug!(
            "Query {}: time range {}ms - {}ms, {} matchers",
            i,
            query.start_timestamp_ms,
            query.end_timestamp_ms,
            query.matchers.len()
        );

        for (j, matcher) in query.matchers.iter().enumerate() {
            debug!(
                "  Matcher {}: name='{}', value='{}', type={}",
                j, matcher.name, matcher.value, matcher.r#type
            );
        }

        if let Some(hints) = &query.hints {
            debug!("  Hints: step={}ms, func='{}'", hints.step_ms, hints.func);
        }
    }

    debug!(
        "Accepted response types: {:?}",
        request.accepted_response_types
    );

    Ok(request)
}

pub fn serialize_read_response(response: &ReadResponse) -> Result<Vec<u8>> {
    let encoded = response.encode_to_vec();
    debug!("Encoded ReadResponse to {} bytes", encoded.len());

    // Compress with snappy
    let compressed = snap::raw::Encoder::new().compress_vec(&encoded)?;
    debug!("Compressed ReadResponse to {} bytes", compressed.len());

    Ok(compressed)
}

#[cfg(test)]
mod tests {
    use super::super::remote_read_models::{
        LabelMatcher, Query, QueryResult, label_matcher, read_request,
    };
    use super::*;

    #[test]
    fn test_decompress_snappy() {
        // We are basically testing the snappy crate here.
        // But at least we check that we call the library correctly.
        use snap::raw::Encoder;
        let input = b"Hello, world!";
        let compressed = Encoder::new().compress_vec(input).unwrap();

        let decompressed = decompress_snappy(&compressed).unwrap();
        assert_eq!(decompressed, input);
    }

    #[test]
    fn test_parse_protobuf() {
        let input_data = ReadRequest {
            queries: vec![],
            accepted_response_types: vec![],
        };
        let input_bytes = input_data.encode_to_vec();

        let _ = parse_protobuf(&input_bytes).unwrap();

        let input = b"not a valid protobuf";
        assert!(parse_protobuf(input).is_err());
    }

    #[test]
    fn test_parse_remote_read_request() {
        let input_data = ReadRequest {
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
        let input_bytes = input_data.encode_to_vec();

        let compressed = snap::raw::Encoder::new()
            .compress_vec(&input_bytes)
            .unwrap();

        let output = parse_remote_read_request(&compressed).unwrap();

        assert_eq!(output.queries.len(), 1);
        assert_eq!(output.queries[0].start_timestamp_ms, 1000);
        assert_eq!(output.queries[0].end_timestamp_ms, 2000);
        assert_eq!(output.queries[0].matchers.len(), 1);
        assert_eq!(output.queries[0].matchers[0].name, "__name__");
        assert_eq!(output.queries[0].matchers[0].value, "test_metric");
        assert_eq!(output.accepted_response_types.len(), 1);
    }

    #[test]
    fn test_serialize_read_response() {
        let response = ReadResponse {
            results: vec![QueryResult { timeseries: vec![] }],
        };

        let serialized = serialize_read_response(&response).unwrap();
        assert!(!serialized.is_empty());

        // Test that we can decompress and parse back
        let decompressed = decompress_snappy(&serialized).unwrap();
        let parsed = ReadResponse::decode(&mut Cursor::new(decompressed)).unwrap();
        assert_eq!(parsed.results.len(), 1);
    }
}
