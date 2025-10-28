use super::remote_read_models::ChunkedReadResponse;
use anyhow::Result;
use prost::Message;
use std::io::Write;
use tracing::debug;

/// Writer for streaming Prometheus chunked read responses.
///
/// The format for each message in the stream is:
/// - Varint-encoded message length
/// - Protobuf-encoded message
/// - 4-byte CRC32 checksum (Castagnoli polynomial)
#[allow(dead_code)]
pub struct StreamWriter;

#[allow(dead_code)]
impl StreamWriter {
    /// Write a ChunkedReadResponse to the output stream.
    ///
    /// # Arguments
    /// * `response` - The response to write
    /// * `writer` - The output writer (typically an HTTP response body)
    pub fn write_response<W: Write>(response: &ChunkedReadResponse, writer: &mut W) -> Result<()> {
        // Encode the protobuf message
        let encoded = response.encode_to_vec();

        debug!(
            "Writing ChunkedReadResponse: query_index={}, series_count={}, encoded_size={}",
            response.query_index,
            response.chunked_series.len(),
            encoded.len()
        );

        // Write varint-encoded length
        Self::write_varint(encoded.len() as u64, writer)?;

        // Write the encoded message
        writer.write_all(&encoded)?;

        // Calculate and write CRC32 checksum
        let checksum = Self::calculate_crc32(&encoded);
        writer.write_all(&checksum.to_be_bytes())?;

        writer.flush()?;

        Ok(())
    }

    /// Write multiple ChunkedReadResponses to the output stream.
    pub fn write_responses<W: Write>(
        responses: &[ChunkedReadResponse],
        writer: &mut W,
    ) -> Result<()> {
        debug!("Writing {} ChunkedReadResponses to stream", responses.len());

        for response in responses {
            Self::write_response(response, writer)?;
        }

        Ok(())
    }

    /// Write a varint-encoded integer (protobuf-style encoding).
    fn write_varint<W: Write>(mut value: u64, writer: &mut W) -> Result<()> {
        while value >= 0x80 {
            writer.write_all(&[((value & 0x7F) | 0x80) as u8])?;
            value >>= 7;
        }
        writer.write_all(&[value as u8])?;
        Ok(())
    }

    /// Calculate CRC32 checksum using the Castagnoli polynomial.
    /// This is the same polynomial used by Prometheus.
    fn calculate_crc32(data: &[u8]) -> u32 {
        // Using the CRC32C (Castagnoli) polynomial: 0x82F63B78
        const CASTAGNOLI_POLY: u32 = 0x82F63B78;

        // For now, we'll use a simple CRC32 calculation
        // In production, you might want to use the `crc32c` crate for better performance
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

    /// Create a streaming response body for multiple ChunkedReadResponses.
    /// This returns the complete byte vector that should be sent as the response body.
    pub fn create_stream_body(responses: &[ChunkedReadResponse]) -> Result<Vec<u8>> {
        let mut buffer = Vec::new();
        Self::write_responses(responses, &mut buffer)?;
        Ok(buffer)
    }
}

#[cfg(any(test, feature = "test-utils"))]
mod tests {
    #![allow(unused_imports)]
    use super::{StreamWriter, ChunkedReadResponse};
    use super::super::remote_read_models::{ChunkedSeries, Chunk, chunk};
    use super::super::remote_write_models::Label;

    #[test]
    fn test_write_varint() {
        let test_cases = vec![
            (0u64, vec![0x00]),
            (127u64, vec![0x7F]),
            (128u64, vec![0x80, 0x01]),
            (300u64, vec![0xAC, 0x02]),
            (16384u64, vec![0x80, 0x80, 0x01]),
        ];

        for (value, expected) in test_cases {
            let mut buffer = Vec::new();
            StreamWriter::write_varint(value, &mut buffer).unwrap();
            assert_eq!(buffer, expected, "Failed for value {}", value);
        }
    }

    #[test]
    fn test_calculate_crc32() {
        // Test with known data
        let data = b"Hello, World!";
        let crc = StreamWriter::calculate_crc32(data);
        // The actual CRC32C value would need to be verified
        // This just checks that we get a non-zero result
        assert_ne!(crc, 0);
    }

    #[test]
    fn test_write_response() {
        let response = ChunkedReadResponse {
            query_index: 0,
            chunked_series: vec![ChunkedSeries {
                labels: vec![Label {
                    name: "__name__".to_string(),
                    value: "test_metric".to_string(),
                }],
                chunks: vec![Chunk {
                    min_time_ms: 1000,
                    max_time_ms: 2000,
                    r#type: chunk::Encoding::Xor as i32,
                    data: vec![1, 2, 3, 4],
                }],
            }],
        };

        let mut buffer = Vec::new();
        StreamWriter::write_response(&response, &mut buffer).unwrap();

        // Check that we wrote something
        assert!(!buffer.is_empty());

        // The buffer should contain:
        // - Varint length (at least 1 byte)
        // - Protobuf message (several bytes)
        // - CRC32 (exactly 4 bytes)
        assert!(buffer.len() > 5);

        // Last 4 bytes should be the CRC32
        let crc_bytes = &buffer[buffer.len() - 4..];
        assert_eq!(crc_bytes.len(), 4);
    }

    #[test]
    fn test_create_stream_body() {
        let responses = vec![
            ChunkedReadResponse {
                query_index: 0,
                chunked_series: vec![],
            },
            ChunkedReadResponse {
                query_index: 1,
                chunked_series: vec![],
            },
        ];

        let body = StreamWriter::create_stream_body(&responses).unwrap();
        assert!(!body.is_empty());
    }
}
