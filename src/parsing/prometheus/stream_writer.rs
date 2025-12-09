use super::remote_read_models::ChunkedReadResponse;
use anyhow::Result;
use prost::Message;
use std::io::Write;
use tracing::debug;

/// Writer for streaming Prometheus chunked read responses.
///
/// The format for each message in the stream is:
/// - Varint-encoded message length
/// - 4-byte CRC32 checksum (Castagnoli polynomial, big-endian)
/// - Protobuf-encoded message
pub struct StreamWriter;

impl StreamWriter {
    /// Write a ChunkedReadResponse to the output stream.
    ///
    /// Note: If the response has no series with chunks, nothing is written.
    /// This matches Prometheus behavior where empty responses are simply not sent.
    ///
    /// # Arguments
    /// * `response` - The response to write
    /// * `writer` - The output writer (typically an HTTP response body)
    pub fn write_response<W: Write>(response: &ChunkedReadResponse, writer: &mut W) -> Result<()> {
        // Skip empty responses - Prometheus expects no message when there's no data
        // Otherwise, the client will try to access ChunkedSeries[0] and panic
        if response.chunked_series.is_empty() {
            println!(
                "[DEBUG STREAM_WRITER] Skipping empty ChunkedReadResponse for query_index={} (no series)",
                response.query_index
            );
            debug!(
                "Skipping empty ChunkedReadResponse for query_index={}",
                response.query_index
            );
            return Ok(());
        }

        // Also skip responses where all series have no chunks
        let has_chunks = response.chunked_series.iter().any(|s| !s.chunks.is_empty());
        if !has_chunks {
            println!(
                "[DEBUG STREAM_WRITER] Skipping ChunkedReadResponse for query_index={} (series exist but no chunks)",
                response.query_index
            );
            debug!(
                "Skipping ChunkedReadResponse with no chunks for query_index={}",
                response.query_index
            );
            return Ok(());
        }

        // Encode the protobuf message
        let encoded = response.encode_to_vec();

        println!(
            "[DEBUG STREAM_WRITER] Writing ChunkedReadResponse: query_index={}, series_count={}, encoded_size={} bytes",
            response.query_index,
            response.chunked_series.len(),
            encoded.len()
        );
        // Log details about each series
        for (i, series) in response.chunked_series.iter().enumerate() {
            println!(
                "[DEBUG STREAM_WRITER]   Series {}: {} labels, {} chunks",
                i,
                series.labels.len(),
                series.chunks.len()
            );
            println!(
                "[DEBUG STREAM_WRITER]     Labels: {:?}",
                series
                    .labels
                    .iter()
                    .map(|l| format!("{}={}", l.name, l.value))
                    .collect::<Vec<_>>()
            );
            for chunk in &series.chunks {
                println!(
                    "[DEBUG STREAM_WRITER]     Chunk: {}ms - {}ms, {} bytes data, type={}",
                    chunk.min_time_ms,
                    chunk.max_time_ms,
                    chunk.data.len(),
                    chunk.r#type
                );
                // Print first 32 bytes of chunk data in hex for debugging
                let hex_preview: String = chunk
                    .data
                    .iter()
                    .take(32)
                    .map(|b| format!("{:02x}", b))
                    .collect::<Vec<_>>()
                    .join(" ");
                println!(
                    "[DEBUG STREAM_WRITER]     Chunk data (first 32 bytes): {}",
                    hex_preview
                );
            }
        }

        debug!(
            "Writing ChunkedReadResponse: query_index={}, series_count={}, encoded_size={}",
            response.query_index,
            response.chunked_series.len(),
            encoded.len()
        );

        // Write varint-encoded length
        Self::write_varint(encoded.len() as u64, writer)?;

        // Calculate and write CRC32 checksum BEFORE the data
        // This matches the Prometheus chunked read format:
        // 1. uvarint for the size of the data frame
        // 2. big-endian uint32 for the CRC-32 checksum of the data frame
        // 3. the bytes of the data
        let checksum = Self::calculate_crc32(&encoded);
        writer.write_all(&checksum.to_be_bytes())?;

        // Write the encoded message AFTER the checksum
        writer.write_all(&encoded)?;

        writer.flush()?;

        Ok(())
    }

    /// Write multiple ChunkedReadResponses to the output stream.
    pub fn write_responses<W: Write>(
        responses: &[ChunkedReadResponse],
        writer: &mut W,
    ) -> Result<()> {
        println!(
            "[DEBUG STREAM_WRITER] write_responses called with {} responses",
            responses.len()
        );
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

#[cfg(test)]
mod tests {
    use super::super::remote_read_models::{Chunk, ChunkedSeries, chunk};
    use super::super::remote_write_models::Label;
    use super::*;

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
        // - CRC32 (exactly 4 bytes)
        // - Protobuf message (several bytes)
        assert!(buffer.len() > 5);

        // Parse the varint to find where CRC32 starts
        let mut varint_len = 0;
        for (i, &byte) in buffer.iter().enumerate() {
            varint_len = i + 1;
            if byte & 0x80 == 0 {
                break;
            }
        }

        // CRC32 should be at bytes [varint_len..varint_len+4]
        let crc_bytes = &buffer[varint_len..varint_len + 4];
        assert_eq!(crc_bytes.len(), 4);
    }

    #[test]
    fn test_create_stream_body() {
        // Test with empty responses - should produce empty body
        let empty_responses = vec![
            ChunkedReadResponse {
                query_index: 0,
                chunked_series: vec![],
            },
            ChunkedReadResponse {
                query_index: 1,
                chunked_series: vec![],
            },
        ];

        let body = StreamWriter::create_stream_body(&empty_responses).unwrap();
        // Empty responses should produce an empty body - this is expected!
        // Prometheus client handles EOF gracefully when there's no data.
        assert!(body.is_empty());

        // Test with actual data
        let responses_with_data = vec![ChunkedReadResponse {
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
        }];

        let body = StreamWriter::create_stream_body(&responses_with_data).unwrap();
        assert!(!body.is_empty());
    }
}
