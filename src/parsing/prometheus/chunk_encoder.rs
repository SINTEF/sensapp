use anyhow::Result;
use rusty_chunkenc::chunk::Chunk;
use rusty_chunkenc::xor::XORSample;
use super::remote_read_models::{ChunkedReadResponse, ChunkedSeries, Chunk as ProtoChunk, chunk};
use super::remote_write_models::{Label, Sample};
use tracing::debug;

/// Encodes time series samples into XOR-compressed chunks for Prometheus remote read.
pub struct ChunkEncoder;

impl ChunkEncoder {
    /// Encode a time series into a ChunkedSeries with XOR-compressed chunks.
    /// 
    /// # Arguments
    /// * `labels` - The labels for this time series
    /// * `samples` - The samples to encode (must be sorted by timestamp)
    /// 
    /// # Returns
    /// A ChunkedSeries with XOR-encoded chunks
    pub fn encode_series(labels: Vec<Label>, samples: Vec<Sample>) -> Result<ChunkedSeries> {
        if samples.is_empty() {
            return Ok(ChunkedSeries {
                labels,
                chunks: vec![],
            });
        }

        debug!(
            "Encoding {} samples for series with {} labels",
            samples.len(),
            labels.len()
        );

        // Convert samples to XORSample format for rusty-chunkenc
        let xor_samples: Vec<XORSample> = samples
            .iter()
            .map(|sample| XORSample {
                timestamp: sample.timestamp,
                value: sample.value,
            })
            .collect();

        // Track min/max timestamps for the chunk metadata
        let min_time_ms = samples.first().map(|s| s.timestamp).unwrap_or(0);
        let max_time_ms = samples.last().map(|s| s.timestamp).unwrap_or(0);

        // Create the XOR chunk
        let chunk = Chunk::new_xor(xor_samples);
        
        // Encode the chunk to bytes
        let mut encoded_data = Vec::new();
        chunk.write(&mut encoded_data)?;

        debug!(
            "Encoded chunk: {} samples into {} bytes (time range: {}ms - {}ms)",
            samples.len(),
            encoded_data.len(),
            min_time_ms,
            max_time_ms
        );

        // Create the protobuf chunk
        let proto_chunk = ProtoChunk {
            min_time_ms,
            max_time_ms,
            r#type: chunk::Encoding::Xor as i32,
            data: encoded_data,
        };

        Ok(ChunkedSeries {
            labels,
            chunks: vec![proto_chunk],
        })
    }

    /// Create a ChunkedReadResponse for a query.
    /// 
    /// # Arguments
    /// * `query_index` - The index of the query this response is for
    /// * `chunked_series` - The series to include in the response
    pub fn create_response(
        query_index: i64,
        chunked_series: Vec<ChunkedSeries>,
    ) -> ChunkedReadResponse {
        debug!(
            "Creating ChunkedReadResponse for query {} with {} series",
            query_index,
            chunked_series.len()
        );

        ChunkedReadResponse {
            chunked_series,
            query_index,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_encode_empty_series() {
        let labels = vec![Label {
            name: "__name__".to_string(),
            value: "test_metric".to_string(),
        }];
        let samples = vec![];

        let series = ChunkEncoder::encode_series(labels.clone(), samples).unwrap();
        assert_eq!(series.labels.len(), 1);
        assert_eq!(series.chunks.len(), 0);
    }

    #[test]
    fn test_encode_series_with_samples() {
        let labels = vec![Label {
            name: "__name__".to_string(),
            value: "test_metric".to_string(),
        }];
        
        let samples = vec![
            Sample {
                timestamp: 1000,
                value: 1.0,
            },
            Sample {
                timestamp: 2000,
                value: 2.0,
            },
            Sample {
                timestamp: 3000,
                value: 3.0,
            },
        ];

        let series = ChunkEncoder::encode_series(labels.clone(), samples).unwrap();
        assert_eq!(series.labels.len(), 1);
        assert_eq!(series.chunks.len(), 1);
        
        let chunk = &series.chunks[0];
        assert_eq!(chunk.min_time_ms, 1000);
        assert_eq!(chunk.max_time_ms, 3000);
        assert_eq!(chunk.r#type, chunk::Encoding::Xor as i32);
        assert!(!chunk.data.is_empty());
    }

    #[test]
    fn test_create_response() {
        let series1 = ChunkedSeries {
            labels: vec![Label {
                name: "__name__".to_string(),
                value: "metric1".to_string(),
            }],
            chunks: vec![],
        };

        let series2 = ChunkedSeries {
            labels: vec![Label {
                name: "__name__".to_string(),
                value: "metric2".to_string(),
            }],
            chunks: vec![],
        };

        let response = ChunkEncoder::create_response(0, vec![series1, series2]);
        assert_eq!(response.query_index, 0);
        assert_eq!(response.chunked_series.len(), 2);
    }
}