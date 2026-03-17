use super::remote_read_models::{Chunk as ProtoChunk, ChunkedReadResponse, ChunkedSeries, chunk};
use super::remote_write_models::{Label, Sample};
use anyhow::Result;
use rusty_chunkenc::xor::{XORChunk, XORSample};
use tracing::debug;

const MAX_SAMPLES_PER_XOR_CHUNK: usize = u16::MAX as usize;

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

        Ok(ChunkedSeries {
            labels,
            chunks: samples
                .chunks(MAX_SAMPLES_PER_XOR_CHUNK)
                .map(Self::encode_chunk)
                .collect::<Result<Vec<_>>>()?,
        })
    }

    fn encode_chunk(samples: &[Sample]) -> Result<ProtoChunk> {
        let xor_samples: Vec<XORSample> = samples
            .iter()
            .map(|sample| XORSample {
                timestamp: sample.timestamp,
                value: sample.value,
            })
            .collect();

        let min_time_ms = samples.first().map(|s| s.timestamp).unwrap_or(0);
        let max_time_ms = samples.last().map(|s| s.timestamp).unwrap_or(0);

        let xor_chunk = XORChunk::new(xor_samples);

        // Prometheus remote read expects raw XOR data starting with the 2-byte BE sample count.
        let mut encoded_data = Vec::new();
        xor_chunk.write(&mut encoded_data)?;

        debug!(
            "Encoded chunk: {} samples into {} bytes (time range: {}ms - {}ms)",
            samples.len(),
            encoded_data.len(),
            min_time_ms,
            max_time_ms
        );

        Ok(ProtoChunk {
            min_time_ms,
            max_time_ms,
            r#type: chunk::Encoding::Xor as i32,
            data: encoded_data,
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
    use rusty_chunkenc::xor::read_xor_chunk_data;

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
    fn test_encode_series_splits_large_sample_sets() {
        let labels = vec![Label {
            name: "__name__".to_string(),
            value: "large_metric".to_string(),
        }];

        let total_samples = MAX_SAMPLES_PER_XOR_CHUNK + 123;
        let samples: Vec<Sample> = (0..total_samples)
            .map(|index| Sample {
                timestamp: index as i64 * 1000,
                value: index as f64,
            })
            .collect();

        let series = ChunkEncoder::encode_series(labels, samples).unwrap();

        assert_eq!(series.chunks.len(), 2);

        let first_chunk = &series.chunks[0];
        let (_, decoded_first_chunk) = read_xor_chunk_data(&first_chunk.data).unwrap();
        assert_eq!(decoded_first_chunk.samples().len(), MAX_SAMPLES_PER_XOR_CHUNK);
        assert_eq!(first_chunk.min_time_ms, 0);
        assert_eq!(
            first_chunk.max_time_ms,
            (MAX_SAMPLES_PER_XOR_CHUNK as i64 - 1) * 1000
        );

        let second_chunk = &series.chunks[1];
        let (_, decoded_second_chunk) = read_xor_chunk_data(&second_chunk.data).unwrap();
        assert_eq!(decoded_second_chunk.samples().len(), 123);
        assert_eq!(
            second_chunk.min_time_ms,
            MAX_SAMPLES_PER_XOR_CHUNK as i64 * 1000
        );
        assert_eq!(
            second_chunk.max_time_ms,
            (total_samples as i64 - 1) * 1000
        );
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
