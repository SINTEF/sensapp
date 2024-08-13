use super::remote_write_models::WriteRequest;
use anyhow::Result;
use prost::Message;
use snap::raw::Decoder;
use std::io::Cursor;

fn decompress_snappy(input: &[u8]) -> Result<Vec<u8>> {
    // We must use the snappy Block format, not the framed format,
    // because the Prometheus remote write protocol uses the block format only.
    //
    // The snap crate documentation says:
    // > Generally, one only needs to use the raw format if some other
    // > source is generating raw Snappy compressed data and you have
    // > no choice but to do the same. Otherwise, the Snappy frame format
    // > should probably always be preferred.

    Ok(Decoder::new().decompress_vec(input)?)
}

fn parse_protobuf(input: &[u8]) -> Result<WriteRequest> {
    Ok(WriteRequest::decode(&mut Cursor::new(input))?)
}

pub fn parse_remote_write_request(input: &[u8]) -> Result<WriteRequest> {
    let decompressed = decompress_snappy(input)?;
    parse_protobuf(&decompressed)
}

#[cfg(test)]
mod tests {
    use super::super::remote_write_models::{Label, Sample, TimeSeries};
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
        let input_data = WriteRequest { timeseries: vec![] };
        let input_bytes = input_data.encode_to_vec();

        let _ = parse_protobuf(&input_bytes).unwrap();

        let input = b"not a valid protobuf";
        assert!(parse_protobuf(input).is_err());
    }

    #[test]
    fn test_parse_remote_write_request() {
        let input_data = WriteRequest {
            timeseries: vec![TimeSeries {
                labels: vec![Label {
                    name: "test".to_string(),
                    value: "test".to_string(),
                }],
                samples: vec![Sample {
                    value: 1.0,
                    timestamp: 1,
                }],
            }],
        };
        let input_bytes = input_data.encode_to_vec();

        let compressed = snap::raw::Encoder::new()
            .compress_vec(&input_bytes)
            .unwrap();

        let output = parse_remote_write_request(&compressed).unwrap();

        assert_eq!(output.timeseries.len(), 1);
        assert_eq!(output.timeseries[0].labels.len(), 1);
        assert_eq!(output.timeseries[0].samples.len(), 1);
    }
}
