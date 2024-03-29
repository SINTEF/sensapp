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
