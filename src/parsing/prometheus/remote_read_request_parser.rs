use super::remote_read_request_models::ReadRequest;
use anyhow::Result;
use prost::Message;
use snap::raw::Decoder;
use std::io::Cursor;

fn decompress_snappy(input: &[u8]) -> Result<Vec<u8>> {
    Ok(Decoder::new().decompress_vec(input)?)
}

fn parse_protobuf(input: &[u8]) -> Result<ReadRequest> {
    Ok(ReadRequest::decode(&mut Cursor::new(input))?)
}

pub fn parse_remote_read_request(input: &[u8]) -> Result<ReadRequest> {
    let decompressed = decompress_snappy(input)?;
    parse_protobuf(&decompressed)
}
