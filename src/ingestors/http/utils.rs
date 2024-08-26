use crate::parsing::compressed::Compression;

use super::app_error::AppError;
use anyhow::Result;
use axum::http::HeaderMap;

pub fn get_potentiall_compressed_data(
    headers: &HeaderMap,
) -> Result<Option<Compression>, AppError> {
    match headers.get("content-encoding") {
        Some(content_encoding) => match content_encoding.to_str() {
            Ok("gzip") => Ok(Some(Compression::Gzip)),
            Ok("snappy") | Ok("snappy-framed") => Ok(Some(Compression::Snappy)),
            Ok("zstd") => Ok(Some(Compression::Zstd)),
            Ok("plain") => Ok(None),
            _ => Err(AppError::BadRequest(anyhow::anyhow!(
                "Unsupported content-encoding: {:?}",
                content_encoding
            ))),
        },
        None => Ok(None),
    }
}
