use anyhow::{Result, bail};
use snap::raw::Decoder;

/// Decompress snappy-compressed data using block format.
///
/// The Prometheus remote write and read protocols use the snappy block format,
/// not the framed format. From the snap crate documentation:
/// > Generally, one only needs to use the raw format if some other
/// > source is generating raw Snappy compressed data and you have
/// > no choice but to do the same. Otherwise, the Snappy frame format
/// > should probably always be preferred.
pub fn decompress_snappy(input: &[u8]) -> Result<Vec<u8>> {
    let max_bytes = crate::config::get()
        .ok()
        .and_then(|config| config.parse_http_body_limit().ok())
        .unwrap_or(64 * 1024 * 1024);
    decompress_snappy_limited(input, max_bytes)
}

fn decompress_snappy_limited(input: &[u8], max_bytes: usize) -> Result<Vec<u8>> {
    let decoded_len = snap::raw::decompress_len(input)?;
    if decoded_len > max_bytes {
        bail!("Decompressed request body exceeds {max_bytes} bytes");
    }
    Ok(Decoder::new().decompress_vec(input)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_decompress_snappy() {
        // Test that we call the snappy library correctly
        use snap::raw::Encoder;
        let input = b"Hello, world!";
        let compressed = Encoder::new().compress_vec(input).unwrap();

        let decompressed = decompress_snappy(&compressed).unwrap();
        assert_eq!(decompressed, input);
        assert!(decompress_snappy_limited(&compressed, input.len() - 1).is_err());
    }
}
