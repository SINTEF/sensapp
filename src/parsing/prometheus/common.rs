use anyhow::Result;
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
    }
}
