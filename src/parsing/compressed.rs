use anyhow::Result;
use axum::async_trait;
use hybridmap::HybridMap;
use std::io::Read;

use crate::datamodel::batch_builder::BatchBuilder;

use super::ParseData;

#[derive(PartialEq)]
pub enum Compression {
    Gzip,
    Snappy,
    Zstd,
}

pub struct CompressedParser {
    parent_parser: Box<dyn ParseData>,
    compression: Compression,
}

impl CompressedParser {
    pub fn new(parent_parser: Box<dyn ParseData>, compression: Compression) -> Self {
        Self {
            parent_parser,
            compression,
        }
    }

    pub fn new_if_needed(
        parent_parser: Box<dyn ParseData>,
        compression: Option<Compression>,
    ) -> Box<dyn ParseData> {
        match compression {
            Some(compression) => Box::new(CompressedParser::new(parent_parser, compression)),
            None => parent_parser,
        }
    }
}

#[async_trait]
impl ParseData for CompressedParser {
    async fn parse_data(
        &self,
        data: &[u8],
        context: Option<HybridMap<String, String>>,
        batch_builder: &mut BatchBuilder,
    ) -> Result<()> {
        let mut uncompressed_data = Vec::new();

        match self.compression {
            Compression::Gzip => {
                let mut d = flate2::read::GzDecoder::new(data);
                d.read_to_end(&mut uncompressed_data)?;
            }
            Compression::Snappy => {
                let mut d = snap::read::FrameDecoder::new(data);
                d.read_to_end(&mut uncompressed_data)?;
            }
            Compression::Zstd => {
                let mut d = zstd::Decoder::new(data)?;
                d.read_to_end(&mut uncompressed_data)?;
            }
        };

        self.parent_parser
            .parse_data(&uncompressed_data, context, batch_builder)
            .await
    }
}
