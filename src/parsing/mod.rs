use anyhow::{bail, Result};
use async_trait::async_trait;
use hybridmap::HybridMap;

use crate::datamodel::batch_builder::BatchBuilder;

pub mod compressed;
pub mod geobuf;
pub mod influx;
pub mod prometheus;
pub mod senml;

#[async_trait]
pub trait ParseData: Send + Sync {
    async fn parse_data(
        &self,
        data: &[u8],
        context: Option<HybridMap<String, String>>,
        batch_builder: &mut BatchBuilder,
    ) -> Result<()>;
}

pub fn get_parser_from_name(name: &str) -> Result<Box<dyn ParseData>> {
    match name {
        "prometheus_remote_write" => Ok(Box::new(prometheus::PrometheusParser)),
        "senml_json" => Ok(Box::new(senml::SenMLParser)),
        "senml_json_gzip" => Ok(Box::new(compressed::CompressedParser::new(
            Box::new(senml::SenMLParser),
            compressed::Compression::Gzip,
        ))),
        "senml_json_snappy" => Ok(Box::new(compressed::CompressedParser::new(
            Box::new(senml::SenMLParser),
            compressed::Compression::Snappy,
        ))),
        "senml_json_zstd" => Ok(Box::new(compressed::CompressedParser::new(
            Box::new(senml::SenMLParser),
            compressed::Compression::Zstd,
        ))),
        "geobuf" => Ok(Box::new(geobuf::GeobufParser)),
        "influx_line_protocol" => Ok(Box::new(influx::InfluxParser::default())),
        "influx_line_protocol_gzip" => Ok(Box::new(compressed::CompressedParser::new(
            Box::new(influx::InfluxParser::default()),
            compressed::Compression::Gzip,
        ))),
        "influx_line_protocol_snappy" => Ok(Box::new(compressed::CompressedParser::new(
            Box::new(influx::InfluxParser::default()),
            compressed::Compression::Snappy,
        ))),
        "influx_line_protocol_zstd" => Ok(Box::new(compressed::CompressedParser::new(
            Box::new(influx::InfluxParser::default()),
            compressed::Compression::Zstd,
        ))),
        _ => bail!("Unsupported parser: {}", name),
    }
}
