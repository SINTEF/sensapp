use super::ParseData;
use crate::datamodel::batch_builder::BatchBuilder;
use anyhow::{bail, Result};
use async_trait::async_trait;
use geobuf::{decode::Decoder, geobuf_pb};
use hybridmap::HybridMap;
use protobuf::Message;

pub struct GeobufParser;

#[async_trait]
impl ParseData for GeobufParser {
    async fn parse_data(
        &self,
        data: &[u8],
        context: Option<HybridMap<String, String>>,
        batch_builder: &mut BatchBuilder,
    ) -> Result<()> {
        let mut geobuf = geobuf_pb::Data::new();
        geobuf.merge_from_bytes(data)?;

        match Decoder::decode(&geobuf) {
            Ok(serde_json::Value::Object(geojson)) => {
                println!("GeoJSON: {:?}", geojson);
            }
            Ok(_) => bail!("Failed to decode Geobuf as GeoJSON"),
            Err(e) => bail!("Failed to decode Geobuf as GeoJSON: {:?}", e),
        }

        Ok(())
    }
}
