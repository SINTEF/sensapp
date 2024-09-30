pub mod remote_read_request_models;
pub mod remote_read_request_parser;
pub mod remote_read_response;
pub mod remote_write_models;
pub mod remote_write_parser;

use super::ParseData;
use crate::datamodel::{
    batch_builder::BatchBuilder, sensapp_datetime::SensAppDateTimeExt, unit::Unit, Sample,
    SensAppDateTime, SensAppLabels, SensAppLabelsExt, Sensor, SensorType, TypedSamples,
};
use anyhow::Result;
use async_trait::async_trait;
use hybridmap::HybridMap;
use remote_write_parser::parse_remote_write_request;
use std::sync::Arc;

pub struct PrometheusParser;

#[async_trait]
impl ParseData for PrometheusParser {
    async fn parse_data(
        &self,
        data: &[u8],
        context: Option<HybridMap<String, String>>,
        batch_builder: &mut BatchBuilder,
    ) -> Result<()> {
        // Parse the content
        let write_request = parse_remote_write_request(data)?;

        // Regularly, prometheus sends metadata on the undocumented reserved field,
        // so we stop immediately when it happens.
        if write_request.timeseries.is_empty() {
            return Ok(());
        }

        for time_serie in write_request.timeseries {
            let mut name: Option<String> = None;
            let mut unit: Option<Unit> = None;
            let labels = SensAppLabels::build_with_context(
                &context,
                Some(time_serie.labels.into_iter().map(|label| {
                    match label.name.as_str() {
                        "__name__" => {
                            name = Some(label.value.clone());
                        }
                        "unit" => {
                            unit = Some(Unit::new(label.value.clone(), None));
                        }
                        _ => {}
                    }
                    (label.name, label.value)
                })),
            );
            let name = match name {
                Some(name) => name,
                None => {
                    return Err(anyhow::anyhow!(
                        "A time serie is missing its __name__ label"
                    ));
                }
            };

            // Prometheus has a very simple model, it's always a float.
            let sensor = Sensor::new_without_uuid(name, SensorType::Float, unit, labels)?;

            // We can now add the samples
            let samples = TypedSamples::Float(
                time_serie
                    .samples
                    .into_iter()
                    // Special prometheus NaN value (Stale Marker)
                    .filter(|sample| sample.value.to_bits() != 0x7ff0000000000002)
                    .map(|sample| Sample {
                        datetime: SensAppDateTime::from_unix_milliseconds_i64(sample.timestamp),
                        value: sample.value,
                    })
                    .collect(),
            );

            batch_builder.add(Arc::new(sensor), samples).await?;
        }

        Ok(())
    }
}
