use async_trait::async_trait;
use hybridmap::HybridMap;
use std::sync::Arc;

use anyhow::{bail, Result};
use sindit_senml::{parse_json, SenMLValueField};

use crate::datamodel::{
    batch_builder::BatchBuilder, sensapp_datetime::SensAppDateTimeExt, unit::Unit, SensAppDateTime,
    SensAppLabels, SensAppLabelsExt, Sensor, SensorType, TypedSamples,
};

use super::ParseData;

pub struct SenMLParser;

#[async_trait]
impl ParseData for SenMLParser {
    async fn parse_data(
        &self,
        data: &[u8],
        context: Option<HybridMap<String, String>>,
        batch_builder: &mut BatchBuilder,
    ) -> Result<()> {
        let data_str = std::str::from_utf8(data)?;
        let records = parse_json(data_str, None)?;

        for record in records {
            let name = record.name;
            let unit = record.unit;
            let value = record.value;
            let sum = record.sum;
            let time = record.time;
            let extra_fields = record.extra_fields;

            let sensapp_time = SensAppDateTime::from_unix_milliseconds_i64(time.timestamp_millis());

            let sensor_type: SensorType;
            let sample: TypedSamples;

            if let Some(sum) = sum {
                if value.is_some() {
                    bail!("Cannot have both value and sum");
                }
                sample = TypedSamples::one_float(sum, sensapp_time);
                sensor_type = SensorType::Float;
            } else if let Some(value) = value {
                match value {
                    SenMLValueField::FloatingPoint(number) => {
                        sample = TypedSamples::one_float(number, sensapp_time);
                        sensor_type = SensorType::Float;
                    }
                    SenMLValueField::StringValue(string) => {
                        sample = TypedSamples::one_string(string, sensapp_time);
                        sensor_type = SensorType::String;
                    }
                    SenMLValueField::BooleanValue(boolean) => {
                        sample = TypedSamples::one_boolean(boolean, sensapp_time);
                        sensor_type = SensorType::Boolean;
                    }
                    SenMLValueField::DataValue(data_value) => {
                        sample = TypedSamples::one_blob(data_value, sensapp_time);
                        sensor_type = SensorType::Blob;
                    }
                }
            } else {
                bail!("No value or sum found");
            }

            let sensapp_unit = unit.map(|unit| Unit::new(unit, None));

            let labels = SensAppLabels::build_with_context(
                &context,
                extra_fields.map(|extra_fields| {
                    extra_fields
                        .into_iter()
                        .map(|(key, value)| (key, value.to_string()))
                }),
            );

            let sensor = Sensor::new_without_uuid(name, sensor_type, sensapp_unit, labels)?;
            batch_builder.add(Arc::new(sensor), sample).await?;
        }

        Ok(())
    }
}
