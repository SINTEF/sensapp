use super::ParseData;
use crate::datamodel::{
    batch_builder::BatchBuilder, sensapp_datetime::SensAppDateTimeExt, SensAppDateTime,
    SensAppLabels, SensAppLabelsExt, Sensor, SensorType, TypedSamples,
};
use anyhow::Result;
use async_trait::async_trait;
use flate2::read::GzDecoder;
use hybridmap::HybridMap;
use influxdb_line_protocol::{parse_lines, FieldValue};
use precision::Precision;
use rust_decimal::Decimal;
use std::{io::Read, sync::Arc};

pub mod precision;

#[derive(PartialEq, Default)]
pub enum InfluxLineProtocolCompression {
    None,
    Gzip,
    #[default]
    Automatic,
}

#[derive(Default)]
pub struct InfluxParser {
    compression: InfluxLineProtocolCompression,
    precision: Precision,
    floats_as_numeric: bool,
}

impl InfluxParser {
    pub fn new(
        compression: InfluxLineProtocolCompression,
        precision: Precision,
        floats_as_numeric: bool,
    ) -> Self {
        Self {
            compression,
            precision,
            floats_as_numeric,
        }
    }

    pub fn bytes_to_string(&self, bytes: &[u8]) -> Result<String> {
        if self.compression == InfluxLineProtocolCompression::Gzip
            || (self.compression == InfluxLineProtocolCompression::Automatic && is_gzip(bytes))
        {
            let mut d = GzDecoder::new(bytes);
            let mut s = String::new();
            d.read_to_string(&mut s)?;
            Ok(s)
        } else {
            let str = std::str::from_utf8(bytes)?;
            Ok(str.to_string())
        }
    }
}

fn is_gzip(bytes: &[u8]) -> bool {
    // Magic number for gzip
    bytes.starts_with(&[0x1F, 0x8B])
}

fn compute_field_name(url_encoded_measurement_name: &str, field_key: &str) -> String {
    let name = urlencoding::encode(field_key);
    let mut string_builder =
        String::with_capacity(url_encoded_measurement_name.len() + name.len() + 1);
    string_builder.push_str(url_encoded_measurement_name);
    string_builder.push(' '); // Space as separator, as it's not allowed in measurement name nor field key
    string_builder.push_str(&name);
    string_builder
}

fn influxdb_field_to_sensapp(
    field_value: FieldValue,
    datetime: SensAppDateTime,
    floats_as_numeric: bool,
) -> Result<(SensorType, TypedSamples)> {
    match (field_value, floats_as_numeric) {
        (FieldValue::I64(value), _) => Ok((
            SensorType::Integer,
            TypedSamples::one_integer(value, datetime),
        )),
        (FieldValue::U64(value), _) => match i64::try_from(value) {
            Ok(value) => Ok((
                SensorType::Integer,
                TypedSamples::one_integer(value, datetime),
            )),
            Err(_) => anyhow::bail!("U64 value is too big to be converted to i64"),
        },
        (FieldValue::F64(value), false) => {
            Ok((SensorType::Float, TypedSamples::one_float(value, datetime)))
        }
        (FieldValue::F64(value), true) => Ok((
            SensorType::Numeric,
            TypedSamples::one_numeric(
                Decimal::from_f64_retain(value)
                    .ok_or(anyhow::anyhow!("Failed to convert f64 to Decimal"))?,
                datetime,
            ),
        )),
        (FieldValue::String(value), _) => Ok((
            SensorType::String,
            TypedSamples::one_string(value.into(), datetime),
        )),
        (FieldValue::Boolean(value), _) => Ok((
            SensorType::Boolean,
            TypedSamples::one_boolean(value, datetime),
        )),
    }
}

#[async_trait]
impl ParseData for InfluxParser {
    async fn parse_data(
        &self,
        data: &[u8],
        context: Option<HybridMap<String, String>>,
        batch_builder: &mut BatchBuilder,
    ) -> Result<()> {
        let bytes_string = self.bytes_to_string(data)?;
        let parser = parse_lines(&bytes_string);

        let precision = self.precision;

        for line in parser {
            let line = line?;
            let measurement = line.series.measurement;

            let labels = SensAppLabels::build_with_context(
                &context,
                line.series.tag_set.map(|tags| {
                    tags.into_iter()
                        .map(|(k, v)| (k.to_string(), v.to_string()))
                }),
            );

            let datetime = match line.timestamp {
                Some(timestamp) => match precision {
                    Precision::Nanoseconds => SensAppDateTime::from_unix_nanoseconds_i64(timestamp),
                    Precision::Microseconds => {
                        SensAppDateTime::from_unix_microseconds_i64(timestamp)
                    }
                    Precision::Milliseconds => {
                        SensAppDateTime::from_unix_milliseconds_i64(timestamp)
                    }
                    Precision::Seconds => SensAppDateTime::from_unix_seconds_i64(timestamp),
                },
                None => SensAppDateTime::now()?,
            };

            let url_encoded_field_name = urlencoding::encode(&measurement).to_string();

            for (field_key, field_value) in line.field_set {
                let unit = None;
                let (sensor_type, value) =
                    influxdb_field_to_sensapp(field_value, datetime, self.floats_as_numeric)?;
                let name = compute_field_name(&url_encoded_field_name, &field_key);
                let sensor = Sensor::new_without_uuid(name, sensor_type, unit, labels.clone())?;
                batch_builder.add(Arc::new(sensor), value).await?;
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use influxdb_line_protocol::EscapedStr;
    use std::io::Write;

    #[test]
    fn test_bytes_to_string() {
        // Plain text
        let parser = InfluxParser::new(
            InfluxLineProtocolCompression::None,
            Precision::Nanoseconds,
            false,
        );
        let bytes = "test".as_bytes();
        assert_eq!(parser.bytes_to_string(bytes).unwrap(), "test".to_string());

        // With gzip compression
        let parser = InfluxParser::new(
            InfluxLineProtocolCompression::Gzip,
            Precision::Nanoseconds,
            false,
        );
        let bytes = "test".as_bytes();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(bytes).unwrap();
        let bytes = encoder.finish().unwrap();
        assert_eq!(parser.bytes_to_string(&bytes).unwrap(), "test".to_string());

        // With automatic detection, plain text
        let parser = InfluxParser::new(
            InfluxLineProtocolCompression::Automatic,
            Precision::Nanoseconds,
            false,
        );
        let bytes = "test".as_bytes();
        assert_eq!(parser.bytes_to_string(bytes).unwrap(), "test".to_string());

        // With automatic detection, gzip compression
        let parser = InfluxParser::new(
            InfluxLineProtocolCompression::Automatic,
            Precision::Nanoseconds,
            false,
        );
        let bytes = "test".as_bytes();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(bytes).unwrap();
        let bytes = encoder.finish().unwrap();
        assert_eq!(parser.bytes_to_string(&bytes).unwrap(), "test".to_string());

        // Invalid UTF-8 bytes
        let parser = InfluxParser::new(
            InfluxLineProtocolCompression::Automatic,
            Precision::Nanoseconds,
            false,
        );
        let bytes = &[0, 159, 146, 150][..];
        assert!(parser.bytes_to_string(bytes).is_err());
    }

    #[test]
    fn test_influxdb_field_to_sensapp() {
        let datetime = SensAppDateTime::from_unix_seconds(0.0);
        let result = influxdb_field_to_sensapp(FieldValue::I64(42), datetime, false).unwrap();
        assert_eq!(
            result,
            (SensorType::Integer, TypedSamples::one_integer(42, datetime))
        );

        let result = influxdb_field_to_sensapp(FieldValue::U64(42), datetime, false).unwrap();
        assert_eq!(
            result,
            (SensorType::Integer, TypedSamples::one_integer(42, datetime))
        );

        let result = influxdb_field_to_sensapp(FieldValue::F64(42.0), datetime, false).unwrap();
        assert_eq!(
            result,
            (SensorType::Float, TypedSamples::one_float(42.0, datetime))
        );

        let result = influxdb_field_to_sensapp(FieldValue::F64(42.0), datetime, true).unwrap();
        assert_eq!(
            result,
            (
                SensorType::Numeric,
                TypedSamples::one_numeric(rust_decimal::Decimal::new(42, 0), datetime)
            )
        );

        let result = influxdb_field_to_sensapp(
            FieldValue::String(EscapedStr::from("test")),
            datetime,
            false,
        )
        .unwrap();
        assert_eq!(
            result,
            (
                SensorType::String,
                TypedSamples::one_string("test".to_string(), datetime)
            )
        );

        let result = influxdb_field_to_sensapp(FieldValue::Boolean(true), datetime, false).unwrap();
        assert_eq!(
            result,
            (
                SensorType::Boolean,
                TypedSamples::one_boolean(true, datetime)
            )
        );
    }

    #[test]
    fn test_convert_too_high_u64_to_i64() {
        let datetime = SensAppDateTime::from_unix_seconds(0.0);
        let result =
            influxdb_field_to_sensapp(FieldValue::U64(i64::MAX as u64 + 1), datetime, false);
        assert!(result.is_err());
    }
}
