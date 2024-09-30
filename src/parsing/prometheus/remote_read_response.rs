use std::io::Write;

use anyhow::Result;
use itertools::Itertools;
use rusty_chunkenc::{crc32c::write_crc32c, uvarint::write_uvarint, xor::XORChunk, XORSample};

// Note that this file is not automatically generated from the .proto file.
use crate::datamodel::{batch::SingleSensorBatch, Sensor, TypedSamples};

#[derive(prost::Message)]
pub struct ReadResponse {
    #[prost(message, repeated, tag = "1")]
    pub results: Vec<QueryResult>,
}

#[derive(prost::Message)]
pub struct ChunkedReadResponse {
    #[prost(message, repeated, tag = "1")]
    pub chunked_series: Vec<ChunkedSeries>,
    #[prost(int64, tag = "2")]
    pub query_index: i64,
}

#[derive(prost::Message)]
pub struct QueryResult {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
}

#[derive(prost::Message)]
pub struct ChunkedSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub chunks: Vec<Chunk>,
}

#[derive(prost::Message)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
}

#[derive(prost::Message)]
pub struct Label {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

#[derive(prost::Message)]
pub struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

#[derive(prost::Message)]
pub struct Chunk {
    #[prost(int64, tag = "1")]
    pub min_time_ms: i64,
    #[prost(int64, tag = "2")]
    pub max_time_ms: i64,
    #[prost(enumeration = "chunk::Encoding", tag = "3")]
    pub r#type: i32,
    #[prost(bytes, tag = "4")]
    pub data: Vec<u8>,
}

pub mod chunk {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Encoding {
        Unknown = 0,
        Xor = 1,
        Histogram = 2,
        FloatHistogram = 3,
    }
}

impl From<&Sensor> for Vec<Label> {
    fn from(sensor: &Sensor) -> Self {
        let mut labels = vec![
            Label {
                name: "__name__".to_string(),
                value: sensor.name.clone(),
            },
            Label {
                name: "__sensor_type__".to_string(),
                value: sensor.sensor_type.to_string(),
            },
        ];

        if let Some(unit) = &sensor.unit {
            labels.push(Label {
                name: "__unit__".to_string(),
                value: unit.name.clone(),
            });
        }

        for (key, value) in &sensor.labels {
            labels.push(Label {
                name: key.clone(),
                value: value.clone(),
            });
        }

        labels
    }
}

impl TimeSeries {
    pub async fn from_single_sensor_batch(batch: &SingleSensorBatch) -> Self {
        use bigdecimal::ToPrimitive;
        let labels: Vec<Label> = batch.sensor.as_ref().into();
        let samples = match &*batch.samples.read().await {
            TypedSamples::Integer(samples) => samples
                .iter()
                .map(|sample| Sample {
                    value: sample.value as f64,
                    timestamp: sample.datetime.to_unix_milliseconds().floor() as i64,
                })
                .collect(),
            TypedSamples::Numeric(samples) => samples
                .iter()
                .map(|sample| Sample {
                    value: sample.value.to_f64().unwrap_or(f64::NAN),
                    timestamp: sample.datetime.to_unix_milliseconds().floor() as i64,
                })
                .collect(),
            TypedSamples::Float(samples) => samples
                .iter()
                .map(|sample| Sample {
                    value: sample.value,
                    timestamp: sample.datetime.to_unix_milliseconds().floor() as i64,
                })
                .collect(),
            TypedSamples::String(samples) => {
                samples
                    .iter()
                    .map(|sample| Sample {
                        value: f64::NAN, // Cannot convert string to f64
                        timestamp: sample.datetime.to_unix_milliseconds().floor() as i64,
                    })
                    .collect()
            }
            TypedSamples::Boolean(samples) => samples
                .iter()
                .map(|sample| Sample {
                    value: if sample.value { 1.0 } else { 0.0 },
                    timestamp: sample.datetime.to_unix_milliseconds().floor() as i64,
                })
                .collect(),
            TypedSamples::Location(samples) => {
                samples
                    .iter()
                    .map(|sample| Sample {
                        value: f64::NAN, // Cannot convert location to f64
                        timestamp: sample.datetime.to_unix_milliseconds().floor() as i64,
                    })
                    .collect()
            }
            TypedSamples::Blob(samples) => {
                samples
                    .iter()
                    .map(|sample| Sample {
                        value: f64::NAN, // Cannot convert blob to f64
                        timestamp: sample.datetime.to_unix_milliseconds().floor() as i64,
                    })
                    .collect()
            }
            TypedSamples::Json(samples) => {
                samples
                    .iter()
                    .map(|sample| Sample {
                        value: f64::NAN, // Cannot convert JSON to f64
                        timestamp: sample.datetime.to_unix_milliseconds().floor() as i64,
                    })
                    .collect()
            }
        };

        TimeSeries { labels, samples }
    }
}

impl ChunkedSeries {
    pub async fn from_single_sensor_batch(batch: &SingleSensorBatch) -> Self {
        use bigdecimal::ToPrimitive;

        let labels: Vec<Label> = batch.sensor.as_ref().into();
        let samples = batch.samples.read().await;

        let converted_samples: Box<dyn Iterator<Item = XORSample>> = match &*samples {
            TypedSamples::Integer(samples) => Box::new(samples.iter().map(|sample| XORSample {
                value: sample.value as f64,
                timestamp: sample.datetime.to_unix_milliseconds().floor() as i64,
            })),
            TypedSamples::Numeric(samples) => Box::new(samples.iter().map(|sample| XORSample {
                value: sample.value.to_f64().unwrap_or(f64::NAN),
                timestamp: sample.datetime.to_unix_milliseconds().floor() as i64,
            })),
            TypedSamples::Float(samples) => Box::new(samples.iter().map(|sample| XORSample {
                value: sample.value,
                timestamp: sample.datetime.to_unix_milliseconds().floor() as i64,
            })),
            TypedSamples::String(samples) => Box::new(samples.iter().map(|sample| XORSample {
                value: f64::NAN,
                timestamp: sample.datetime.to_unix_milliseconds().floor() as i64,
            })),
            TypedSamples::Boolean(samples) => Box::new(samples.iter().map(|sample| XORSample {
                value: if sample.value { 1.0 } else { 0.0 },
                timestamp: sample.datetime.to_unix_milliseconds().floor() as i64,
            })),
            TypedSamples::Blob(samples) => Box::new(samples.iter().map(|sample| XORSample {
                value: f64::NAN,
                timestamp: sample.datetime.to_unix_milliseconds().floor() as i64,
            })),
            TypedSamples::Json(samples) => Box::new(samples.iter().map(|sample| XORSample {
                value: f64::NAN,
                timestamp: sample.datetime.to_unix_milliseconds().floor() as i64,
            })),
            TypedSamples::Location(samples) => Box::new(samples.iter().map(|sample| XORSample {
                value: f64::NAN,
                timestamp: sample.datetime.to_unix_milliseconds().floor() as i64,
            })),
        };

        let chunks: Vec<Chunk> = converted_samples
            .chunks(120) // Prometheus aims for 120 samples per chunk
            .into_iter()
            .map(|chunk| {
                let xor_chunk = XORChunk::new(chunk.collect::<Vec<_>>());

                let mut chunk_data: Vec<u8> = Vec::new();
                xor_chunk
                    .write(&mut chunk_data)
                    .expect("Failed to marshal XOR chunk");

                Chunk {
                    min_time_ms: xor_chunk
                        .samples()
                        .first()
                        .map(|s| s.timestamp)
                        .unwrap_or(0),
                    max_time_ms: xor_chunk.samples().last().map(|s| s.timestamp).unwrap_or(0),
                    r#type: chunk::Encoding::Xor as i32,
                    data: chunk_data,
                }
            })
            .collect();

        ChunkedSeries { labels, chunks }
    }
}

impl ChunkedReadResponse {
    pub fn promotheus_stream_encode(&self, buffer: &mut Vec<u8>) -> Result<()> {
        use prost::Message;
        // Serialise to protobuf binary in a temporary buffer
        // Because we need to know the final size and the checksum,
        // before the serialization.
        let mut proto_buffer: Vec<u8> = Vec::new();
        self.encode(&mut proto_buffer)?;

        // Start with the size of the buffer
        write_uvarint(proto_buffer.len() as u64, buffer)?;

        // Then the CRC32 Castagnoli checksum
        write_crc32c(&proto_buffer, buffer)?;

        // Then the proto
        buffer.write_all(&proto_buffer)?;

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datamodel::{unit::Unit, SensAppLabels, SensorType};
    use smallvec::smallvec;
    use std::sync::Arc;

    #[test]
    fn test_sensor_to_labels() {
        let mut labels = SensAppLabels::new();
        labels.push(("location".to_string(), "office".to_string()));

        let sensor = Sensor::new(
            uuid::Uuid::new_v4(),
            "temperature".to_string(),
            SensorType::Numeric,
            Some(Unit::new("celsius".to_string(), None)),
            Some(labels),
        );

        let prometheus_labels: Vec<Label> = (&sensor).into();

        assert_eq!(prometheus_labels.len(), 4);
        assert!(prometheus_labels
            .iter()
            .any(|l| l.name == "__name__" && l.value == "temperature"));
        assert!(prometheus_labels
            .iter()
            .any(|l| l.name == "sensor_type" && l.value == "Numeric"));
        assert!(prometheus_labels
            .iter()
            .any(|l| l.name == "unit" && l.value == "celsius"));
        assert!(prometheus_labels
            .iter()
            .any(|l| l.name == "location" && l.value == "office"));
    }

    #[tokio::test]
    async fn test_single_sensor_batch_to_time_series() {
        let sensor = Arc::new(Sensor::new(
            uuid::Uuid::new_v4(),
            "temperature".to_string(),
            SensorType::Numeric,
            Some(Unit::new("celsius".to_string(), None)),
            None,
        ));

        let samples = TypedSamples::Numeric(smallvec![
            crate::datamodel::Sample {
                datetime: SensAppDateTime::from_unix_seconds(1000.0),
                value: rust_decimal::Decimal::new(2000, 2), // 20.00
            },
            crate::datamodel::Sample {
                datetime: SensAppDateTime::from_unix_seconds(2000.0),
                value: rust_decimal::Decimal::new(2100, 2), // 21.00
            },
        ]);

        let batch = SingleSensorBatch::new(sensor, samples);
        let time_series = TimeSeries::from_single_sensor_batch(&batch).await;

        assert_eq!(time_series.labels.len(), 3);
        assert_eq!(time_series.samples.len(), 2);
        assert_eq!(time_series.samples[0].value, 20.00);
        assert_eq!(time_series.samples[0].timestamp, 1000000);
        assert_eq!(time_series.samples[1].value, 21.00);
        assert_eq!(time_series.samples[1].timestamp, 2000000);
    }
}
