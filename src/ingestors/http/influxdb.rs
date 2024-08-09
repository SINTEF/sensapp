use super::{app_error::AppError, state::HttpServerState};
use crate::datamodel::{
    batch_builder::BatchBuilder, sensapp_datetime::SensAppDateTimeExt, sensapp_vec::SensAppLabels,
    SensAppDateTime, Sensor, SensorType, TypedSamples,
};
use anyhow::Result;
use axum::{
    debug_handler,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
};
use flate2::read::GzDecoder;
use influxdb_line_protocol::{parse_lines, FieldValue};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;
use std::{io::Read, str::from_utf8};
use std::{str, sync::Arc};
use tokio_util::bytes::Bytes;

#[derive(Debug, Deserialize)]
pub struct InfluxDBQueryParams {
    pub bucket: String,
    pub org: Option<String>,
    #[serde(rename = "orgID")]
    pub org_id: Option<String>,
    pub precision: Option<String>,
}

fn bytes_to_string(headers: &HeaderMap, bytes: &Bytes) -> Result<String, AppError> {
    match headers.get("content-encoding") {
        Some(value) => match value.to_str() {
            Ok("gzip") => {
                let mut d = GzDecoder::new(&bytes[..]);
                let mut s = String::new();
                d.read_to_string(&mut s)
                    .map_err(|e| AppError::BadRequest(anyhow::anyhow!(e)))?;
                Ok(s)
            }
            _ => Err(AppError::BadRequest(anyhow::anyhow!(
                "Unsupported content-encoding: {:?}",
                value
            ))),
        },
        // No content-encoding header
        None => {
            let str = from_utf8(bytes).map_err(|e| AppError::BadRequest(anyhow::anyhow!(e)))?;
            Ok(str.to_string())
        }
    }
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
) -> Result<(SensorType, TypedSamples)> {
    match field_value {
        FieldValue::I64(value) => Ok((
            SensorType::Integer,
            TypedSamples::one_integer(value, datetime),
        )),
        FieldValue::U64(value) => match i64::try_from(value) {
            Ok(value) => Ok((
                SensorType::Integer,
                TypedSamples::one_integer(value, datetime),
            )),
            Err(_) => anyhow::bail!("U64 value is too big to be converted to i64"),
        },
        //FieldValue::F64(value) => Ok((SensorType::Float, TypedSamples::one_float(value, datetime))),
        FieldValue::F64(value) => Ok((
            SensorType::Numeric,
            TypedSamples::one_numeric(
                Decimal::from_f64_retain(value)
                    .ok_or(anyhow::anyhow!("Failed to convert f64 to Decimal"))?,
                datetime,
            ),
        )),
        FieldValue::String(value) => Ok((
            SensorType::String,
            TypedSamples::one_string(value.into(), datetime),
        )),
        FieldValue::Boolean(value) => Ok((
            SensorType::Boolean,
            TypedSamples::one_boolean(value, datetime),
        )),
    }
}

#[derive(Debug, Default, PartialEq)]
enum Precision {
    #[default]
    Nanoseconds,
    Microseconds,
    Milliseconds,
    Seconds,
}

impl FromStr for Precision {
    type Err = ();

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ns" => Ok(Precision::Nanoseconds),
            "us" => Ok(Precision::Microseconds),
            "ms" => Ok(Precision::Milliseconds),
            "s" => Ok(Precision::Seconds),
            _ => Err(()),
        }
    }
}

#[debug_handler]
pub async fn publish_influxdb(
    State(state): State<HttpServerState>,
    headers: HeaderMap,
    Query(InfluxDBQueryParams {
        bucket,
        org,
        org_id,
        precision,
    }): Query<InfluxDBQueryParams>,
    bytes: Bytes,
) -> Result<StatusCode, AppError> {
    // println!("InfluxDB publish");
    // println!("bucket: {}", bucket);
    // println!("org: {:?}", org);
    // println!("org_id: {:?}", org_id);
    // println!("precision: {:?}", precision);
    // println!("bytes: {:?}", bytes);
    // println!("headers: {:?}", headers);

    // Requires org or org_id
    if org.is_none() && org_id.is_none() {
        return Err(AppError::BadRequest(anyhow::anyhow!(
            "org or org_id must be specified"
        )));
    }

    // Org or org_id, this is the same for SensApp.
    let common_org_name = match org {
        Some(org) => org,
        None => org_id.unwrap_or_default(),
    };

    // Convert the precision string to a Precision enum
    let precision_enum = match precision {
        Some(precision) => match precision.parse() {
            Ok(precision) => precision,
            Err(_) => {
                return Err(AppError::BadRequest(anyhow::anyhow!(
                    "Invalid precision: {}",
                    precision
                )));
            }
        },
        None => Precision::default(),
    };

    let bytes_string = bytes_to_string(&headers, &bytes)?;
    let parser = parse_lines(&bytes_string);

    let mut batch_builder = BatchBuilder::new()?;

    for line in parser {
        match line {
            Ok(line) => {
                let measurement = line.series.measurement;

                let tags = match &line.series.tag_set {
                    None => None,
                    Some(tags) => {
                        let mut tags_vec = SensAppLabels::new();
                        tags_vec.push(("influxdb_bucket".to_string(), bucket.clone()));
                        tags_vec.push(("influxdb_org".to_string(), common_org_name.clone()));

                        for (key, value) in tags.iter() {
                            tags_vec.push((key.to_string(), value.to_string()));
                        }
                        Some(tags_vec)
                    }
                };

                let datetime = match line.timestamp {
                    Some(timestamp) => match precision_enum {
                        Precision::Nanoseconds => {
                            SensAppDateTime::from_unix_nanoseconds_i64(timestamp)
                        }
                        Precision::Microseconds => {
                            SensAppDateTime::from_unix_microseconds_i64(timestamp)
                        }
                        Precision::Milliseconds => {
                            SensAppDateTime::from_unix_milliseconds_i64(timestamp)
                        }
                        Precision::Seconds => SensAppDateTime::from_unix_seconds_i64(timestamp),
                    },
                    None => match SensAppDateTime::now() {
                        Ok(datetime) => datetime,
                        Err(error) => {
                            return Err(AppError::InternalServerError(anyhow::anyhow!(error)));
                        }
                    },
                };

                let url_encoded_field_name = urlencoding::encode(&measurement).to_string();

                for (field_key, field_value) in line.field_set {
                    let unit = None;
                    let (sensor_type, value) =
                        match influxdb_field_to_sensapp(field_value, datetime) {
                            Ok((sensor_type, value)) => (sensor_type, value),
                            Err(error) => {
                                return Err(AppError::BadRequest(anyhow::anyhow!(error)));
                            }
                        };
                    let name = compute_field_name(&url_encoded_field_name, &field_key);
                    let sensor = Sensor::new_without_uuid(name, sensor_type, unit, tags.clone())?;
                    batch_builder.add(Arc::new(sensor), value).await?;
                }
            }
            Err(error) => {
                return Err(AppError::BadRequest(anyhow::anyhow!(error)));
            }
        }
    }

    // TODO: Remove this println once debugged
    println!("INfluxDB: Sending to the event bus soon");

    match batch_builder.send_what_is_left(state.event_bus).await {
        Ok(Some(mut receiver)) => {
            println!("INfluxDB: Waiting for the receiver");
            receiver.wait().await?;
            println!("INfluxDB: Receiver done");
        }
        Ok(None) => {
            println!("INfluxDB: No receiver");
        }
        Err(error) => {
            println!("INfluxDB: Error: {:?}", error);
            return Err(AppError::InternalServerError(anyhow::anyhow!(error)));
        }
    }

    // OK no content
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use crate::bus::{self, message};

    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
    use influxdb_line_protocol::EscapedStr;
    use std::io::Write;

    #[test]
    fn test_bytes_to_string() {
        let headers = HeaderMap::new();
        let bytes = Bytes::from("test");
        let result = bytes_to_string(&headers, &bytes).unwrap();
        assert_eq!(result, "test".to_string());

        // Gziped bytes
        let mut headers = HeaderMap::new();
        headers.insert("content-encoding", "gzip".parse().unwrap());
        let raw_bytes = "test".as_bytes();
        let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
        encoder.write_all(raw_bytes).unwrap();
        let bytes = Bytes::from(encoder.finish().unwrap());
        let result = bytes_to_string(&headers, &bytes).unwrap();
        assert_eq!(result, "test".to_string());

        // Unsupported content-encoding
        let mut headers = HeaderMap::new();
        headers.insert("content-encoding", "deflate".parse().unwrap());
        let bytes = Bytes::from("test");
        let result = bytes_to_string(&headers, &bytes);
        assert!(result.is_err());

        // Invalid UTF-8 bytes
        let headers = HeaderMap::new();
        // Starts with a 0
        let bytes = Bytes::from(&[0, 159, 146, 150][..]);
        let result = bytes_to_string(&headers, &bytes);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn test_publish_influxdb() {
        let event_bus = bus::event_bus::init_event_bus();
        let mut wololo = event_bus.main_bus_receiver.activate_cloned();
        tokio::spawn(async move {
            while let Ok(message) = wololo.recv().await {
                match message {
                    message::Message::Publish(message::PublishMessage {
                        batch: _,
                        sync_receiver: _,
                        sync_sender,
                    }) => {
                        println!("Received publish message");
                        sync_sender.broadcast(()).await.unwrap();
                    }
                }
            }
        });
        let state = State(HttpServerState {
            name: Arc::new("influxdb test".to_string()),
            event_bus: event_bus.clone(),
        });
        let headers = HeaderMap::new();
        let query = Query(InfluxDBQueryParams {
            bucket: "test".to_string(),
            org: Some("test".to_string()),
            org_id: None,
            precision: None,
        });
        let bytes = Bytes::from("cpu,host=A,region=west usage_system=64i 1590488773254420000");
        let result = publish_influxdb(state.clone(), headers, query, bytes)
            .await
            .unwrap();
        assert_eq!(result, StatusCode::NO_CONTENT);

        // with wrong gzip encoding
        let mut headers = HeaderMap::new();
        headers.insert("content-encoding", "gzip".parse().unwrap());
        let query = Query(InfluxDBQueryParams {
            bucket: "test".to_string(),
            org: None,
            org_id: Some("test".to_string()),
            precision: None,
        });
        let bytes = Bytes::from("definetely not gzip");
        let result = publish_influxdb(state.clone(), headers, query, bytes).await;
        assert!(result.is_err());
        // Check it's an AppError::BadRequest
        assert!(matches!(result, Err(AppError::BadRequest(_))));

        // With wrong line protocol
        let headers = HeaderMap::new();
        let query = Query(InfluxDBQueryParams {
            bucket: "test".to_string(),
            org: Some("test".to_string()),
            org_id: Some("test2".to_string()),
            precision: None,
        });
        let bytes = Bytes::from("wrong line protocol");
        let result = publish_influxdb(state.clone(), headers, query, bytes).await;
        assert!(result.is_err());
        // Check it's an AppError::BadRequest
        assert!(matches!(result, Err(AppError::BadRequest(_))));

        // With no org or org_id
        let headers = HeaderMap::new();
        let query = Query(InfluxDBQueryParams {
            bucket: "test".to_string(),
            org: None,
            org_id: None,
            precision: None,
        });
        let bytes = Bytes::from("cpu,host=A,region=west usage_system=64i 1590488773254420000");
        let result = publish_influxdb(state.clone(), headers, query, bytes).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(AppError::BadRequest(_))));

        // Without tags
        let headers = HeaderMap::new();
        let query = Query(InfluxDBQueryParams {
            bucket: "test".to_string(),
            org: Some("test".to_string()),
            org_id: None,
            precision: None,
        });
        let bytes = Bytes::from("cpu usage_system=64i 1590488773254420000");
        let result = publish_influxdb(state.clone(), headers, query, bytes)
            .await
            .unwrap();
        assert_eq!(result, StatusCode::NO_CONTENT);

        // Without datetime
        let headers = HeaderMap::new();
        let query = Query(InfluxDBQueryParams {
            bucket: "test".to_string(),
            org: Some("test".to_string()),
            org_id: None,
            precision: None,
        });
        let bytes = Bytes::from("cpu,host=A,region=west usage_system=64i");
        let result = publish_influxdb(state.clone(), headers, query, bytes)
            .await
            .unwrap();
        assert_eq!(result, StatusCode::NO_CONTENT);

        // Too high u64 value
        let headers = HeaderMap::new();
        let query = Query(InfluxDBQueryParams {
            bucket: "test".to_string(),
            org: Some("test".to_string()),
            org_id: None,
            precision: None,
        });
        let bytes = Bytes::from("cpu usage_system=9223372036854775808u");
        let result = publish_influxdb(state.clone(), headers, query, bytes).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(AppError::BadRequest(_))));

        // With various precisions
        let headers = HeaderMap::new();
        let query = Query(InfluxDBQueryParams {
            bucket: "test".to_string(),
            org: Some("test".to_string()),
            org_id: None,
            precision: Some("ns".to_string()),
        });
        let bytes = Bytes::from("cpu,host=A,region=west usage_system=64i 1590488773254420000");
        let result = publish_influxdb(state.clone(), headers, query, bytes)
            .await
            .unwrap();
        assert_eq!(result, StatusCode::NO_CONTENT);

        let headers = HeaderMap::new();
        let query = Query(InfluxDBQueryParams {
            bucket: "test".to_string(),
            org: Some("test".to_string()),
            org_id: None,
            precision: Some("us".to_string()),
        });
        let bytes = Bytes::from("cpu,host=A,region=west usage_system=64i 1590488773254420");
        let result = publish_influxdb(state.clone(), headers, query, bytes)
            .await
            .unwrap();
        assert_eq!(result, StatusCode::NO_CONTENT);

        let headers = HeaderMap::new();
        let query = Query(InfluxDBQueryParams {
            bucket: "test".to_string(),
            org: Some("test".to_string()),
            org_id: None,
            precision: Some("ms".to_string()),
        });
        let bytes = Bytes::from("cpu,host=A,region=west usage_system=64i 1590488773254");
        let result = publish_influxdb(state.clone(), headers, query, bytes)
            .await
            .unwrap();
        assert_eq!(result, StatusCode::NO_CONTENT);

        let headers = HeaderMap::new();
        let query = Query(InfluxDBQueryParams {
            bucket: "test".to_string(),
            org: Some("test".to_string()),
            org_id: None,
            precision: Some("s".to_string()),
        });
        let bytes = Bytes::from("cpu,host=A,region=west usage_system=64i 1590488773");
        let result = publish_influxdb(state.clone(), headers, query, bytes)
            .await
            .unwrap();
        assert_eq!(result, StatusCode::NO_CONTENT);

        // With wrong precision
        let headers = HeaderMap::new();
        let query = Query(InfluxDBQueryParams {
            bucket: "test".to_string(),
            org: Some("test".to_string()),
            org_id: None,
            precision: Some("wrong".to_string()),
        });
        let bytes = Bytes::from("cpu,host=A,region=west usage_system=64i 1590488773");
        let result = publish_influxdb(state.clone(), headers, query, bytes).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(AppError::BadRequest(_))));
    }

    #[test]
    fn test_influxdb_field_to_sensapp() {
        let datetime = SensAppDateTime::from_unix_seconds(0.0);
        let result = influxdb_field_to_sensapp(FieldValue::I64(42), datetime).unwrap();
        assert_eq!(
            result,
            (SensorType::Integer, TypedSamples::one_integer(42, datetime))
        );

        let result = influxdb_field_to_sensapp(FieldValue::U64(42), datetime).unwrap();
        assert_eq!(
            result,
            (SensorType::Integer, TypedSamples::one_integer(42, datetime))
        );

        let result = influxdb_field_to_sensapp(FieldValue::F64(42.0), datetime).unwrap();
        assert_eq!(
            result,
            (SensorType::Float, TypedSamples::one_float(42.0, datetime))
        );

        let result =
            influxdb_field_to_sensapp(FieldValue::String(EscapedStr::from("test")), datetime)
                .unwrap();
        assert_eq!(
            result,
            (
                SensorType::String,
                TypedSamples::one_string("test".to_string(), datetime)
            )
        );

        let result = influxdb_field_to_sensapp(FieldValue::Boolean(true), datetime).unwrap();
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
        let result = influxdb_field_to_sensapp(FieldValue::U64(i64::MAX as u64 + 1), datetime);
        assert!(result.is_err());
    }

    #[test]
    fn test_precision_enum() {
        let result = Precision::from_str("ns").unwrap();
        assert_eq!(result, Precision::Nanoseconds);

        let result = Precision::from_str("us").unwrap();
        assert_eq!(result, Precision::Microseconds);

        let result = Precision::from_str("ms").unwrap();
        assert_eq!(result, Precision::Milliseconds);

        let result = Precision::from_str("s").unwrap();
        assert_eq!(result, Precision::Seconds);

        let result = Precision::from_str("wrong");
        assert!(result.is_err());

        let result = Precision::default();
        assert_eq!(result, Precision::Nanoseconds);
    }
}
