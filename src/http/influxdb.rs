use super::{app_error::AppError, state::HttpServerState};
use crate::datamodel::{
    SensAppDateTime, Sensor, SensorType, TypedSamples, batch_builder::BatchBuilder,
    sensapp_datetime::SensAppDateTimeExt, sensapp_vec::SensAppLabels,
};
use anyhow::Result;
use axum::{
    debug_handler,
    extract::{Query, State},
    http::{HeaderMap, StatusCode},
};
use flate2::read::GzDecoder;
use influxdb_line_protocol::{FieldValue, parse_lines};
use rust_decimal::Decimal;
use serde::Deserialize;
use std::str::FromStr;
use std::{io::Read, str::from_utf8};
use std::{str, sync::Arc};
use tokio_util::bytes::Bytes;
use tracing::{debug, error, info};

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
                d.read_to_string(&mut s).map_err(AppError::bad_request)?;
                Ok(s)
            }
            _ => Err(AppError::bad_request(anyhow::anyhow!(
                "Unsupported content-encoding: {:?}",
                value
            ))),
        },
        // No content-encoding header
        None => {
            let str = from_utf8(bytes).map_err(AppError::bad_request)?;
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
    with_numeric: bool,
) -> Result<(SensorType, TypedSamples)> {
    match field_value {
        FieldValue::I64(value) => {
            if with_numeric {
                Ok((
                    SensorType::Numeric,
                    TypedSamples::one_numeric(Decimal::from(value), datetime),
                ))
            } else {
                Ok((
                    SensorType::Integer,
                    TypedSamples::one_integer(value, datetime),
                ))
            }
        }
        FieldValue::U64(value) => {
            if with_numeric {
                Ok((
                    SensorType::Numeric,
                    TypedSamples::one_numeric(Decimal::from(value), datetime),
                ))
            } else {
                match i64::try_from(value) {
                    Ok(value) => Ok((
                        SensorType::Integer,
                        TypedSamples::one_integer(value, datetime),
                    )),
                    Err(_) => anyhow::bail!("U64 value is too big to be converted to i64"),
                }
            }
        }
        FieldValue::F64(value) => {
            if with_numeric {
                Ok((
                    SensorType::Numeric,
                    TypedSamples::one_numeric(
                        Decimal::from_f64_retain(value).ok_or_else(|| {
                            anyhow::anyhow!(
                                "Failed to convert f64 value {} to Decimal - precision may be too high",
                                value
                            )
                        })?,
                        datetime,
                    ),
                ))
            } else {
                Ok((SensorType::Float, TypedSamples::one_float(value, datetime)))
            }
        }
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

/// InfluxDB Compatible Write API.
///
/// Allows you to write data from InfluxDB or Telegraf to SensApp.
/// [More information.](https://github.com/SINTEF/sensapp/blob/main/docs/INFLUX_DB.md)
#[utoipa::path(
    post,
    path = "/api/v2/write",
    tag = "InfluxDB",
    request_body(
        content = String,
        content_type = "text/plain",
        description = "InfluxDB Line Protocol endpoint. [Reference](https://docs.influxdata.com/influxdb/v2/reference/syntax/line-protocol/).",
        example = "cpu,host=A,region=west usage_system=64.2 1590488773254420000"
    ),
    params(
        ("bucket" = String, Query, description = "Bucket name", example = "sensapp"),
        ("org" = Option<String>, Query, description = "Organization name", example = "sensapp"),
        ("org_id" = Option<String>, Query, description = "Organization ID"),
        ("precision" = Option<String>, Query, description = "Precision of the timestamps. One of ns, us, ms, s"),
    ),
    responses(
        (status = 204, description = "No Content"),
        (status = 400, description = "Bad Request", body = AppError),
        (status = 500, description = "Internal Server Error", body = AppError),
    )
)]
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
    debug!(
        "InfluxDB publish: bucket={}, org={:?}, org_id={:?}, precision={:?}",
        bucket, org, org_id, precision
    );

    // Requires org or org_id
    if org.is_none() && org_id.is_none() {
        return Err(AppError::bad_request(anyhow::anyhow!(
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
                return Err(AppError::bad_request(anyhow::anyhow!(
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
                            return Err(AppError::internal_server_error(error));
                        }
                    },
                };

                let url_encoded_field_name = urlencoding::encode(&measurement).to_string();

                for (field_key, field_value) in line.field_set {
                    let unit = None;
                    let (sensor_type, value) = match influxdb_field_to_sensapp(
                        field_value,
                        datetime,
                        state.influxdb_with_numeric,
                    ) {
                        Ok((sensor_type, value)) => (sensor_type, value),
                        Err(error) => {
                            return Err(AppError::bad_request(error));
                        }
                    };
                    let name = compute_field_name(&url_encoded_field_name, &field_key);
                    let sensor = Sensor::new_without_uuid(name, sensor_type, unit, tags.clone())?;
                    batch_builder.add(Arc::new(sensor), value).await?;
                }
            }
            Err(error) => {
                return Err(AppError::bad_request(error));
            }
        }
    }

    match batch_builder.send_what_is_left(state.storage.clone()).await {
        Ok(true) => {
            info!("InfluxDB: Batch sent successfully");
        }
        Ok(false) => {
            debug!("InfluxDB: No data to send");
        }
        Err(error) => {
            error!("InfluxDB: Error sending batch: {:?}", error);
            return Err(AppError::internal_server_error(error));
        }
    }

    // OK no content
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::config::load_configuration_for_tests;
    use crate::storage::storage_factory::create_storage_from_connection_string;
    use flate2::Compression;
    use flate2::write::GzEncoder;
    use influxdb_line_protocol::EscapedStr;
    use serial_test::serial;
    use std::io::Write;

    /// Helper to get test database URL - uses the centralized constant from test_utils
    fn get_test_database_url() -> String {
        sensapp::test_utils::get_test_database_url()
    }

    #[test]
    #[serial]
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
    #[serial]
    async fn test_publish_influxdb() {
        _ = load_configuration_for_tests();

        let connection_string = get_test_database_url();
        let storage = create_storage_from_connection_string(&connection_string)
            .await
            .unwrap();
        storage.create_or_migrate().await.unwrap();
        storage.cleanup_test_data().await.unwrap();

        let state = State(HttpServerState {
            name: Arc::new("influxdb test".to_string()),
            storage,
            influxdb_with_numeric: false,
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

        // Cleanup test data
        state.0.storage.cleanup_test_data().await.unwrap();
    }

    #[test]
    #[serial]
    fn test_influxdb_field_to_sensapp() {
        let datetime = SensAppDateTime::from_unix_seconds(0.0);

        // Test integer types with with_numeric=false (default Integer)
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

        // Test integer types with with_numeric=true (Numeric/Decimal mode)
        let result = influxdb_field_to_sensapp(FieldValue::I64(42), datetime, true).unwrap();
        assert_eq!(
            result,
            (
                SensorType::Numeric,
                TypedSamples::one_numeric(Decimal::from(42), datetime)
            )
        );

        let result = influxdb_field_to_sensapp(FieldValue::U64(42), datetime, true).unwrap();
        assert_eq!(
            result,
            (
                SensorType::Numeric,
                TypedSamples::one_numeric(Decimal::from(42), datetime)
            )
        );

        // Test F64 with with_numeric=false (default Float)
        let result = influxdb_field_to_sensapp(FieldValue::F64(42.0), datetime, false).unwrap();
        assert_eq!(
            result,
            (SensorType::Float, TypedSamples::one_float(42.0, datetime))
        );

        // Test F64 with with_numeric=true (Numeric/Decimal mode)
        let result = influxdb_field_to_sensapp(FieldValue::F64(42.0), datetime, true).unwrap();
        assert_eq!(
            result,
            (
                SensorType::Numeric,
                TypedSamples::one_numeric(Decimal::from_f64_retain(42.0).unwrap(), datetime)
            )
        );

        // Test string type
        let result =
            influxdb_field_to_sensapp(FieldValue::String(EscapedStr::from("test")), datetime, true)
                .unwrap();
        assert_eq!(
            result,
            (
                SensorType::String,
                TypedSamples::one_string("test".to_string(), datetime)
            )
        );

        // Test boolean type
        let result = influxdb_field_to_sensapp(FieldValue::Boolean(true), datetime, true).unwrap();
        assert_eq!(
            result,
            (
                SensorType::Boolean,
                TypedSamples::one_boolean(true, datetime)
            )
        );
    }

    #[test]
    #[serial]
    fn test_convert_too_high_u64_to_i64() {
        let datetime = SensAppDateTime::from_unix_seconds(0.0);

        // With with_numeric=false, too high u64 values should fail (can't convert to i64)
        let result =
            influxdb_field_to_sensapp(FieldValue::U64(i64::MAX as u64 + 1), datetime, false);
        assert!(result.is_err());

        // With with_numeric=true, high u64 values should succeed (converted to Decimal)
        let result =
            influxdb_field_to_sensapp(FieldValue::U64(i64::MAX as u64 + 1), datetime, true);
        assert!(result.is_ok());
        assert_eq!(
            result.unwrap(),
            (
                SensorType::Numeric,
                TypedSamples::one_numeric(Decimal::from(i64::MAX as u64 + 1), datetime)
            )
        );
    }

    #[tokio::test]
    #[serial]
    async fn test_publish_influxdb_with_numeric_enabled() {
        _ = load_configuration_for_tests();

        let connection_string = get_test_database_url();
        let storage = create_storage_from_connection_string(&connection_string)
            .await
            .unwrap();
        storage.create_or_migrate().await.unwrap();
        storage.cleanup_test_data().await.unwrap();

        // Test with influxdb_with_numeric enabled
        let state = State(HttpServerState {
            name: Arc::new("influxdb numeric test".to_string()),
            storage: storage.clone(),
            influxdb_with_numeric: true,
        });

        // Test with integer value
        let headers = HeaderMap::new();
        let query = Query(InfluxDBQueryParams {
            bucket: "test_numeric".to_string(),
            org: Some("test_numeric".to_string()),
            org_id: None,
            precision: None,
        });
        let bytes = Bytes::from("memory,host=B usage_int=42i,usage_float=3.14 1590488773254420000");
        let result = publish_influxdb(state.clone(), headers, query, bytes)
            .await
            .unwrap();
        assert_eq!(result, StatusCode::NO_CONTENT);

        // Test with high u64 value that exceeds i64::MAX - should succeed with numeric enabled
        let headers = HeaderMap::new();
        let query = Query(InfluxDBQueryParams {
            bucket: "test_numeric".to_string(),
            org: Some("test_numeric".to_string()),
            org_id: None,
            precision: None,
        });
        let bytes = Bytes::from("memory usage_big=9223372036854775808u 1590488773254420000");
        let result = publish_influxdb(state.clone(), headers, query, bytes)
            .await
            .unwrap();
        assert_eq!(result, StatusCode::NO_CONTENT);

        //storage.cleanup_test_data().await.unwrap();
    }

    #[test]
    #[serial]
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
