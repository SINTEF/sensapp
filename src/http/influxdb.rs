use super::app_error::AppError;
use anyhow::Result;
use axum::{
    extract::Query,
    http::{HeaderMap, StatusCode},
};
use flate2::read::GzDecoder;
use influxdb_line_protocol::{parse_lines, FieldValue, ParsedLine};
use serde::Deserialize;
use std::str;
use std::{io::Read, str::from_utf8};
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

pub async fn publish_influxdb(
    headers: HeaderMap,
    Query(InfluxDBQueryParams {
        bucket,
        org,
        org_id,
        precision,
    }): Query<InfluxDBQueryParams>,
    bytes: Bytes,
) -> Result<StatusCode, AppError> {
    println!("InfluxDB publish");
    println!("bucket: {}", bucket);
    println!("org: {:?}", org);
    println!("org_id: {:?}", org_id);
    println!("precision: {:?}", precision);
    println!("bytes: {:?}", bytes);
    println!("headers: {:?}", headers);

    // Requires org or org_id
    if org.is_none() && org_id.is_none() {
        return Err(AppError::BadRequest(anyhow::anyhow!(
            "org or org_id must be specified"
        )));
    }

    let bytes_string = bytes_to_string(&headers, &bytes)?;
    let mut parser = parse_lines(&bytes_string);

    /*let lines = parser
    .map(|line| match line {
        Ok(line) => Ok(line),
        Err(error) => Err(AppError::BadRequest(anyhow::anyhow!(error))),
    })
    .collect::<Result<Vec<ParsedLine>, AppError>>()?;*/

    //println!("lines: {:?}", lines);
    for line in parser {
        match line {
            Ok(line) => {
                println!("line: {:?}", line);
            }
            Err(error) => {
                return Err(AppError::BadRequest(anyhow::anyhow!(error)));
            }
        }
    }

    // OK no content
    Ok(StatusCode::NO_CONTENT)
}

#[cfg(test)]
mod tests {
    use super::*;
    use flate2::write::GzEncoder;
    use flate2::Compression;
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
        let headers = HeaderMap::new();
        let query = Query(InfluxDBQueryParams {
            bucket: "test".to_string(),
            org: Some("test".to_string()),
            org_id: None,
            precision: None,
        });
        let bytes = Bytes::from("cpu,host=A,region=west usage_system=64i 1590488773254420000");
        let result = publish_influxdb(headers, query, bytes).await.unwrap();
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
        let result = publish_influxdb(headers, query, bytes).await;
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
        let result = publish_influxdb(headers, query, bytes).await;
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
        let result = publish_influxdb(headers, query, bytes).await;
        assert!(result.is_err());
        assert!(matches!(result, Err(AppError::BadRequest(_))));
    }
}
