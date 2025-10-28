use crate::datamodel::SensAppDateTime;
use crate::exporters::{ArrowConverter, CsvConverter, JsonlConverter, SenMLConverter};
use crate::http::app_error::AppError;
use crate::http::state::HttpServerState;
use axum::Json;
use axum::extract::{Path, Query, State};
use serde::Deserialize;
use serde_json::{Value, json};
use std::str::FromStr;

#[derive(Debug, Clone, PartialEq)]
pub enum ExportFormat {
    Senml, // SenML JSON format (RFC 8428) - also accessible as "json"
    Csv,   // Comma-separated values
    Jsonl, // JSON Lines (one JSON object per line)
    Arrow, // Apache Arrow IPC file format (.arrow)
}

impl ExportFormat {
    /// Parse format from file extension
    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext.to_lowercase().as_str() {
            "json" | "senml" => Some(ExportFormat::Senml), // Both json and senml map to SenML
            "csv" => Some(ExportFormat::Csv),
            "jsonl" | "ndjson" => Some(ExportFormat::Jsonl), // Support both extensions
            "arrow" | "ipc" => Some(ExportFormat::Arrow),    // Apache Arrow file format
            _ => None,
        }
    }

    /// Get the appropriate Content-Type header
    pub fn content_type(&self) -> &'static str {
        match self {
            ExportFormat::Senml => "application/json", // SenML is JSON
            ExportFormat::Csv => "text/csv",
            ExportFormat::Jsonl => "application/x-ndjson",
            ExportFormat::Arrow => "application/vnd.apache.arrow.file",
        }
    }
}

/// Parse ISO8601/RFC3339 datetime string to SensAppDateTime using hifitime
///
/// Supports formats like:
/// - 2024-01-15T10:30:00Z
/// - 2024-01-15T10:30:00.123Z
/// - 2024-01-15T10:30:00+01:00
/// - 2024-01-15 (date only, treated as midnight UTC)
fn parse_datetime_string(datetime_str: &str) -> Result<SensAppDateTime, String> {
    // Use hifitime's built-in parsing
    SensAppDateTime::from_gregorian_str(datetime_str)
        .map_err(|e| format!("Invalid datetime format '{}': {}", datetime_str, e))
}

#[derive(Debug, Deserialize)]
pub struct SensorDataQuery {
    pub start: Option<String>,
    pub end: Option<String>,
    pub limit: Option<usize>,
    pub format: Option<String>,
}

#[derive(Debug, Deserialize)]
pub struct SeriesQuery {
    pub metric: Option<String>,
    pub limit: Option<usize>,
    pub bookmark: Option<String>,
}

/// List unique metrics (measurement types) with aggregated information in DCAT catalog format.
#[utoipa::path(
    get,
    path = "/metrics",
    tag = "SensApp",
    responses(
        (status = 200, description = "Metrics catalog in DCAT format", body = Value)
    )
)]
pub async fn list_metrics(State(state): State<HttpServerState>) -> Result<Json<Value>, AppError> {
    let metrics = state.storage.list_metrics().await?;

    // Create DCAT catalog structure for metrics
    let datasets: Vec<Value> = metrics
        .iter()
        .map(|metric| {
            // Create keywords from metric type and label dimensions
            let mut keywords = vec!["metric", "aggregated", "time-series"];
            keywords.push(match metric.sensor_type {
                crate::datamodel::SensorType::Integer => "integer",
                crate::datamodel::SensorType::Float => "float",
                crate::datamodel::SensorType::String => "string",
                crate::datamodel::SensorType::Boolean => "boolean",
                crate::datamodel::SensorType::Location => "location",
                crate::datamodel::SensorType::Json => "json",
                crate::datamodel::SensorType::Blob => "blob",
                crate::datamodel::SensorType::Numeric => "numeric",
            });

            // Add label dimensions as keywords
            for label_key in &metric.label_keys {
                keywords.push(label_key);
            }

            let mut dataset = json!({
                "@type": "dcat:Dataset",
                "@id": metric.name.clone(),
                "dct:identifier": format!("metric:{}", metric.name),
                "dct:title": metric.name,
                "dct:description": format!("Aggregated metric '{}' containing {} time series with dimensions: {}",
                    metric.name,
                    metric.series_count,
                    if metric.label_keys.is_empty() {
                        "none".to_string()
                    } else {
                        metric.label_keys.join(", ")
                    }
                ),
                "dcat:keyword": keywords,
                "dct:format": "DCAT",
                "dcat:mediaType": "application/json",
                "sensor:type": metric.sensor_type,
                "sensor:seriesCount": metric.series_count,
                "sensor:labelDimensions": metric.label_keys,
                "dct:temporal": {
                    "@type": "dct:PeriodOfTime"
                },
                "dcat:distribution": [
                    {
                        "@type": "dcat:Distribution",
                        "dcat:accessURL": format!("/series?metric={}", urlencoding::encode(&metric.name)),
                        "dcat:mediaType": "application/json",
                        "dct:format": "DCAT Series Catalog",
                        "dct:description": format!("All {} time series for this metric", metric.series_count)
                    }
                ]
            });

            // Only include the unit field if the metric has a unit
            if let Some(unit) = &metric.unit {
                dataset["sensor:unit"] = json!(unit.name);
            }

            dataset
        })
        .collect();

    let catalog = json!({
        "@context": {
            "dcat": "http://www.w3.org/ns/dcat#",
            "dct": "http://purl.org/dc/terms/",
            "foaf": "http://xmlns.com/foaf/0.1/",
            "sensor": "http://sensapp.io/ns/sensor#"
        },
        "@type": "dcat:Catalog",
        "@id": "sensapp_metrics_catalog",
        "dct:title": "SensApp Metrics Catalog",
        "dct:description": "Catalog of aggregated metrics available in SensApp platform",
        "dct:publisher": {
            "@type": "foaf:Organization",
            "foaf:name": "SensApp"
        },
        "dcat:dataset": datasets
    });

    Ok(Json(catalog))
}

/// List all series (time series) in DCAT catalog format.
#[utoipa::path(
    get,
    path = "/series",
    tag = "SensApp",
    params(
        ("metric" = Option<String>, Query, description = "Filter series by metric name")
    ),
    responses(
        (status = 200, description = "Time series catalog in DCAT format", body = Value)
    )
)]
pub async fn list_series(
    State(state): State<HttpServerState>,
    Query(query): Query<SeriesQuery>,
) -> Result<axum::response::Response, AppError> {
    // Validate and cap the limit
    let limit = query.limit.map(|l| l.min(crate::storage::MAX_LIST_SERIES_LIMIT));

    // Get the series metadata including labels and UUIDs, optionally filtered by metric
    let result = state
        .storage
        .list_series(query.metric.as_deref(), limit, query.bookmark.as_deref())
        .await?;

    // Create DCAT catalog structure
    let datasets: Vec<Value> = result
        .series
        .iter()
        .map(|sensor| {
            // Create keywords from sensor type, unit, and labels
            let mut keywords = vec!["sensor", "IoT", "time-series"];
            keywords.push(match sensor.sensor_type {
                crate::datamodel::SensorType::Integer => "integer",
                crate::datamodel::SensorType::Float => "float",
                crate::datamodel::SensorType::String => "string",
                crate::datamodel::SensorType::Boolean => "boolean",
                crate::datamodel::SensorType::Location => "location",
                crate::datamodel::SensorType::Json => "json",
                crate::datamodel::SensorType::Blob => "blob",
                crate::datamodel::SensorType::Numeric => "numeric",
            });

            // Add label keys as keywords
            for (key, _) in sensor.labels.iter() {
                keywords.push(key);
            }

            let sensor_uuid = sensor.uuid.to_string();

            // Build Prometheus-style ID like: metric_name{label1="value1",label2="value2"}
            let prometheus_id = if sensor.labels.is_empty() {
                sensor.name.clone()
            } else {
                let labels_str = sensor.labels.iter()
                    .map(|(k, v)| format!("{}=\"{}\"", k, v))
                    .collect::<Vec<_>>()
                    .join(",");
                format!("{}{{{}}}", sensor.name, labels_str)
            };

            let mut dataset = json!({
                "@type": "dcat:Dataset",
                "@id": prometheus_id,
                "dct:identifier": sensor_uuid,
                "dct:title": sensor.name,
                "dct:description": format!("Sensor data from {} ({})", sensor.name, sensor.sensor_type),
                "dcat:keyword": keywords,
                "dct:format": "JSON",
                "dcat:mediaType": "application/json",
                "sensor:type": sensor.sensor_type,
                "sensor:labels": sensor.labels.iter().map(|(k, v)| json!({k: v})).collect::<Vec<_>>(),
                "dct:temporal": {
                    "@type": "dct:PeriodOfTime"
                },
                "dcat:distribution": [
                    {
                        "@type": "dcat:Distribution",
                        "dcat:downloadURL": format!("/series/{}?format=senml", sensor_uuid),
                        "dcat:mediaType": "application/senml+json",
                        "dct:format": "SenML JSON"
                    },
                    {
                        "@type": "dcat:Distribution",
                        "dcat:downloadURL": format!("/series/{}?format=csv", sensor_uuid),
                        "dcat:mediaType": "text/csv",
                        "dct:format": "CSV"
                    },
                    {
                        "@type": "dcat:Distribution",
                        "dcat:downloadURL": format!("/series/{}?format=jsonl", sensor_uuid),
                        "dcat:mediaType": "application/x-ndjson",
                        "dct:format": "JSON Lines"
                    }
                ]
            });

            // Only include the unit field if the sensor has a unit
            if let Some(unit) = &sensor.unit {
                dataset["sensor:unit"] = json!(unit.name);
            }

            dataset
        })
        .collect();

    // Build catalog with Hydra pagination metadata if there's a next page
    let mut catalog = json!({
        "@context": {
            "dcat": "http://www.w3.org/ns/dcat#",
            "dct": "http://purl.org/dc/terms/",
            "foaf": "http://xmlns.com/foaf/0.1/",
            "hydra": "http://www.w3.org/ns/hydra/core#"
        },
        "@type": "dcat:Catalog",
        "@id": "sensapp_series_catalog",
        "dct:title": "SensApp Series Catalog",
        "dct:description": "Catalog of available time series in SensApp platform",
        "dct:publisher": {
            "@type": "foaf:Organization",
            "foaf:name": "SensApp"
        },
        "dcat:dataset": datasets
    });

    // Add Hydra pagination metadata if there's a next page
    let link_header = if let Some(bookmark) = &result.bookmark {
        // Build the next page URL
        let mut next_url = format!("/series?limit={}&bookmark={}", limit.unwrap_or(crate::storage::DEFAULT_LIST_SERIES_LIMIT), bookmark);
        if let Some(metric) = &query.metric {
            next_url = format!("{}&metric={}", next_url, urlencoding::encode(metric));
        }

        // Add Hydra view to catalog
        catalog["hydra:view"] = json!({
            "@type": "hydra:PartialCollectionView",
            "hydra:next": next_url,
            "hydra:itemsPerPage": limit.unwrap_or(crate::storage::DEFAULT_LIST_SERIES_LIMIT)
        });

        // Return Link header value
        Some(format!("<{}>; rel=\"next\"", next_url))
    } else {
        None
    };

    // Build response with optional Link header
    let mut response = axum::response::Response::builder()
        .status(200)
        .header("Content-Type", "application/json");

    if let Some(link) = link_header {
        response = response.header("Link", link);
    }

    let body = serde_json::to_string(&catalog)
        .map_err(|e| AppError::internal_server_error(anyhow::anyhow!("Failed to serialize catalog: {}", e)))?;

    response
        .body(axum::body::Body::from(body))
        .map_err(|e| AppError::internal_server_error(anyhow::anyhow!("Failed to build response: {}", e)))
}

/// Get series data in various formats based on query parameter.
#[utoipa::path(
    get,
    path = "/series/{series_uuid}",
    tag = "SensApp",
    params(
        ("series_uuid" = String, Path, description = "UUID of the series"),
        ("format" = Option<String>, Query, description = "Output format: senml, csv, or jsonl (default: senml)"),
        ("start" = Option<String>, Query, description = "Start datetime in ISO 8601 format (e.g., '2024-01-15T10:30:00Z')"),
        ("end" = Option<String>, Query, description = "End datetime in ISO 8601 format (e.g., '2024-01-15T11:00:00Z')"),
        ("limit" = Option<usize>, Query, description = "Maximum number of samples (default: 10,000,000)")
    ),
    responses(
        (status = 200, description = "Series data in requested format", body = Value),
        (status = 404, description = "Series not found"),
        (status = 400, description = "Invalid format")
    )
)]
pub async fn get_series_data(
    State(state): State<HttpServerState>,
    Path(series_uuid): Path<String>,
    Query(query): Query<SensorDataQuery>,
) -> Result<axum::response::Response, AppError> {
    // Parse format from query parameter, default to SenML/JSON
    let format = match query.format.as_deref() {
        Some(format_str) => ExportFormat::from_extension(format_str).ok_or_else(|| {
            AppError::bad_request(anyhow::anyhow!(
                "Unsupported export format '{}'. Supported formats: senml, csv, jsonl, arrow",
                format_str
            ))
        })?,
        None => ExportFormat::Senml, // Default to SenML/JSON format
    };

    // Validate UUID format
    let _parsed_uuid = uuid::Uuid::from_str(&series_uuid).map_err(|_| {
        AppError::bad_request(anyhow::anyhow!("Invalid UUID format: '{}'", series_uuid))
    })?;

    // Parse datetime parameters
    let start_time = match query.start.as_ref() {
        Some(start_str) => Some(parse_datetime_string(start_str).map_err(|e| {
            AppError::bad_request(anyhow::anyhow!("Invalid start datetime: {}", e))
        })?),
        None => None,
    };

    let end_time =
        match query.end.as_ref() {
            Some(end_str) => Some(parse_datetime_string(end_str).map_err(|e| {
                AppError::bad_request(anyhow::anyhow!("Invalid end datetime: {}", e))
            })?),
            None => None,
        };

    // Query series data from storage by UUID
    let series_data = state
        .storage
        .query_sensor_data(&series_uuid, start_time, end_time, query.limit)
        .await?;

    let series_data = match series_data {
        Some(data) => data,
        None => {
            return Err(AppError::not_found(anyhow::anyhow!(
                "Series with UUID '{}' not found",
                series_uuid
            )));
        }
    };

    // Convert based on requested format
    let response = match format {
        ExportFormat::Senml => {
            let json_value = SenMLConverter::to_senml_json(&series_data)
                .map_err(AppError::internal_server_error)?;
            axum::response::Response::builder()
                .header("content-type", format.content_type())
                .body(json_value.to_string().into())
        }
        ExportFormat::Csv => {
            let csv_content =
                CsvConverter::to_csv(&series_data).map_err(AppError::internal_server_error)?;
            axum::response::Response::builder()
                .header("content-type", format.content_type())
                .body(csv_content.into())
        }
        ExportFormat::Jsonl => {
            let jsonl_content =
                JsonlConverter::to_jsonl(&series_data).map_err(AppError::internal_server_error)?;
            axum::response::Response::builder()
                .header("content-type", format.content_type())
                .body(jsonl_content.into())
        }
        ExportFormat::Arrow => {
            let arrow_bytes = ArrowConverter::to_arrow_file(&series_data)
                .map_err(AppError::internal_server_error)?;
            axum::response::Response::builder()
                .header("content-type", format.content_type())
                .body(arrow_bytes.into())
        }
    }
    .map_err(|e| {
        AppError::internal_server_error(anyhow::anyhow!("Failed to build response: {}", e))
    })?;

    Ok(response)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_export_format_from_extension() {
        assert_eq!(
            ExportFormat::from_extension("json"),
            Some(ExportFormat::Senml)
        );
        assert_eq!(
            ExportFormat::from_extension("senml"),
            Some(ExportFormat::Senml)
        );
        assert_eq!(ExportFormat::from_extension("csv"), Some(ExportFormat::Csv));
        assert_eq!(
            ExportFormat::from_extension("jsonl"),
            Some(ExportFormat::Jsonl)
        );
        assert_eq!(
            ExportFormat::from_extension("ndjson"),
            Some(ExportFormat::Jsonl)
        );
        assert_eq!(
            ExportFormat::from_extension("arrow"),
            Some(ExportFormat::Arrow)
        );
        assert_eq!(
            ExportFormat::from_extension("ipc"),
            Some(ExportFormat::Arrow)
        );
        assert_eq!(ExportFormat::from_extension("txt"), None);
    }

    #[test]
    fn test_export_format_content_type() {
        assert_eq!(ExportFormat::Senml.content_type(), "application/json");
        assert_eq!(ExportFormat::Csv.content_type(), "text/csv");
        assert_eq!(ExportFormat::Jsonl.content_type(), "application/x-ndjson");
        assert_eq!(
            ExportFormat::Arrow.content_type(),
            "application/vnd.apache.arrow.file"
        );
    }

    #[test]
    fn test_parse_datetime_string() {
        // Test basic ISO 8601 format
        let result = parse_datetime_string("2024-01-15T10:30:00Z");
        assert!(result.is_ok());

        // Test with fractional seconds
        let result = parse_datetime_string("2024-01-15T10:30:00.123Z");
        assert!(result.is_ok());

        // Test with timezone offset
        let result = parse_datetime_string("2024-01-15T10:30:00+01:00");
        assert!(result.is_ok());

        // Test with negative timezone offset
        let result = parse_datetime_string("2024-01-15T10:30:00-05:00");
        assert!(result.is_ok());

        // Test invalid format
        let result = parse_datetime_string("invalid-date");
        assert!(result.is_err());

        // Test malformed format
        let result = parse_datetime_string("2024-13-45T25:75:99Z");
        assert!(result.is_err());

        // Test date-only format (should succeed - hifitime treats as midnight)
        let result = parse_datetime_string("2024-01-15");
        assert!(result.is_ok(), "Date-only format should parse to midnight");
    }

    #[test]
    fn test_parse_datetime_timezone_conversion() {
        use crate::storage::common::datetime_to_micros;

        // Test that times in different timezones are properly converted to UTC microseconds
        let utc_time = parse_datetime_string("2024-01-15T10:30:00Z").unwrap();
        let plus_one_time = parse_datetime_string("2024-01-15T11:30:00+01:00").unwrap();
        let minus_five_time = parse_datetime_string("2024-01-15T05:30:00-05:00").unwrap();

        // All should be equal when converted to UTC microseconds
        let utc_micros = datetime_to_micros(&utc_time);
        let plus_one_micros = datetime_to_micros(&plus_one_time);
        let minus_five_micros = datetime_to_micros(&minus_five_time);

        assert_eq!(utc_micros, plus_one_micros);
        assert_eq!(utc_micros, minus_five_micros);
    }

    #[test]
    fn test_parse_datetime_precision() {
        use crate::storage::common::datetime_to_micros;

        // Test microsecond precision
        let base_time = parse_datetime_string("2024-01-15T10:30:00Z").unwrap();
        let with_millis = parse_datetime_string("2024-01-15T10:30:00.001Z").unwrap();

        let base_micros = datetime_to_micros(&base_time);
        let with_millis_micros = datetime_to_micros(&with_millis);

        // Should differ by 1000 microseconds (1 millisecond)
        assert_eq!(with_millis_micros - base_micros, 1000);
    }

    #[test]
    fn test_parse_datetime_edge_cases() {
        // Test leap year
        let result = parse_datetime_string("2024-02-29T00:00:00Z");
        assert!(result.is_ok(), "Should handle leap year correctly");

        // Test non-leap year (should fail)
        let result = parse_datetime_string("2023-02-29T00:00:00Z");
        assert!(
            result.is_err(),
            "Should reject invalid date in non-leap year"
        );

        // Test end of year
        let result = parse_datetime_string("2024-12-31T23:59:59Z");
        assert!(result.is_ok(), "Should handle end of year");
    }

    #[test]
    fn test_prometheus_id_generation() {
        use crate::datamodel::{Sensor, SensorType, sensapp_vec::SensAppLabels, unit::Unit};
        use smallvec::smallvec;
        use uuid::Uuid;

        // Test with no labels
        let sensor_no_labels = Sensor::new(
            Uuid::new_v4(),
            "temperature".to_string(),
            SensorType::Float,
            Some(Unit::new("celsius".to_string(), None)),
            None,
        );

        let prometheus_id = if sensor_no_labels.labels.is_empty() {
            sensor_no_labels.name.clone()
        } else {
            let labels_str = sensor_no_labels
                .labels
                .iter()
                .map(|(k, v)| format!("{}=\"{}\"", k, v))
                .collect::<Vec<_>>()
                .join(",");
            format!("{}{{{}}}", sensor_no_labels.name, labels_str)
        };
        assert_eq!(prometheus_id, "temperature");

        // Test with single label
        let mut labels_single: SensAppLabels = smallvec![];
        labels_single.push(("location".to_string(), "office".to_string()));
        let sensor_single_label = Sensor::new(
            Uuid::new_v4(),
            "temperature".to_string(),
            SensorType::Float,
            Some(Unit::new("celsius".to_string(), None)),
            Some(labels_single),
        );

        let prometheus_id = if sensor_single_label.labels.is_empty() {
            sensor_single_label.name.clone()
        } else {
            let labels_str = sensor_single_label
                .labels
                .iter()
                .map(|(k, v)| format!("{}=\"{}\"", k, v))
                .collect::<Vec<_>>()
                .join(",");
            format!("{}{{{}}}", sensor_single_label.name, labels_str)
        };
        assert_eq!(prometheus_id, "temperature{location=\"office\"}");

        // Test with multiple labels (should be sorted)
        let mut labels_multiple: SensAppLabels = smallvec![];
        labels_multiple.push(("location".to_string(), "office".to_string()));
        labels_multiple.push(("device".to_string(), "sensor1".to_string()));
        let sensor_multiple_labels = Sensor::new(
            Uuid::new_v4(),
            "temperature".to_string(),
            SensorType::Float,
            Some(Unit::new("celsius".to_string(), None)),
            Some(labels_multiple),
        );

        let prometheus_id = if sensor_multiple_labels.labels.is_empty() {
            sensor_multiple_labels.name.clone()
        } else {
            let labels_str = sensor_multiple_labels
                .labels
                .iter()
                .map(|(k, v)| format!("{}=\"{}\"", k, v))
                .collect::<Vec<_>>()
                .join(",");
            format!("{}{{{}}}", sensor_multiple_labels.name, labels_str)
        };
        // Labels should be sorted by key (device comes before location)
        assert_eq!(
            prometheus_id,
            "temperature{device=\"sensor1\",location=\"office\"}"
        );
    }

    #[test]
    fn test_dcat_catalog_structure() {
        // Test metrics catalog structure
        let metrics_catalog = json!({
            "@context": {
                "dcat": "http://www.w3.org/ns/dcat#",
                "dct": "http://purl.org/dc/terms/",
                "foaf": "http://xmlns.com/foaf/0.1/",
                "sensor": "http://sensapp.io/ns/sensor#"
            },
            "@type": "dcat:Catalog",
            "@id": "sensapp_metrics_catalog",
            "dct:title": "SensApp Metrics Catalog",
            "dct:description": "Catalog of aggregated metrics available in SensApp platform",
            "dcat:dataset": [{
                "@type": "dcat:Dataset",
                "@id": "temperature",
                "dct:identifier": "metric:temperature",
                "dct:title": "temperature"
            }]
        });

        // Validate required DCAT fields are present
        assert_eq!(metrics_catalog["@type"], "dcat:Catalog");
        assert_eq!(metrics_catalog["@id"], "sensapp_metrics_catalog");
        assert!(metrics_catalog["dcat:dataset"].is_array());

        // Test series catalog structure
        let series_catalog = json!({
            "@context": {
                "dcat": "http://www.w3.org/ns/dcat#",
                "dct": "http://purl.org/dc/terms/",
                "foaf": "http://xmlns.com/foaf/0.1/"
            },
            "@type": "dcat:Catalog",
            "@id": "sensapp_series_catalog",
            "dct:title": "SensApp Series Catalog",
            "dct:description": "Catalog of available time series in SensApp platform",
            "dcat:dataset": [{
                "@type": "dcat:Dataset",
                "@id": "temperature{location=\"office\"}",
                "dct:identifier": "uuid-here",
                "dcat:distribution": [{
                    "@type": "dcat:Distribution",
                    "dcat:downloadURL": "/series/uuid-here?format=senml",
                    "dcat:mediaType": "application/senml+json",
                    "dct:format": "SenML JSON"
                }]
            }]
        });

        // Validate required DCAT fields are present
        assert_eq!(series_catalog["@type"], "dcat:Catalog");
        assert_eq!(series_catalog["@id"], "sensapp_series_catalog");
        assert!(series_catalog["dcat:dataset"].is_array());

        // Validate distribution format
        let distribution = &series_catalog["dcat:dataset"][0]["dcat:distribution"][0];
        assert_eq!(distribution["dcat:mediaType"], "application/senml+json");
        assert!(
            distribution["dcat:downloadURL"]
                .as_str()
                .unwrap()
                .starts_with("/series/")
        );
    }
}
