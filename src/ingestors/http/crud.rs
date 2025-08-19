use crate::exporters::{CsvConverter, JsonlConverter, SenMLConverter};
use crate::ingestors::http::app_error::AppError;
use crate::ingestors::http::state::HttpServerState;
use axum::Json;
use axum::extract::{Path, Query, State};
use serde::Deserialize;
use serde_json::{Value, json};

#[derive(Debug, Clone, PartialEq)]
pub enum ExportFormat {
    Senml, // SenML JSON format (RFC 8428) - also accessible as "json"
    Csv,   // Comma-separated values
    Jsonl, // JSON Lines (one JSON object per line)
}

impl ExportFormat {
    /// Parse format from file extension
    pub fn from_extension(ext: &str) -> Option<Self> {
        match ext.to_lowercase().as_str() {
            "json" | "senml" => Some(ExportFormat::Senml), // Both json and senml map to SenML
            "csv" => Some(ExportFormat::Csv),
            "jsonl" | "ndjson" => Some(ExportFormat::Jsonl), // Support both extensions
            _ => None,
        }
    }

    /// Get the appropriate Content-Type header
    pub fn content_type(&self) -> &'static str {
        match self {
            ExportFormat::Senml => "application/json", // SenML is JSON
            ExportFormat::Csv => "text/csv",
            ExportFormat::Jsonl => "application/jsonlines", // or "application/x-ndjson"
        }
    }
}

#[derive(Debug, Deserialize)]
pub struct SensorDataQuery {
    pub start: Option<i64>,
    pub end: Option<i64>,
    pub limit: Option<usize>,
}

/// List all sensors in DCAT catalog format.
#[utoipa::path(
    get,
    path = "/sensors",
    tag = "SensApp",
    responses(
        (status = 200, description = "Sensors catalog in DCAT format", body = Value)
    )
)]
pub async fn list_sensors(State(state): State<HttpServerState>) -> Result<Json<Value>, AppError> {
    // Get the simple sensor names and create a DCAT catalog
    let sensor_names = state.storage.list_sensors().await?;

    // Create DCAT catalog structure
    let datasets: Vec<Value> = sensor_names
        .iter()
        .enumerate()
        .map(|(index, sensor_name)| {
            json!({
                "@type": "dcat:Dataset",
                "@id": format!("sensor_{}", index + 1),
                "dct:identifier": sensor_name,
                "dct:title": sensor_name,
                "dct:description": format!("Sensor data from {}", sensor_name),
                "dcat:keyword": ["sensor", "IoT", "time-series"],
                "dct:format": "JSON",
                "dcat:mediaType": "application/json",
                "dct:temporal": {
                    "@type": "dct:PeriodOfTime"
                },
                "dcat:distribution": [
                    {
                        "@type": "dcat:Distribution",
                        "dcat:downloadURL": format!("/sensors/{}.json", sensor_name),
                        "dcat:mediaType": "application/json",
                        "dct:format": "SenML JSON"
                    },
                    {
                        "@type": "dcat:Distribution",
                        "dcat:downloadURL": format!("/sensors/{}.senml", sensor_name),
                        "dcat:mediaType": "application/json",
                        "dct:format": "SenML"
                    },
                    {
                        "@type": "dcat:Distribution",
                        "dcat:downloadURL": format!("/sensors/{}.csv", sensor_name),
                        "dcat:mediaType": "text/csv",
                        "dct:format": "CSV"
                    },
                    {
                        "@type": "dcat:Distribution",
                        "dcat:downloadURL": format!("/sensors/{}.jsonl", sensor_name),
                        "dcat:mediaType": "application/jsonlines",
                        "dct:format": "JSON Lines"
                    }
                ]
            })
        })
        .collect();

    let catalog = json!({
        "@context": {
            "dcat": "http://www.w3.org/ns/dcat#",
            "dct": "http://purl.org/dc/terms/",
            "foaf": "http://xmlns.com/foaf/0.1/"
        },
        "@type": "dcat:Catalog",
        "@id": "sensapp_catalog",
        "dct:title": "SensApp Sensors Catalog",
        "dct:description": "Catalog of available sensors in SensApp platform",
        "dct:publisher": {
            "@type": "foaf:Organization",
            "foaf:name": "SensApp"
        },
        "dcat:dataset": datasets
    });

    Ok(Json(catalog))
}

/// Get sensor data in various formats based on file extension.
#[utoipa::path(
    get,
    path = "/sensors/{sensor_name_with_ext}",
    tag = "SensApp",
    params(
        ("sensor_name_with_ext" = String, Path, description = "Name of the sensor with format extension (.json, .senml, .csv, .jsonl)"),
        ("start" = Option<i64>, Query, description = "Start timestamp in milliseconds"),
        ("end" = Option<i64>, Query, description = "End timestamp in milliseconds"),
        ("limit" = Option<usize>, Query, description = "Maximum number of samples")
    ),
    responses(
        (status = 200, description = "Sensor data in requested format", body = Value),
        (status = 404, description = "Sensor not found"),
        (status = 400, description = "Invalid format")
    )
)]
pub async fn get_sensor_data(
    State(state): State<HttpServerState>,
    Path(sensor_name_with_ext): Path<String>,
    Query(query): Query<SensorDataQuery>,
) -> Result<axum::response::Response, AppError> {
    // Parse sensor name and format from path
    let (sensor_name, format) = if let Some(dot_pos) = sensor_name_with_ext.rfind('.') {
        let sensor_name = sensor_name_with_ext[..dot_pos].to_string();
        let ext = &sensor_name_with_ext[dot_pos + 1..];
        let format = ExportFormat::from_extension(ext)
            .ok_or_else(|| AppError::bad_request(anyhow::anyhow!("Unsupported format: {}", ext)))?;
        (sensor_name, format)
    } else {
        // Default to SenML if no extension
        (sensor_name_with_ext, ExportFormat::Senml)
    };

    // Query sensor data from storage
    let sensor_data = state
        .storage
        .query_sensor_data(&sensor_name, query.start, query.end, query.limit)
        .await?;

    let sensor_data = match sensor_data {
        Some(data) => data,
        None => {
            return Err(AppError::not_found(anyhow::anyhow!(
                "Sensor '{}' not found",
                sensor_name
            )));
        }
    };

    // Convert based on requested format
    let (content, content_type) = match format {
        ExportFormat::Senml => {
            let json_value = SenMLConverter::to_senml_json(&sensor_data)
                .map_err(AppError::internal_server_error)?;
            (json_value.to_string(), format.content_type())
        }
        ExportFormat::Csv => {
            let csv_content =
                CsvConverter::to_csv(&sensor_data).map_err(AppError::internal_server_error)?;
            (csv_content, format.content_type())
        }
        ExportFormat::Jsonl => {
            let jsonl_content =
                JsonlConverter::to_jsonl(&sensor_data).map_err(AppError::internal_server_error)?;
            (jsonl_content, format.content_type())
        }
    };

    // Build response with appropriate content type
    let response = axum::response::Response::builder()
        .header("content-type", content_type)
        .body(content.into())
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
        assert_eq!(ExportFormat::from_extension("txt"), None);
    }

    #[test]
    fn test_export_format_content_type() {
        assert_eq!(ExportFormat::Senml.content_type(), "application/json");
        assert_eq!(ExportFormat::Csv.content_type(), "text/csv");
        assert_eq!(ExportFormat::Jsonl.content_type(), "application/jsonlines");
    }
}
