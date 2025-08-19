use crate::ingestors::http::app_error::AppError;
use crate::ingestors::http::state::HttpServerState;
use axum::Json;
use axum::extract::State;
use serde_json::{Value, json};

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
                "dcat:distribution": [{
                    "@type": "dcat:Distribution",
                    "dcat:downloadURL": format!("/sensors/{}/data", sensor_name),
                    "dcat:mediaType": "application/json",
                    "dct:format": "JSON"
                }]
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
