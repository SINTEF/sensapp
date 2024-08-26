use crate::crud::list_cursor::ListCursor;
use crate::crud::viewmodel::sensor_viewmodel::SensorViewModel;
use crate::datamodel::matchers::SensorMatcher;
use crate::ingestors::http::app_error::AppError;
use crate::ingestors::http::state::HttpServerState;
use anyhow::Result;
use axum::extract::{Path, Query, State};
use axum::Json;
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
pub struct ListSensorsQuery {
    pub query: Option<String>,
    pub cursor: Option<String>,
    pub limit: Option<usize>,
}

#[derive(Debug, Serialize)]
pub struct ListSensorsResponse {
    pub sensors: Vec<SensorViewModel>,
    pub cursor: Option<String>,
}

/// List all the sensors.
#[utoipa::path(
    get,
    path = "/api/v1/sensors",
    tag = "SensApp",
    responses(
        (status = 200, description = "List of sensors", body = Vec<SensorViewModel>)
    ),
    params(
        ("query" = Option<String>, Query, description = "Prometheus like query string to filter sensors. All sensors are returned if not specified"),
        ("cursor" = Option<String>, Query, description = "Cursor to start listing from"),
        ("limit" = Option<u64>, Query, description = "Limit the number of sensors to return, 1000 by default", maximum = 100_000, minimum = 1),
    ),
)]
pub async fn list_sensors(
    State(state): State<HttpServerState>,
    Query(query): Query<ListSensorsQuery>,
) -> Result<Json<ListSensorsResponse>, AppError> {
    let cursor = query
        .cursor
        .map(|cursor| ListCursor::parse(&cursor))
        .unwrap_or_else(|| Ok(ListCursor::default()))
        .map_err(AppError::BadRequest)?;

    let limit = query.limit.unwrap_or(1000);
    if limit == 0 || limit > 100_000 {
        return Err(AppError::BadRequest(anyhow::anyhow!(
            "Limit must be between 1 and 100,000"
        )));
    }

    let matcher = match query.query {
        Some(query_str) => SensorMatcher::from_prometheus_query(&query_str)?,
        None => SensorMatcher::default(),
    };

    let (sensors, next_cursor) = state
        .storage
        .list_sensors(matcher, cursor, limit)
        .await
        .map_err(|error| {
            eprintln!("Failed to list sensors: {:?}", error);
            AppError::InternalServerError(error)
        })?;

    Ok(Json(ListSensorsResponse {
        sensors,
        cursor: next_cursor.map(|cursor| cursor.to_string()),
    }))
}

#[utoipa::path(
    get,
    path = "/api/v1/sensors/:uuid",
    tag = "SensApp",
    responses(
        (status = 200, description = "Sensor metadata", body = SensorViewModel),
        (status = 400, description = "Bad Request", body = AppError),
        (status = 404, description = "Not Found", body = AppError),
        (status = 500, description = "Internal Server Error", body = AppError),
    ),
    params(
        ("uuid" = String, Path, description = "Sensor UUID", example = "20115fa5-aecd-8271-835d-07bfee981d6a"),
    )
)]
pub async fn get_sensor(
    State(state): State<HttpServerState>,
    Path(uuid): Path<String>,
) -> Result<Json<SensorViewModel>, AppError> {
    todo!();
    /*let sensor = state.storage.get_sensor(uuid).await.map_err(|error| {
        eprintln!("Failed to get sensor: {:?}", error);
        AppError::InternalServerError(error)
    })?;

    match sensor {
        Some(sensor) => Ok(Json(sensor)),
        None => Err(AppError::NotFound(anyhow::anyhow!("Sensor not found"))),
    }*/
}
