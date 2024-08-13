use crate::ingestors::http::app_error::AppError;
use crate::ingestors::http::state::HttpServerState;
use axum::extract::State;
use axum::Json;

/// List all the sensors.
#[utoipa::path(
    get,
    path = "/sensors",
    tag = "SensApp",
    responses(
        (status = 200, description = "List of sensors", body = Vec<String>)
    )
)]
pub async fn list_sensors(
    State(state): State<HttpServerState>,
) -> Result<Json<Vec<String>>, AppError> {
    let sensors = state.storage.list_sensors().await?;
    Ok(Json(sensors))
}
