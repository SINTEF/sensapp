use crate::datamodel::{Sensor, SensorData, TypedSamples};
use crate::storage::StorageInstance;
use anyhow::{Context, Result};
use std::sync::Arc;

/// Count the total number of samples across all sensors
pub async fn count_total_samples(storage: &Arc<dyn StorageInstance>) -> Result<usize> {
    let sensors = storage.list_series(None).await?;
    let mut total = 0;

    for sensor in sensors {
        let sensor_data = storage
            .query_sensor_data(&sensor.uuid.to_string(), None, None, None)
            .await?;
        if let Some(data) = sensor_data {
            total += match data.samples {
                TypedSamples::Float(ref s) => s.len(),
                TypedSamples::Integer(ref s) => s.len(),
                TypedSamples::String(ref s) => s.len(),
                TypedSamples::Boolean(ref s) => s.len(),
                TypedSamples::Location(ref s) => s.len(),
                TypedSamples::Json(ref s) => s.len(),
                TypedSamples::Blob(ref s) => s.len(),
                TypedSamples::Numeric(ref s) => s.len(),
            };
        }
    }

    Ok(total)
}

/// Get a sensor by name
pub async fn get_sensor_by_name(
    storage: &Arc<dyn StorageInstance>,
    name: &str,
) -> Result<Option<Sensor>> {
    let sensors = storage.list_series(None).await?;
    Ok(sensors.into_iter().find(|s| s.name == name))
}

/// Verify sensor data contains expected values
pub async fn verify_sensor_data(
    storage: &Arc<dyn StorageInstance>,
    sensor_name: &str,
    expected_count: usize,
) -> Result<SensorData> {
    let sensor = get_sensor_by_name(storage, sensor_name)
        .await?
        .with_context(|| format!("Sensor '{}' not found", sensor_name))?;

    let sensor_data = storage
        .query_sensor_data(&sensor.uuid.to_string(), None, None, None)
        .await?;
    let data =
        sensor_data.with_context(|| format!("No data found for sensor '{}'", sensor_name))?;

    let actual_count = match &data.samples {
        TypedSamples::Float(s) => s.len(),
        TypedSamples::Integer(s) => s.len(),
        TypedSamples::String(s) => s.len(),
        TypedSamples::Boolean(s) => s.len(),
        TypedSamples::Location(s) => s.len(),
        TypedSamples::Json(s) => s.len(),
        TypedSamples::Blob(s) => s.len(),
        TypedSamples::Numeric(s) => s.len(),
    };

    if actual_count != expected_count {
        return Err(anyhow::anyhow!(
            "Expected {} samples for sensor '{}', found {}",
            expected_count,
            sensor_name,
            actual_count
        ));
    }

    Ok(data)
}

/// Get all sensor names from storage
pub async fn get_sensor_names(storage: &Arc<dyn StorageInstance>) -> Result<Vec<String>> {
    let sensors = storage.list_series(None).await?;
    Ok(sensors.into_iter().map(|s| s.name).collect())
}
