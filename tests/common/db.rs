/// Database testing utilities
use anyhow::Result;
use sensapp::datamodel::{Sensor, SensorData};
use sensapp::storage::StorageInstance;
use std::sync::Arc;

/// Database test helpers
#[allow(dead_code)] // Test helper struct
pub struct DbHelpers;

impl DbHelpers {
    /// Count total number of sensor samples in the database
    #[allow(dead_code)] // Test helper method
    pub async fn count_total_samples(storage: &Arc<dyn StorageInstance>) -> Result<usize> {
        let sensors = storage.list_series(None).await?;
        let mut total = 0;

        for sensor in sensors {
            if let Some(sensor_data) = storage
                .query_sensor_data(&sensor.uuid.to_string(), None, None, None)
                .await?
            {
                match sensor_data.samples {
                    sensapp::datamodel::TypedSamples::Integer(samples) => total += samples.len(),
                    sensapp::datamodel::TypedSamples::Float(samples) => total += samples.len(),
                    sensapp::datamodel::TypedSamples::Numeric(samples) => total += samples.len(),
                    sensapp::datamodel::TypedSamples::String(samples) => total += samples.len(),
                    sensapp::datamodel::TypedSamples::Boolean(samples) => total += samples.len(),
                    sensapp::datamodel::TypedSamples::Location(samples) => total += samples.len(),
                    sensapp::datamodel::TypedSamples::Blob(samples) => total += samples.len(),
                    sensapp::datamodel::TypedSamples::Json(samples) => total += samples.len(),
                }
            }
        }

        Ok(total)
    }

    /// Get sensor by name for testing
    #[allow(dead_code)] // Test helper method
    pub async fn get_sensor_by_name(
        storage: &Arc<dyn StorageInstance>,
        name: &str,
    ) -> Result<Option<Sensor>> {
        let sensors = storage.list_series(None).await?;
        Ok(sensors.into_iter().find(|s| s.name == name))
    }

    /// Verify sensor data exists and has expected sample count
    #[allow(dead_code)] // Test helper method
    pub async fn verify_sensor_data(
        storage: &Arc<dyn StorageInstance>,
        sensor_name: &str,
        expected_count: usize,
    ) -> Result<SensorData> {
        // First, find the sensor by name to get its UUID
        let sensor = Self::get_sensor_by_name(storage, sensor_name)
            .await?
            .ok_or_else(|| anyhow::anyhow!("No sensor found with name: {}", sensor_name))?;

        let sensor_data = storage
            .query_sensor_data(&sensor.uuid.to_string(), None, None, None)
            .await?
            .ok_or_else(|| anyhow::anyhow!("No data found for sensor: {}", sensor_name))?;

        let actual_count = match &sensor_data.samples {
            sensapp::datamodel::TypedSamples::Integer(samples) => samples.len(),
            sensapp::datamodel::TypedSamples::Float(samples) => samples.len(),
            sensapp::datamodel::TypedSamples::Numeric(samples) => samples.len(),
            sensapp::datamodel::TypedSamples::String(samples) => samples.len(),
            sensapp::datamodel::TypedSamples::Boolean(samples) => samples.len(),
            sensapp::datamodel::TypedSamples::Location(samples) => samples.len(),
            sensapp::datamodel::TypedSamples::Blob(samples) => samples.len(),
            sensapp::datamodel::TypedSamples::Json(samples) => samples.len(),
        };

        if actual_count != expected_count {
            return Err(anyhow::anyhow!(
                "Expected {} samples for sensor '{}', found {}",
                expected_count,
                sensor_name,
                actual_count
            ));
        }

        Ok(sensor_data)
    }

    /// Get all sensor names in the database
    #[allow(dead_code)] // Test helper method
    pub async fn get_sensor_names(storage: &Arc<dyn StorageInstance>) -> Result<Vec<String>> {
        let sensors = storage.list_series(None).await?;
        Ok(sensors.into_iter().map(|s| s.name).collect())
    }
}

#[cfg(test)]
mod tests {

    // These tests would need a real database connection to run
    // They serve as documentation of the API we expect
}
