/// Database testing utilities
use anyhow::Result;
use sensapp::datamodel::{Sensor, SensorData};
use sensapp::storage::StorageInstance;
use std::sync::Arc;

/// Database test helpers
pub struct DbHelpers;

impl DbHelpers {
    /// Count total number of sensor samples in the database
    pub async fn count_total_samples(storage: &Arc<dyn StorageInstance>) -> Result<usize> {
        let sensors = storage.list_series().await?;
        let mut total = 0;

        for sensor in sensors {
            if let Some(sensor_data) = storage
                .query_sensor_data(&sensor.name, None, None, None)
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
    pub async fn get_sensor_by_name(
        storage: &Arc<dyn StorageInstance>,
        name: &str,
    ) -> Result<Option<Sensor>> {
        let sensors = storage.list_series().await?;
        Ok(sensors.into_iter().find(|s| s.name == name))
    }

    /// Verify sensor data exists and has expected sample count
    pub async fn verify_sensor_data(
        storage: &Arc<dyn StorageInstance>,
        sensor_name: &str,
        expected_count: usize,
    ) -> Result<SensorData> {
        let sensor_data = storage
            .query_sensor_data(sensor_name, None, None, None)
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
    pub async fn get_sensor_names(storage: &Arc<dyn StorageInstance>) -> Result<Vec<String>> {
        let sensors = storage.list_series().await?;
        Ok(sensors.into_iter().map(|s| s.name).collect())
    }

    /// Clean up all data in test database (for test isolation)
    pub async fn clean_all_data(_storage: &Arc<dyn StorageInstance>) -> Result<()> {
        // For now, we rely on database isolation
        // In the future, we might implement a truncate all tables operation
        Ok(())
    }
}

#[cfg(test)]
mod tests {

    // These tests would need a real database connection to run
    // They serve as documentation of the API we expect

    #[tokio::test]
    #[ignore] // Run with --ignored when database is available
    async fn test_db_helpers() {
        // This test would need actual database setup
        // let storage = setup_test_storage().await.unwrap();
        // let count = DbHelpers::count_total_samples(&storage).await.unwrap();
        // assert_eq!(count, 0);
    }
}
