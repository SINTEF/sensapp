use crate::datamodel::{SensAppDateTime, SensorData};
use crate::storage::{LabelMatcher, StorageInstance};
use std::sync::Arc;

use super::app_error::AppError;

pub const MAX_DIRECT_SAMPLES: usize = 100_000;
pub const MAX_SELECTOR_SERIES: usize = 256;
pub const MAX_SELECTOR_SAMPLES_PER_SERIES: usize = 100_000;
pub const MAX_SELECTOR_SAMPLES_TOTAL: usize = 100_000;

pub fn validate_direct_sample_count(count: usize) -> Result<(), AppError> {
    if count > MAX_DIRECT_SAMPLES {
        return Err(AppError::bad_request(anyhow::anyhow!(
            "Query exceeds {MAX_DIRECT_SAMPLES} samples; narrow the time range or use aggregation"
        )));
    }
    Ok(())
}

pub fn validate_selector_result(result: &[SensorData]) -> Result<(), AppError> {
    if result.len() > MAX_SELECTOR_SERIES {
        return Err(AppError::bad_request(anyhow::anyhow!(
            "Query exceeds {MAX_SELECTOR_SERIES} series; narrow the selector"
        )));
    }
    let mut total = 0usize;
    for data in result {
        if data.samples.len() > MAX_SELECTOR_SAMPLES_PER_SERIES {
            return Err(AppError::bad_request(anyhow::anyhow!(
                "Query exceeds {MAX_SELECTOR_SAMPLES_PER_SERIES} samples per series; narrow the time range or use aggregation"
            )));
        }
        total = total.saturating_add(data.samples.len());
        if total > MAX_SELECTOR_SAMPLES_TOTAL {
            return Err(AppError::bad_request(anyhow::anyhow!(
                "Query exceeds {MAX_SELECTOR_SAMPLES_TOTAL} samples in total; narrow the selector or time range"
            )));
        }
    }
    Ok(())
}

/// Discover matching sensors with one sample each, then spend one shared sample budget.
pub async fn query_selector_bounded(
    storage: &Arc<dyn StorageInstance>,
    matchers: &[LabelMatcher],
    start_time: Option<SensAppDateTime>,
    end_time: Option<SensAppDateTime>,
    numeric_only: bool,
) -> Result<Vec<SensorData>, AppError> {
    let discovered = storage
        .query_sensors_by_labels(matchers, start_time, end_time, Some(1), numeric_only)
        .await?;
    if discovered.len() > MAX_SELECTOR_SERIES {
        return Err(AppError::bad_request(anyhow::anyhow!(
            "Query exceeds {MAX_SELECTOR_SERIES} series; narrow the selector"
        )));
    }

    let mut remaining = MAX_SELECTOR_SAMPLES_TOTAL;
    let mut result = Vec::with_capacity(discovered.len());
    for found in discovered {
        let data = storage
            .query_sensor_data(
                &found.sensor.uuid.to_string(),
                start_time,
                end_time,
                Some(remaining + 1),
            )
            .await?;
        if let Some(data) = data {
            if data.samples.len() > remaining {
                return Err(AppError::bad_request(anyhow::anyhow!(
                    "Query exceeds {MAX_SELECTOR_SAMPLES_TOTAL} samples in total; narrow the selector or time range"
                )));
            }
            remaining -= data.samples.len();
            result.push(data);
        }
    }
    validate_selector_result(&result)?;
    Ok(result)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::datamodel::{Sensor, SensorType, TypedSamples};
    use uuid::Uuid;

    #[test]
    fn direct_sample_limit_has_an_explicit_boundary() {
        assert!(validate_direct_sample_count(MAX_DIRECT_SAMPLES).is_ok());
        assert!(validate_direct_sample_count(MAX_DIRECT_SAMPLES + 1).is_err());
    }

    #[test]
    fn selector_rejects_too_many_series() {
        let series = (0..=MAX_SELECTOR_SERIES)
            .map(|_| {
                SensorData::new(
                    Sensor {
                        uuid: Uuid::nil(),
                        name: "temperature".into(),
                        sensor_type: SensorType::Integer,
                        unit: None,
                        labels: Default::default(),
                    },
                    TypedSamples::Integer(Default::default()),
                )
            })
            .collect::<Vec<_>>();
        assert!(validate_selector_result(&series).is_err());
    }
}
