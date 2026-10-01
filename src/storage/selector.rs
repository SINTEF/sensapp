//! Reading the series of a selector within a series limit and a shared sample budget.

use super::{LabelMatcher, StorageInstance};
use crate::datamodel::{SensAppDateTime, SensorData};
use anyhow::Result;

/// Which limit of a selector read was exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SelectorLimitExceeded {
    /// More matching series than allowed
    Series,
    /// More samples in total than allowed
    Samples,
}

/// The series of a selector, or the limit that was exceeded.
pub type SelectorRead = std::result::Result<Vec<SensorData>, SelectorLimitExceeded>;

/// Read the series matching `matchers` one after the other: the portable implementation, used
/// by backends that have no cheaper way, and when a token only sees some of the sensors.
///
/// The matching sensors are discovered first, then each series is read with what is left of the
/// `max_samples` budget, which is exceeded as soon as one more sample than the budget is found.
pub async fn query_selector_sequential<S: StorageInstance + ?Sized>(
    storage: &S,
    matchers: &[LabelMatcher],
    start_time: Option<SensAppDateTime>,
    end_time: Option<SensAppDateTime>,
    numeric_only: bool,
    max_series: usize,
    max_samples: usize,
) -> Result<SelectorRead> {
    let discovered = storage
        .query_sensors_by_labels(matchers, start_time, end_time, Some(1), numeric_only)
        .await?;
    if discovered.len() > max_series {
        return Ok(Err(SelectorLimitExceeded::Series));
    }

    let mut remaining = max_samples;
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
                return Ok(Err(SelectorLimitExceeded::Samples));
            }
            remaining -= data.samples.len();
            result.push(data);
        }
    }
    Ok(Ok(result))
}
