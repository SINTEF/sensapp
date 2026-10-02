use crate::datamodel::{SensAppDateTime, SensorData};
use crate::storage::cross_series::{CrossSeriesQuery, CrossSeriesStats, read_cross_series};
use crate::storage::{LabelMatcher, SelectorLimitExceeded, StorageInstance};
use std::sync::Arc;

use super::app_error::AppError;

pub const MAX_DIRECT_SAMPLES: usize = 100_000;
pub const MAX_SELECTOR_SERIES: usize = 256;
pub const MAX_SELECTOR_SAMPLES_PER_SERIES: usize = 100_000;
pub const MAX_SELECTOR_SAMPLES_TOTAL: usize = 100_000;
/// Series a cross-series aggregation may combine. The raw samples are aggregated by the database,
/// so only buckets come back (at most `MAX_SELECTOR_SAMPLES_TOTAL` of them), and the number of
/// series is bounded by the labels and sensors held in memory, not by the volume of samples.
pub const MAX_AGGREGATED_SELECTOR_SERIES: usize = 10_000;

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

/// Read the series of a selector within the series limit and the shared sample budget.
pub async fn query_selector_bounded(
    storage: &Arc<dyn StorageInstance>,
    matchers: &[LabelMatcher],
    start_time: Option<SensAppDateTime>,
    end_time: Option<SensAppDateTime>,
    numeric_only: bool,
) -> Result<Vec<SensorData>, AppError> {
    let result = storage
        .query_selector(
            matchers,
            start_time,
            end_time,
            numeric_only,
            MAX_SELECTOR_SERIES,
            MAX_SELECTOR_SAMPLES_TOTAL,
        )
        .await?;
    let result = match result {
        Ok(result) => result,
        Err(SelectorLimitExceeded::Series) => {
            return Err(AppError::bad_request(anyhow::anyhow!(
                "Query exceeds {MAX_SELECTOR_SERIES} series; narrow the selector"
            )));
        }
        Err(SelectorLimitExceeded::Samples) => {
            return Err(AppError::bad_request(anyhow::anyhow!(
                "Query exceeds {MAX_SELECTOR_SAMPLES_TOTAL} samples in total; narrow the selector or time range"
            )));
        }
    };
    validate_selector_result(&result)?;
    Ok(result)
}

/// Aggregate the numeric series of a selector across series, within
/// `MAX_AGGREGATED_SELECTOR_SERIES` series and `MAX_SELECTOR_SAMPLES_TOTAL` buckets.
pub async fn query_cross_series_bounded(
    storage: &Arc<dyn StorageInstance>,
    matchers: &[LabelMatcher],
    start_time: Option<SensAppDateTime>,
    end_time: Option<SensAppDateTime>,
    query: &CrossSeriesQuery,
) -> Result<(Vec<SensorData>, CrossSeriesStats), AppError> {
    query.validate().map_err(AppError::bad_request)?;
    let read = read_cross_series(
        storage.as_ref(),
        matchers,
        start_time,
        end_time,
        query,
        MAX_AGGREGATED_SELECTOR_SERIES,
        MAX_SELECTOR_SAMPLES_TOTAL,
    )
    .await?;
    match read {
        Ok(read) => Ok(read),
        Err(SelectorLimitExceeded::Series) => Err(AppError::bad_request(anyhow::anyhow!(
            "Query exceeds {MAX_AGGREGATED_SELECTOR_SERIES} series; narrow the selector"
        ))),
        Err(SelectorLimitExceeded::Samples) => Err(AppError::bad_request(anyhow::anyhow!(
            "Query exceeds {MAX_SELECTOR_SAMPLES_TOTAL} buckets in total; use a larger step or narrow the selector or time range"
        ))),
    }
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
