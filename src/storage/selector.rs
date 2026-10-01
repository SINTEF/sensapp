//! Reading the series of a selector within a series limit and a shared sample budget.

use super::{LabelMatcher, StorageInstance};
use crate::datamodel::{SensAppDateTime, Sensor, SensorData, SensorType, TypedSamples};
use anyhow::Result;
use async_trait::async_trait;
use std::collections::HashMap;
use std::hash::Hash;

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

/// What a backend provides to read the series of a selector in bulk. The reading itself, with
/// its limits and its shared sample budget, is `read_selector_in_bulk`, the same for all of them.
#[async_trait]
pub trait BulkSelectorBackend: StorageInstance {
    /// How the backend designates a sensor in its value tables
    type SensorKey: Copy + Eq + Hash + Send + Sync;

    /// The sensors matching the matchers (a non-empty list), with their labels and units.
    async fn find_selector_sensors(
        &self,
        matchers: &[LabelMatcher],
        numeric_only: bool,
    ) -> Result<Vec<(Self::SensorKey, Sensor)>>;

    /// The samples of many sensors of one numeric type (`Integer`, `Numeric` or `Float`),
    /// at most `limit` in total, with `start_us <= timestamp <= end_us` (microseconds since the
    /// epoch, either bound optional), oldest first within each sensor. Sensors without a sample
    /// may be absent from the result.
    async fn read_numeric_samples(
        &self,
        sensor_type: SensorType,
        sensors: &[Self::SensorKey],
        start_us: Option<i64>,
        end_us: Option<i64>,
        limit: usize,
    ) -> Result<HashMap<Self::SensorKey, TypedSamples>>;

    /// The samples of one sensor of any type, at most `limit`. Used for the types that are rare
    /// in selectors (strings, booleans, locations, json, blobs).
    async fn read_other_samples(
        &self,
        _sensor_key: Self::SensorKey,
        sensor: &Sensor,
        start_time: Option<SensAppDateTime>,
        end_time: Option<SensAppDateTime>,
        limit: usize,
    ) -> Result<TypedSamples> {
        Ok(self
            .query_sensor_data(&sensor.uuid.to_string(), start_time, end_time, Some(limit))
            .await?
            .map(|data| data.samples)
            .unwrap_or_else(|| empty_samples(sensor.sensor_type)))
    }
}

fn is_numeric(sensor_type: SensorType) -> bool {
    matches!(
        sensor_type,
        SensorType::Integer | SensorType::Numeric | SensorType::Float
    )
}

pub fn empty_samples(sensor_type: SensorType) -> TypedSamples {
    match sensor_type {
        SensorType::Integer => TypedSamples::Integer(Default::default()),
        SensorType::Numeric => TypedSamples::Numeric(Default::default()),
        SensorType::Float => TypedSamples::Float(Default::default()),
        SensorType::String => TypedSamples::String(Default::default()),
        SensorType::Boolean => TypedSamples::Boolean(Default::default()),
        SensorType::Location => TypedSamples::Location(Default::default()),
        SensorType::Json => TypedSamples::Json(Default::default()),
        SensorType::Blob => TypedSamples::Blob(Default::default()),
    }
}

/// Read the series of a selector with one query for the sensors, one per numeric type for
/// their samples, and one per series for the other types.
///
/// Gives the same answer as `query_selector_sequential`: every matching series is returned,
/// empty when it has no sample in the window, and a limit is exceeded as soon as there is one
/// series or one sample more than allowed. The series limit is checked before any sample is read.
pub async fn read_selector_in_bulk<B: BulkSelectorBackend>(
    backend: &B,
    matchers: &[LabelMatcher],
    start_time: Option<SensAppDateTime>,
    end_time: Option<SensAppDateTime>,
    numeric_only: bool,
    max_series: usize,
    max_samples: usize,
) -> Result<SelectorRead> {
    // No matcher selects nothing (Prometheus behaviour)
    if matchers.is_empty() {
        return Ok(Ok(Vec::new()));
    }
    let sensors = backend
        .find_selector_sensors(matchers, numeric_only)
        .await?;
    if sensors.len() > max_series {
        return Ok(Err(SelectorLimitExceeded::Series));
    }

    let start_us = start_time.as_ref().map(super::common::datetime_to_micros);
    let end_us = end_time.as_ref().map(super::common::datetime_to_micros);
    let mut remaining = max_samples;
    let mut samples_by_sensor: HashMap<B::SensorKey, TypedSamples> = HashMap::new();

    for sensor_type in [SensorType::Integer, SensorType::Numeric, SensorType::Float] {
        let keys: Vec<B::SensorKey> = sensors
            .iter()
            .filter(|(_, sensor)| sensor.sensor_type == sensor_type)
            .map(|(key, _)| *key)
            .collect();
        if keys.is_empty() {
            continue;
        }
        let read = backend
            .read_numeric_samples(sensor_type, &keys, start_us, end_us, remaining + 1)
            .await?;
        let count: usize = read.values().map(TypedSamples::len).sum();
        if count > remaining {
            return Ok(Err(SelectorLimitExceeded::Samples));
        }
        remaining -= count;
        samples_by_sensor.extend(read);
    }

    for (key, sensor) in &sensors {
        if is_numeric(sensor.sensor_type) {
            continue;
        }
        let samples = backend
            .read_other_samples(*key, sensor, start_time, end_time, remaining + 1)
            .await?;
        if samples.len() > remaining {
            return Ok(Err(SelectorLimitExceeded::Samples));
        }
        remaining -= samples.len();
        samples_by_sensor.insert(*key, samples);
    }

    Ok(Ok(sensors
        .into_iter()
        .map(|(key, sensor)| {
            let samples = samples_by_sensor
                .remove(&key)
                .unwrap_or_else(|| empty_samples(sensor.sensor_type));
            SensorData::new(sensor, samples)
        })
        .collect()))
}
