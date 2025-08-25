use crate::datamodel::{SensorType, unit::Unit};
use serde::{Deserialize, Serialize};

/// Represents an aggregated metric (multiple time series with the same name)
#[derive(Debug, Clone, PartialEq, Serialize, Deserialize)]
pub struct Metric {
    /// The metric name (e.g., "temperature", "humidity")
    pub name: String,

    /// The data type of this metric
    pub sensor_type: SensorType,

    /// The unit of measurement (optional)
    pub unit: Option<Unit>,

    /// Total number of time series for this metric
    pub series_count: i64,

    /// Common label keys used across series of this metric
    pub label_keys: Vec<String>,
}

impl Metric {
    pub fn new(
        name: String,
        sensor_type: SensorType,
        unit: Option<Unit>,
        series_count: i64,
        label_keys: Vec<String>,
    ) -> Self {
        Self {
            name,
            sensor_type,
            unit,
            series_count,
            label_keys,
        }
    }
}
