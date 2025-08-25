use super::{Sensor, TypedSamples};

/// Container for sensor data retrieved from storage
#[derive(Debug)]
pub struct SensorData {
    pub sensor: Sensor,
    pub samples: TypedSamples,
}

impl SensorData {
    pub fn new(sensor: Sensor, samples: TypedSamples) -> Self {
        Self { sensor, samples }
    }
}
