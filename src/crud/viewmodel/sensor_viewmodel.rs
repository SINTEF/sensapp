use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Serialize, Deserialize)]
pub struct SensorViewModel {
    pub uuid: Uuid,
    pub name: String,
    pub created_at: Option<String>,
    pub sensor_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub unit: Option<String>,
    pub labels: BTreeMap<String, String>,
}

// From Sensor model to SensorViewModel
impl From<crate::datamodel::Sensor> for SensorViewModel {
    fn from(sensor: crate::datamodel::Sensor) -> Self {
        Self {
            uuid: sensor.uuid,
            name: sensor.name,
            created_at: None, // non view Sensors do not have a created_at field
            sensor_type: sensor.sensor_type.to_string(),
            unit: sensor.unit.map(|unit| unit.name),
            labels: sensor
                .labels
                .iter()
                .map(|(key, value)| (key.clone(), value.clone()))
                .collect(),
        }
    }
}
