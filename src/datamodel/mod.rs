/*use serde::{Deserialize, Serialize};
use uuid::Uuid;*/

pub mod batch;

pub enum SensorType {
    Integer,
    Numeric,
    Float,
    String,
    Boolean,
    Location,
    JSON,
    Blob,
}

// Implement to_string() for SensorType
impl ToString for SensorType {
    fn to_string(&self) -> String {
        match self {
            SensorType::Integer => "Integer".to_string(),
            SensorType::Numeric => "Numeric".to_string(),
            SensorType::Float => "Float".to_string(),
            SensorType::String => "String".to_string(),
            SensorType::Boolean => "Boolean".to_string(),
            SensorType::Location => "Location".to_string(),
            SensorType::JSON => "JSON".to_string(),
            SensorType::Blob => "Blob".to_string(),
        }
    }
}

/*
// Units
#[derive(Serialize, Deserialize, Debug)]
struct Unit {
    id: i32,
    name: String,
    description: Option<String>,
}

// Sensors
#[derive(Serialize, Deserialize, Debug)]
struct Sensor {
    sensor_id: i32,
    uuid: Uuid,
    name: String,
    sensor_type: String, // Represent 'type' as 'sensor_type' to avoid keyword conflict
    unit_id: Option<i32>,
}

// Labels
#[derive(Serialize, Deserialize, Debug)]
struct Label {
    sensor_id: i32,
    named: i32,
    description: Option<i32>,
}

// LabelsNameDictionary
#[derive(Serialize, Deserialize, Debug)]
struct LabelsNameDictionary {
    id: i32,
    name: String,
}

// LabelsDescriptionDictionary
#[derive(Serialize, Deserialize, Debug)]
struct LabelsDescriptionDictionary {
    id: i32,
    description: String,
}

// StringsValuesDictionary
#[derive(Serialize, Deserialize, Debug)]
struct StringsValuesDictionary {
    id: i32,
    value: String,
}

// Sample model (for different value types)
#[derive(Serialize, Deserialize, Debug)]
struct Sample<T> {
    datetime: i64,
    value: T,
}

// Higher-level model containing a list of samples and the sensor UUID
#[derive(Serialize, Deserialize, Debug)]
struct SensorData<T> {
    sensor_uuid: Uuid,
    samples: Vec<Sample<T>>,
}

// Values tables (Integer, Numeric, Float, String, Boolean)
type IntegerValue = Sample<i32>;
type NumericValue = Sample<f64>; // Assuming 'Numeric' is represented as f64
type FloatValue = Sample<f32>;
type StringValue = Sample<i32>; // Reference to StringsValuesDictionary ID
type BooleanValue = Sample<bool>;

// Localisations
#[derive(Serialize, Deserialize, Debug)]
struct Localisation {
    sensor_id: i32,
    datetime: i64,
    latitude: f32,
    longitude: f32,
}
*/
