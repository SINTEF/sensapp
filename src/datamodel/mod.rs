/*use serde::{Deserialize, Serialize};*/

pub mod batch;
pub mod batch_builder;
pub mod sample;
pub mod sensapp_datetime;
pub mod sensapp_vec;
pub mod sensor;
pub mod sensor_type;
pub mod typed_samples;

pub use sample::Sample;
pub use sensapp_datetime::SensAppDateTime;
pub use sensapp_vec::SensAppVec;
pub use sensor::Sensor;
pub use sensor_type::SensorType;
pub use typed_samples::TypedSamples;
/*
// Units
#[derive(Serialize, Deserialize, Debug)]
struct Unit {
    id: i32,
    name: String,
    description: Option<String>,
}*/

/*
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
