use smallvec::SmallVec;
use std::sync::Arc;

#[derive(Debug)]
pub struct Sample<V> {
    pub timestamp_ms: i64,
    pub value: V,
}

// Small vec size
const SMALLVEC_BUFFER_SIZE: usize = 1;

#[derive(Debug)]
pub enum TypedSamples {
    Integer(SmallVec<[Sample<i64>; SMALLVEC_BUFFER_SIZE]>),
    Numeric(SmallVec<[Sample<rust_decimal::Decimal>; SMALLVEC_BUFFER_SIZE]>),
    Float(SmallVec<[Sample<f64>; SMALLVEC_BUFFER_SIZE]>),
    String(SmallVec<[Sample<String>; SMALLVEC_BUFFER_SIZE]>),
    Boolean(SmallVec<[Sample<bool>; SMALLVEC_BUFFER_SIZE]>),
    Location(SmallVec<[Sample<geo::Point>; SMALLVEC_BUFFER_SIZE]>),
    Blob(SmallVec<[Sample<Vec<u8>>; SMALLVEC_BUFFER_SIZE]>),
    Json(SmallVec<[Sample<serde_json::Value>; SMALLVEC_BUFFER_SIZE]>),
}

#[derive(Debug)]
pub struct SingleSensorBatch {
    pub sensor_uuid: uuid::Uuid,
    pub sensor_name: String,
    pub samples: Arc<TypedSamples>,
}

#[derive(Debug)]
pub struct Batch {
    pub sensor_batches: Arc<SmallVec<[SingleSensorBatch; SMALLVEC_BUFFER_SIZE]>>,
}
