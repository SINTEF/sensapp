use std::sync::Arc;

#[derive(Debug)]
pub struct Sample<V> {
    pub timestamp_ms: i64,
    pub value: V,
}

#[derive(Debug)]
pub enum TypedSamples {
    Integer(Vec<Sample<i64>>),
    Numeric(Vec<Sample<rust_decimal::Decimal>>),
    Float(Vec<Sample<f64>>),
    String(Vec<Sample<String>>),
    Boolean(Vec<Sample<bool>>),
    Location(Vec<Sample<geo::Point>>),
    Blob(Vec<Sample<Vec<u8>>>),
}

#[derive(Debug)]
pub struct Batch {
    pub sensor_uuid: uuid::Uuid,
    pub sensor_name: String,
    pub samples: Arc<TypedSamples>,
}
