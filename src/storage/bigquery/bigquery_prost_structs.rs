use prost::Message;

// Units table
#[derive(Clone, PartialEq, Message)]
pub struct Unit {
    #[prost(int64, required, tag = "1")]
    pub id: i64,
    #[prost(string, required, tag = "2")]
    pub name: String,
    #[prost(string, optional, tag = "3")]
    pub description: Option<String>,
}

// Sensors table
#[derive(Clone, PartialEq, Message)]
pub struct Sensor {
    #[prost(int64, required, tag = "1")]
    pub sensor_id: i64,
    #[prost(string, required, tag = "2")]
    pub uuid: String,
    #[prost(string, required, tag = "3")]
    pub name: String,
    #[prost(string, required, tag = "4")]
    pub created_at: String,
    #[prost(string, required, tag = "5")]
    pub r#type: String,
    #[prost(int64, optional, tag = "6")]
    pub unit: Option<i64>,
}

// Labels name dictionary table
#[derive(Clone, PartialEq, Message)]
pub struct LabelNameDictionary {
    #[prost(int64, required, tag = "1")]
    pub id: i64,
    #[prost(string, required, tag = "2")]
    pub name: String,
}

// Labels description dictionary table
#[derive(Clone, PartialEq, Message)]
pub struct LabelDescriptionDictionary {
    #[prost(int64, required, tag = "1")]
    pub id: i64,
    #[prost(string, required, tag = "2")]
    pub description: String,
}

// Labels table
#[derive(Clone, PartialEq, Message)]
pub struct Label {
    #[prost(int64, required, tag = "1")]
    pub sensor_id: i64,
    #[prost(int64, required, tag = "2")]
    pub name: i64,
    #[prost(int64, optional, tag = "3")]
    pub description: Option<i64>,
}

// Strings values dictionary table
#[derive(Clone, PartialEq, Message)]
pub struct StringValueDictionary {
    #[prost(int64, required, tag = "1")]
    pub id: i64,
    #[prost(string, required, tag = "2")]
    pub value: String,
}

// Integer values table
#[derive(Clone, PartialEq, Message)]
pub struct IntegerValue {
    #[prost(int64, required, tag = "1")]
    pub sensor_id: i64,
    #[prost(string, required, tag = "2")]
    pub timestamp: String,
    #[prost(int64, required, tag = "3")]
    pub value: i64,
}

// Numeric values table
#[derive(Clone, PartialEq, Message)]
pub struct NumericValue {
    #[prost(int64, required, tag = "1")]
    pub sensor_id: i64,
    #[prost(string, required, tag = "2")]
    pub timestamp: String,
    #[prost(bytes, required, tag = "3")]
    pub value: Vec<u8>,
}

// Float values table
#[derive(Clone, PartialEq, Message)]
pub struct FloatValue {
    #[prost(int64, required, tag = "1")]
    pub sensor_id: i64,
    #[prost(string, required, tag = "2")]
    pub timestamp: String,
    // /!\ This is currently *NOT* working! Only NULL are inserted instead.
    // See https://github.com/lquerel/gcp-bigquery-client/issues/106
    //#[prost(double, tag = "3")]
    //pub value: f64,
    #[prost(float, required, tag = "3")]
    pub value: f32, // reverting back to f32 for now, decimal/numeric value is heavily recommended instead
}

// String values table
#[derive(Clone, PartialEq, Message)]
pub struct StringValue {
    #[prost(int64, required, tag = "1")]
    pub sensor_id: i64,
    #[prost(string, required, tag = "2")]
    pub timestamp: String,
    #[prost(int64, required, tag = "3")]
    pub value: i64,
}

// Boolean values table
#[derive(Clone, PartialEq, Message)]
pub struct BooleanValue {
    #[prost(int64, required, tag = "1")]
    pub sensor_id: i64,
    #[prost(string, required, tag = "2")]
    pub timestamp: String,
    #[prost(bool, required, tag = "3")]
    pub value: bool,
}

// Location values table
#[derive(Clone, PartialEq, Message)]
pub struct LocationValue {
    #[prost(int64, required, tag = "1")]
    pub sensor_id: i64,
    #[prost(string, required, tag = "2")]
    pub timestamp: String,
    // Like for FloatValue, this is currently *NOT* working! Only NULL are inserted instead.
    // We use f32 for now, which is more than enough for GPS coordinates by the way.
    //#[prost(double, required, tag = "3")]
    //pub latitude: f64,
    //#[prost(double, required, tag = "4")]
    //pub longitude: f64,
    #[prost(float, required, tag = "3")]
    pub latitude: f32,
    #[prost(float, required, tag = "4")]
    pub longitude: f32,
}

// JSON values table
#[derive(Clone, PartialEq, Message)]
pub struct JsonValue {
    #[prost(int64, required, tag = "1")]
    pub sensor_id: i64,
    #[prost(string, required, tag = "2")]
    pub timestamp: String,
    #[prost(string, required, tag = "3")]
    pub value: String, // Using String to represent JSON
}

// Blob values table
#[derive(Clone, PartialEq, Message)]
pub struct BlobValue {
    #[prost(int64, required, tag = "1")]
    pub sensor_id: i64,
    #[prost(string, required, tag = "2")]
    pub timestamp: String,
    #[prost(bytes, tag = "3")]
    pub value: Vec<u8>,
}
