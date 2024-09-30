// Note that this file is not automatically generated from the .proto file.

#[derive(prost::Message)]
pub struct ReadResponse {
    #[prost(message, repeated, tag = "1")]
    pub results: Vec<QueryResult>,
}

#[derive(prost::Message)]
pub struct ChunkedReadResponse {
    #[prost(message, repeated, tag = "1")]
    pub chunked_series: Vec<ChunkedSeries>,
    #[prost(int64, tag = "2")]
    pub query_index: i64,
}

#[derive(prost::Message)]
pub struct QueryResult {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
}

#[derive(prost::Message)]
pub struct ChunkedSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub chunks: Vec<Chunk>,
}

#[derive(prost::Message)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
}

#[derive(prost::Message)]
pub struct Label {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

#[derive(prost::Message)]
pub struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}

#[derive(prost::Message)]
pub struct Chunk {
    #[prost(int64, tag = "1")]
    pub min_time_ms: i64,
    #[prost(int64, tag = "2")]
    pub max_time_ms: i64,
    #[prost(enumeration = "chunk::Encoding", tag = "3")]
    pub r#type: i32,
    #[prost(bytes, tag = "4")]
    pub data: Vec<u8>,
}

pub mod chunk {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, ::prost::Enumeration)]
    #[repr(i32)]
    pub enum Encoding {
        Unknown = 0,
        Xor = 1,
        Histogram = 2,
        FloatHistogram = 3,
    }
}
