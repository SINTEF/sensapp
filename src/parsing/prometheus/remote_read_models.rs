// This file is manually edited because setting up an automatic
// compilation of the protocol buffer to Rust code did sound cumbersome
// for such a simple structure.
//
// The code is based on the Prometheus remote read specification.
// https://github.com/prometheus/prometheus/blob/main/prompb/remote.proto
// Check the remote_read_api.md file for more information.
//
// The code uses the crate PROST for protobuf serialization/deserialization.

#[derive(prost::Message)]
pub struct ReadRequest {
    #[prost(message, repeated, tag = "1")]
    pub queries: Vec<Query>,
    #[prost(enumeration = "read_request::ResponseType", repeated, tag = "2")]
    pub accepted_response_types: Vec<i32>,
}

pub mod read_request {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
    #[repr(i32)]
    pub enum ResponseType {
        /// Server will return a single ReadResponse message with matched series that includes list of raw samples.
        /// It's recommended to use streamed response types instead.
        ///
        /// Response headers:
        /// Content-Type: "application/x-protobuf"
        /// Content-Encoding: "snappy"
        Samples = 0,
        /// Server will stream a delimited ChunkedReadResponse message that
        /// contains XOR or HISTOGRAM(!) encoded chunks for a single series.
        /// Each message is following varint size and fixed size bigendian
        /// uint32 for CRC32 Castagnoli checksum.
        ///
        /// Response headers:
        /// Content-Type: "application/x-streamed-protobuf; proto=prometheus.ChunkedReadResponse"
        /// Content-Encoding: ""
        StreamedXorChunks = 1,
    }

    impl ResponseType {
        /// String value of the enum field names used in the ProtoBuf definition.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                ResponseType::Samples => "SAMPLES",
                ResponseType::StreamedXorChunks => "STREAMED_XOR_CHUNKS",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "SAMPLES" => Some(Self::Samples),
                "STREAMED_XOR_CHUNKS" => Some(Self::StreamedXorChunks),
                _ => None,
            }
        }
    }
}

#[derive(prost::Message)]
pub struct ReadResponse {
    /// In same order as the request's queries.
    #[prost(message, repeated, tag = "1")]
    pub results: Vec<QueryResult>,
}

#[derive(prost::Message)]
pub struct Query {
    #[prost(int64, tag = "1")]
    pub start_timestamp_ms: i64,
    #[prost(int64, tag = "2")]
    pub end_timestamp_ms: i64,
    #[prost(message, repeated, tag = "3")]
    pub matchers: Vec<LabelMatcher>,
    #[prost(message, optional, tag = "4")]
    pub hints: ::core::option::Option<ReadHints>,
}

#[derive(prost::Message)]
pub struct QueryResult {
    /// Samples within a time series must be ordered by time.
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<super::remote_write_models::TimeSeries>,
}

#[derive(prost::Message)]
pub struct LabelMatcher {
    #[prost(enumeration = "label_matcher::Type", tag = "1")]
    pub r#type: i32,
    #[prost(string, tag = "2")]
    pub name: String,
    #[prost(string, tag = "3")]
    pub value: String,
}

pub mod label_matcher {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
    #[repr(i32)]
    pub enum Type {
        Eq = 0,
        Neq = 1,
        Re = 2,
        Nre = 3,
    }

    impl Type {
        /// String value of the enum field names used in the ProtoBuf definition.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Type::Eq => "EQ",
                Type::Neq => "NEQ",
                Type::Re => "RE",
                Type::Nre => "NRE",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "EQ" => Some(Self::Eq),
                "NEQ" => Some(Self::Neq),
                "RE" => Some(Self::Re),
                "NRE" => Some(Self::Nre),
                _ => None,
            }
        }
    }
}

#[derive(prost::Message)]
pub struct ReadHints {
    #[prost(int64, tag = "1")]
    pub step_ms: i64,
    #[prost(string, tag = "2")]
    pub func: String,
    #[prost(int64, tag = "3")]
    pub start_ms: i64,
    #[prost(int64, tag = "4")]
    pub end_ms: i64,
    #[prost(string, repeated, tag = "5")]
    pub grouping: Vec<String>,
    #[prost(bool, tag = "6")]
    pub by: bool,
    #[prost(int64, tag = "7")]
    pub range_ms: i64,
}

#[derive(prost::Message)]
pub struct ChunkedReadResponse {
    #[prost(message, repeated, tag = "1")]
    pub chunked_series: Vec<ChunkedSeries>,
    /// query_index represents an index of the query from ReadRequest.queries these chunks relates to.
    #[prost(int64, tag = "2")]
    pub query_index: i64,
}

#[derive(prost::Message)]
pub struct ChunkedSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<super::remote_write_models::Label>,
    #[prost(message, repeated, tag = "2")]
    pub chunks: Vec<Chunk>,
}

#[derive(prost::Message)]
pub struct Chunk {
    #[prost(int64, tag = "1")]
    pub min_time_ms: i64,
    #[prost(int64, tag = "2")]
    pub max_time_ms: i64,
    #[prost(enumeration = "chunk::Encoding", tag = "3")]
    pub r#type: i32,
    #[prost(bytes = "vec", tag = "4")]
    pub data: Vec<u8>,
}

pub mod chunk {
    #[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, prost::Enumeration)]
    #[repr(i32)]
    pub enum Encoding {
        Unknown = 0,
        Xor = 1,
        Histogram = 2,
        FloatHistogram = 3,
    }

    impl Encoding {
        /// String value of the enum field names used in the ProtoBuf definition.
        pub fn as_str_name(&self) -> &'static str {
            match self {
                Encoding::Unknown => "UNKNOWN",
                Encoding::Xor => "XOR",
                Encoding::Histogram => "HISTOGRAM",
                Encoding::FloatHistogram => "FLOAT_HISTOGRAM",
            }
        }
        /// Creates an enum from field names used in the ProtoBuf definition.
        pub fn from_str_name(value: &str) -> ::core::option::Option<Self> {
            match value {
                "UNKNOWN" => Some(Self::Unknown),
                "XOR" => Some(Self::Xor),
                "HISTOGRAM" => Some(Self::Histogram),
                "FLOAT_HISTOGRAM" => Some(Self::FloatHistogram),
                _ => None,
            }
        }
    }
}
