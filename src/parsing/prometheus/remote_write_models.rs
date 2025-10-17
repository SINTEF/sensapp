// This file is manually edited because setting up an automatic
// compilation of the protocol buffer to Rust code did sound cumbersome
// for such a simple structure.
//
// The code is inspired by prom-write.
// https://github.com/theduke/prom-write/blob/b434cb64c305044b78bb772115217026512b7b9b/lib/src/lib.rs
// Licensed under Apache 2.0 and MIT licenses.
//
// The code uses the crate PROST, which has nothing to do with Alain Prost.
//
// Check the prometheus_remote_write.proto file and https://prometheus.io/docs/concepts/remote_write_spec/
// for more information.

#[derive(prost::Message, Clone)]
pub struct WriteRequest {
    #[prost(message, repeated, tag = "1")]
    pub timeseries: Vec<TimeSeries>,
}

#[derive(prost::Message, Clone)]
pub struct TimeSeries {
    #[prost(message, repeated, tag = "1")]
    pub labels: Vec<Label>,
    #[prost(message, repeated, tag = "2")]
    pub samples: Vec<Sample>,
}

#[derive(prost::Message, Clone)]
pub struct Label {
    #[prost(string, tag = "1")]
    pub name: String,
    #[prost(string, tag = "2")]
    pub value: String,
}

#[derive(prost::Message, Clone)]
pub struct Sample {
    #[prost(double, tag = "1")]
    pub value: f64,
    #[prost(int64, tag = "2")]
    pub timestamp: i64,
}
