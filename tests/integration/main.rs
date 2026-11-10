//! All integration tests in one binary: one link of the whole dependency tree
//! instead of one per file. Filter by module, e.g.
//! `cargo test --test integration rrdcached_integration::`.

mod common;

mod advanced_backend_queries;
mod aggregated_windows;
mod arrow_integration;
mod batched_inserts;
mod bigquery_integration;
mod cli;
mod clickhouse_http_lifecycle;
mod clickhouse_integration;
mod cross_series_reads;
mod crud_dcat_api;
mod csv_timestamps;
mod data_lifecycle;
mod datamodel;
mod deduplication;
mod exact_timestamps;
mod health_check;
mod identity_over_http;
mod influxdb_integration;
mod ingestion;
mod jwt_auth;
mod long_texts;
mod numeric_values;
mod parser_edge_cases;
mod payload_errors;
mod prometheus_metrics;
mod prometheus_remote_read_integration;
mod prometheus_write_integration;
mod publish_robustness;
mod query_export;
mod query_sensors_by_labels;
mod real_router;
mod regex_matchers;
mod request_panics;
mod rolled_back_publish;
mod rrdcached_integration;
mod selector_aggregated;
mod selector_reads;
mod sensor_identity;
mod simple_promql;
mod time_window_reads;
#[cfg(feature = "timescaledb")]
mod timescale_compressed;
#[cfg(feature = "timescaledb")]
mod timescale_deadlock;
