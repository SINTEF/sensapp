//! All integration tests in one binary: one link of the whole dependency tree
//! instead of one per file. Filter by module, e.g.
//! `cargo test --test integration rrdcached_integration::`.

mod common;

mod advanced_backend_queries;
mod arrow_integration;
mod batched_inserts;
mod clickhouse_http_lifecycle;
mod clickhouse_integration;
mod crud_dcat_api;
mod data_lifecycle;
mod datamodel;
mod deduplication;
mod health_check;
mod influxdb_integration;
mod ingestion;
mod jwt_auth;
mod parser_edge_cases;
mod prometheus_metrics;
mod prometheus_remote_read_integration;
mod prometheus_write_integration;
mod publish_robustness;
mod query_export;
mod query_sensors_by_labels;
mod real_router;
mod regex_matchers;
mod rrdcached_integration;
mod selector_reads;
mod simple_promql;
