# ClickHouse Label Query Support

## Goal

Implement `query_sensors_by_labels` for the ClickHouse backend so the core query path works for Prometheus-style label selection.

## Context

- ClickHouse is the current reference backend for pre-production hardening.
- The storage trait already requires label-based queries.
- ClickHouse was returning a not-implemented error for that path.

## Completed Work

- Added matcher-based sensor discovery for ClickHouse
- Reused the existing per-sensor sample query path for matched sensors
- Added ClickHouse integration coverage for exact-match and `numeric_only` queries
- Fixed the default ClickHouse test URL to use the HTTP port expected by the backend and test services

## Status

Completed.
