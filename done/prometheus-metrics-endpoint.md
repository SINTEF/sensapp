# Add Prometheus Metrics Endpoint

## Context

SensApp already exposes API and health endpoints, but it does not expose Prometheus-compatible service metrics for scraping.

## Goal

1. Add a Prometheus scrape endpoint.
2. Expose a small set of useful runtime metrics.
3. Keep the implementation lightweight and aligned with the existing Axum server.

## Outcome

1. Added a Prometheus scrape endpoint at `/prometheus/metrics`.
2. Kept the existing DCAT metrics catalog endpoint at `/metrics`.
3. Exposed lightweight runtime metrics for HTTP request counts, request duration, in-flight requests, uptime, and storage readiness.
4. Added coverage for the new endpoint and updated existing tests for the catalog route change.

## Notes

- The implementation uses the `prometheus-client` crate instead of generating the text format manually.
