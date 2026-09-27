# HTTP Resource Limits

## Goal

Make configured upload limits effective and bound expensive read requests.

## Context

`DefaultBodyLimit` does not protect routes consuming a raw `Body`; JSON ingestion currently calls `to_bytes` with `usize::MAX`, and CSV and Arrow ingestion buffer complete uploads. Selector queries can return up to 10 million samples per series with no total result bound.

## Work

- Enforce the configured request size on all ingest routes, including streamed bodies.
- Add straightforward limits for broad queries and prevent a single request from consuming excessive memory.
- Return clear client errors when limits are exceeded.
- Cover boundary cases with focused tests and document the effective limits.

## Done when

Oversized ingestion is rejected, and read endpoints have predictable bounds without silently truncating results.

## Progress

- Added a request body limit middleware to all ingest routes and Prometheus remote read.
- Bounded gzip and Snappy decompression before parsing.
- Added direct-series, selector-query, remote-read, and latest-sample scrape limits.
- Added focused boundary tests and `docs/HTTP_LIMITS.md`.
