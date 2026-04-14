# Prometheus remote read large range bug

## Goal

Fix the Prometheus remote read streamed XOR response path so large query windows keep returning data instead of silently producing an empty stream.

## Context

- Storage queries succeeded and returned the expected sensor plus samples.
- The failure appeared only after the storage result was converted into streamed XOR chunks.
- Logs showed a working path around 40k samples and an empty response around 77k samples.

## Root cause

- The underlying XOR chunk encoder writes the per-chunk sample count as a big-endian `u16`.
- SensApp tried to encode an entire series as a single XOR chunk.
- Once a query returned more than `65535` samples, encoding failed with `too many samples for one chunk` and the handler silently skipped the series.

## Fix

- Split large streamed remote-read sample sets into multiple XOR chunks, each capped at `u16::MAX` samples.
- Keep a single `ChunkedSeries` per sensor while allowing multiple chunks inside it.
- Log chunk encoding failures with context instead of silently swallowing them.

## Tests

- Added a unit test for encoder chunk splitting beyond `65535` samples.
- Added an integration test covering `STREAMED_XOR_CHUNKS` for a `70000`-sample series.

## Validation

- `DUCKDB_DOWNLOAD_LIB=1 cargo test test_encode_series_splits_large_sample_sets`: passed
- `DUCKDB_DOWNLOAD_LIB=1 cargo test test_remote_read_streamed_xor_chunks_large_series`: passed
- `DUCKDB_DOWNLOAD_LIB=1 cargo check`: passed
- `DUCKDB_DOWNLOAD_LIB=1 cargo clippy --tests`: passed
