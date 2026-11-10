# Arrow import: nulls, sensor columns and compressed bodies

Source: code review of 10 November 2026. Nulls and row-0 identity were confirmed by reading; the compressed
body issue was read in the `arrow-ipc` source by a reviewer and not reproduced.

## Problem

1. **Validity bitmaps are never consulted** (`src/importers/arrow.rs`: `timestamp_data.value(i)` at `:133`
   and every `*.value(i)` in `convert_arrow_array_to_typed_samples`, `:195-320`). There is no `is_null` or
   `null_count` in the file. Null values are stored as 0, 0.0, "", false or an empty blob; a null timestamp
   becomes 1970-01-01. The request succeeds.
2. **Only row 0 identifies the sensor** (`extract_sensor_id` `:356-360`, `extract_sensor_name` `:378-380`).
   A batch whose `sensor_name` column holds `a,b,a,b` is stored entirely under `a`.
3. **Compressed IPC bodies bypass the decompression limit.** `ipc_compression` and `lz4` are enabled
   (`Cargo.toml:37-38`). `arrow-ipc` 60.1.0 `compression.rs` reads the uncompressed size from a prefix in the
   body and allocates it (`Vec::with_capacity(decompressed_size)`). A body under 64 MiB can expand by orders
   of magnitude, and a forged length can abort on allocation. `parse_arrow_file` collects every batch before
   anything else (`arrow.rs:59-84`). `docs/HTTP_LIMITS.md` only covers gzip and snappy.

## Suggested solution

- Nulls: skip the rows whose timestamp or value is null (a null value means "no sample"), or reject the
  batch with a 400. Suggested: a null timestamp is a 400, a null value is skipped. Document the choice.
- Sensor columns: group the rows by `sensor_id` / `sensor_name` and build one series per distinct value.
  If that is more than wanted now, reject a batch whose column is not constant with a 400.
- Compression: the simplest safe option is to turn the `ipc_compression` feature off and answer 400 for
  compressed bodies, since the HTTP layer already accepts gzip. If compressed IPC must stay, pre-scan the
  message headers and cap the total declared uncompressed size at the decompression limit of
  `src/http/limits.rs` before decoding. Check what the Python SDK sends before choosing.

## Tests

- A batch with null values and a batch with a null timestamp.
- A batch with two sensor names in one column.
- A compressed IPC body: rejected, or accepted within the limit and refused above it.
- Existing `tests/integration/arrow_integration.rs` still passes.

## Done when

The tests pass and `docs/HTTP_LIMITS.md` covers Arrow IPC compression.
