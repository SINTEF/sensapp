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

## Outcome (10 November 2026)

All three findings were right; each got a failing test first.

- **Nulls.** A null timestamp is refused with `400`. A null value is no sample: the rows without a value are
  dropped with `filter_record_batch` before conversion, and a location with a null latitude or longitude is
  dropped too. A batch of only null values stores nothing (the type and identity checks still run).
  Tests: `test_null_values_are_skipped`, `test_a_null_timestamp_is_refused`, and over HTTP
  `arrow_integration::import_tests::test_import_arrow_skips_null_values` (one sample stored, not two).
- **Sensor columns.** Decision: refuse rather than group. The Python SDK uploads one sensor per batch
  (`build_upload_table`), and grouping would also need per-row type, unit and labels. A `sensor_id` or
  `sensor_name` column must hold one value on every row, without nulls, or the batch is refused with `400`.
  The columns are checked even when the `sensapp.sensor.*` metadata gives the identity, and before null
  values are dropped. The columns may be text or a dictionary of text (the multi-series export writes
  dictionaries; a dictionary `sensor_name` column was silently ignored before, and a dictionary
  `sensor_id` column was an error). Tests: `test_a_sensor_column_naming_two_sensors_is_refused`,
  `test_a_dictionary_sensor_column_names_the_sensor`.
- **Compressed bodies.** Compression stays: the Python SDK writes LZ4 streams (`_stream_write_options`).
  Before decoding, `check_decompressed_size` walks the IPC messages (in order for a stream, through the
  footer blocks for a file, as `arrow-ipc` reads them) and adds up the uncompressed lengths that the
  compressed buffers declare, dictionaries included. Over the body limit (`SENSAPP_HTTP_BODY_LIMIT`, as
  for gzip and snappy) the request is refused with `400`. Measured: 4,000 rows compress to about 17 KB and
  expand to about 100 KB. Test: `test_compressed_bodies_are_limited_by_their_declared_size` (stream and
  file, within and over the limit, and lengths forged to 2^62 bytes, which would have aborted on
  allocation). ZSTD is not enabled in `arrow-ipc`, so ZSTD bodies were and are refused.
- `docs/HTTP_LIMITS.md` covers Arrow compression; `docs/DATAMODEL.md` gives the null and one-sensor rules.

Suites: unit tests, `arrow_integration` (18), clippy `--all-features --tests`; full SQLite (296 unit, 337
integration) and PostgreSQL (338 integration), all green.
The importer is the same for every backend, so other backends were not run.
