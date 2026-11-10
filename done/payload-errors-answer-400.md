# Payload errors must answer 4xx, not 500

Source: code review of 10 November 2026. Confirmed by reading for the CSV and Prometheus write handlers; the
others were reported.

## Problem

Prometheus and Telegraf retry 5xx answers. A payload that can never succeed and gets a 500 is retried
forever and blocks that remote-write shard.

- **CSV**: `src/http/server.rs:512-514` maps every `publish_csv_async` error to
  `AppError::internal_server_error`, including "CSV contains no data rows". CSV is also the default for
  unknown content types.
  *Partly done in task 3 (`handler-panics-kill-the-instance.md`)*: the handler now reads, parses and
  publishes in three steps, and parse errors (`parse_csv_data_grid`) answer 400. Errors while reading the
  records (malformed quoting, the body limit) still answer 500.
- **Prometheus remote write**: `src/http/prometheus_write.rs:128-130` (`parse_remote_write_request(&bytes)?`)
  and `:166` (`Sensor::new_without_uuid(...)?`) go through `From<anyhow::Error>` to 500. This covers bad
  snappy, bad protobuf, "decompressed body exceeds the limit", and a label value containing 0x1F (legal in
  Prometheus). Remote read maps the same parse error to 400 (`src/http/prometheus_read.rs:313`).
- **InfluxDB**: `src/http/influxdb.rs:304`, same shape.
- **Arrow**: errors after parsing (`publish_arrow_sensors`).
- **Regex matchers**: an invalid pattern reaches the database and comes back as 500
  (`src/http/app_error.rs:129-135`). Handled in `postgres-regex-validation-and-statement-timeout.md`.
- **Unknown Prometheus matcher type** becomes `Equal` (`src/storage/query.rs:197`).
- **`application/senml+json`** does not match `contains("application/json")` (`src/http/server.rs:438`) and
  is parsed as CSV.
- **NUL characters** (suspected, check first): a name, label or string value with U+0000, or JSON with
  `\u0000`, is rejected by PostgreSQL and the batch fails with 500.
- **Large error text**: `src/datamodel/batch.rs:87` formats all the new samples with `{:?}` into the error,
  and `src/importers/csv.rs:228-231` dumps a whole column.

## Suggested solution

- Give the importers a typed error (for example `ImportError::InvalidPayload(String)`), or wrap at the
  handler: everything that fails before `storage.publish` is a 400 (413 for the size limit), everything from
  storage keeps its current mapping. The second option is smaller: split each handler into "parse" (400) and
  "publish" (existing mapping).
- Prometheus remote write: answer 400 for parse and validation errors. Consider skipping an invalid series
  and storing the rest, with a 400 that says how many were dropped, as Prometheus receivers do. Keep it
  simple if in doubt: 400 for the whole request.
- Reject unknown matcher types with a 400.
- Match `+json` content types as JSON.
- Reject U+0000 in names, labels and string values at validation (`contains_special_chars` in
  `src/datamodel/sensor.rs`) so every backend behaves the same.
- Shorten the two error messages (counts, not contents).

## Tests

Through the real router: malformed CSV, empty CSV, bad snappy, bad protobuf, a label with 0x1F, malformed
line protocol, a `senml+json` body, a NUL in a string value. Each answers 4xx, and a storage failure still
answers 5xx.

## Done when

The tests pass and `docs/` lists the status codes of the write endpoints.

## Outcome (10 November 2026)

`tests/integration/payload_errors.rs` sends each bad payload through the real router and lists every case
that does not answer 4xx (or answers with more than 1,000 bytes). Before the fix, 10 of 14 cases answered
500 on PostgreSQL:

- a CSV row longer than its header;
- a NUL in a CSV string value, in a CSV sensor name, in a SenML JSON value and in a SenML label;
- a 0x1F in an InfluxDB tag;
- bad Snappy and bad protobuf on the remote write;
- a 0x1F and a NUL in a remote-write label.

`application/senml+json` was parsed as CSV (400 "CSV contains no data rows"). An unknown remote-read
matcher type answered 200 as if it were `=`.

Fixed:

- **Validation errors are typed.** Names, units and labels with a NUL, a vertical tab or a separator, a
  NUL in a string or JSON value (`BatchBuilder::add`, which every importer goes through), and samples of
  another type than their series are `StorageError::InvalidDataFormat`. `From<anyhow::Error>` already
  maps that to 400, so every handler gets it without a change at its call sites. This covers InfluxDB,
  remote write, SenML, CSV and Arrow, including the SenML `batch_builder.add` errors. Units were not
  validated at all before; they now are, as names are. NUL is refused on every backend, though only
  PostgreSQL fails on it, so that every backend behaves the same.
- **CSV reading errors are 400.** The body is already buffered by the body-limit layer, so reading the
  records can only fail on the payload. The body limit itself is a 413 before the handler.
- **Remote write parse errors are 400**, as on the remote read.
- **`+json` media types are SenML.**
- **An unknown matcher type is a 400** (`TryFrom` instead of `From`).
- **Short messages**: the type mismatch of `SingleSensorBatch::append` no longer prints every sample,
  the CSV type error shows the first 80 characters of the value, and the name and label errors show the
  first 64. The time-column error that printed a whole column was already fixed in an earlier task.
- **Regex matchers** are left to `postgres-regex-validation-and-statement-timeout.md` (task 12), as the
  task says.

A storage failure still answers 500: `real_router::real_server_errors_are_still_logged_as_errors`.

Decision: a remote write with one invalid series is refused whole with a 400. Nothing is stored, and the
message names the series. Skipping the bad series and storing the rest would be friendlier to Prometheus,
but a partial write with a 400 is harder to reason about. KISS, as the task allows.

Documented in `docs/CONFIGURATION.md`, which now has a table of the status codes of the writes.

Suites: SQLite full (305 unit, 350 integration) and PostgreSQL full (311 unit, 350 integration) passed.
`cargo clippy --all-features --tests` is clean. The validation runs before the storage, so no other
backend was run.
