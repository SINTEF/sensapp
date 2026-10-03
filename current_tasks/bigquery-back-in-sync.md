# BigQuery: back in sync with the storage interface

Replaces `ideas/bigquery-backend-reconciliation.md`.

## Goal

BigQuery is an optional, R&D backend (like RRDCached): simple, documented limitations, cheap to test.
Bring it to the level of the other backends **without running anything against Google Cloud yet**
(queries cost money, the user will create the project), then say what to set up in GCP and run the
tests.

## Audit of the old module (3 Oct 2026)

`cargo check --features bigquery` passes, but only because the trait has default methods:

| # | Finding |
|---|---------|
| 1 | `query_sensor_data` returns the sensor with **no samples** (a comment says so), ignores the window and the limit; `query_sensors_by_labels` is a `bail!`; `list_series` ignores filter, limit and bookmark and runs one labels query per sensor |
| 2 | Floats and locations are written as `f32` (the table says FLOAT64): precision loss |
| 3 | JSON values are written as `value.as_str().unwrap_or("")`: anything but a JSON string becomes `""` |
| 4 | `jobs.query` waits 10 s then answers `jobComplete=false`; `ResultSet::new_from_query_response` turns that into **zero rows**, silently. Only the first page (10 MB) of a result is read |
| 5 | The answer of the Storage Write API is checked for `row_errors` only, not for the `error` of the response; an append that is rejected is reported as a success |
| 6 | Every append takes the **write** lock of the whole client: all writes of the process are serialized |
| 7 | Sensor ids come from sinteflake after a read: two instances registering a sensor at once get two ids and duplicate rows. Caches are process-global statics, shared by every dataset |
| 8 | SQL built with `format!` and the sensor UUID of the caller in it (`WHERE s.uuid = '{}'`) |
| 9 | Timestamps are sent as ISO strings (nanoseconds are not valid for TIMESTAMP) |
| 10 | Dictionaries for label names, label values and strings: 3 more tables, a join per read, a lookup per write |

## Plan

- [ ] Move the sensor/unit id helpers to `storage::common`, shared with ClickHouse
- [ ] New schema and writes: deterministic ids, no dictionaries, micros timestamps, doubles, checked responses
- [ ] Query helper: pages, incomplete jobs, parameters, `maximum_bytes_billed`
- [ ] Reads: sensors, samples of the 8 types, labels, matchers, listing, selectors, deletes
- [ ] Integration tests in the generic harness (`TEST_DATABASE_URL=bigquery://...`), unit tests for the SQL
- [ ] Docs, CI compile check
- [ ] GCP setup instructions for the user
