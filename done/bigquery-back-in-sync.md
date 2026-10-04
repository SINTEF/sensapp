# BigQuery: back in sync with the storage interface

Replaces `ideas/bigquery-backend-reconciliation.md` (removed). The old local checkout
`/Users/antoinep/work/sensapp-vibe-prom-read` was read for its ideas (cache lifespans, identifier
validation, parallel reads); nothing of it was copied, and it can be deleted once this task is done.

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

## Done (3 Oct 2026)

- [x] Id helpers shared with ClickHouse in `storage::common`
- [x] New schema and writes: ids from the data, no dictionaries, microsecond timestamps, doubles, NUMERIC as
      text, checked answers, no global write lock, JSON as JSON
- [x] `client.rs`: waits for jobs, reads all pages, parameters, `maximum_bytes_billed`, error sorting
- [x] Reads: sensors and labels, 8 sample types, paginated listing, matchers, bulk selectors, latest sample,
      deletes; aggregation in BigQuery (`GROUP BY` of buckets, single series and selectors), checked in the
      integration tests against the local reference of `apply_query_options`
- [x] Unit tests (SQL builders, rows against the migration, connection string, errors): 27, no credentials needed
- [x] Integration tests: `tests/integration/bigquery_integration.rs` (round trip of every type, a unit and a
      window, concurrent instances, paging, Prometheus matcher semantics, a result over one page, the cost
      cap), BigQuery added to `data_lifecycle`, `advanced_backend_queries`, `deduplication`
- [x] Docs (`docs/BIGQUERY.md` with the Google Cloud setup), CI job `bigquery-checks`, `test-bigquery-live`
- [x] Dependencies: `gcp-bigquery-client` 0.28, `tonic`, `prost` were already the latest; `bigdecimal`, its
      encoder, `clru`, `tonic` and `sinteflake` (with `SENSAPP_INSTANCE_ID`) are gone

## Live run (4 Oct 2026, project `smartbuildinghub`, dataset in europe-north1, user credentials)

All the integration tests that apply to BigQuery pass: the 9 of `bigquery_integration`, the 42 of the
`data_lifecycle`, `advanced_backend_queries`, `query_sensors_by_labels`, `selector_reads`,
`selector_aggregated`, `aggregated_windows` and `cross_series_reads` modules, and the 242 others (arrow,
ingestion, InfluxDB, Prometheus remote read and write, PromQL, JWT, exports, CRUD, publish robustness...).
Measured: 16 901 statements (3 415 `DELETE`, 9 639 `SELECT`), **25 GiB billed**, for these runs and the
partial or repeated ones. Time: 143 s for the 9, 1 300 s for the 42, 1 705 s for the 242.

Found by the first live run and fixed (`994221d`):

| # | Finding |
|---|---------|
| 1 | The gRPC client of the Storage Write API panicked: no default rustls provider when `ring` and `aws-lc-rs` are both compiled in (the server installs one, a test does not). `connect` installs it |
| 2 | `AVG` of BigQuery is not the exact sum over the count in the last bits (-5.5e-17 for integers that average to 0). Kept: it is faster, and the difference does not matter in a warehouse. The test compares with a tolerance |
| 3 | Rows still in the streaming buffer are billed 0 bytes, so `max_bytes_billed` cannot be tested on them: the test uses the metadata statement, which bills 10 MB at least |
| 4 | `TRUNCATE` is not faster than `DELETE ... WHERE TRUE` (about 2.5 s a statement either way), kept `DELETE` |

Not a finding but worth knowing: doubles are exact on `gcp-bigquery-client` 0.28 (`0.1 + 0.2` and `f64::MAX`
round trip). The `f32` of the first backend was the `Float64` descriptor of 0.22 declared as a protobuf `float`
(lquerel/gcp-bigquery-client#106), fixed since 0.26.

Not measured: latency of a small write and of a read in isolation, behaviour under real load, the Storage
Write API quotas.
