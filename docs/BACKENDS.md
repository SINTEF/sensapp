# Storage Backends: What Is Maintained And What Is Experimental

SensApp can store its data in several databases, selected by the scheme of
`SENSAPP_STORAGE_CONNECTION_STRING` (see [CONFIGURATION.md](CONFIGURATION.md#backends)). They are not equally
mature. This page says which ones to rely on, and what each one is for.

## Summary

| Backend | Status | Tested in CI | Use it for |
| --- | --- | --- | --- |
| ClickHouse | **Reference backend** for pre-production | yes, with a real server, plus a container smoke test with outage and recovery | The deployment SensApp is being hardened for: large volumes, external persistent database |
| PostgreSQL | **Maintained**, main development backend | yes | Small and medium deployments, development |
| TimescaleDB | **Maintained** | yes | PostgreSQL deployments that want hypertables and compression |
| SQLite | **Maintained** | yes | Tests, demos, single-node edge devices, one writer |
| DuckDB | Compatibility path, **less mature** | yes | Local analysis of a dataset, notebooks. Stores millisecond timestamps |
| RRDCached | **Experimental** | yes (its own job) | Fixed-size round-robin storage, monitoring-style data; no deletion |
| BigQuery | **Experimental, parked** | compile only | Nothing yet: it has not been brought back in line with the current storage interface (see `ideas/bigquery-backend-reconciliation.md`) |

"Maintained" means a regression in it fails CI, the backend-generic integration tests run on it, and bugs
found on it are fixed. "Experimental" means it works for the basic ingest and query paths, may lag behind new
features, and has no promise.

## What each backend gives you

| | ClickHouse | PostgreSQL | TimescaleDB | SQLite | DuckDB | RRDCached | BigQuery |
| --- | --- | --- | --- | --- | --- | --- | --- |
| Values of every type | yes | yes | yes | yes | yes | numeric data points | yes |
| Delete a series or its samples | yes | yes | yes | yes | yes | no | no |
| Aggregations (`step`) in the database | yes | yes | yes | yes | yes | no | no |
| Remove duplicate samples (vacuum) | yes | yes | yes | yes | yes | no | no |
| Registration of many new series per write in bulk | yes | yes | yes | one by one, fast locally | one by one | no | no |
| A selector reads its series with a few queries | yes | yes | yes | yes | yes (numeric series; aggregated ones one at a time) | one series at a time | one series at a time |
| Prometheus remote read with a `step` aggregates its series with a few queries | yes | yes | yes | yes | one series at a time (each aggregation is a single query) | one series at a time | one series at a time |
| Replication | not created by SensApp | the database's own | the database's own | no | no | no | managed |

The last two rows are a consequence of how the code is written, not of the databases: a backend that reads
or writes series one by one still works, it costs more round trips with many series.

### Things to know about each

- **ClickHouse**: tables are plain `MergeTree` (and `ReplacingMergeTree` for the metadata), for a single server.
  Writes are at least once and not atomic across tables. Details, failure behaviour, TLS with a private CA and
  backups: [CLICKHOUSE.md](CLICKHOUSE.md). TLS is verified by hand against a server with a private CA.
- **PostgreSQL**: the value tables only have a BRIN index on `(sensor_id, timestamp)`: it is tiny and fits
  append-mostly data, but cannot return rows in order, so a read of a wide window sorts what it finds.
- **TimescaleDB**: the same schema as PostgreSQL with a `time` column and hypertables, partitioned by week and
  by a hash of the sensor over two slices, so every week has two chunks. The chunks older than a week are
  compressed by a background policy; reads, writes, deletes and the vacuum give the same results on compressed
  chunks (tested by comparing every answer before and after compressing, also with partially compressed
  chunks). Two things come from TimescaleDB 2.17, the version CI runs: SensApp sets `plan_cache_mode =
  force_custom_plan` on its connections, because a cached generic plan returns wrong rows after a chunk
  changes state, and the aggregated `count` is written `COUNT(value)`, because `COUNT(*)` fails to plan over
  many chunks. Details: `done/timescaledb-compressed-chunks.md`.
- **SQLite**: one writer at a time; units keep their name but not their description.
- **DuckDB**: timestamps are stored with a millisecond precision, unlike the other backends (microseconds).
- **RRDCached**: only its dedicated integration module runs against a real `rrdcached` (the backend-generic
  suite does not apply: no labels, no deletion, consolidated data). Data is consolidated according to the chosen preset, old precision is lost by design. Details:
  [RRDCACHED.md](RRDCACHED.md).
- **BigQuery**: needs Google Cloud credentials and is only compiled in CI.

## Features: production-oriented and research-oriented

Production-oriented (hardened, tested, documented for operators):

- HTTP ingestion of SenML, CSV, Arrow, InfluxDB line protocol and Prometheus remote write, with request body
  limits, a write concurrency limit that answers `503` with `Retry-After`, a request timeout (`504`) and a
  request id on every response.
- Optional JWT authentication with scopes and sensor allow lists.
- Health and readiness endpoints, Prometheus metrics of the service, structured request logs.
- Selectors (`/api/v1/query`, Prometheus remote read) with series and sample limits.
- The container image and Helm chart, and the ClickHouse backend as described above.
- The Python SDK with retries.

Research-oriented (useful, but not a promise of stability):

- Running the same data on several backends to compare them (the reason the backends exist).
- The Arrow export and the DCAT catalog endpoints, the Polars and pandas helpers of the SDK.
- The simple PromQL subset: selectors and `sum|avg|min|max|count`, no `rate()` or arithmetic.
- The experimental backends (RRDCached, BigQuery).
