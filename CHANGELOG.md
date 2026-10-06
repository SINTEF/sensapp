# Changelog

SensApp is pre-production: breaking changes happen between minor versions and are listed first.
Releases are published from a `vX.Y.Z` tag, see [docs/RELEASING.md](docs/RELEASING.md).

## 0.4.1

### Added

- **A Download button** in the explorer (one file per series, downloaded in the background), backed by
  `?download=true` on `GET /series/{uuid}`, which answers with a `Content-Disposition` header.
- `sensapp serve`, the explicit name of what plain `sensapp` does.
- `SENSAPP_HTTP_MAX_QUERY_SAMPLES` (default `100000`): the most samples a read may return, instead of constants.
- The explorer header wraps on narrow screens, and has a 1 year preset.

### Changed and fixed

- **Prometheus remote read of a plain selector over a long range** answers with one sample per step (the last
  sample of each step, with its own timestamp) instead of the raw samples, so graphing a metric over weeks at a
  coarse step no longer fails with `Query exceeds 100000 samples`. Prometheus gets the same answer as with the raw
  samples. Checked live against Prometheus 3.8, 3.13 and 3.15.
- The command line is parsed with clap: `sensapp --help` prints the help instead of making a token for the
  subject `--help`.
- SQLite: `delete_series` takes the write lock first, so concurrent deletes wait instead of failing, and
  `SQLITE_BUSY` and `SQLITE_LOCKED` answer `503`.
- ClickHouse: deletes are checked and repeated, as ClickHouse 24.8 can lose one.

## 0.4.0

### Breaking changes

- **SensApp no longer runs open by default.** Without `SENSAPP_JWT_SECRET`, a run on a loopback address makes a
  secret for that run and prints an admin token and a link to the UI that signs in; a run on any other address
  (the container image, Helm) refuses to start. Set `SENSAPP_JWT_SECRET`, or `SENSAPP_AUTH_DISABLED=true` to
  run open on purpose. See [docs/JWT_AUTH.md](docs/JWT_AUTH.md).
- Tokens have a new `admin` scope, a `jti`, `iss` and `aud`. `SENSAPP_JWT_PREVIOUS_SECRETS` rotates the secret
  without refusing the tokens of the previous one. `sensapp generate-secret` makes a secret.
- **ClickHouse databases created by 0.3.0 are refused at startup** with a clear message: the sensor and unit
  ids and the schema changed (stable ids, partitions pinned to UTC, no materialized views). There are no
  deployments yet, so create a new database.
- DuckDB stores timestamps with a microsecond precision, like the other backends.
- RRDCached connection strings `rrdcached+unix` and `rrdcached+tcp` are routed to the RRDCached backend, which
  now stores, reads and lists what it keeps.

### Added

- **A web UI** served by SensApp at `/ui/`: a data explorer (series by label, time window by drag, step and
  aggregation, dark theme, shareable address), code snippets (Python SDK, curl) from the state of the explorer,
  a Load Data tab (Python SDK, Telegraf, Prometheus, curl) and a Credentials tab to make tokens.
- `POST /api/v1/admin/tokens`, to make tokens with an admin token. The `Token` scheme of InfluxDB clients is
  accepted next to `Bearer`.
- Cross-series aggregation in the database (`avg by (room) (temperature[24h])` on every backend), and
  aggregated reads for Prometheus remote read hints on a whole selector at once.
- Removal of duplicate samples with the vacuum operation (all SQL backends and DuckDB), and an opt-in
  deduplication at ingestion (`SENSAPP_DEDUPLICATE_ON_INGEST`).
- Write backpressure (`SENSAPP_HTTP_MAX_CONCURRENT_WRITES`, `503` with `Retry-After`), request ids in logs and
  responses, and timeouts of their own for writes and for the vacuum.
- A rewritten **BigQuery backend** on the current storage interface (experimental, integration suite passed
  on a real dataset), and an RRDCached backend that works against a real daemon (experimental).
- Python SDK: retries of overload answers, connection errors and timeouts, a timeout per operation.
- Helm chart: secret handling for the authentication (made secret, `existingSecret`, `previousSecrets`).

### Changed and fixed

- ClickHouse: outages answer `503`, no more duplicated sensors or labels, stable ids, TLS with the system store,
  writes spanning more than 100 months, bulk registration and bulk reads.
- PostgreSQL and TimescaleDB: bulk registration of sensors and writes of numeric and string samples, integer
  time buckets and plannable time bounds (aggregations 2 to 4 times faster), a retry after a deadlock on the
  first write of new series, correct answers on compressed chunks, TimescaleDB 2.30.2 in CI.
- SQLite: windows and the last sample use the index (`/last` from 54 ms to 0.5 ms), writes start with
  `BEGIN IMMEDIATE`. DuckDB: writes with a handful of statements and microsecond timestamps.
- Label regex matchers are anchored like Prometheus does. Remote read hints aggregate only when Prometheus gets
  the right answer.
- Read timeout 120 s on the server and 125 s in the SDK.
- The binary is built on the library crate instead of compiling the tree twice.

## 0.3.0

First tagged release of this line, 30 September 2026.
