# Changelog

SensApp is pre-production: breaking changes happen between minor versions and are listed first.
Releases are published from a `vX.Y.Z` tag, see [docs/RELEASING.md](docs/RELEASING.md).

## Unreleased

### Breaking

- **NaN is a gap in Arrow and CSV, not a `400`.** A NaN or a null in an Arrow value column, and in a numeric CSV
  column an empty cell, `NaN`, `null`, `NA` or `N/A`, is skipped instead of refusing the request or turning the
  CSV column into text. `POST /publish` answers with `X-SensApp-Skipped-Samples` and counts them in
  `sensapp_samples_processed_total{kind="skipped"}`; Prometheus remote write counts its NaN in
  `sensapp_prometheus_remote_write_dropped_total{part="nan"}`. SenML and InfluxDB still refuse a NaN. CSV `inf`,
  `-inf` and `Infinity` are infinities, an infinite coordinate is a `400`, and a SenML `"v": null` is no value
  instead of `0`. SQLite's `sum` and `avg` of `+Inf` and `-Inf` is NaN, as on the other backends (it was `0`).

- **A restricted token is authorized by UUID, and a write to an existing UUID with another name appends.** The
  `sensors` claim holds names and UUIDs. A write passes when its UUID is listed, or its name is listed and its UUID
  is the one derived from its name, type, unit and labels: a listed name can no longer be claimed for the UUID of
  another series (`403`), and a series created with a UUID of its own needs that UUID in the list. The `409` for
  another **name** is gone (the type keeps it): the stored name, unit and labels stay. A NUL or a separator byte in
  the name, unit or label of a series with a UUID of its own is a `400`.
- **PostgreSQL and TimescaleDB: no rolling restart from 0.4.2.** The new migrations drop the unique constraints
  that a 0.4.2 binary writes with (`ON CONFLICT (value)`): its writes of String samples and of new sensors fail
  with `42P10` until it is replaced, and it cannot start on a migrated database. Stop every old instance, then
  start the new version. See [docs/UPGRADING.md](docs/UPGRADING.md). The migrations now run on a connection of
  their own, without the statement timeout of the requests (they rolled back above 130 s on a large database).
- **`SENSAPP_ALLOWED_HOSTS`** names the hosts of an open server (`SENSAPP_AUTH_DISABLED`). An open server behind
  a proxy that keeps its public `Host` answered `403` to everything, and one that rewrites the `Host` refused the
  writes of a browser; both work when the public name is listed. With it, DNS rebinding is refused on any
  address, not only on a loopback bind, and a request without a `Host` is refused. What the guard refuses is now
  counted and logged with a request id. Without the setting on a non-loopback address SensApp warns at startup.
- **A PromQL query or selector is no longer limited in nesting** (it was 64 levels, counted by a text scan that a
  `#` comment containing a quote, or a chain of `^`, got around: the parser then overflowed its stack, which
  aborts the process). The parse runs on a thread whose stack fits the 4,096 bytes allowed. A `simplify` that
  would cost more than 100 million distance evaluations (a tolerance near 0 on a large zigzag) is a `400`.
- **SensApp serves HTTP/1.1 only**, no HTTP/2 over cleartext, and a client has `SENSAPP_HTTP_HEADER_READ_TIMEOUT_SECONDS`
  (75 s by default, 30 s before) to send its headers or to send its next request on an idle connection. A
  connection that sends nothing is now closed too. The slow body of a write is a `503` with `Retry-After` instead
  of a `408`, which Prometheus does not retry.
- **ClickHouse and BigQuery key a series by the keyed BLAKE3 hash of its UUID**, keyed by
  `SENSAPP_SENSOR_SALT`, instead of the XOR of its two
  halves, which gave structured UUIDs chosen by SenML and Arrow writers the same key: `(a, b)` and `(b, a)`, and
  every UUID with equal halves. Those were refused with `409`; they are now separate series. Every stored
  `sensor_id` changes, and there is no migration: **empty a ClickHouse or BigQuery database and load it again**.
  The new version refuses to start on a database written before (it reads ten stored sensors and compares their keys), instead of registering every stored UUID a second time. The salt is now
  part of the key: **changing `SENSAPP_SENSOR_SALT` orphans the series of a ClickHouse or BigQuery database**,
  explicit UUIDs included. Set your own salt before ingesting: with the public default, anyone can search
  for two UUIDs of the same key offline.
- **Prometheus remote write no longer stores `__name__` among the labels** of a series, which made remote
  read return it twice. The labels are part of a series' identity, so a series written by remote write
  before this version and written again after it becomes a new series. The old one keeps its samples and
  is still read with one `__name__`.
- `/series?selector=` and `/prometheus/metrics?selector=` select by the metric name: `temperature{room="a"}`
  and `{__name__="temperature"}` used to ignore the name.
- `offset` and `@` in a selector or a `/api/v1/query` answer `400` instead of being ignored.
- **NaN is refused** with `400` in every write, instead of being stored on some backends and dropped on
  SQLite. Prometheus remote write drops its NaN samples (stale markers). The infinities are stored on every
  backend, SQLite included, and the JSON Lines export and `/last` write them, and NaN, as `"+Inf"`, `"-Inf"`
  and `"NaN"` instead of `null`. The SenML export leaves them out.
- **Numeric aggregations are exact**: SQLite compared the values as text, PostgreSQL averaged large values
  with 16 significant digits, and DuckDB through a float. A sum that does not fit, Numeric or Integer,
  answers `400` instead of `500`, and so does its average. SQLite stores Numeric values normalised.
- The Arrow export of Numeric values lowers the scale of its `Decimal128` below 18 when a value needs more
  than 20 digits before the point. A value with more than 18 digits after the point is rounded, where it was
  written off by a power of ten, and a large value wrapped.
- A label name given twice to one series (`cpu,host=a,host=b`) is refused with `400`. DuckDB answered `500`
  and the other backends kept one of the two values.
- RRDCached refuses a sample more than 10 minutes in the future with `400` (the `future` parameter of the
  connection string): the series could not take a sample before it any more. `/metrics` answers `501` on
  RRDCached instead of an empty list.
- **An open server (`SENSAPP_AUTH_DISABLED`) refuses browser requests from other origins** (`Origin` that
  is not the `Host`), and on a loopback address the `Host` names that are not loopback ones, with `403`. A web
  page could write to it, or run the vacuum, in the name of a visitor. `curl` and the SDKs are not affected.
- SensApp refuses to start with a timeout of `0`, a body limit of `0` or over 2 GiB (it was 128 GiB),
  `SENSAPP_HTTP_MAX_QUERY_SAMPLES` of `0` or over 1,000,000,000, a token duration of `0` or over ten years, or
  two JWT secrets with the same key id.
- An admin token that lists sensors is refused, and `generate-token` does not make one: admin is for every
  sensor.
- The Helm chart refuses to render SQLite or DuckDB with more than one replica or with autoscaling, and no
  longer sets a 30 s timeout over SensApp's 120 s.
- Arrow exports write their timestamps as `Timestamp(Microsecond, "UTC")` instead of without a timezone.
- The CSV export of a selector query puts a label named like a fixed column (`type`, `value`, …) in a column
  `label_<name>` instead of a second column of that name.
- The Prometheus remote write models keep the exemplars, histograms and metadata as bytes: code that builds a
  `WriteRequest` or a `TimeSeries` needs `..Default::default()`.

### Changed and fixed

- **CSV**: a time offset is subtracted (the stored instants change), a CSV without a time column is a `400`, the
  epoch unit comes from the magnitude of the number, a date alone is a time, week and ordinal dates are no longer
  times, and a wide CSV with a duplicate header is a `400`.
- **Arrow**: a null timestamp is a `400`, null values are dropped, a `sensor_id` or `sensor_name` column that is
  not constant is a `400`, a metadata type that differs from the column type is a `400`, and the decompressed size
  of a body is limited (an LZ4 bomb used to allocate without bound).
- **SenML**: a record that mixes types or only has a sum is a `400`, only the hyphenated form of a UUID is a UUID,
  `_type` and `_labels` are read, and the export writes `bu`, `_type`, microsecond times and no record for an empty
  series.
- **Every write**: a time outside years 1 to 9999, a NUL and the separator bytes in a name, unit or label are a
  `400`, `+json` is read as SenML, a remote write that cannot be parsed is a `400`, and a panic in a handler is a
  `500` for that request instead of stopping the instance.
- **Queries**: regex matchers are validated (a pattern a backend would read differently is a `400`) and anchored.
- **PostgreSQL and TimescaleDB**: `SENSAPP_PG_STATEMENT_TIMEOUT_SECONDS` and `SENSAPP_PG_LOCK_TIMEOUT_SECONDS`,
  dictionaries unique by `md5`, new migrations and indexes. See [docs/UPGRADING.md](docs/UPGRADING.md).

- PostgreSQL and TimescaleDB: a write that waits for a lock longer than `SENSAPP_PG_LOCK_TIMEOUT_SECONDS`
  (a series being deleted) or loses a deadlock is a `503`, which clients retry, instead of a `500`; PostgreSQL
  retries a deadlock like TimescaleDB did (five attempts). With both timeouts at `0` no startup option is sent
  (PgBouncer). The latest sample of a series is read again when a delete removed the samples of its time
  between the two statements: it returned nothing, or no sample, while older ones remained (SQLite,
  PostgreSQL, TimescaleDB, ClickHouse; DuckDB still does, see `ideas/`). The `make` test tasks of PostgreSQL
  and TimescaleDB no longer cap the pool at one connection, which hid the races.

- Prometheus remote read: the buckets of a `*_over_time` hint start with the range of their own selector,
  not with the earliest selector of the query (`max_over_time(x[2m]) > y` was off by one minute), and stop
  at the end of the query. They are only used when the evaluations are on multiples of the step, as a
  subquery evaluates there: the Prometheus UI, which does not align its queries, gets the raw samples. A
  plain selector or an aggregation whose end is off the step is answered on both grids, which a subquery
  (`max_over_time(sum(x)[1h:5m])`) needs. Checked live against Prometheus 3.13.
- Deleting a series while another instance writes to it: PostgreSQL and TimescaleDB answered `500` on a
  foreign key, and ClickHouse left the samples of the write without a sensor row, back when the series was
  written again. Both now answer, see [docs/DATA_LIFECYCLE.md](docs/DATA_LIFECYCLE.md). TimescaleDB deletes
  over more than 100,000 compressed samples no longer fail on the decompression limit.
- Prometheus remote read: the series are sorted by label set, a series that fails to encode fails the
  request instead of missing from it, hints out of range get the raw samples instead of overflowing, and
  nothing is printed on stdout (the library denies `print_stdout`).
- SQLite: writers queue on a write pool of one connection instead of holding the read connections (reads
  under 16 writers from 43 ms to 0.2 ms at the median), the vacuum deduplicates one series at a time (a
  concurrent write waited 0.94 s, now 8 ms), labels and sensor names are indexed, a selector matching more
  than 32,766 series no longer fails on the SQL variable limit, and multi-row inserts are not cached (143 MB
  of memory after 300 writes of varied sizes, now 19 MB).
- `/metrics` lists the label keys of a metric on SQLite without splitting a name that holds a
  comma, and on DuckDB and ClickHouse, which listed none. DuckDB groups metrics by name and type, not unit.
- First, last and raw reads of samples with equal timestamps follow the order of the writes on SQLite and
  DuckDB, where they picked any of them.
- DuckDB: reads and the health check use connections of their own and no longer wait for a write (reads
  under 16 writers from 31 ms to 2.7 ms at the median), a write whose request timed out while it waited is
  not committed, the schema has a version, and a read without a limit is capped like on the other backends.
- A series read with an upper-case or hyphen-less UUID is found (`404` on SQLite and DuckDB), and the
  availability of a series without a step no longer fails on DuckDB.
- RRDCached: a read whose end is after now stops shortly after now (an end a year ahead read 3 million empty
  rows), the health check waits 3 s at most, and the `metric` filter of `/series` is the whole UUID.
- BigQuery: a write is a success only when BigQuery confirmed every request it was cut into, availability is
  one statement instead of reading every sample, and `/metrics` lists the label keys. Not run live yet.
- A token with a sensor allow list deletes nothing it cannot read the name of: a delete of a series whose read
  failed went through.
- `/health/ready` answers a fixed text instead of the database error, which it gave anyone; the error is logged.
- A client has 75 s to send the headers of a request, and an instance holds at most 4,096 connections: idle
  connections could hold every file descriptor.
- InfluxDB writes read every member of a gzip body (a body of two concatenated gzip streams lost the
  second), and the parsing of InfluxDB, CSV, Arrow and Prometheus bodies no longer blocks the async runtime.
- The `sensapp_series_processed_total` of InfluxDB writes counts each series once, not once per sample.
- A CSV upload moves its cells instead of copying them: a 4.5 MB file peaked at 63 to 69 MB over the body,
  down from 97 to 105 MB. The UUIDs of sensors are no longer cached, which spent memory to save little.
- The CSV export of a selector query quotes the label names and a lone carriage return.
- `GET /series/{uuid}` refuses a `start` after the `end` with `400`, like the other reads.
- `sensapp_prometheus_remote_write_dropped_total{part}` counts the exemplars, native histograms and metadata
  that the remote write drops.
- Sentry is no longer in debug mode, `docker compose` creates the `sensapp-test` database, and
  `settings.toml` tells how authentication works.

## 0.4.2

A maintenance release: dependencies and security fixes, no change in behaviour.

### Changed and fixed

- Dependencies refreshed to their latest versions: Rust (DuckDB 2.0, tower-http 0.7, sentry 0.49,
  prometheus-client 0.25, sha2 0.11, and more), frontend (TypeScript 6.0, vite 8.3.6, jsdom 31),
  and the Python SDK (polars 2, pyarrow 26).
- JWTs are handled by the aws-lc-rs backend of `jsonwebtoken`, which removes `rsa` and `crypto-bigint` 0.5
  from the build along with their advisories (RUSTSEC-2023-0071, RUSTSEC-2026-0387). `npm audit` runs
  clean, and `cargo audit` has one ignore, below.
- The open Dependabot alerts are fixed: `zip` 6 (critical, a build dependency of the previous DuckDB crate,
  gone with DuckDB 2.0), rustls 0.23.46, and, in the frontend tooling, vite and undici.
- Known: the SQLite bundled by sqlx 0.9.0 is affected by RUSTSEC-2026-0459 (CVE-2026-119375), a memory leak when
  SQLite opens a database with a crafted `-wal` file. No sqlx release has the fix yet. SensApp only opens the
  SQLite database it is configured with, so the risk is limited to whoever can already write in that directory.
  `cargo audit` ignores it until sqlx is updated.
- Tested on ClickHouse 26.8 LTS and TimescaleDB 2.31 on PostgreSQL 18. The local TimescaleDB test volume
  has a new name, as the PostgreSQL 18 image keeps its data elsewhere.

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
