# Configuration Reference

SensApp is configured with environment variables, an optional TOML settings file, or both. Environment variables win over the file. Every setting has a default except where noted, so `sensapp` starts with nothing configured (it then expects PostgreSQL on `localhost`).

## Where settings come from

1. Environment variables (`SENSAPP_*`).
2. A TOML file, `settings.toml` in the working directory by default. Point `SENSAPP_SETTINGS_FILE` at another path to change it. The file is optional; a missing file is not an error. Keys are the lowercase variable name without the prefix, e.g. `storage_connection_string`. See the commented example in [`settings.toml`](../settings.toml).
3. The defaults below.

The Helm chart sets variables through `env:` in `values.yaml` (see the [chart README](../charts/sensapp/README.md)). Secrets (the connection string, the JWT secret) belong in a Kubernetes Secret, not in `env:`.

## Settings

### Network

| Variable | Default | Description |
| --- | --- | --- |
| `SENSAPP_ENDPOINT` | `127.0.0.1` | Address to listen on. Use `0.0.0.0` in a container. |
| `SENSAPP_PORT` | `3000` | TCP port to listen on. |

### Storage

| Variable | Default | Description |
| --- | --- | --- |
| `SENSAPP_STORAGE_CONNECTION_STRING` | `postgres://postgres:postgres@localhost:5432/sensapp` | Selects and configures the storage backend by URL scheme, see [Backends](#backends). Contains credentials: it is redacted from logs and errors, keep it in a secret. |
| `SENSAPP_PG_POOL_MAX_CONNECTIONS` | `10` | Size of the connection pool for `postgres:` and `timescaledb:`. Invalid or `0` falls back to the default. Not part of the settings file: it can only be set as an environment variable. |
| `SENSAPP_DEDUPLICATE_ON_INGEST` | `false` | Do not write the samples that are stored already (same series, same timestamp, same value), nor the repeated samples of a request. Costs a few milliseconds per write and has a PostgreSQL condition, see [Duplicate samples](DATA_LIFECYCLE.md#duplicate-samples). The server **refuses to start** on a backend that cannot do it: PostgreSQL, TimescaleDB, SQLite and DuckDB can, ClickHouse, BigQuery and RRDCached cannot. |
| `SENSAPP_BATCH_SIZE` | `8192` | Number of samples collected before being sent to storage in one batch. Larger batches mean fewer round trips and more memory per request. |

### HTTP server

| Variable | Default | Description |
| --- | --- | --- |
| `SENSAPP_HTTP_BODY_LIMIT` | `64MiB` | Largest accepted request body, also after gzip or Snappy decompression. Over the limit gives `413`. Accepts units such as `512KiB`, `64MiB`, `1GiB`, up to 128 GiB. |
| `SENSAPP_HTTP_SERVER_TIMEOUT_SECONDS` | `30` | Time a request may take before it is answered with `504 Gateway Timeout`. |
| `SENSAPP_HTTP_MAINTENANCE_TIMEOUT_SECONDS` | `3600` | Same as above for `POST /api/v1/admin/vacuum` only, which scans every value table and is expected to be slow on a large database. |
| `SENSAPP_HTTP_MAX_CONCURRENT_WRITES` | `16` | Write requests (`/publish`, InfluxDB write, Prometheus remote write, admin) handled at the same time. See [Backpressure](#backpressure). `0` disables the limit. |

Details on what the limits protect against are in [HTTP_LIMITS.md](HTTP_LIMITS.md).

### Authentication

| Variable | Default | Description |
| --- | --- | --- |
| `SENSAPP_JWT_SECRET` | unset | Enables JWT authentication when set (at least 32 characters). Unset means every endpoint is open. See [JWT_AUTH.md](JWT_AUTH.md). |

### Data

| Variable | Default | Description |
| --- | --- | --- |
| `SENSAPP_SENSOR_SALT` | `sensapp` | Salt of the hash that turns a sensor's name, type, unit and labels into its UUID. **Changing it changes the UUID of every sensor**: existing data is no longer matched by new writes. Set it once per deployment, before ingesting. |
| `SENSAPP_INFLUXDB_WITH_NUMERIC` | `false` | Store InfluxDB numbers as exact decimals instead of floats. Precise but slower. See [INFLUX_DB.md](INFLUX_DB.md). |

### Observability

| Variable | Default | Description |
| --- | --- | --- |
| `RUST_LOG` | `info,tower_http=info` | Log filter, in [`tracing-subscriber` syntax](https://docs.rs/tracing-subscriber/latest/tracing_subscriber/filter/struct.EnvFilter.html) (e.g. `sensapp=debug,sqlx=warn`). |
| `SENSAPP_SENTRY_DSN` | unset | Sends errors to Sentry when set. |

Service metrics are exposed at `/prometheus/metrics`; health checks at `/health/live` and `/health/ready`.

Failed responses are logged at the level that fits them: a `503` (load shedding, or an unavailable database, which logs its own error where it is detected) at `DEBUG` and counted in `sensapp_http_requests_total{status="503"}`, a `504` (timeout) at `WARN`, any other `5xx` at `ERROR`. Every line of a request carries its `request_id`.

## Backends

The scheme of `SENSAPP_STORAGE_CONNECTION_STRING` picks the backend. A backend only works if it was compiled in: the default build includes `postgres` and `sqlite`, the container image adds `timescaledb`, `duckdb`, `clickhouse` and `rrdcached`, and `--features all-storage` builds everything.

| Connection string | Backend | Status |
| --- | --- | --- |
| `clickhouse://user:pass@host:8123/db`, `clickhouses://` for TLS | ClickHouse | Reference backend for pre-production, see [CLICKHOUSE.md](CLICKHOUSE.md). |
| `postgres://user:pass@host:5432/db` | PostgreSQL | Main development backend. |
| `timescaledb://user:pass@host:5432/db` | TimescaleDB | PostgreSQL with the TimescaleDB extension. |
| `sqlite://path/to/sensapp.db` | SQLite | Single file, for small setups and tests. |
| `duckdb://path/to/sensapp.db` | DuckDB | Experimental. |
| `rrdcached://host:42217?preset=munin&heartbeat=3600`, `rrdcached+unix:///path/to.sock` | RRDCached | Experimental: numbers only, no names or labels. `preset` is `hoarder` (default) or `munin`, `heartbeat` is in seconds. See [RRDCACHED.md](RRDCACHED.md). |
| `bigquery://key.json?project_id=P&dataset_id=D` | BigQuery | Experimental, not in sync with the current storage interface. |

Backends create and migrate their own schema at startup. [BACKENDS.md](BACKENDS.md) says which ones are maintained and which are experimental, and what each is good for.

## Backpressure

Writes are buffered in memory before being parsed, so the number of writes in flight bounds the memory SensApp can use: roughly `SENSAPP_HTTP_MAX_CONCURRENT_WRITES × SENSAPP_HTTP_BODY_LIMIT` in the worst case, plus decompression.

SensApp sheds load instead of queueing it. When all slots are taken, a new write is rejected at once with `503 Service Unavailable` and a `Retry-After` header, before its body is read, so a rejection costs almost nothing and the latency of accepted writes does not grow. Reads, health checks and metrics are not limited. Rate limiting is deliberately not done here: put a reverse proxy in front if you need it.

`Retry-After` is a whole number of seconds, picked at random (full jitter) between 1 and twice the moving average of how long recent writes held their slot, at most 30. Clients rejected together therefore come back spread over the time it takes the slots to turn over, and the hint grows when the database gets slower. Before the first write it is 1 or 2 seconds.

The header also means "this request was not processed", so it can be resent safely. The other `503` SensApp sends, `Database unavailable`, has no `Retry-After` and may follow a partial write. On the SQL backends it means that the database cannot be reached: the connection pool timed out or was closed, the connection was refused or cut, or PostgreSQL reported a connection failure, a shutdown or too many connections. A statement that is merely slow (a statement timeout, a cancelled query) or a misconfiguration (an SQLite file that cannot be opened) is a `500`, which clients do not retry, so that a slow database is not hit by a retry storm.

Rejections are counted by `sensapp_http_writes_shed_total`, which only the load shedding increments. `sensapp_http_requests_total{status="503"}` on the write paths also holds the `503 Database unavailable` answers, so use the shed counter to tell the two apart. A steady increase of the shed counter means the database is the bottleneck (or the limit is too low for it): look at storage latency before raising the limit. As a starting point keep the limit close to the database connection pool (`SENSAPP_PG_POOL_MAX_CONNECTIONS` for PostgreSQL); a limit far above the pool only moves the waiting from the HTTP layer to the pool.

A client that sends a large body may see the connection closed instead of the `503` response, because SensApp answers without reading the body. Treat it as any transient network error, and see below for resending.

### What writers should do

Backpressure only works if clients listen to it. The [Python SDK](PYTHON_SDK.md#retries) does all of this by default.

- Retry `503`, `504`, `429`, connection errors and timeouts, never other `4xx`. A write rejected with `Retry-After` was not processed at all. After a timeout, a connection error or a `503` without it the write may have been committed in whole or in part, and resending it can store samples twice: SensApp does not reject duplicates when they are written, and they stay until the vacuum operation is run: nothing runs it automatically (see [DATA_LIFECYCLE.md](DATA_LIFECYCLE.md#duplicate-samples)).
- Wait for the `Retry-After` value when present, but cap it: if it is longer than you are willing to wait, drop the request instead of coming back early. Otherwise use exponential backoff with full jitter, `sleep = random(0, min(cap, base * 2^n))`, for example a 100 ms base and a 30 s cap.
- Bound the retrying: 2 to 5 attempts and a total time limit. After that, drop the request and report the failure (or, for a device, keep it in a bounded local buffer and drop the oldest data first). The server cannot do this for you: it keeps no state per client.
- Retry at one layer only, not in the client library and again in a proxy or an agent: 3 layers × 3 retries is 27 attempts for one request.
- Stop retrying when your own deadline has passed.

## Request IDs

Every request gets an `x-request-id` header in its response, errors and `503`s included. A value sent by the client or by a reverse proxy is kept, otherwise SensApp generates a UUID. The same ID is in the `request_id` field of the request's log lines, so a failure reported by a client can be found in the logs.
