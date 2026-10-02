# ClickHouse Deployment Guide

This guide describes the current recommended way to run SensApp with ClickHouse when the goal is operational credibility rather than local experimentation.

## Position

For SensApp, ClickHouse is the first backend that should feel pre-production ready.

That means:

- the database must be external and persistent
- SensApp instances should stay stateless
- readiness should depend on real ClickHouse connectivity
- migrations should be safe to run repeatedly
- operators should validate ingest and query paths explicitly after deployment

## Connection String

SensApp expects the ClickHouse HTTP interface, not the native TCP port.

Use a connection string like this:

```text
clickhouse://default:password@clickhouse:8123/sensapp
```

For a TLS endpoint, use `clickhouses://`:

```text
clickhouses://default:password@clickhouse.example.com:8443/sensapp
```

Notes:

- Port `8123` is the expected HTTP port.
- `clickhouses://` uses HTTPS and defaults to port `8443` when no port is specified.
- Port `9000` is the native ClickHouse protocol and is not what the current Rust client path uses.
- SensApp creates the target database if it does not already exist. Database names with a hyphen (`sensapp-prod`) work.
- The string is a regular URL. The user, the password and the database name are percent-decoded, so a generated password containing `/`, `#` or `?` must be percent-encoded (`/` is `%2F`, `#` is `%23`, `?` is `%3F`, `%` is `%25`). An `@` or `:` in the password works as is, but `%40` and `%3A` are fine too.

### TLS and private certificate authorities

`clickhouses://` trusts the system certificate store, like most Unix tools. For a ClickHouse behind a private CA, point `SSL_CERT_FILE` (a PEM bundle) or `SSL_CERT_DIR` at your CA, or add it to the system store of the image. The official image ships `ca-certificates`. This was verified by hand against a ClickHouse container serving HTTPS with a private CA: there is no TLS integration test in CI, on purpose. A certificate that no trusted CA signed is refused at startup (`invalid peer certificate: UnknownIssuer`). The server name in the URL must be in the certificate (a DNS or IP subject alternative name).

## Schema And Guarantees

SensApp creates its tables at startup (`CREATE TABLE IF NOT EXISTS`, safe to repeat):

- one table per value type (`float_values`, `integer_values`, ...), partitioned by month in UTC and ordered by `(sensor_id, timestamp_us)`, so a read of one series over a time range touches few parts;
- `sensors`, `labels` and `units` as `ReplacingMergeTree` tables, read with `FINAL`. Two writers registering the same new series at once both insert it, and the identical rows collapse. Labels are written when a sensor is created, since a sensor UUID is derived from its labels.
- the table ids are a fixed function of the sensor UUID and of the unit name, so they survive upgrades of SensApp and of Rust.

What to expect:

- **The tables are not replicated.** They are plain `MergeTree` tables for a single ClickHouse server. A replicated or clustered deployment needs its own table definitions (`ReplicatedMergeTree`, `ON CLUSTER`), which SensApp does not create.
- **Writes are at least once, not atomic.** A write touches several tables, and a failure in the middle (a crash, a timeout) can leave part of a request stored. A client that retries then stores some samples twice: SensApp does not reject duplicates when they are written, and `POST /api/v1/admin/vacuum` removes them only when an operator runs it, nothing does it automatically (see [DATA_LIFECYCLE.md](DATA_LIFECYCLE.md#duplicate-samples)).
- **Reads do not hide duplicates.** The value tables are plain `MergeTree` tables, not `ReplacingMergeTree`: no merge removes a duplicate sample in the background and the reads use neither `FINAL` nor a deduplication, so a duplicated sample is returned twice and counted twice until the vacuum removes it. It is the same as on the other backends. Only `units`, `sensors` and `labels` are `ReplacingMergeTree`, and they are read with `FINAL`.
- **One request may span any period** (up to 200 years): the limit of 100 partitions per insert is raised.
- **Databases from before the first release are refused** at startup with a clear message: create a new database.
- `POST /api/v1/admin/vacuum` removes duplicate samples (`OPTIMIZE TABLE .. FINAL DEDUPLICATE BY` on every value table, which rewrites their data: it costs what a full merge costs, and an HTTP request that waits for it can time out while ClickHouse carries on). It is not needed for normal operation, run it after incidents that may have produced duplicates, such as a retried write that had timed out.

Float values use `CODEC(Gorilla, ZSTD(1))` and timestamps `DoubleDelta`: on millions of samples that is between 1 and 7 bytes per float depending on how noisy the series is, and under 0.01 byte per regularly spaced timestamp.

## Failures

| Situation | What SensApp answers |
| --- | --- |
| ClickHouse down, unreachable, timing out, read-only, or refusing inserts with `Too many parts` or a memory limit | `503 Database unavailable`, writes and reads. Retry later. |
| ClickHouse hangs for longer than `SENSAPP_HTTP_SERVER_TIMEOUT_SECONDS` | `504 Gateway Timeout`. |
| Any other error from ClickHouse (a syntax error, access denied, a missing table) | `500`, details in the SensApp log. |
| `/health/ready` | `503` as soon as ClickHouse does not answer `SELECT 1` within 3 seconds, `200` again when it does. No restart of SensApp is needed. |

A retried write that got a `503` without a `Retry-After` header may have been stored in part, see above.

## Recommended Topology

For a practical deployment:

- run ClickHouse separately from SensApp
- persist ClickHouse data on durable storage
- run one or more stateless SensApp replicas behind a load balancer
- keep the SensApp container filesystem ephemeral
- use JWT auth if the deployment is not fully private

## Local Docker Example

Example ClickHouse container:

```bash
docker run -d --name sensapp-clickhouse \
  -p 8123:8123 \
  -p 9000:9000 \
  -e CLICKHOUSE_DB=sensapp \
  -e CLICKHOUSE_USER=default \
  -e CLICKHOUSE_PASSWORD=password \
  clickhouse/clickhouse-server:24.8
```

Example SensApp run command:

```bash
SENSAPP_STORAGE_CONNECTION_STRING=clickhouse://default:password@127.0.0.1:8123/sensapp \
cargo run --no-default-features --features clickhouse
```

Verify readiness:

```bash
curl http://127.0.0.1:3000/health/ready
```

## Helm Example

For Helm deployments, put the connection string for an external ClickHouse service in a Kubernetes Secret as described in the [chart README](../charts/sensapp/README.md):

```bash
helm install sensapp ./charts/sensapp \
  --set storage.existingSecret=sensapp-storage
```

`persistence.enabled=false` is the default because ClickHouse owns persistence, not the SensApp pod.

## Startup And Readiness Expectations

Current behavior:

- SensApp runs ClickHouse migrations during startup
- repeated migrations are expected to be safe
- `/health/ready` depends on the storage backend health check
- ClickHouse readiness currently uses a simple `SELECT 1`

Operational implication:

- if ClickHouse is down, misconfigured, or unreachable, SensApp should fail readiness even if the HTTP server is alive

## Post-Deploy Validation

After deployment, validate the full basic lifecycle.

### 1. Check readiness

```bash
curl http://127.0.0.1:3000/health/ready
```

### 2. Publish one sample

```bash
curl -X POST http://127.0.0.1:3000/publish \
  -H 'content-type: text/csv' \
  --data-raw $'datetime,sensor_name,value,unit\n2026-03-16T12:00:00Z,temperature,21.5,C'
```

### 3. Query it back

```bash
curl 'http://127.0.0.1:3000/api/v1/query?query=temperature'
curl 'http://127.0.0.1:3000/series'
curl 'http://127.0.0.1:3000/metrics'
```

### 4. Check service metrics

```bash
curl http://127.0.0.1:3000/prometheus/metrics
```

## Backup And Restore

Backups are the operator's job. ClickHouse's own `BACKUP` and `RESTORE` statements work on a SensApp database. They need the destination to be allowed in the server configuration, for example in `config.d/backups.xml`:

```xml
<clickhouse><backups><allowed_path>/backups/</allowed_path></backups></clickhouse>
```

Without it `BACKUP` fails with `Path '/backups/b1' is not allowed for backups`. Cloud object stores (`TO S3(...)`) and backup disks are configured the same way, see the ClickHouse documentation.

```sql
BACKUP DATABASE sensapp TO File('/backups/sensapp-2026-10-01');
-- a disaster later:
DROP DATABASE sensapp;
RESTORE DATABASE sensapp FROM File('/backups/sensapp-2026-10-01');
```

This round trip was exercised on a SensApp database (ClickHouse 24.8): after `DROP DATABASE` and `RESTORE`, a freshly started SensApp listed the same series with the same labels, returned the same samples, and accepted new writes. Test your own backup and restore on disposable data before relying on it, and monitor the size and the age of the backups.

## Operational Caveats

- SensApp currently targets ClickHouse through the HTTP endpoint only.
- Schema creation is embedded in the binary through the migration SQL file. It runs at every startup, so a future change must keep it safe to repeat.
- The backend is tested against a real ClickHouse service, but production readiness still depends on good external operations: backups, disk sizing, and ClickHouse monitoring remain the operator's responsibility.
- A selector query (`/api/v1/query`, Prometheus remote read) reads all its series with a handful of queries whatever their number: about 20 ms for 100 series on a laptop, and a selector over the 256 series limit is rejected after one metadata query. The read of a window larger than the 100 000 samples budget stops as soon as the budget is exceeded.
- If you expose SensApp beyond a trusted network, enable JWT auth.

## Recommended Near-Term Checks

Before calling a deployment pre-production ready, confirm:

- repeated restarts do not break migrations
- `/health/ready` turns unhealthy when ClickHouse is unavailable
- publish, list, query, and export endpoints all work against the same ClickHouse instance
- ClickHouse storage and query latency are monitored externally
- backup and restore procedures are documented outside the app runtime
