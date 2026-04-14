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

Notes:

- Port `8123` is the expected HTTP port.
- Port `9000` is the native ClickHouse protocol and is not what the current Rust client path uses.
- SensApp creates the target database if it does not already exist.

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

For Helm deployments, set the storage connection string to an external ClickHouse service:

```bash
helm install sensapp ./charts/sensapp \
  --set storage.connectionString=clickhouse://default:password@clickhouse:8123/sensapp \
  --set persistence.enabled=false
```

`persistence.enabled=false` is intentional here because ClickHouse should own persistence, not the SensApp pod.

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

## Operational Caveats

- SensApp currently targets ClickHouse through the HTTP endpoint only.
- Schema creation is embedded in the binary through the migration SQL file.
- The backend is tested against a real ClickHouse service, but production readiness still depends on good external operations: backups, disk sizing, and ClickHouse monitoring remain the operator's responsibility.
- If you expose SensApp beyond a trusted network, enable JWT auth.

## Recommended Near-Term Checks

Before calling a deployment pre-production ready, confirm:

- repeated restarts do not break migrations
- `/health/ready` turns unhealthy when ClickHouse is unavailable
- publish, list, query, and export endpoints all work against the same ClickHouse instance
- ClickHouse storage and query latency are monitored externally
- backup and restore procedures are documented outside the app runtime
