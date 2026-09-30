# Data retention

Sensor and research data is often kept forever, so retention is not part of the first
data lifecycle work. When a deployment needs it:

- One global, opt-in setting, e.g. `SENSAPP_RETENTION="400d"`, unset means keep forever.
  This matches Prometheus (`--storage.tsdb.retention.time`) and VictoriaMetrics.
- Hourly background task deleting samples older than `now - retention`; keep sensor metadata.
- Reuse `parse_duration_millis` from `src/http/crud.rs`.
- Backends can do it cheaply: TimescaleDB `drop_chunks` / `add_retention_policy`,
  ClickHouse table `TTL` or `DROP PARTITION` (tables are already partitioned by month).
- Per-series or per-metric retention is what the industry keeps for paid tiers; skip it.
