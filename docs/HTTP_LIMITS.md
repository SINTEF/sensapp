# HTTP Request and Query Limits

SensApp bounds HTTP work to keep one request from using excessive memory.

- `SENSAPP_HTTP_BODY_LIMIT` defaults to `64MiB`. It applies to `/publish`, InfluxDB write, Prometheus remote write, and Prometheus remote read. Requests over the limit return HTTP 413. The server buffers each accepted request body before parsing it.
- Gzip-compressed InfluxDB writes and Snappy-compressed Prometheus requests are also limited after decompression. An expanded body over the configured size is rejected.
- `GET /series/{uuid}` allows at most 100,000 samples. An explicit `limit` above that is rejected. Without `limit`, the server detects a larger result and asks the client to narrow the time range or use aggregation.
- `limit` on `GET /series/{uuid}` returns the **oldest** N samples of the window, in time order, and does not signal that more exist. Use `start`/`end` to page through a series.
- `simplify` runs on the rows read for the window, after bucketing when `step` is set and after `limit`. If that read holds more than 100,000 rows, the request is rejected with HTTP 400 rather than simplified from a truncated read. Add `step` and `aggregation`, or narrow `start`/`end`. The 100,000 cap applies to the rows simplify reads, not only to its output.
- Selector queries and Prometheus remote read allow at most 256 series and 100,000 samples total. A query over a limit returns a client error instead of a truncated result. Cross-series aggregations (`avg by (room) (temperature[24h])`) fetch the raw samples first, so the same limits apply to the samples read, not to the aggregated output: a larger `step` does not help, narrow the selector or the window instead.
- `/prometheus/metrics?include_latest_samples=true` allows at most 10,000 matching series. Filter by `metric` or `selector` when needed.

These limits are fixed for now. Operators can change the request body size with `SENSAPP_HTTP_BODY_LIMIT`; query limits are intentionally simple and shared across deployments.
