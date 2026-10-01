# Prometheus remote read with `step` hints still reads series one by one

`query_sensor_data_for_prometheus` (`src/http/prometheus_read.rs`) has a second path, used when the
read request carries hints with a `step`: it discovers the sensors with
`query_sensors_by_labels(.., limit 1)` and then calls `query_sensor_data_advanced` for each of them,
sequentially. The plain selector path now reads in bulk (`StorageInstance::query_selector`, see
`done/bulk-selector-reads.md`); this one still costs several queries per series, and its discovery
reads one sample per series that it throws away.

Fix idea: a bulk "bucketed read" in the same spirit (one query per numeric type with
`GROUP BY sensor_id, bucket`), shared budget enforced the same way. Only worth doing if Prometheus
is used with large remote read ranges.
