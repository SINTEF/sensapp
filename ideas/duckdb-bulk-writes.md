# DuckDB: register many new series in bulk

Measured with `tests/perf/scale.sh 3000` on a release build (after the bulk read work):

- write of 3000 new series (30 000 samples): 2.6 to 3.0 s; the same series again: 0.35 to 0.39 s
- strings, 3000 series x 10 samples, 50 distinct strings: 4.5 s; one series of 10 000 distinct strings: 2.1 s

Series registration (`get_sensor_id_or_create_sensor`) is one `SELECT`, one `INSERT` and one `INSERT` per label,
and the string dictionary is one statement per distinct string. The value tables already use appenders. The
same pattern as `pg_sensor_registration` (sort, multi-row insert, read the ids back) would apply with
appenders on `sensors`, `labels` and the dictionaries.

Not done because DuckDB is the "less mature" backend (local analysis, not ingestion); worth doing if DuckDB
becomes an ingestion target. The aggregated selector read (`BulkSelectorBackend::read_aggregated_samples`)
is also still one query per series on DuckDB (0.3 s for a 100 series remote read with a step); the
`time_bucket` SQL of `duckdb_bucketed_cte` extends to many sensors with `GROUP BY sensor_id, bucket`.
