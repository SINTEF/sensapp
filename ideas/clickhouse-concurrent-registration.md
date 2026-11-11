# ClickHouse and BigQuery: two writers can register one UUID with two identities

Found on 11 November 2026 while adding `two_writers_registering_one_uuid_with_two_types_cannot_both_succeed`
(`tests/integration/sensor_identity.rs`). On PostgreSQL, TimescaleDB, SQLite and DuckDB exactly one of two
concurrent writers of a new UUID with two types wins and the other gets `SensorIdentityMismatch`. On ClickHouse,
the test failed in round 0: the registration looks the sensor up and then inserts it, with nothing between the
two, so both writers can succeed and the series has two identities (the second insert is the one a
`ReplacingMergeTree` keeps, or both rows stay, depending on the table). The same shape is expected on BigQuery,
not run (it bills the project).

Not new: it was always so. It matters only for writers that race on a fresh UUID with different names or types,
which is a bug in the writers (two instances of one device, a UUID collision).

Options: a unique insert through a lightweight transaction (ClickHouse 25+ experimental), an
`INSERT ... SELECT ... WHERE NOT EXISTS` followed by a read to see who won, or accept it and document it in
`docs/CLICKHOUSE.md`. The test skips these two backends until then.
