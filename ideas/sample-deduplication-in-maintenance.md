# Sample deduplication in the maintenance path

Writing the same `(sensor, timestamp)` twice creates two rows. This is by design: a unique
index and `ON CONFLICT` on every insert has a real ingestion cost.

Where dedup should live instead: the vacuum / maintenance operation
(`POST /api/v1/admin/vacuum`), which today only runs `VACUUM` on PostgreSQL and SQLite.
SQLite already has an unused `deduplicate()` (`src/storage/sqlite/storage.rs`) as a starting point.

To do:

- Check whether Prometheus remote write actually sends duplicate samples to us
  (client retries after a timeout on a request we already committed, HA replica pairs, ...).
  Measure before deciding anything.
- Decide whether dedup is optional (a maintenance call / config flag) or on by default.
- Decide the conflict policy for same timestamp with different values (last write wins like
  InfluxDB, or keep both).
- Once vacuum removes rows, it should require the `delete` scope, not only `write`.
- ClickHouse: `OPTIMIZE ... DEDUPLICATE` or a `ReplacingMergeTree` migration.
