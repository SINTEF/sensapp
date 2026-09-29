# Data lifecycle controls: delete series / delete sample range

## Goal

Give operators a supported remedy for bad or unwanted data, in line with what
Prometheus, InfluxDB and friends offer: delete by series and time range.

## Decisions

- Delete only. No update API: correct data by deleting the range and re-publishing.
- Deletes require a new `delete` JWT scope, never part of the default `read write`.
- No retention yet (see `ideas/data-retention.md`).
- Duplicate `(sensor, timestamp)` rows are by design; dedup is a separate task
  (see `ideas/sample-deduplication-in-maintenance.md`).

## What was done

- `StorageInstance::delete_series` (`Ok(false)` if unknown) and `delete_series_samples`
  (`Ok(None)` if unknown, so the API can answer 404), implemented for PostgreSQL, SQLite,
  TimescaleDB, DuckDB and ClickHouse. BigQuery and RRDcached return the new
  `StorageError::Unsupported`, mapped to `501`.
- `AuthorizedStorage` hides sensors outside a token's allow list (reported as not found, like reads).
- `delete` scope, `require_delete_auth`, and `generate-token --scope` now accepts a list
  (`readwrite,delete`).
- `DELETE /series/{uuid}` (204) and `DELETE /series/{uuid}/samples?start=&end=` (200, `deleted_samples`),
  both bounds required and inclusive, with audit logging and metrics.
- `docs/DATA_LIFECYCLE.md`, README and `docs/JWT_AUTH.md` updated.

## Things found on the way

- **Cached sensor ids went stale after a series delete.** Every SQL backend caches
  `uuid -> sensor_id` for 120 s (`#[cached]` in the `*_utilities.rs` files). Re-publishing a
  deleted series inside that window failed on the foreign key (PostgreSQL, TimescaleDB) and,
  on SQLite (foreign keys off), silently wrote samples under a sensor id that no longer existed.
  Fixed with `forget_sensor_id` after `delete_series`, covered by the `correction_recipe` tests.
  Other instances (or manual SQL) can still hold a stale id, so on PostgreSQL and TimescaleDB
  `publish` retries once after a foreign-key violation, having forgotten the ids of the batch
  (`is_foreign_key_violation` in `src/storage/common.rs`, tested by
  `*_publish_recovers_from_stale_sensor_id`).
- SenML and Arrow imports use random UUIDs, so each publish creates a new series. Not fixed here:
  `ideas/senml-arrow-random-sensor-uuids.md`.
- ClickHouse: the `sensor_catalog_view` and `metrics_summary_view` materialized views are not
  read by any code and are not cleaned on delete.
- DuckDB checks foreign keys against committed data, so `delete_series` commits the sample and
  label deletes before deleting the sensor row.

## Verification

- `tests/data_lifecycle.rs`, generic over backends: passes on PostgreSQL, SQLite, TimescaleDB 2.17,
  DuckDB and ClickHouse 24.8.
- `tests/jwt_auth.rs`: delete scope enforcement, parameter validation, sensor-scoped tokens.
- End to end on the real binary (SQLite, JWT enabled, InfluxDB ingestion): correction recipe,
  delete then re-publish under the same UUID, no orphaned samples.
- `cargo fmt`, `cargo clippy --all-targets`, `cargo clippy --features all-storage --all-targets`,
  `cargo test` all clean.
