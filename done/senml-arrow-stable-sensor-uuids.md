# Stable sensor UUIDs for SenML and Arrow imports

## Problem

`src/importers/senml.rs` built sensors with `Uuid::new_v4()`, and `src/importers/arrow.rs` did the same
when the payload carried no UUID. CSV, InfluxDB and Prometheus remote write use
`Sensor::new_without_uuid`, which derives the UUID from name, type, unit and labels (keyed blake3, keyed
by `sensor_salt`, so reproducible). Publishing SenML for `"n":"temp"` three times created three series,
and the "delete a range, publish it again" recipe in `docs/DATA_LIFECYCLE.md` did not restore the
original series.

## Decisions

- **SenML**: if the resolved sensor name parses as a UUID (what the SenML exporter writes in `bn`), it is
  the series UUID and the name comes from `_name` when present (else the UUID string). Otherwise the UUID
  is derived with `Sensor::new_without_uuid(name, type, unit, None)`.
- **Arrow**: an explicit UUID (`sensapp.sensor.uuid` metadata or `sensor_id` column) still wins.
  Otherwise the name (`sensapp.sensor.name` metadata or `sensor_name` column) is required and the UUID
  is derived from name, type, unit and labels. No UUID and no name: the import is rejected with a 400.
- Arrow payload validation errors are 400s, not 500s.

## Progress

- [x] SenML importer
- [x] Arrow importer and HTTP error mapping
- [x] Unit and integration tests (identical publishes give one series; delete then republish)
- [x] Remove the caveat from `docs/DATA_LIFECYCLE.md`
- [x] `cargo check`, `cargo test`, `cargo clippy --tests`

## Notes

- `publish_arrow_async` was replaced by `parse_arrow_sensors` + `publish_arrow_sensors`, so payload
  errors map to 400 in the HTTP handler and the test router uses the same path.
- Tests ran on the default features (SQLite); PostgreSQL tests skip without a database.
- A SenML UUID `bn` pointing at an existing series of a different type is handled by storage like any
  explicit UUID (Arrow already worked this way).
