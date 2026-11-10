# Check the sensor identity when a write names an existing UUID

Source: code review of 10 November 2026. Confirmed by reading, not reproduced.

## Problem

Two importers let the client choose the UUID separately from the name and type:

- SenML: a UUID as `bn`, the name from the `_name` extension field (`src/importers/senml.rs:86-96`, `:107-130`).
- Arrow: the `sensor_id` column or `sensapp.sensor.uuid`, with `sensapp.sensor.name` and
  `sensapp.sensor.type` (`src/importers/arrow.rs:144-157`).

Storage resolves an existing sensor by UUID only and never compares name, type, unit or labels
(`src/storage/pg_sensor_registration.rs:40-48`, `src/storage/sqlite/sqlite_utilities.rs:169-183`,
`src/storage/duckdb/duckdb_registration.rs:36-49`, `src/storage/clickhouse/clickhouse_utilities.rs:104-115`,
`src/storage/bigquery/publishers.rs:183-197`). Two consequences:

1. **Allow-list bypass.** `AuthorizedStorage::publish` (`src/http/authorized_storage.rs:57-66`) checks the
   name in the payload. A token restricted to `temperature` posts
   `[{"bn":"<uuid of pressure>","_name":"temperature","t":1700000000,"v":0}]` and the sample is appended to
   `pressure`. With the default `SENSAPP_SENSOR_SALT`, the UUID of a series is computable from its name,
   type, unit and labels.
2. **Samples in a table no read looks at.** The publishers pick the table from the `TypedSamples` variant,
   the reads pick it from `sensors.type`. A SenML document with `bn = <uuid of an Integer series>` writes to
   `float_values`; the request succeeds and the samples are invisible. `delete_series_samples` does not reach
   them. `done/senml-arrow-stable-sensor-uuids.md:35` says storage handles this case; it does not.
   Arrow has a second way in: `extract_sensor_type(batch).unwrap_or(derived_sensor_type)` (`arrow.rs:147`)
   lets the metadata type disagree with the column type.

## Suggested solution

Reject the write when the UUID exists with another name or type.

- Registration: when a sensor UUID exists, compare the stored `name` and `type` with the incoming ones and
  return a typed error (new `StorageError` variant, mapped to 409 or 400 in `src/http/app_error.rs`). Do it in
  the shared registration code of each backend so it is one rule everywhere. Unit and labels: decide and
  write the decision here; the simplest consistent rule is to compare them too, since a derived UUID hashes
  all four.
- Arrow: reject a batch whose metadata type differs from the column type, before storage.
- Keep the name check in `AuthorizedStorage::publish`. With the registration check, the claimed name is the
  stored name, so the check becomes sound.

Open decision for the maintainer: this makes importing an export under a renamed sensor an error. That is
the intended trade-off unless decided otherwise.

## Tests

- A restricted token cannot append to another series through SenML `bn` + `_name`, nor through the Arrow
  metadata: expect 403 or the mismatch error, and no new sample on the target.
- Publishing an existing UUID with another type is refused, on every backend (generic test).
- Export then import of the same series still works (same name, same type). Note: the SenML exporter writes
  Integer as plain numbers, which the importer reads as Float, so check what the round trip does for an
  Integer series and decide whether the importer should follow the stored type.

## Done when

The tests pass on all backends in CI, and `docs/JWT_AUTH.md` and `docs/DATAMODEL.md` state the rule.
