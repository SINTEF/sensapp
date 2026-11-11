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

## Outcome (10 November 2026)

**Both findings confirmed by tests.** Through the real router, a token limited to `temperature_*` posted the
UUID of a `pressure` series with the name `temperature_*` as SenML and got `200` (the bypass). Through storage,
a write under a stored UUID with another type was accepted on SQLite. An Arrow export of a JSON series came
back as string samples under a JSON series (invisible), and a Float64 column declared `integer` was accepted.

**Decisions.**

- The rule compares the **name and the type**, not the unit and the labels. The name is what allow lists
  check and the type chooses the table; neither unit nor labels affect security or visibility. Comparing the
  labels would refuse legitimate imports (SenML carries no labels) and cost a labels read per known series on
  every Prometheus write; a derived UUID already hashes them, so only client-chosen UUIDs can differ. The
  stored unit and labels stay, as `DATAMODEL.md` already said.
- A write that gives **only the UUID** (the importers then use the UUID text as the name: SenML without
  `_name`, Arrow with only `sensor_id`) appends to the stored series whatever its name; the type must match.
  `DATA_LIFECYCLE.md` and the Python SDK document this way of appending. It opens no way around the allow
  lists: they then check the UUID text, which a list holds only if an administrator put it there.
- The error is a new `StorageError::SensorIdentityMismatch`, answered `409 Conflict`. The message gives the
  UUID and the name and type that were sent, **not the stored name**, which the writer may not be allowed to
  see. A 409 does tell a restricted token that the UUID exists; UUIDs are salted, so this was judged
  acceptable.
- Importing an export under a renamed sensor is an error (the trade-off the task proposed). The docs say to
  import without the name or as a new series.
- `AuthorizedStorage::publish` keeps its name check unchanged.

**Fix.**

- `b231a25`: `unique_sensors` and `check_sensor_identity` in `src/storage/common.rs`, called from the shared
  registration of each backend with a catalog: `pg_sensor_registration.rs` (PostgreSQL and TimescaleDB, also on
  the path where a concurrent transaction created the sensor), `sqlite_utilities.rs`, `duckdb_registration.rs`,
  `clickhouse_utilities.rs`, `bigquery/publishers.rs`. The lookups that existed now also read `name` and
  `type`; no extra query. One batch that gives one UUID two names or two types is refused before anything is
  written. The BigQuery cache of registered series (`RegisteredIds`) keeps a fingerprint of the name and type,
  so a cached UUID sent with another identity is looked up and refused.
- `9b035ce`: the Arrow importer refuses a `sensapp.sensor.type` that is not the type of the value column (400),
  except `json` on a text column, whose values are parsed: that is how the Arrow exporter writes JSON series.
- `c9ed6a6`: the UUID-only rule above.
- `5111368`: the rule in `DATAMODEL.md`, `JWT_AUTH.md` and `DATA_LIFECYCLE.md`.

**Not covered.**

- RRDcached stores no name nor type (one file per UUID): nothing to compare, and the tests skip it.
- ClickHouse and BigQuery have no transaction: two requests that create one new UUID with two identities at the
  same moment both succeed. Written in the code and in `DATAMODEL.md`.
- SenML Integer round trip: the SenML exporter writes Integer as plain numbers, which the importer reads as
  Float, so importing the SenML export of an Integer series is now refused with 409 instead of being stored
  where no read finds it. The fix (a `_type` extension field) is in `senml-import-export-consistency.md`
  (task 8). A SenML `bn` in a non-canonical UUID form (uppercase, braces) without `_name` keeps that text as
  its name and is refused against a named series; task 8 restricts UUIDs to the canonical form.

**Tests.**

- `tests/integration/sensor_identity.rs` (storage, generic): another name refused, another type refused, one
  batch with two identities for a new UUID refused and nothing created, same name and type with another unit
  and no labels appended (stored unit and labels kept), UUID-only write appended (and refused with another
  type). The refusals failed on SQLite before the fix.
- `tests/integration/identity_over_http.rs` (real router, auth on): the bypass above as SenML and Arrow gets
  409, without the stored name in the answer, and no sample is added; an unrestricted SenML and Arrow export of
  a series imported again goes to that series. The first test failed (200) on `b4bb5f9`.
- `src/importers/arrow.rs`: metadata/column type mismatch refused; every type exported as one Arrow series
  imports back with its type and samples (failed for JSON before).
- BigQuery `RegisteredIds` unit test: another identity is not a cache hit.

Run times of the new integration tests: 0.05 s (SQLite) to 2.6 s (ClickHouse) for the storage tests.

**Suites run (10 November 2026, local Docker).** Full suites: SQLite (284 unit, 325 integration), PostgreSQL
(290, 326), DuckDB (287, 322; the doctest step fails locally because the doctest binary does not find
`libduckdb.dylib`, an environment issue), ClickHouse (298, 338). TimescaleDB shares the registration code
with PostgreSQL, so only the new tests ran on it (`sensor_identity`, `identity_over_http`,
`rolled_back_publish`: all green, 0.6–1.5 s each); CI runs its full suite. RRDcached: the CI subset
(`rrdcached_integration::`, 27 tests) green. BigQuery: unit tests (316) green; not run against a live
project. `cargo clippy --all-features --tests` clean.
