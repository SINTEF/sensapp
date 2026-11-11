# Writes to an existing UUID: authorize by UUID, convert the type when exact

Source: maintainer feedback on pull request 60, 11 November 2026. This replaces part of
`done/sensor-identity-check-on-write.md`.

## Context

Pull request 60 answers `409` when a write names an existing sensor UUID with another name or type. The name
comparison closed an allow-list bypass; the type comparison stopped samples from landing in a table no read
looks at. The maintainer does not want the name rule: the same protection can come from the allow list
itself. The type rule refuses two normal uses (see below).

## Part 1: allow list (decided)

Remove the name comparison at registration. Authorize writes in `AuthorizedStorage::publish`
(`src/http/authorized_storage.rs`) with no storage lookup. For a restricted token, each sensor of a batch
passes when either:

- its UUID is in the token's list, or
- its name is in the token's list **and** its UUID equals the one SensApp derives from its name, type, unit
  and labels (`Sensor::new_without_uuid`). The derived UUID is a keyed hash that includes the name, so a
  claimed name cannot point at another series.

Anything else is a 403. A write that gives UUIDs alone passes only when they are listed.

- The `sensors` claim accepts names and UUIDs (canonical hyphenated form). Reads and deletes honour UUID
  entries too: `restrict()`, `can_see_sensor`, `list_filtered_series`.
- An unrestricted token writing an existing UUID with another name appends; the stored name stays.
- This also closes the remaining bypass on RRDcached (`src/storage/rrdcached/mod.rs:647`), where the name of
  a series is its UUID text and storage has nothing to compare.
- Consequence to document in `docs/JWT_AUTH.md`: a series created with a client-chosen UUID is writable by a
  restricted token only when that UUID is in its list, even if its name is.
- `Sensor::new` (explicit UUID) validates nothing, while `new_without_uuid` validates name, unit and labels
  (`src/datamodel/sensor.rs:199-220`). Validate in both, so a NUL in `_name`, `_labels` or `bu` with a UUID
  `bn` is a 400 and not a database error.

## Part 2: type mismatch (recommended, awaiting the maintainer's decision)

Two normal uses send another type than the stored one: SenML only has float numbers, and pandas turns an
integer column with gaps into `float64`. Recommended rule: convert the samples to the stored type when the
conversion is exact, refuse the request otherwise.

| Incoming | Stored | Converts when |
| --- | --- | --- |
| Float | Integer | every value is a whole number within the `i64` range |
| Integer | Float | every value is within ±2^53 |
| Integer | Numeric | always |
| Float | Numeric | always, through the shortest decimal form of the float |
| Numeric | Integer or Float | the value is exactly representable |
| anything else | | never: refuse, and name the stored type in the message |

- Registration already reads the stored type for today's check. Return it to the caller, and apply one
  shared conversion (`TypedSamples::convert_to(SensorType) -> Result<TypedSamples>`) before the publishers
  pick a table, in the shared code so every backend behaves the same.
- All or nothing per request: one inexact value refuses the request (keep the current status code).
- Under several instances the stored type is read inside the write transaction where the backend has one,
  as the identity check is today.
- The SenML `_type` field stays for creating a series with the right type; it is no longer needed for a
  round trip.

The alternative is to keep refusing (what the pull request does). Do not start Part 2 before the decision.

## Tests

- Part 1: a token with names only, UUIDs only, and both; a claimed name with a foreign UUID (403); UUID-only
  writes; reads and deletes by UUID entry; the RRDcached case; NUL in `_name` with a UUID `bn` (400).
- Part 2: each row of the table, an inexact value (refused, nothing stored), and the SenML and pandas
  round trips into an Integer series. Generic across backends.

## Done when

The tests pass, the name `409` is gone, and `docs/JWT_AUTH.md`, `docs/DATAMODEL.md` and the changelog state
the rules.

## Part 1 outcome (11 November 2026): done. Part 2 is still waiting for the maintainer's decision

- `AccessContext::can_write(&Sensor)`: a restricted token passes a sensor when its UUID (the canonical hyphenated
  text) is listed, or its name is listed and `Sensor::derived_uuid()` equals its UUID. `AuthorizedStorage::publish`
  uses it with no storage lookup. `AccessContext::can_access(&Sensor)` (name or UUID listed) serves the reads, the
  lists and the deletes (`can_see_sensor`, `list_filtered_series`, the filters of `AuthorizedStorage`).
- The name comparison is gone from every backend: `check_sensor_identity(sensor, stored_type)` and
  `unique_sensors` compare the type only (the error no longer carries a name, the message says "another type"),
  the lookups no longer read the name, and the BigQuery cache fingerprint is the UUID and the type. This also
  closes the bypass on RRDcached, since the check is on the token and not on the storage.
- `Sensor::validate()` (and `derived_uuid`) run in the SenML and Arrow importers for a series with a UUID of its
  own, so a NUL in `_name`, `bu` or `_labels` is a 400.
- Tests: unit `a_write_needs_a_listed_uuid_or_a_listed_name_with_its_derived_uuid`; integration
  `a_restricted_token_writes_by_derived_uuid_or_by_listed_uuid` and `a_nul_in_the_name_of_a_series_with_a_uuid_is_a_400`
  (`jwt_auth`), the name tests of `sensor_identity` rewritten (another name appends, the stored name stays; two
  names in a batch are created under the first), and the 409 of `identity_over_http` is a 403 now.
  SQLite and PostgreSQL in full, ClickHouse and DuckDB on the identity, `jwt_auth` and importer tests.
- **Limitation, documented in `docs/JWT_AUTH.md`**: a UUID entry cannot be pushed into the `__name__` matcher
  that restricts selector reads (no backend filters by UUID there), so in `/series?selector=`, Prometheus remote
  read and `/prometheus/metrics` a UUID entry only matches a series that is *named* by its UUID. Reads by UUID, the
  lists, the deletes and the writes honour it fully.
- Not done: the `list_metrics` view is by name, so UUID entries do not show there.

Part 2 (conversion of the type) is not started, as the task says.

## Part 2 outcome (11 November 2026): done, as the maintainer approved it

- `TypedSamples::convert_to(SensorType)` follows the table (unit tests for every row and the inexact cases: a
  fraction, NaN and infinities, 2^63, beyond 2^53, a float that overflows a `Decimal`, a `Decimal` that is not a
  float, other types).
- **Where**: not before the publishers, as the task suggested, but in `BatchBuilder`, around `storage.publish`. Every
  backend already checks the type where it registers the sensor, in the transaction of the write, and now reports
  the stored type in `SensorIdentityMismatch { stored_type }`. The builder converts the sensor's samples to that type
  and tries again (and remembers it for the next batches of the request). No extra read on the happy path, one
  retry on a mismatch, and the check stays where the write is atomic, so it holds across instances. A write that
  goes through `storage.publish` directly is not converted. Needs the storage to give the batch back
  (`Arc::try_unwrap`); if it does not, the original 409 is returned.
- A value that does not convert is `SensorTypeConversion` (409) naming the stored type. Batches are stored one after
  the other, so a bad value in a later batch leaves the earlier ones stored: "all or nothing" holds per batch, as
  every write does (documented).
- Tests: `a_write_of_another_type_is_converted_when_exact_and_refused_otherwise` (generic, passes on SQLite,
  PostgreSQL, TimescaleDB, DuckDB and ClickHouse) and `plain_senml_numbers_go_to_an_integer_series_when_whole`.
