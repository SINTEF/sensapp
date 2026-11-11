# SenML import and export consistency

Source: code review of 10 November 2026. Reported from reading; the behaviour of the `sindit-senml` crate was
only partly read. The two timestamp bugs of the SenML paths are in `integer-timestamp-conversions.md`, the
negative-time panic in `handler-panics-kill-the-instance.md`.

## Problem

Import (`src/importers/senml.rs`):

1. **Records that do not match the first record's value type are stored as 0.0, "" or false**
   (`:42`, `:155-173`, `unwrap_or(0.0)` and similar). `[{"n":"door","v":1},{"n":"door","vb":true}]` stores
   1.0 and 0.0. A record with only a sum (`"s"`) is stored as 0.0.
2. **Numbers are always Float** (`:45-75` handle Float, String, Boolean and Blob only).
3. **UUID detection uses `Uuid::parse_str`**, which accepts 32-hex, braced and `urn:uuid:` forms: a device id
   of 32 hex characters becomes the series UUID.
4. `exported_sensor_names` parses the whole body a second time into `serde_json::Value` (`:113`).

Export (`src/exporters/senml.rs`):

5. **`bu` leaks to the next series** in a multi-series document (`:131-133`, `:151-154`): base fields are
   sticky in RFC 8428, so a series without a unit after one with `Cel` inherits `Cel`.
6. **An empty series is exported as a record without a value** (`{bn, _name, bt: 0, bver}`), which the
   importer turns into a Float sample 0.0 at 1970-01-01 (`importers/senml.rs:139-148`).
7. **Round-trip losses**: labels are dropped, Integer comes back as Float, Numeric goes through f64, a
   location becomes two series.

## Suggested solution

- Import: a record whose value field does not match the series type is a 400 that names the record. A record
  with no value field is skipped when it only carries base fields, and is a 400 otherwise. Sum-only records
  are a 400 (not supported).
- Only the canonical hyphenated UUID form is treated as a UUID.
- Export: write `bu` on every series' first record, with an empty string when the series has no unit, or
  write `u` per record instead of `bu`. Do not emit a record for an empty series (or emit only base fields
  that the importer skips, per the rule above).
- Round trip: state in `docs/` what SenML preserves. With `sensor-identity-check-on-write.md` in place, an
  Integer series exported and imported again must not be refused as a type mismatch: when the UUID exists,
  convert the incoming Float values to the stored type when lossless, or export the type in an extension
  field (`_type`) next to `_name` and honour it on import. The extension field is the simple, explicit fix.

## Tests

Mixed-type records (400), a sum-only record (400), a multi-series export with and without units parsed by
the importer, an empty series exported then imported (no sample), an Integer series exported then imported
(same series, same values).

## Done when

The tests pass and the SenML section of the docs lists what a round trip keeps.

## Outcome (10 November 2026)

Every finding was right; each got a failing test first.

Import (`src/importers/senml.rs`, rewritten around one pass over the parsed records and their raw JSON,
which `sindit-senml` resolves one for one):

1. **Mixed value types** are refused (`400`), naming the series and the time of the record. **A record
   without a value** (only base fields) is no sample; `sindit-senml` gives it the value 0, so it is
   recognised on the raw record (no `v`, `vs`, `vb`, `vd`). **A sum without a value** is refused.
2. **Types**: a `_type` extension field gives the type; Integer reads the number as written (exact for
   every `i64`, or a whole float within ±2^53), Numeric reads it as written into a `Decimal`, JSON parses
   `vs`. Without `_type`, the value field decides, as before. `_type: location` is refused.
3. **Only the hyphenated UUID form** is a series UUID.
4. Already done in task 3 (the raw records are parsed once).
- Also: `_labels` is imported (the export wrote it, the import dropped it), an empty `bu` is no unit, and
  the series come back in document order instead of `HashMap` order.

Export (`src/exporters/senml.rs`):

5. **`bu` on every series**, empty when the series has no unit.
6. **An empty series writes no record.** `bver` stays on the first record of the document even when the
   first series is empty.
7. **`_type`** on the base record of every series but Location (its first record is the `lat` series).
   Location still exports as two Float series; Numeric goes through a JSON number (an `f64`).

Tests: unit `test_records_of_another_value_type_are_refused`, `test_a_sum_without_a_value_is_refused`,
`test_a_record_without_a_value_is_no_sample`, `test_only_a_hyphenated_uuid_is_a_uuid`,
`test_export_then_import_keeps_types_units_and_labels` (Integer 2^60 + 1, Numeric, JSON, Float, Boolean,
with and without units, with labels, one document), `test_an_empty_series_exports_nothing`; over HTTP
`identity_over_http::an_integer_senml_export_imported_again_goes_to_its_series` (was `409`).

`docs/SENML.md` (new) lists the import rules, the extension fields and what a round trip keeps;
`docs/DATA_LIFECYCLE.md` no longer says SenML has no labels.

One existing test passed only because of finding 6:
`simple_promql::test_simple_promql_hyphenated_metric_name_via_name_matcher` read a 5 minute window over
samples 28 to 30 minutes old, and the empty series came back as one SenML record without a value. Its window
is now 1 hour.

Suites, all green: SQLite full (302 unit, 342 integration) and PostgreSQL full (308, 343), clippy
`--all-features --tests`. SenML is the same for every backend, so other backends were not run.
