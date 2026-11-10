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
