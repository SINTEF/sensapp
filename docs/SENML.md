# SenML

`POST /publish` with `Content-Type: application/senml+json` reads SenML JSON ([RFC 8428](https://www.rfc-editor.org/rfc/rfc8428)),
and the SenML export (`format=senml`) writes it. A document that cannot be read answers `400` with the reason.

## Import

- Records are grouped into series by their resolved name (`bn` + `n`).
- **The type of a series** is its `_type` field when given (see below), or else the value field of its first
  record: `v` is a Float series, `vs` String, `vb` Boolean, `vd` Blob. Every record of the series must
  have a value of that type; a record that does not is refused with `400`, naming the series and the time.
- **A record without a value** (only base fields, such as `bn` and `bt`) is no sample. A record with a sum
  (`s`) and no value is refused: sums are not supported.
- **A name that is a hyphenated UUID** (`3f2a…-…`, as the export writes `bn`) is the UUID of the series, and
  `_name` gives its name. Any other name (32 hex digits too) is the name of the series, and its UUID is
  derived from the name, type, unit and labels.
- `t` and `bt` are seconds, read to the microsecond. Times must be within years 1 to 9999, as for every write.

### SensApp extension fields

On any record of a series, usually its first:

| Field | Meaning |
| --- | --- |
| `_name` | The name of a series whose base name is its UUID |
| `_type` | `integer`, `numeric`, `float`, `string`, `boolean`, `json` or `blob`. `integer` and `numeric` read `v`, `json` reads `vs` as JSON text. An `integer` must be a whole number: written as an integer, or a float within ±2^53. `location` is refused |
| `_labels` | An object of text labels |

An empty `bu` means no unit (the export writes it for a series without a unit, because `bu` carries over to
the next records).

## Export

One base record per series carries `bn` (the UUID), `_name`, `bt`, `bu`, `_type` and `_labels`; `bver` is on
the first record of the document. An empty series writes no record.

## What a round trip keeps

An export imported again goes to the same series (by its UUID), with:

| | Kept |
| --- | --- |
| UUID, name, unit, labels | yes |
| Integer, String, Boolean, JSON, Blob values | exactly |
| Float values | exactly |
| Numeric values | through a JSON number, a `f64`: about 15 significant digits |
| Times | to the microsecond |
| Location | **no**: exported as two Float series, `<uuid>lat` and `<uuid>lon` |
