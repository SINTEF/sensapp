# CSV type inference splits a series across uploads

The type of a series is part of its identity: the UUID is derived from the name, the type, the unit and the labels
(`docs/DATAMODEL.md`). A CSV upload infers the type of each value column from all its cells
(`docs/CSV_IMPORT.md`). So the same column sent in parts can land in several series:

- `temp` holds `21,22` in one upload and `21.5` in the next: an integer series and a float series, both named
  `temp`.
- One upload has an empty cell in `temp`: the column is text, and a third `temp` series, of strings, appears.

Nothing is lost, but a dashboard that reads `temp` sees one part of the data, and the UI lists the name twice
with a Type column. Raised by the review of November 2026 (task "auth, config and deployment hardening",
item 24); no code change was made.

## Options

1. **An explicit type**: `POST /sensors/publish?type=float`, or `?types=temp:float,state:string`, that every value
   column (or the named ones) is parsed as; a cell that does not parse is a `400` naming the row and column. The
   simplest, and the one a script that uploads every day should use.
2. **Integers as floats**: infer `float` for a column of integers, as InfluxDB does for fields without `i`. Fixes the
   first case only, and changes the type of existing integer series (a new UUID for them).
3. **Empty cells are no sample**, not text, as the Arrow import does with nulls. Fixes the second case; an empty
   cell then cannot be stored as an empty string.
4. **A warning in the response**: the answer already counts series and samples; it could name the series that were
   created by this upload while a series of the same name and another type exists. Cheap, and it makes the split
   visible.

1 and 3 together, with 4 as a safety net, look like the best mix.
