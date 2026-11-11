# NaN and nulls on import: one rule, and no silent zeros

Source: maintainer feedback and review of pull request 60, 11 November 2026 (HEAD `7d8b396`).

## What the pull request does today

- **NaN** is refused with a 400 in `BatchBuilder::add` (`src/datamodel/batch_builder.rs:36-43`, `:89`), for
  Float values and both Location coordinates, on every importer. Prometheus remote write drops its NaN
  samples before that.
- **Arrow nulls**: a null value is dropped, a null timestamp is a 400.
- **CSV**: a `NaN` or `inf` cell is not a number for `parse_float` (`src/infer/parsing.rs:36-38`), so the
  whole column becomes a String series. `docs/DATAMODEL.md` says a NaN is refused with a 400; for CSV it is
  not.
- **Infinities** are stored.

So the same meaning, "no reading here", is dropped (Arrow null), refused (Arrow NaN) or turns the column into
strings (CSV NaN), depending on how the client spelled it. NaN is the usual missing-value marker of pandas,
polars and numpy, so one gap fails a whole upload.

## Decided direction

Not storing NaN is right. Refusing the whole request is too strict where NaN is the conventional gap marker.

- **Arrow and CSV**: a NaN is "no sample", like a null: skipped, and counted.
- **CSV**: an empty cell, `NaN`, `nan`, `null`, `NA` and `N/A` in an otherwise numeric column are missing
  values, skipped and counted; they no longer turn the column into strings. `inf`, `+inf`, `-inf` and
  `Infinity` parse as infinities.
- **SenML and InfluxDB line protocol**: keep the 400. JSON cannot carry NaN, and line protocol does not
  accept it, so a NaN there is a client error.
- **Prometheus remote write**: unchanged (dropped; these are stale markers). Count them in a metric.
- **Report the skips**: the write answer carries the number of skipped samples (a JSON field or a response
  header such as `X-SensApp-Skipped-Samples`), and a counter metric is incremented, so nothing is silent.
- **Location**: a NaN coordinate skips the sample; an infinite coordinate is a 400 (only NaN is checked
  today, `batch_builder.rs:41-43`).

## Related defects found in the review

1. **SenML `"v": null` is stored as 0.0** (`src/importers/senml.rs:73`): `raw.get(field).is_some()` is true
   for a JSON null, and the crate then defaults the value. Same for `"vs": null` and the others. Earlier
   SensApp exports wrote exactly this shape for NaN and infinities. Fix: `is_some_and(|v| !v.is_null())`,
   and treat the record as having no value.
2. **SQLite `sum` and `avg` over `+Inf` and `-Inf` return 0** (`src/storage/sqlite/storage_query_helpers.rs`
   `:86-91`): SQLite turns the NaN result into NULL, and sqlx decodes NULL into a non-optional `f64` as 0.0.
   PostgreSQL and DuckDB return NaN, as `docs/DATAMODEL.md:232-233` says. Decode as `Option<f64>` and map
   NULL to NaN. The test `tests/integration/non_finite_values.rs:160` only covers `min`, `max` and `count`.
3. **A float series holding only infinities exports a SenML base record without a value**
   (`src/exporters/senml.rs:169-172`), which other SenML readers may read as 0. Emit no record.
4. A SenML record with `bv` and no `v` is dropped as "no sample" although its resolved value is `bv`.

## Tests

- Arrow: a Float64 column with NaN and nulls: the other samples are stored, the answer reports the count.
- CSV: a numeric column with empty cells, `NaN` and `NA`: it stays numeric, gaps are skipped and counted;
  `inf` is stored as infinity.
- SenML and InfluxDB: NaN-like input is a 400; `"v": null` stores nothing.
- Generic storage test: `sum` and `avg` over `-Inf, 1.5, 2.5, +Inf` is NaN on every backend.

## Done when

The tests pass and `docs/DATAMODEL.md`, `docs/CSV_IMPORT.md` and the changelog state one rule per format.

## Outcome (11 November 2026)

Done as decided, with the measures below.

- **Arrow**: `present_values` drops nulls, NaN and locations with a null or NaN coordinate; the rows dropped are
  counted (`parse_arrow_sensors` returns `(maps, skipped)`). Unit test `test_nan_values_are_skipped_and_counted`.
- **CSV**: an empty cell, `NaN`, `null`, `NA`, `N/A` (any case) in an otherwise numeric column is skipped and counted,
  and the column stays Integer or Float. The gap stands in as `0` while the column is typed, and if what remains is
  not numeric the cells are put back and the column is text as before. A missing time is an error with its row.
  `inf`, `+inf`, `-inf`, `Infinity` parse as infinities (nom's `double` only reads the unsigned ones). The all-gap
  column is left as it was (text). Unit tests in `importers/csv.rs` and `infer/parsing.rs`; the old
  `test_special_numeric_values` pinned `inf` as text and changed.
- **Report**: `IngestionStats.skipped`; `POST /publish` answers with `X-SensApp-Skipped-Samples` (always, 0 when none)
  and counts `sensapp_samples_processed_total{kind="skipped",operation="native_publish"}`. Integration test
  `csv_gaps_are_skipped_and_reported`. OpenAPI document regenerated.
- **SenML and InfluxDB**: unchanged, a NaN is a 400 (`nan_is_refused` covers the storage rule).
- **Prometheus remote write**: unchanged, and counted in `sensapp_prometheus_remote_write_dropped_total{part="nan"}`
  (extended `test_prometheus_write_counts_what_it_drops`).
- **Location**: a NaN coordinate is skipped on Arrow; an infinite one is a 400 in `BatchBuilder` (case added to
  `nan_is_refused`).
- **Related defects**: (1) SenML `"v": null` (and `vs`, `vb`, `vd`) is no value: test
  `test_a_null_value_is_no_sample`. (2) SQLite `sum` and `avg` of `+Inf` and `-Inf` returned 0.0 (reproduced by
  extending `infinities_are_stored_and_exported_as_text` with `sum` and `avg`): both decode paths read an
  `Option<f64>` and map NULL to NaN; the test passes on SQLite, PostgreSQL, TimescaleDB, DuckDB and ClickHouse.
  (3) A Float series of only infinities exports no SenML record: test
  `a_float_series_of_infinities_has_no_record`. (4) **Not changed**: a record with `bv` and no `v` stays "no
  sample": RFC 8428 adds the base value to the value found in the record, and there is none, so I read the finding
  as wrong; say so if the maintainer reads the RFC otherwise.
