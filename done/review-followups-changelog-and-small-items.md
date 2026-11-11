# Review follow-ups: changelog, docs and small items

Source: review of pull request 60 on 11 November 2026 (HEAD `7d8b396`). Each item was read in the code; none
was executed.

## Changelog and docs

The *Unreleased* section misses these user-visible changes:

- **CSV**: offsets are subtracted (stored instants change); a CSV without a time column is a 400; the epoch
  unit comes from the magnitude; a date alone is a time; week and ordinal dates are no longer times;
  duplicate wide headers are a 400.
- **Arrow**: a null timestamp is a 400; null values are dropped; a non-constant `sensor_id` or `sensor_name`
  column is a 400; a metadata type that differs from the column type is a 400; the decompressed-size limit.
- **SenML**: mixed types and sum-only records are a 400; only the hyphenated form is a UUID; `_type` and
  `_labels`; the export writes `bu`, `_type`, microsecond times and no record for an empty series.
- **All writes**: times outside years 1 to 9999 are a 400; NUL and separator bytes are a 400; `+json` is
  read as SenML; remote write parse errors are a 400; handler panics no longer stop the instance.
- **Queries**: regex matchers are validated and restricted.
- **PostgreSQL and TimescaleDB**: `SENSAPP_PG_STATEMENT_TIMEOUT_SECONDS` and
  `SENSAPP_PG_LOCK_TIMEOUT_SECONDS`; dictionaries unique by md5; the new migrations and indexes; the upgrade
  note (see `postgres-upgrade-path-and-lock-errors.md`).

Also: `docs/SENML.md` says times round trip "to the microsecond". That holds for 1978 to 2106 only: a `bt`
under 2^28 is read as relative to now, so an export of 1975 data re-imports around 2031 with a 200, and
after 2106 `bt + t` in `f64` loses the microsecond. Refuse to export a `bt` under 2^28 (or write absolute
`t` values there) and state the range. `done/handler-panics-kill-the-instance.md:77` still says DuckDB uses
a tokio mutex.

## Small code items

1. **Arrow import refuses SensApp's own export of a Numeric of about 7.9e10 or more**
   (`src/importers/arrow.rs:502-505`): the export writes `Decimal128(38, 18)`, and
   `try_from_i128_with_scale` refuses a raw value over 96 bits without stripping trailing zeros. Divide the
   raw value by 10 while it is divisible and the scale is positive, then convert. The round-trip test only
   uses 12.34.
2. **A CSV time cell with trailing whitespace is no longer a time** (`src/infer/parsing.rs:162-165`): chrono
   rejects trailing characters. `main` accepted it. `trim_end()` the cell.
3. **SQLite Numeric aggregation reads every row of the window with no cap**
   (`src/storage/sqlite/storage.rs:1662-1666`, `Some(usize::MAX)`). Apply `DEFAULT_QUERY_LIMIT` and return
   an error above it, and document it.
4. **Tie-break on equal timestamps is missing for String reads** (`src/storage/duckdb/mod.rs:684`,
   `src/storage/sqlite/storage.rs:1789`, and `src/storage/sqlite/batch_queries.rs`), while the changelog
   says raw reads follow write order. Add `rowid ASC` and a String case to the test.
5. **DuckDB `delete_series` commits twice** (`src/storage/duckdb/mod.rs:228-238`); readers on cloned
   connections can now see a series with no labels and no samples, and a failure of the second statement
   leaves that state for good. Try one transaction (the bundled DuckDB 2.0 may now allow it); otherwise
   delete the sensor row first.
6. **SQLite Numeric rows stored before this version are not normalised**, so `1.0` (old) and `1` (new) are
   not duplicates. Add a migration that rewrites `numeric_values.value`, or say "new values only" in the
   changelog.
7. **DuckDB reads are unbounded** (`ReaderPool::get`, `src/storage/duckdb/mod.rs:67-79`): only the idle list
   is capped. Bound concurrent readers with a semaphore.
8. **The SQLite read pool is not read-only** (`src/storage/sqlite/storage.rs:68`). Open it with
   `read_only(true)` so a future write through it fails loudly.
9. **One bad remote-write series drops its whole batch** (`src/http/prometheus_write.rs:197`): a label value
   with 0x1F is a 400 for the request, so Prometheus drops the other series of that batch at every send.
   Skip the invalid series, store the rest, and count the skipped ones in a metric.
10. **Duplicate `__name__` labels in remote write**: the last one silently wins
    (`prometheus_write.rs:176-178`), while other duplicate labels are a 400. Refuse it too.
11. **RRDcached readiness fails when the one connection is busy for more than 3 s**
    (`src/storage/rrdcached/mod.rs:909-921`): a bulk load takes the instance out of rotation. Report "busy"
    as ready, and "unavailable" only on a connection error.
12. **The remote-read `Latest` grid assumes a 5-minute lookback** (`src/http/prometheus_read.rs:137-141`):
    with another `--query.lookback-delta` the check can pass by coincidence. Make the lookback a setting,
    or correct the sentence at `docs/DATAMODEL.md:284`.
13. Tests that assert less than they seem to:
    `jwt_auth::sensor_allow_list_names_are_not_patterns_in_selectors` would not catch a missing escape of
    `|` and has no prefix or suffix decoy; the "chunked responses must be sorted by the server" claim has no
    source and no live test.

## Done when

The changelog and docs are complete, and each code item is fixed with a test or moved to `ideas/` with a
reason.

## Outcome (11 November 2026)

Done: the changelog block (CSV, Arrow, SenML, all writes, queries, PostgreSQL), the `docs/SENML.md` range of the
microsecond claim, the wording of `done/handler-panics-kill-the-instance.md` (DuckDB uses a `std::sync::Mutex` whose
poisoning is recovered, not a tokio mutex). Code items 1 (Arrow decimals of 7.9e10 and above, with trailing zeros
removed; test `test_a_numeric_of_the_export_over_7_9e10_imports_back`), 2 (a CSV time cell with trailing whitespace;
test `a_time_cell_with_trailing_whitespace_is_a_time`) and 10 (two `__name__` labels are a 400; test
`test_prometheus_write_refuses_a_series_with_two_names`). The other code items, the SenML `bt` export and the weak
tests are in `ideas/pr-60-review-small-items-left.md` with the reason for each.
