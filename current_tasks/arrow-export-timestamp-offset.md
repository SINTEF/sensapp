# Arrow export returns timestamps shifted by the leap-second offset

## Status

Bug, highest priority of the series-query follow-ups. Found on 1 October 2026 while loading a real 2024-2026 dataset.

## Symptom

`format=arrow` returns every timestamp 37 s too late. The same series in `csv`, `jsonl` and `senml` is correct, and so is the SQLite file.

Reproduction: write `m value=1 1707815726000000` with `precision=us` (2024-02-13T09:15:26Z), then:

- `GET /series/{uuid}?format=csv` gives `2024-02-13T09:15:26+00:00`
- `GET /series/{uuid}?format=arrow` gives `2024-02-13 09:16:03`

Aggregated buckets look shifted too (`step=1d` buckets start at `00:00:37`) because they go through the same exporter. The buckets themselves are aligned correctly in the database.

## Cause

`src/exporters/arrow/mod.rs`, `ToMicroseconds::to_microseconds_since_epoch`:

```rust
(self.to_duration_since_j1900().total_nanoseconds() / 1000 - 2_208_988_800_000_000) as i64
```

`to_duration_since_j1900()` is a TAI-based duration, so it includes the leap seconds accumulated since 1972. `2_208_988_800` is the plain 1900 to 1970 offset in UTC seconds. The difference is TAI - UTC at the sample's date, 37 s since 2017. Everything else in SensApp converts with `to_unix(Unit::Microsecond)` (`datetime_to_micros` in `src/storage/common.rs`, `sensapp_datetime_to_offset_datetime`), which is correct.

Both Arrow paths use the trait: `to_arrow_stream` (`/series/{uuid}`) and `to_arrow_stream_multi` (`/api/v1/query`). The Python SDK reads Arrow for both, so every SDK user sees the shift.

Stored data is fine, so no migration is needed. Only the export is wrong.

## Why the tests missed it

No Arrow exporter test asserts a timestamp value. `test_data_helpers` builds samples from `SensAppDateTime::now()` and the tests only check schema, row counts and column names. The Python SDK tests use mocked responses.

## Plan

1. Delete the `ToMicroseconds` trait and call `datetime_to_micros` (drop its `#[allow(dead_code)]`), so there is one conversion in the codebase.
2. Add exporter unit tests with fixed timestamps: `from_unix_microseconds_i64(1_707_815_726_000_000)` must come out as `1_707_815_726_000_000` in both `to_record_batch` and the multi-series builder. Also cover a date before 2017 (TAI - UTC was different then) and one with sub-second microseconds.
3. Add an integration test that goes through the real stack: write with a known timestamp (Influx line protocol), read back as Arrow, compare to the CSV timestamp. Run it on every backend the integration suite covers.
4. Add a Python SDK test against a live server (or at least a recorded real Arrow response) that checks a decoded timestamp, so mocked tests are not the only coverage.
5. After the fix, switch `zeblab_alarm.ipynb` in `sensapp-quickstart-test` back to the SDK (`SensAppClient.get_series`), and look for any workaround or hand-written offset in `python/` and the roleplay notebooks.

## Validation

- `cargo test exporters::arrow`, the integration test above, `cargo clippy --tests`.
- Re-run the 1 October 2026 reproduction and compare Arrow and CSV timestamps on the Zeblab dataset (1.3 M samples).

## Related

- `current_tasks/python-sdk-boolean-query-params.md`
- `current_tasks/series-limit-and-simplify-semantics.md`
