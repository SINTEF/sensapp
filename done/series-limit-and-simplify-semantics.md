# `GET /series/{uuid}`: `simplify` without `step` silently truncates, and `limit` is undocumented

## Status

Done on 1 October 2026 for problem 1 (option A) and the documentation of `limit`/`simplify`. Problems 2 (`order=desc`, truncation header), 3 and option B moved to `ideas/series-latest-points-and-truncation-signal.md`.

What was done:

- `get_series_data` no longer lets the storage backends simplify. It reads with `simplify: None`, applies the 100 000 sample check, then calls `storage::common::simplify_sensor_data`. A read cut at the cap (default `limit` is cap + 1) is now a 400, for every backend, with or without `step`. An explicit `limit` is at most the cap, so it still works and bounds the rows simplify sees.
- Test: `advanced_series_query_tests::test_series_query_rejects_simplify_over_sample_cap` (100 001 samples: 400; with `step`: 200; narrower window: 200; `limit=1000`: 200). It fails without the change.
- `docs/HTTP_LIMITS.md` and the OpenAPI text for `limit` and `simplify`.
- Decision on the truncation header: not added; `limit` truncation is documented instead.

Original description:

Design and bug fix. Found on 1 October 2026 on a series of 1.3 M samples (SQLite). Follow-up to `done/series-downsampling-and-simplify.md`.

## Problem 1: `simplify` without `step` only sees the oldest 100 001 samples

`get_series_data` (`src/http/crud.rs`) defaults `limit` to `MAX_DIRECT_SAMPLES + 1` (100 001) and the storage layer applies it in SQL (`ORDER BY timestamp_us ASC LIMIT`). `simplify` then runs on those rows and the 100 000 sample check (`validate_direct_sample_count`) is done on the **simplified** result, so it passes.

Observed on the Zeblab alarm series (data from 2024-02-13 to 2026-10-01):

| query | result |
|---|---|
| raw, no `limit` | HTTP 400 "Query exceeds 100000 samples" (correct) |
| `simplify=true&simplify_tolerance=0.001` | 200, 168 rows, **last timestamp 2024-04-24**, no warning |
| `step=1h&aggregation=avg&simplify=true&simplify_tolerance=0.001` | 200, 675 rows over the whole range (correct) |

The raw read is cut off while the client believes it simplified the whole series. This breaks the rule the other limits follow: never return a silently truncated result.

### Options

- **A. Reject (recommended first step).** Check the sample count before simplify: if the raw read returned more than `MAX_DIRECT_SAMPLES` rows, answer 400 with the same message as a plain raw fetch, pointing at `step`/`aggregation` or a narrower `start`/`end`. Small change in `get_series_data` / `apply_query_options`, identical for every backend because simplify is a Rust-side step.
- **B. Let simplify scan more than the response cap.** The 100 000 limit protects the response size, not memory. Simplify exists to reduce big raw series, so it could read up to a separate, larger scan budget (millions of samples) and only require the output to be under 100 000. Needs a measurement of memory and time on a 1 M+ series (Douglas-Peucker is O(n log n) typical, O(n²) worst case; `simplify_indices_f64` also does a linear `position` search per kept point) and a decision on the budget. Do this only if A turns out to be too restrictive in practice.

### Plan

1. Implement A with a test per backend in the generic backend query tests: more than 100 000 raw samples plus `simplify=true` returns 400, and a window under the cap still works.
2. Add the same check to `limit` combined with `simplify`: `limit` bounds the raw rows read before simplification, which should be stated in the docs.
3. Revisit B with numbers.

## Problem 2: `limit` returns the oldest N samples

This is the right default (it matches SQL and InfluxQL: ascending time, then cut), and the SQL does exactly that. What is missing:

- The OpenAPI text only says "Maximum number of samples (at most 100,000)". Say "the oldest N samples in the window, in time order".
- There is no way to ask for the **latest** N samples. `GET /series/{uuid}/last` returns one sample. Consider `order=desc` (still return samples in ascending time order, or document otherwise) so a client can fetch "the last 1 000 points" without computing a window. Cheap with the existing index on `(sensor_id, timestamp_us)`.
- An explicit `limit` truncates silently, unlike the default cap. Either document it, or add a way for the client to tell, for example an `X-SensApp-Truncated: true` header. Decide when working on this.

## Problem 3: no "about N points" option

A client that wants a plot of a window has to pick `step` itself (window / number of points). A `max_points` parameter that picks `step` server-side would remove that arithmetic. Not urgent: `step` + `aggregation` already covers it. Record the idea here and decide after problems 1 and 2.

## Documentation

- `docs/HTTP_LIMITS.md`: add that `simplify` and `limit` are applied to the raw read in the order described above.
- OpenAPI descriptions in `src/http/crud.rs` for `limit`, `simplify`, `simplify_tolerance`.

## Validation

- `cargo test --test query_export advanced_series_query_tests`
- `cargo test --test advanced_backend_queries --features "sqlite timescaledb clickhouse"`
- `cargo test --test advanced_backend_queries --features "duckdb"`
- Re-run the Zeblab queries (1.3 M samples) and check the 400 and the window cases.

## Related

- `current_tasks/arrow-export-timestamp-offset.md`
- `current_tasks/python-sdk-boolean-query-params.md`
