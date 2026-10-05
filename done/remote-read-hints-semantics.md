# Prometheus remote read hints: only aggregate when Prometheus gets the right answer

## Question

When Prometheus asks for `avg_over_time(x[1h])` over a year with a one hour step, does SensApp send all the
samples, or aggregate in the database?

## Findings (Prometheus v3.8.0, SensApp on PostgreSQL, 6 hours of one sample a minute)

SensApp aggregated in the database (one bucket per `step`, bulk on every SQL backend and BigQuery). But
Prometheus evaluates the query again on what it receives, and the buckets only gave the right answer in
some cases. The buckets start at `query.start_timestamp_ms`, which Prometheus 3 sets to the start of the
first window (`t - range + 1 ms`), so for a range equal to the step a bucket is exactly one window: the
shift of one bucket I feared does not happen.

| Query (step 1h unless said) | Before | Why |
| --- | --- | --- |
| `avg/min/max/sum/last_over_time(x[1h])` | right | one bucket per window |
| `count_over_time(x[1h])` | 1 instead of 60 | Prometheus counts the buckets |
| `avg_over_time(x[30m])`, `max_over_time(x[10m])` at 15m | wrong | the bucket is wider than the window (`range_ms` < `step_ms`) |
| `sum(x)`, `avg(x)`, `min(x)`, `max(x)` across series | `sum` 36004 instead of 576 | `func` is the operator, `range_ms` is 0: Prometheus wants the value at each step, not the aggregate of the following hour |
| `avg_over_time(x[2h])` | right when the buckets are full | average of averages: a compromise, kept |

## Done

- [x] `aggregation_from_read_hints` only maps the mergeable `*_over_time` functions (no `count`, no bare operator names).
- [x] `query_options_from_read_hints` requires `range_ms` to be a positive multiple of `step_ms`; otherwise raw samples.
- [x] Unit tests of both rules; the existing backend-generic hint test (range = step) still passes.
- [x] `tests/prometheus_live/test_live.py::test_remote_read_hints`: 20 queries through a real Prometheus compared with the raw samples (it
  failed on 8 of them before the change). `run.sh` now takes `PROMETHEUS_IMAGE` and defaults to v3.8.0.
- [x] Documented in `docs/DATAMODEL.md`.

## Not checked

- Prometheus 2.x (the harness used to pin v2.55.1, not pulled locally): its windows include their start, so a bucket per step probably does not
  fit. If 2.x matters, run `PROMETHEUS_IMAGE=prom/prometheus:v2.55.1 bash tests/prometheus_live/run.sh`.
- A big range at a small step with raw fallback is now an error above the sample limits (HTTP 400, "narrow the selector") instead of a wrong answer.
