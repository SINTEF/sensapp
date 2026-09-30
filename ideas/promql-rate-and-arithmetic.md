# PromQL `rate()` And Arithmetic

## Status

Deliberately not implemented. `/api/v1/query` accepts selectors and cross-series `sum|avg|min|max|count` only (see `current_tasks/cross-series-aggregation.md`).

## Why Not

Full PromQL means reimplementing Prometheus (staleness, counter resets, vector matching, subqueries). SensApp already supports Prometheus remote read and write, so a real Prometheus in front is the answer for those queries.

## Revisit If

A concrete user needs one specific function, most likely `rate`/`increase` on counters or `series * constant` for unit conversion. Add that single function rather than a general evaluator.
