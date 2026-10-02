# A missing label should behave as an empty string, like in Prometheus

Checked 2 October 2026 on SQLite (a throwaway test, not kept). PostgreSQL, TimescaleDB and ClickHouse
build the same `IN` / `NOT IN` subqueries on the labels table, and the in-memory listing of
`src/http/crud.rs` (`sensor_matches_matchers`) has the same rule; they were not run.

For a label `zone` that one of three series lacks:

| selector | SensApp | Prometheus |
| --- | --- | --- |
| `zone=""` | nothing | the series without `zone` |
| `zone!=""` | all three | only the series with a non-empty `zone` |
| `zone=~".*"`, `zone=~"a\|"` | only the series with `zone` | also the series without `zone` |
| `zone!~".*"` | the series without `zone` | nothing |

`=`, `!=`, `=~".+"`, `!~".+"` with a non-empty value already agree. Documented in `docs/DATAMODEL.md`
("Known differences from Prometheus").

Why it matters: `label!=""` is the idiom for "has this label" and `label=""` for "lacks it", and Grafana's
"All" option on a variable can become `.*`. The wrong answers are silent. Real exposure is small while
the series of a metric share the same label names, as Influx and SenML data usually do.

## Fix idea

Decide once, in `LabelMatcher`, whether a matcher selects the empty string (`=""`, `!=` with a
non-empty value, a regex whose anchored pattern matches `""`, ...). Every backend then builds its
condition as: series that have a non-empty label matching the value, plus the series without a non-empty
label when the matcher selects the empty string. An explicitly empty label value counts as missing,
as in Prometheus. About half a day: the five SQL backends and the in-memory listing, with a
backend-generic test of the table above.

DuckDB, BigQuery and RRDCached should be checked in the same pass.
