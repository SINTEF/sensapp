# Label regex matchers are anchored, like in Prometheus

## Problem (found 1 Oct 2026, measuring selectors on PostgreSQL)

`{__name__="cpu usage",core=~"c7"}` matched 111 series (`c7`, `c70`..`c79`, `c700`..`c799`, ...). In
Prometheus a regex matcher is fully anchored: it matches only the series whose label is exactly `c7`.
Every backend used the unanchored regex operator of its database (`~`, `match()`, `regexp`, ...), and the
in-memory series listing used `Regex::is_match`.

## Fix (2 Oct 2026)

`LabelMatcher::new` (`src/storage/query.rs`) anchors the pattern of a regex matcher, `^(?:pattern)$`,
and every entry point builds its matchers through it: the PromQL parser of `/api/v1/query`, the
selector of `/series`, and the Prometheus remote read conversion. The backends are untouched: they receive
the anchored pattern, so none can forget, and the in-memory listing of `crud.rs` matches the same way.

- Leading flag groups stay in front of the anchors, `(?i)abc` becomes `(?i)^(?:abc)$`. PostgreSQL only
  accepts embedded flags at the very start of a regex (`^(?:(?i)c7)$` is "invalid regular expression:
  quantifier operand invalid", checked), and `(?i)` is the usual PromQL idiom for case-insensitive matching.
- `Display` of a matcher shows the pattern that is evaluated.
- Documented in `docs/DATAMODEL.md` (Label Selectors), with the behaviour change.

## Tests

- Unit tests (`src/storage/query.rs`): anchoring, literals left alone, flag hoisting, the remote read conversion.
- `tests/integration/regex_matchers.rs`, backend-generic, passing on SQLite, PostgreSQL, TimescaleDB
  and ClickHouse: exact match (`c7` is not `c70`, `c700`, `xc7` nor `C7`), wildcards, a single `.`,
  alternation anchored as a whole, the empty pattern, negated patterns excluding exact matches only,
  name regexes (`rx_cpu` is not `rx_cpu_usage`), `(?i)`, and the HTTP selectors (`/series?selector=`).
  Each case is checked through `query_sensors_by_labels` and `query_selector`, which must agree.
  Run against the code without the fix, the five tests fail with the reported symptom.

## Not done

- DuckDB, BigQuery and RRDCached receive the anchored pattern too, but their regex engines
  were not run here.
- The Prometheus rule that a missing label is an empty string (`foo=~".*"` and `foo=""` select the series
  without `foo`) is not implemented on the SQL backends: a series without the label is not selected.
  Not checked in detail, worth its own test if someone relies on it.
