# Selector and PromQL semantics on the HTTP endpoints

Source: code review of 10 November 2026. Items 1 to 3 were confirmed by reading.

## Problem

1. **`/series?selector=` drops the metric name and does not implement `__name__`**
   (`src/http/crud.rs:215-258` never uses `vs.name`; `sensor_matches_matchers` at `:261-301` looks `__name__`
   up in the labels).
   - `selector={__name__="temperature"}` returns nothing.
   - `{__name__!="x"}` returns everything.
   - `temperature{room="a"}` returns every series with `room=a`, whatever its name.
   - `docs/DATAMODEL.md` ("Label Selectors") says `__name__` is supported here.
2. **Invalid regex is swallowed** in the same function: `=~` matches nothing and `!~` matches everything
   (`unwrap_or(true)`, `:289`). The regex is compiled again for every sensor and matcher (up to 16,384 per
   page, and again in the metrics scrape). The engine is the Rust `regex` crate here and the database's
   elsewhere.
3. **Remote write stores `__name__` and `unit` as ordinary labels too**
   (`src/http/prometheus_write.rs:145-156`: `labels.push` runs for every label). `build_prometheus_labels`
   (`src/parsing/prometheus/converter.rs:45-56`) then emits `__name__` twice for any series that arrived by
   remote write. The live tests seed through SenML, so they miss it.
4. **`/api/v1/query` ignores `offset` and `@`** (`src/http/simple_promql.rs:120-146`, `:174-177`):
   `temperature[5m] offset 1d` returns current data with a 200.
5. **Scoped catalog scan** (`src/http/crud.rs:571`): a scoped token or a selector with few matches scans the
   whole catalog in 256-row pages; `:609-612` assumes the page is unchanged between two calls.
6. **Restricted tokens load every matching sensor before the series cap**
   (`src/storage/selector.rs:36-41` with `limit: None` at `src/storage/postgresql/mod.rs:934`).

## Suggested solution

1. Push the selector to storage: build `LabelMatcher`s including `__name__` from the metric name and call the
   existing `query_sensors_by_labels` path, as `/api/v1/query` does. That removes `sensor_matches_matchers`,
   gives one regex engine, and removes the catalog scan for unscoped tokens. If the in-memory filter must
   stay for scoped tokens, make it handle `__name__` against `sensor.name` and compile each regex once.
2. Validate regexes when the matchers are built and answer 400 (see
   `postgres-regex-validation-and-statement-timeout.md`).
3. Do not push `__name__` into the labels in `prometheus_write.rs`. Decide for `unit`: suggested, keep it as
   a label only (Prometheus label sets must round trip) and stop treating it as the sensor unit, or keep both
   and skip it when building the labels on read. Note that the labels are hashed into the sensor UUID, so
   this changes the identity of series already written by remote write; breaking changes are allowed, say so
   in the changelog. Add a remote write then remote read live test.
4. Answer 400 for `offset` and `@` until they are implemented.
5. and 6. Pass the allow list down as a name filter (it is a list of sensor names, so `s.name = ANY($n)`),
   which makes the storage limit usable for scoped tokens.

## Tests

`tests/integration/query_sensors_by_labels.rs` and `simple_promql.rs` styles: each selector form above, an
invalid regex (400), `offset` (400), a remote-write series read back with a single `__name__`.

## Done when

The tests pass on all backends and `docs/DATAMODEL.md` matches the behaviour.
