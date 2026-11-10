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

## Outcome (10 November 2026)

Each item was reproduced by a failing test first. The exception is the halving page loop of item 5,
which `crud_dcat_api::test_series_selector_paginates_matching_series` exercises.

1. **The metric name in selectors.**
   - `parse_selector_to_matchers` turns the metric name into a `__name__=` matcher.
   - A new `SensorFilter` (`crud.rs`) replaces `sensor_matches_matchers`. It compares `__name__` with
     the sensor name, compiles each regex once (the value is already anchored), and gives the matchers
     the meaning the backends give them: a negative matcher matches a sensor without the label.
   - `/series` and the latest-sample scrape both use it.
   - Before the fix, `{__name__="temperature"}` listed nothing.
   - Decision: the filter stays in memory, because `query_sensors_by_labels` has no pagination and
     `/series` pages with bookmarks. Pushing it to storage would need a paginated selector read on
     every backend.
2. **Invalid regexes.** They already answered 400 since task 12 (`LabelMatcher::try_new`). The in-memory
   filter no longer swallows a compile error, and the new test covers `=~` and `!~` on both endpoints.
3. **`__name__` stored as a label by remote write.** Remote write no longer stores it. Remote read
   returned it twice before the fix. `build_prometheus_labels` also skips a stored `__name__`, so the
   series written before this version are read with one name.
   - Decision for `unit`: it stays both a label and the unit of the sensor. The label set then round
     trips, and the read does not add it twice.
   - Breaking: a remote-write series written again becomes a new series (CHANGELOG, Unreleased).
4. **`offset` and `@`.** They answer 400, in `/api/v1/query` and in the selectors of `/series` and
   `/prometheus/metrics`. Before the fix, they were ignored with a 200.
5. **Filtered catalog pages.**
   - A filtered page no longer reads a prefix of the storage again to find its bookmark. That second
     read assumed that nothing had changed in between: a series deleted between the two reads shifted
     the bookmark and skipped a series.
   - Each page now ends where a storage page ends. A storage page that holds more matches than the page
     still lacks is read again, half as long.
   - Decision: a scoped token still lists the catalog page by page. The allow list is a list of exact
     names, and `list_series` takes one metric name.
6. **Selector reads with a restricted token.** `AuthorizedStorage` no longer discovers every matching
   sensor and then reads them one by one. It adds a `__name__=~` matcher of the allowed names, escaped
   with `regex::escape`, and calls the backend's own `query_selector` and `query_selector_aggregated`.
   The series and sample limits therefore apply in the database.
   - Test: `jwt_auth::sensor_allow_list_names_are_not_patterns_in_selectors`. The names hold `.` and
     `|`, next to a decoy (`cpuXusage`) that an unescaped name would match.

**Suites run.**
- Full: SQLite and PostgreSQL (358 integration tests each).
- TimescaleDB, DuckDB and ClickHouse: `selector_semantics`, `jwt_auth`, `crud_dcat`, `remote_read`,
  `simple_promql`, `prometheus_metrics` and the `selector_*` tests (105 each).
- BigQuery was not run live. Its `query_selector` now receives two `__name__` matchers when a token is
  restricted.
- `cargo clippy --all-features --tests` is clean.
