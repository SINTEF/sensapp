# Auth, configuration and deployment hardening

Source: code review of 10 November 2026. Low-severity items and near misses, reported from reading. Each is
small; they can go in one branch, one commit per item.

## Auth

1. **Scoped delete fails open** (`src/http/authorized_storage.rs:37-48`):
   `Ok(sensor_data.is_none_or(|data| self.allows(&data.sensor.name)))`. Correct only while every backend
   returns `Some` for an existing series with no sample in the `[0,0]` window. Use `is_some_and`: an unknown
   UUID then gives `false`, which callers already answer as 404.
2. **An admin token ignores its own `sensors` claim** (`src/http/server.rs:625-651`). Refuse `sensors`
   together with the `admin` scope when generating and when validating a token.
3. **`kid` collision** (`src/http/auth.rs:132`): the `kid` is 32 bits, and a previous secret colliding with
   the current one overwrites it in the map. Refuse to start in that case.
4. **Disabled-auth mode accepts cross-origin writes**: with `SENSAPP_AUTH_DISABLED`, a web page can send a
   `text/plain` POST to `/publish` or a bodiless POST to `/api/v1/admin/vacuum` without a preflight. In
   disabled mode, reject requests whose `Origin` header is present and not same-origin, and check `Host`
   against the listen address to stop DNS rebinding.
5. **`/docs` loads an unpinned script from a CDN** (`src/http/server.rs:196`, utoipa-scalar 0.4.0), on the
   origin where the UI keeps the token in `sessionStorage`. Serve the script from the binary (embed a pinned
   copy) or pin a version with SRI. This also makes `/docs` work on air-gapped installs.
6. **`/health/ready` returns the backend error string unauthenticated** (`src/http/health.rs:64-70`). Return
   a fixed text and log the detail. Check what each backend puts in that error.

## Configuration (`src/config/mod.rs`)

7. Validate at startup: the three timeouts must be > 0; `http_max_query_samples` and
   `token_max_duration_seconds` must be > 0 and below a sane maximum (`remaining + 1` wraps at
   `src/storage/selector.rs:95` for `usize::MAX`); `SENSAPP_HTTP_BODY_LIMIT` is accepted up to 128 GiB
   although bodies are buffered whole, cap it.
8. `settings.toml:30-32`: the comment still says endpoints are open when the secret is unset, which
   contradicts secure-by-default.
9. `src/main.rs:69`: Sentry is initialised with `.debug(true)`.

## Deployment

10. `charts/sensapp/values.yaml` sets `SENSAPP_HTTP_SERVER_TIMEOUT_SECONDS: "30"`, overriding the documented
    120 s default.
11. Helm with SQLite: the default storage is SQLite on `emptyDir` and nothing stops `replicaCount > 1` or
    autoscaling. Fail the template render (`fail`) for that combination.
12. `docker-compose.yml:8`: `POSTGRES_DB: sensapp,sensapp-test` (suspected: the image does not split on
    commas, giving one database with that literal name). Check and fix with an init script.
13. `axum::serve` does not appear to configure a header-read timeout or a connection cap (not verified).
    Check, and set one if missing.

## Performance notes found on the way

14. `#[cached]` on `uuid_v8_blake3` (`src/datamodel/sensor.rs:146-151`): a global lock, a `Vec` clone and a
    1,024-entry cache that thrashes on any Prometheus request, to save two BLAKE3 calls. Remove it and
    measure.
15. Gzip decoding and Arrow/CSV parsing of up to 64 MiB run on async worker threads. Move them to
    `spawn_blocking`.
16. CSV import holds several copies of the file in memory (rows, columns, a clone for inference, inferred
    vectors). Estimate 50 to 100 times the body size for small cells; measure and remove the clones.

## Other small items

17. Multi-member gzip bodies (`src/http/influxdb.rs:45`): `GzDecoder` reads the first member only. Use
    `MultiGzDecoder`.
18. The unit is not checked for separator bytes when hashing the UUID (`src/datamodel/sensor.rs:98-132`),
    unlike name and labels: unit `x\x1ek\x1fv` with no labels hashes like unit `x` with label `k=v`.
19. Influx `series += 1` per field value (`src/http/influxdb.rs:305`): the series metric counts samples.
20. CSV multi export (`src/exporters/csv.rs:140-145`): label keys are written to the header unescaped; a
    label named `type`, `value`, `timestamp`, `sensor_id` or `sensor_name` duplicates a fixed column; a lone
    `\r` does not trigger quoting.
21. Arrow export timestamps have no timezone (`Timestamp(Microsecond, None)`); add `"UTC"`.
22. `get_series_data` does not reject `start > end`, unlike the other handlers.
23. Prometheus exemplars, native histograms and metadata are dropped with a 204. Document it, and count
    them in a metric.
24. Type inference splits a series across CSV uploads because the type is part of the UUID
    (`21,22` then `21.5` gives two `temp` series; one empty cell gives a third, of strings). No code change
    proposed here: add it to `ideas/` with the options (explicit type parameter, warning in the response).

## Done when

Each item is fixed with a test where one is practical, or moved to `ideas/` with a reason.

## Outcome (11 November 2026)

Every finding was turned into a test first where one was practical. Four commits.

### Fixed

1. Scoped delete: `can_see_sensor` uses `is_some_and`. Test `a_restricted_token_deletes_nothing_it_cannot_see`,
   with a storage whose read finds nothing.
2. Admin with sensors: refused by `TokenRequest::new` and by `validate_token` (`401`). Test
   `an_admin_token_cannot_list_sensors`.
3. `kid` collision: `with_previous_secrets` refuses a secret whose key id is taken. Test
   `a_previous_secret_with_the_key_id_of_another_is_refused`.
4. Open server: new `src/http/open_server.rs`, applied when authentication is disabled. `403` for an `Origin`
   that is not the `Host`, and on a loopback address for a `Host` that is not a loopback name. Two tests.
6. `/health/ready`: fixed text ("the database does not answer, see the logs"), detail logged. Test on
   `readiness_response`.
7. Configuration: `validate()` at load: timeouts ≥ 1 s, `http_max_query_samples` in 1..=10^9, token duration in
   1 s..=10 years, body limit in 1 B..=2 GiB. `remaining.saturating_add(1)` in the selector.
8. `settings.toml` comment rewritten. 9. Sentry `.debug(true)` removed.
10. Chart: the 30 s timeout is gone from `values.yaml`. 11. The deployment template `fail`s for SQLite or DuckDB
    with `replicaCount > 1` or autoscaling; CI checks that `helm template --set replicaCount=2` fails.
12. Confirmed: the postgres image created one database named `sensapp,sensapp-test`. Fixed with
    `POSTGRES_DB: sensapp` and `docker/postgres-init/create-test-database.sql`, checked with a throwaway container.
13. Confirmed: `axum::serve` sets no timer, so no header timeout. New `src/http/serve.rs` with hyper-util:
    30 s header read timeout, at most 4,096 connections, graceful shutdown. Three tests; the header one fails
    without the timer.
14. `#[cached]` removed from `uuid_v8_blake3`, and the `cached` crate. Release build, 100,000 sensors: varied
    ones 88 → 80 ms, the same one 100,000 times 13 → 26 ms. The cache only wins on a repeated sensor, by
    0.13 µs each, and costs a global lock.
15. `run_blocking` (in `app_error.rs`) runs the gzip decoding, the CSV, Arrow and Prometheus parsing on the
    blocking pool.
16. CSV import: the cells are moved, rows to columns to samples, never cloned; inference borrows a column
    (`infer_typed_column`) and stops at the first text cell; a typed column keeps its text only when it is read
    as text (sensor names, units, times in a value column); a series name is allocated once per series, not
    once per cell. Peak RSS, release build, a 4.5 MB CSV of 200,000 rows (process baseline 18.6 MB removed):

    | Layout | Before | After |
    | --- | --- | --- |
    | wide, 3 value columns | 97 MB (21×) | 69 MB (15×) |
    | long, 50 series | 105 MB (23×) | 63 MB (14×) |

    The review's 50 to 100 times was too high; what is left is one `String` per cell (about 40 bytes for a small
    cell) and one sample per cell. Streaming the CSV would be the next step, not needed now.
17. `MultiGzDecoder`. The two-member case of `test_bytes_to_string` failed before (`"a v=1 1\n"`).
19. InfluxDB counts distinct series (a `HashSet` of UUIDs). The metrics test writes each series twice: it
    reported 4 series before.
20. CSV multi export: one `csv_field` quotes on `,`, `"`, `\n` and `\r`, for names, values, label values and the
    label keys of the header; a label named like a fixed column, or like a renamed one, gets a `label_` prefix.
    Test `test_multi_sensor_csv_reads_back_with_any_label_and_value` reads the output back with `csv-async`.
21. Arrow export timestamps are `Timestamp(Microsecond, "UTC")`; the timestamp tests check the type.
22. `get_series_data` uses `parse_optional_time_bounds`. Test
    `test_series_data_refuses_a_window_ending_before_it_starts` (200 before).
23. The remote write models keep exemplars, histograms (tags 3 and 4 of `TimeSeries`) and metadata (tag 3 of
    `WriteRequest`) as bytes, counted in `sensapp_prometheus_remote_write_dropped_total{part}`. Test
    `test_prometheus_write_counts_what_it_drops`, a metadata-only request included. Documented in
    `docs/HTTP_LIMITS.md`.

### Found on the way

- The test of item 7 set invalid values in the process environment, and `datamodel::batch::tests::test_append`,
  which loads the configuration at the same time, failed once on SQLite. The test now changes the fields of a
  loaded configuration and calls `validate()`.

### Suites run

`cargo check`, `cargo clippy`, `cargo clippy --all-features --tests`, `cargo test` on SQLite (325 + 373 + 2)
and PostgreSQL (333 + 374 + 2) in full; DuckDB and ClickHouse on the CSV, remote write and export tests. The
earlier commits of this task ran the same, and TimescaleDB on the auth tests.

### Already fixed, moved to ideas

- 18 was already fixed by commit 9e01531 (task 11): the unit is checked for separator bytes.
- 5 is in `ideas/docs-page-without-cdn-script.md`: embedding a copy of Scalar or choosing a version and its SRI
  hash means fetching and vetting third-party JavaScript, a maintainer's call.
- 24 is in `ideas/csv-type-inference-splits-series.md` with the options, and noted in `docs/CSV_IMPORT.md`.

### Docs

`docs/JWT_AUTH.md` (open server, key ids, admin tokens, scoped deletes), `docs/CONFIGURATION.md` (startup
checks, 2 GiB), `docs/HTTP_LIMITS.md` (header timeout, connection cap, CSV memory, dropped remote write parts),
`docs/CSV_IMPORT.md` (type splits, export columns), CHANGELOG.
