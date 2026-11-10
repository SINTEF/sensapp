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
