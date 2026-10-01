# Test the real HTTP stack, and stop logging load shedding as errors

## Goal

The layers that protect the server (write concurrency limit, `x-request-id`, request timeout,
authentication order) must be covered by automated tests that run the real router, and shedding
load must not flood the logs.

## Context

- `run_http_server` (`src/http/server.rs`) builds the router and the middleware stack inside the
  function that binds the socket, so no test can use it. The integration `TestApp`
  (`tests/integration/common/http.rs`) builds its own minimal router "without middleware that might
  interfere": the limiter, the request id, the timeout (now 504) and the auth route layers are only
  checked by hand (curl drills) or by unit tests of one layer.
- Every request rejected by the write limiter is logged at ERROR by `TraceLayer` (all 5xx are).
  Under sustained overload that is one error line per rejected request.
- Review notes in `done/write-backpressure-and-request-id.md` list both as follow-ups.

## Design

- Extract `build_router(state, &config) -> Router` (routes and middleware, no socket) from
  `run_http_server`, which keeps only binding and graceful shutdown. `TestApp` can then use it
  (an option for a real router, keeping the cheap one where it is enough).
- Tests on the real router: `x-request-id` generated, kept when sent, present on errors and on 503;
  write limiter answers 503 with a `Retry-After` between 1 and 30 and lets reads, health and
  metrics through; the limiter sits behind authentication (a request without a token gets 401,
  not 503, and does not take a slot); a slow handler gets 504 after the configured timeout.
- A load-shed 503 is logged at WARN or lower (or counted instead of logged); real 5xx stay ERROR.

## Done when

- [x] `build_router` extracted, `run_http_server` unchanged in behaviour.
- [x] Tests above, passing on SQLite and on the CI backends.
- [x] Load-shed responses no longer produce ERROR lines (test or drill showing it).
- [x] No regression: default, ClickHouse and TimescaleDB suites, clippy, Python SDK tests.

## Progress

Done 1 Oct 2026, one commit per step:

1. `build_router(state, &RouterSettings)` extracted from `run_http_server` (routes and layers, no
   socket); `RouterSettings` holds the body limit, the request timeout and the write limit, with
   `from_config`. `run_http_server` only builds the settings, binds and serves. No change of behaviour.
2. `tests/integration/real_router.rs` runs the real router over the configured backend, with a
   storage wrapper that delays (or fails) `publish`: request ids generated, kept, and present on
   404 and 503; the write limit answers at once (under 300 ms) with a `Retry-After` between 1 and
   30 and an id, spares reads, health, readiness and metrics, frees the slot afterwards and shows
   in `sensapp_http_requests_total{status="503"}`; it sits behind authentication (no token gets
   401 and a read-only token 403 while the slot is taken, the authorised writer gets the 503); slow
   requests get a 504; bodies over the limit a 413.
3. Logging: `log_failed_response` replaces the default `on_failure` of the trace layer. A `503` is
   logged at `DEBUG` (counted in the metrics, and an unavailable database logs its own error where it
   is detected), a `504` at `WARN`, any other `5xx` at `ERROR`. Tests capture the tracing output:
   five rejected writes produce no error line, a timeout a warning carrying its request id, and a
   failing write is still an error with its id.

Full suites pass on SQLite, PostgreSQL, TimescaleDB and ClickHouse, the Python SDK tests, and
`clippy -D warnings` on all features. The new tests were repeated several times to check for flakiness
(they use delays of 150 to 600 ms against a 600 ms write).

Not covered: the JWT tests (`tests/integration/jwt_auth.rs`) still build their own minimal routers;
they could use `build_router` too.
