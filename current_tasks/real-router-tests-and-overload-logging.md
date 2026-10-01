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

- [ ] `build_router` extracted, `run_http_server` unchanged in behaviour.
- [ ] Tests above, passing on SQLite and on the CI backends.
- [ ] Load-shed responses no longer produce ERROR lines (test or drill showing it).
- [ ] No regression: default, ClickHouse and TimescaleDB suites, clippy, Python SDK tests.

## Progress

(none yet)
