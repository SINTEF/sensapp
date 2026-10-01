# Write backpressure, request IDs, configuration reference

## Status (1 Oct 2026)

Implemented.

- `src/http/backpressure.rs`: a semaphore bounds concurrent write requests
  (`SENSAPP_HTTP_MAX_CONCURRENT_WRITES`, default 16, `0` = unlimited). A write with no free slot is
  rejected at once (no queue) with `503` and a `Retry-After`, before its body is read. Applied to the
  write routes after authentication and before body buffering. `Retry-After` is a full-jitter random
  value between 1 and 2x the moving average write duration (cap 30 s), so it follows how slow the
  database is. The header marks a write that was not processed; the other `503` (`Database
  unavailable`) has none. The first version waited 1 s for a slot and sent a fixed `Retry-After: 1`
  (queueing, and a synchronised retry herd), and a second one used an arbitrary 1 to 5 s range.
  Rejections are visible through the existing `sensapp_http_requests_total{status="503"}`.
- Rate limiting is deliberately not done in SensApp: use a reverse proxy.
- `x-request-id`: set (or kept when the client sent one), put in the request log span, echoed in
  every response. `tower-http` `SetRequestId` / `PropagateRequestId`.
- `docs/CONFIGURATION.md`: all settings, backends, backpressure and request-id notes.
- `settings.toml`: removed the stale `storage_sync_timeout_seconds`.
- Python SDK: `RetryPolicy` (`python/sensapp/src/sensapp/_retry.py`, documented in
  `docs/PYTHON_SDK.md#retries`). Retries `503`/`429` only; honours `Retry-After`, else exponential
  backoff with full jitter; 3 attempts, 60 s total, a `Retry-After` above 30 s drops the request;
  `publish` is only resent when the `503` has `Retry-After`; reads are always resent.
- Tests: seven Rust unit tests with a paused clock (excess rejected without waiting, `Retry-After`
  follows the write duration and is jittered and capped, average moves gradually, slot released,
  `0` disables) and eleven Python tests for the SDK policy.
- Live check of the first version on SQLite with limit 1 and 40 concurrent 150 000-row CSV uploads:
  1 accepted, 38 `503`, one connection closed by the server before the client finished sending.
  The adaptive version is covered by unit tests only.

## Possible follow-ups

- Limit expensive reads the same way if staging shows overload from queries.
- Each rejected request is logged at ERROR level by `TraceLayer`; under sustained overload that is noisy.
- The request-id layers live inside `run_http_server`, so no automated test covers them.
- Client retries after a connection error or timeout are not done for writes. A large body can be answered
  with a closed connection instead of the `503`, so the SDK reports an error there.
- A retry budget or circuit breaker in the SDK, if callers ask for one.
