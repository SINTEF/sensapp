# Python SDK: retry connection errors and timeouts too

## Goal

The SDK retries overload answers (`503`/`429`) with backoff and `Retry-After`. It must also retry
connection errors and timeouts, the same way, for reads and writes. A write that timed out may have been
stored: the duplicate it can create is accepted, vacuum removes duplicates
(`current_tasks/sample-deduplication-in-vacuum.md`).

## Design

- Same `RetryPolicy` (attempts, exponential backoff with full jitter, total timeout, cap), no new knobs
  unless needed.
- Retried: `niquests` connection errors, connect and read timeouts. Not retried: invalid URL / schema
  errors, TLS certificate errors (they will not get better), anything that is a programming error.
- When the policy gives up the last exception is raised (not wrapped), as the server's last error is.
- `publish`: a bare `503` (storage unavailable) and a `504` are now retried too (the earlier rule that a
  write is only resent with `Retry-After` existed because of duplicates).

## Checklist

- [x] `_retry.py` handles exceptions and the wider set of statuses; unit tests for each case, with the clock
  and the sleep mocked as the existing tests do.
- [x] `docs/PYTHON_SDK.md` retries section and `docs/CONFIGURATION.md` "What writers should do" updated.
- [x] A live test against a server that is stopped and restarted, if it can be done without flakiness.
- [x] SDK tests, ruff.

## Progress

Done 2 Oct 2026.

- `_retry.py` rewritten: `send_with_retry` catches the exceptions of the connection. Retried: niquests
  `ConnectionError` (so connect timeouts too), `Timeout` (connect and read) and `ChunkedEncodingError`;
  not retried: `SSLError`, `ProxyError`, `InvalidURL`, `MissingSchema` and anything else (raised at once).
  Statuses retried: `503`, `429`, `504`, for reads and writes alike (the `idempotent` flag is gone). The
  last exception is raised unchanged when the policy gives up.
- 66 SDK tests (was 61): each error class for reads and writes, bare `503` and `504` on a write, giving up
  with the last exception, errors raised at once, retries disabled, total timeout with exceptions.
- Live drill (debug server on SQLite): nothing listening, 3 attempts, `ConnectionError` after 0.42 s; server
  started one second after the call, `publish` succeeds and the series is stored.
- `docs/PYTHON_SDK.md` and `docs/CONFIGURATION.md` describe the new rule. They say that the vacuum removes
  duplicates, which `current_tasks/sample-deduplication-in-vacuum.md` makes true: do not close that task
  without it.
