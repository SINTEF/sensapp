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

- [ ] `_retry.py` handles exceptions and the wider set of statuses; unit tests for each case, with the clock
  and the sleep mocked as the existing tests do.
- [ ] `docs/PYTHON_SDK.md` retries section and `docs/CONFIGURATION.md` "What writers should do" updated.
- [ ] A live test against a server that is stopped and restarted, if it can be done without flakiness.
- [ ] SDK tests, ruff.

## Progress

(none yet)
