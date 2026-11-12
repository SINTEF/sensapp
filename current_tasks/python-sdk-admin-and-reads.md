# Python SDK: admin namespace, last sample and availability

## Goal

The SDK had no admin feature and two read calls of the server were missing. Add:

- `client.last_sample()` and `client.availability()` (reads, `read` scope)
- `client.admin`, a namespace: `vacuum()`, `delete_series()`, `delete_samples()`, `create_token()`

The SDK retries timed-out writes, which can store samples twice, and the docs tell users to run the vacuum
of the server: the SDK could not.

## Decisions

- Admin calls live in `AdminClient` (`_admin.py`), a sub-client held as `client.admin`. Reads stay on the client.
- **Vacuum is never retried**: a 504 means the server carries on, and two vacuums must not run together
  (`docs/DATA_LIFECYCLE.md`). It has a timeout of its own, above the server's one hour.
- Deletes keep the retry policy. After a lost answer the retry gives `deleted_samples=0` or a 404: documented.
- Times are `datetime | str`, a naive `datetime` is refused. `get_series` takes them too.
- `last_sample` with no sample in the window raises the server's 404, like `query_one` raises.
- Not added: list/revoke tokens (no endpoint), admin tokens (need the secret), retention.

## Progress

- [ ] Models and the time helper
- [ ] `last_sample`, `availability`
- [ ] `AdminClient`
- [ ] Mocked and live tests
- [ ] Docs (`docs/PYTHON_SDK.md`, SDK README), example `maintenance.py`
- [ ] Lint, unit tests, live tests on SQLite
