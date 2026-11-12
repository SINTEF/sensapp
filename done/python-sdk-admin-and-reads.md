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

## Delivered

- `client.last_sample()` returns a `LastSample` (float and numeric values converted, timestamps aware),
  `client.availability()` an `Availability` (bucket fields `None` without `step`).
- `client.admin` (`AdminClient`, `_admin.py`): `vacuum()`, `delete_series()`, `delete_samples()`,
  `create_token()`. `CreatedToken.token` is out of its `repr`.
- `_post` takes a `timeout` and a `retry` override, and there is a `_delete`. The vacuum uses `NO_RETRY`
  and `timeout=3630`.
- `format_time()` for every time bound (`get_series` too): `datetime | str`, naive datetimes refused.
- Docs: `docs/PYTHON_SDK.md` (two sections, the Retries bullet now names the vacuum call), SDK README,
  `examples/maintenance.py`.

## Verification

- 101 mocked unit tests (32 new), ruff clean.
- 7 live tests (5 new) against the SQLite server on a scratch port, authentication disabled like CI: deletes
  inclusive, a sample written twice is 2 before the vacuum and 1 after (`duplicates_removed=1`), `create_token`
  answers 404.
- By hand against a server with JWT on: an admin token makes a `read write` token limited to one sensor, which
  publishes and reads it back; `admin` asked for is 403, vacuum with an admin token 403, the new token cannot make
  tokens or delete (403), a `delete` token runs the vacuum and deletes the series.
- Not run here: CI's ClickHouse runtime job (the live tests there are backend-neutral).

## Not done

- Listing and revoking tokens: no endpoint (`ideas/token-registry-and-revocation.md`).
