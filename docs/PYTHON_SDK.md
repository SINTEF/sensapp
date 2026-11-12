# Python SDK

SensApp now includes a small Python client in `python/sensapp`.
It requires Python 3.14 or newer.

## Design goals

- keep the client thin and predictable
- make Apache Arrow the default choice for data exchange
- stay ready for PyPI publication with a standard `pyproject.toml`
- keep examples and tests short enough to copy into real code
- present a clean enough public face for demos and early adopters

## Install locally

```bash
cd python/sensapp
uv pip install -e '.[dev]'
uv run ruff check .
uv run ruff format --check .
uv run pytest
```

## Install from GitHub

Before publishing to PyPI, end users can install the package directly from this repository:

```bash
uv pip install 'git+https://github.com/SINTEF/sensapp.git@main#subdirectory=python/sensapp'
```

## Quick example

```python
import asyncio

from sensapp import SensAppClient


async def main() -> None:
    async with SensAppClient.from_env() as client:
        await client.publish("temperature", 21.5)
        series = await client.query_one("temperature[1h]")
        print(series.frame)


asyncio.run(main())
```

## Arrow conventions

The backend currently exposes two Arrow layouts:

- upload uses a typed per-sensor table with `timestamp` and `value`, plus a `sensor_name` or a `sensor_id` (a UUID). The server derives the series UUID from the name, so republishing the same name appends to the same series
- download uses streamed Arrow IPC and the client normalizes that into `TimeSeries` objects backed by Polars

The client keeps upload Arrow explicit and hides download wire-format details behind a compact API.

Use `query()` when you expect multiple series and `query_one()` when you expect exactly one.

The package also exposes `__version__` and includes runnable examples in `python/sensapp/examples/`.

You can upload either simple scalar values, explicit `SamplePoint` lists, or a Polars DataFrame with `timestamp` and `value` columns.

If you want the frame itself to carry upload metadata, `build_upload_table_from_polars()` also accepts uniform `sensor_name` and optional `sensor_id` columns. `sensor_id` must be a valid UUID, otherwise the server rejects the upload.

## Timeouts

`SensAppClient(timeout=125, write_timeout=330)`: a read may take 125 s and a write 330 s. They sit a little above the server's own timeouts (120 s and 300 s, see [HTTP_LIMITS.md](HTTP_LIMITS.md#timeouts)), so that the server's `504` answer reaches you instead of a client-side timeout. Sending a large history is slower than reading it back: prefer requests of 100,000 to 200,000 samples to one huge `publish`. If you change the timeouts of the server, change the ones of the client with them.

A request that took longer than `RetryPolicy.total_timeout` (60 s) is not retried, see below.

## Retries

When SensApp is overloaded or unreachable the client retries, within limits (`RetryPolicy`, three attempts by default). It retries the answers `503`, `429` and `504`, and the connection errors and timeouts (`ConnectionError`, connect and read timeouts, a connection cut while the response is read), for reads and writes (the admin calls retry less, see [Admin operations](#admin-operations)):

- It waits for the server's `Retry-After`, or backs off exponentially with full jitter when there is none (`random(0, min(30 s, 0.1 s * 2^n))`).
- It drops the request after `max_attempts`, when the next wait would pass `total_timeout` (60 s), or when `Retry-After` is above `max_delay` (30 s). It then raises the last error: `SensAppHTTPError` for an answer of the server, the exception of `niquests` for a connection error or a timeout.
- Other errors are never retried, they would come back the same: `4xx`, `500`, and the errors that waiting cannot fix (a bad TLS certificate, a bad proxy, an invalid URL).
- A write that timed out, or that got a `503` without `Retry-After`, may have been stored in whole or in part, so retrying it can store samples twice. That is accepted, with its cost: the duplicates stay, and are counted by aggregations, until an operator runs the vacuum operation of the server (`client.admin.vacuum()`, see [Admin operations](#admin-operations)). Nothing runs it automatically (see [DATA_LIFECYCLE.md](DATA_LIFECYCLE.md#duplicate-samples)).
- `SensAppClient(..., retry=None)` disables retries, `retry=RetryPolicy(max_attempts=5, ...)` tunes them. If you retry in a proxy or another layer too, disable one of them.

## Last sample and availability

```python
from datetime import UTC, datetime, timedelta

async with SensAppClient.from_env() as client:
    last = await client.last_sample(series_uuid)
    print(last.timestamp, last.value)

    now = datetime.now(UTC)
    day = await client.availability(series_uuid, start=now - timedelta(days=1), end=now, step="1h")
    print(day.present, day.sample_count, day.coverage_ratio)
```

- `last_sample(uuid, *, start=None, end=None)` returns a `LastSample`: the newest sample, optionally inside a window. A float comes back as a `float` (NaN and the infinities included, the server writes them as text), a numeric as a `Decimal`, other values as the server sent them (a location is `{"longitude": ..., "latitude": ...}`). With nothing in the window it raises `SensAppHTTPError` with status `404`, the same answer as an unknown series.
- `availability(uuid, *, start, end, step=None)` returns an `Availability`: `present`, `sample_count`, `first_sample_at` and `last_sample_at` inside the window. With a `step` (a Prometheus duration, `"5m"`) the window is cut in buckets and `covered_buckets`, `total_buckets` and `coverage_ratio` say how many hold a sample, otherwise they are `None`. It is the cheap way to find gaps in a series without downloading it.
- Times are timezone-aware `datetime` objects, or ISO 8601 strings with an offset (`"2026-03-16T12:00:00Z"`). A time without a time zone raises `SensAppValidationError`, a naive `datetime` or a string like `"2026-03-16T12:00:00"` or `"2026-03-16"`: the server would read it as UTC, and the client does not guess. `get_series()` takes the same.
- Both need the `read` scope and follow the retry policy.

## Admin operations

Calls that remove data, maintain the database or make tokens are in their own namespace, `client.admin`. A normal `read write` token cannot use them:

| Call | Scope | What it does |
|---|---|---|
| `admin.vacuum()` | `delete`, token without a sensor allow list | Removes duplicate samples, then cleans up and compacts the database. Returns a `VacuumResult` |
| `admin.delete_samples(uuid, *, start, end)` | `delete` | Deletes the samples between two inclusive bounds, keeps the series. Returns a `DeletedSamples` |
| `admin.delete_series(uuid)` | `delete` | Deletes the samples, the labels and the series |
| `admin.create_token(subject, scope, *, duration_seconds, sensors=None)` | `admin` | Makes a token. Returns a `CreatedToken` |

```python
async with SensAppClient(token=os.environ["SENSAPP_ADMIN_TOKEN"]) as client:
    result = await client.admin.vacuum()
    print(result.duplicates_removed)  # None where the backend cannot remove duplicates

    await client.admin.delete_samples(
        series_uuid,
        start=datetime(2026, 3, 16, 12, 0, tzinfo=UTC),
        end=datetime(2026, 3, 16, 12, 5, tzinfo=UTC),
    )

    created = await client.admin.create_token("edge-device-7", ["write"], duration_seconds=86400)
    print(created.token)  # shown once, and left out of repr(created)
```

- **The vacuum is never retried**, whatever the retry policy: after a `504` or a lost connection the server carries on, and two vacuums running together compete for the same rows. Its timeout is `timeout=3630` seconds by default, a little above the server's maintenance timeout (one hour, `SENSAPP_HTTP_MAINTENANCE_TIMEOUT_SECONDS`, see [DATA_LIFECYCLE.md](DATA_LIFECYCLE.md#duplicate-samples)). Pass another `timeout` if you changed the server's. `examples/maintenance.py` is a job for a scheduler.
- The deletes are retried when the server is overloaded (`503`, `429`) or the connection fails, within the retry policy, but **not after a `504` or a timeout**: the server may still be deleting, and a second delete would compete with it for the same rows. If a lost answer hid a delete that had worked, the retry of `delete_samples` reports `0` deleted samples and the retry of `delete_series` raises a `404`.
- **`create_token` is never retried**: after a lost answer the server may have made the token, and a retry would make a second one. Neither can be revoked.
- Backends that cannot delete (RRDCached) answer `501`, and BigQuery and RRDCached cannot remove duplicates (`duplicates_removed` is `None`, or `501` for deletes). See [DATA_LIFECYCLE.md](DATA_LIFECYCLE.md#backend-support).
- `create_token` never makes an `admin` token (`403`, those come from `sensapp generate-token`) and answers `404` when authentication is disabled. The server keeps no list of tokens: there is nothing to list or revoke, see [JWT_AUTH.md](JWT_AUTH.md).
- Errors are `SensAppHTTPError` like everywhere else: `403` for a missing scope, `404` for an unknown series.

## What is included

- `SensAppClient` for the HTTP API, `client.admin` (`AdminClient`) for the vacuum, deletes and tokens, and `RetryPolicy` for its overload retries
- `LastSample`, `Availability`, `VacuumResult`, `DeletedSamples` and `CreatedToken` for their answers
- `SamplePoint` for lightweight upload payloads
- `TimeSeries` for query results with Polars and pandas conversion helpers
- `build_upload_table()` to build Arrow upload tables explicitly
- `build_upload_table_from_polars()` to normalize Polars frames into upload tables
- `serialize_arrow_table()` and `read_arrow_table()` helpers
- mocked unit tests in `python/sensapp/tests`

## Packaging

The package uses a normal `pyproject.toml` and can be built with:

```bash
cd python/sensapp
uv build
```

That keeps the path to a future PyPI release straightforward.

## Quality checks

The SDK now uses Ruff for both linting and formatting. The intended local loop is:

```bash
cd python/sensapp
uv run ruff check .
uv run ruff format .
uv run pytest
```
