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

## Retries

When SensApp is overloaded or unreachable the client retries, within limits (`RetryPolicy`, three attempts by default). It retries the answers `503`, `429` and `504`, and the connection errors and timeouts (`ConnectionError`, connect and read timeouts, a connection cut while the response is read), for reads and writes:

- It waits for the server's `Retry-After`, or backs off exponentially with full jitter when there is none (`random(0, min(30 s, 0.1 s * 2^n))`).
- It drops the request after `max_attempts`, when the next wait would pass `total_timeout` (60 s), or when `Retry-After` is above `max_delay` (30 s). It then raises the last error: `SensAppHTTPError` for an answer of the server, the exception of `niquests` for a connection error or a timeout.
- Other errors are never retried, they would come back the same: `4xx`, `500`, and the errors that waiting cannot fix (a bad TLS certificate, a bad proxy, an invalid URL).
- A write that timed out, or that got a `503` without `Retry-After`, may have been stored in whole or in part, so retrying it can store samples twice. That is accepted: the vacuum operation of the server removes duplicate samples (see [DATA_LIFECYCLE.md](DATA_LIFECYCLE.md)).
- `SensAppClient(..., retry=None)` disables retries, `retry=RetryPolicy(max_attempts=5, ...)` tunes them. If you retry in a proxy or another layer too, disable one of them.

## What is included

- `SensAppClient` for the HTTP API, and `RetryPolicy` for its overload retries
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
