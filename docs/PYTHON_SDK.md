# Python SDK

SensApp now includes a small Python client in `python/sensapp`.

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

- upload uses a typed per-sensor table with `timestamp` and `value`, plus optional `sensor_id` and `sensor_name`
- download uses streamed Arrow IPC and the client normalizes that into `TimeSeries` objects backed by Polars

The client keeps upload Arrow explicit and hides download wire-format details behind a compact API.

Use `query()` when you expect multiple series and `query_one()` when you expect exactly one.

The package also exposes `__version__` and includes runnable examples in `python/sensapp/examples/`.

You can upload either simple scalar values, explicit `SamplePoint` lists, or a Polars DataFrame with `timestamp` and `value` columns.

If you want the frame itself to carry upload metadata, `build_upload_table_from_polars()` also accepts uniform `sensor_name` and optional `sensor_id` columns.

## What is included

- `SensAppClient` for the HTTP API
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
