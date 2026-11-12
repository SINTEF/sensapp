# sensapp

Async Python client for the [SensApp](https://github.com/SINTEF/sensapp) HTTP API.

Requires Python 3.14 or newer.

Arrow-first data exchange, typed DCAT catalog models, and Polars-first query results, powered by [niquests](https://niquests.readthedocs.io/).

## Why this client

- async-only API built around `niquests`
- Arrow over the wire, Polars in memory
- typed DCAT catalog models instead of loose dictionaries
- small public surface aimed at common SensApp workflows

## Install

Local editable install:

```bash
cd python/sensapp
uv pip install -e '.[dev]'
```

Install directly from GitHub before PyPI publication:

```bash
uv pip install 'git+https://github.com/SINTEF/sensapp.git@main#subdirectory=python/sensapp'
```

Set a server URL once and let the examples use it:

```bash
export SENSAPP_URL=http://127.0.0.1:3000
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

Runnable examples live in `examples/quickstart.py`, `examples/catalog_overview.py` and `examples/maintenance.py`.

## Publish with explicit timestamps

```python
from datetime import UTC, datetime
from sensapp import SensAppClient, SamplePoint

async with SensAppClient() as client:
    await client.publish(
        "temperature",
        [
            SamplePoint(datetime(2026, 4, 13, 12, 0, tzinfo=UTC), 21.5),
            SamplePoint(datetime(2026, 4, 13, 12, 1, tzinfo=UTC), 21.7),
        ],
    )
```

## Publish from Polars

```python
from datetime import UTC, datetime

import polars as pl
from sensapp import SensAppClient, build_upload_table_from_polars

frame = pl.DataFrame(
    {
        "timestamp": [
            datetime(2026, 4, 13, 12, 0, tzinfo=UTC),
            datetime(2026, 4, 13, 12, 1, tzinfo=UTC),
        ],
        "value": [21.5, 21.7],
    }
)

async with SensAppClient() as client:
    await client.publish("temperature", frame)

table = build_upload_table_from_polars(
    frame.with_columns(sensor_name=pl.lit("temperature"))
)
```

For `publish()`, the sensor name still comes from the method argument. If you want the Polars frame itself to carry a uniform `sensor_name`, use `build_upload_table_from_polars()` directly.

The series UUID is derived from the sensor name, type, unit and labels, so publishing `temperature` again appends to the same series. To target a series by UUID instead, add a `sensor_id` column holding a valid UUID (the `sensor_id` argument of `publish()` does the same). A payload with neither a name nor a UUID is rejected.

## Query results

`query()` returns a list of `TimeSeries` objects. Each series stores metadata once and exposes a Polars DataFrame with only `timestamp` and `value`.

If you expect exactly one result, use `query_one()` for a simpler and stricter happy path.

```python
async with SensAppClient() as client:
    results = await client.query('{__name__="temperature",room="lab"}[6h]')

    for item in results:
        print(item.name, item.sensor_type, item.labels)
        print(item.frame)
        print(item.to_pandas())
```

## Typed DCAT catalogs

```python
async with SensAppClient() as client:
    catalog = await client.list_metrics()
    for metric in catalog.metrics:
        print(metric.name, metric.sensor_type, metric.series_count)

    series_catalog = await client.list_series(metric="temperature")
    for series in series_catalog.series:
        data = await client.get_series(series.uuid)
        print(data.frame)
```

## Last sample and availability

```python
last = await client.last_sample(series_uuid)
day = await client.availability(series_uuid, start=start, end=end, step="1h")
print(last.value, day.coverage_ratio)
```

## Admin operations

Deletes, the vacuum and tokens are in the `client.admin` namespace and need a token with the `delete` or `admin` scope:

```python
async with SensAppClient(token=os.environ["SENSAPP_ADMIN_TOKEN"]) as client:
    result = await client.admin.vacuum()  # removes duplicate samples, never retried
    await client.admin.delete_samples(series_uuid, start=start, end=end)
    await client.admin.delete_series(series_uuid)
    created = await client.admin.create_token(
        "edge-device-7", ["write"], duration_seconds=86400
    )
```

See `examples/maintenance.py` and [docs/PYTHON_SDK.md](../../docs/PYTHON_SDK.md#admin-operations).

## Public API

- `SensAppClient`, and `client.admin` (`AdminClient`)
- `LastSample`, `Availability`, `VacuumResult`, `DeletedSamples`, `CreatedToken`
- `SamplePoint`
- `TimeSeries`
- `build_upload_table()`
- `build_upload_table_from_polars()`
- `serialize_arrow_table()` and `read_arrow_table()`
- `__version__`

## Quality checks

```bash
cd python/sensapp
uv run ruff check .
uv run ruff format --check .
uv run pytest
```
