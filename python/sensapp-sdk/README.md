# sensapp-sdk

Small Python client for the SensApp HTTP API.

The SDK is intentionally narrow:

- simple synchronous client built on `httpx`
- Arrow-first reads and writes with `pyarrow`
- thin wrappers over the current HTTP endpoints
- small dependency surface so publishing to PyPI stays straightforward

## Install

```bash
pip install sensapp-sdk
```

For local development from this repository:

```bash
cd python/sensapp-sdk
python -m pip install -e '.[dev]'
python -m ruff check .
python -m ruff format --check .
python -m pytest
```

## Quick start

```python
from datetime import datetime, timezone

from sensapp_sdk import SamplePoint, SensAppClient

with SensAppClient("http://127.0.0.1:3000") as client:
    client.publish_samples(
        sensor_name="temperature",
        samples=[
            SamplePoint(datetime(2026, 3, 16, 12, 0, tzinfo=timezone.utc), 21.5),
            SamplePoint(datetime(2026, 3, 16, 12, 1, tzinfo=timezone.utc), 21.7),
        ],
    )

    table = client.query_arrow("temperature[1h]")
    print(table.to_pylist())
```

## Arrow model

SensApp currently uses two Arrow shapes:

- upload Arrow is a typed per-sensor table with at least `timestamp` and `value`, plus optional `sensor_id` and `sensor_name`
- download Arrow is a long table with `timestamp`, `sensor_id`, `sensor_name`, `value`, `type`, and `labels`

The SDK keeps that distinction explicit.

## Main API

```python
from sensapp_sdk import SensAppClient
```

Main methods:

- `health_live()`
- `health_ready()`
- `list_metrics()`
- `list_series()`
- `publish_csv()`
- `publish_senml()`
- `publish_arrow()`
- `publish_samples()`
- `write_influx()`
- `query_arrow()`
- `query_rows()`
- `query_csv()`
- `query_senml()`
- `get_series_arrow()`
- `get_series_rows()`

## Authentication

Pass a JWT token directly:

```python
client = SensAppClient("http://127.0.0.1:3000", token="...")
```

Or load it from the environment:

```python
import os

os.environ["SENSAPP_URL"] = "http://127.0.0.1:3000"
os.environ["SENSAPP_TOKEN"] = "..."

client = SensAppClient.from_env()
```

## Examples

See the `examples/` directory:

- `publish_arrow_samples.py`
- `query_arrow_table.py`
- `browse_series.py`

## Typing

The package ships typed APIs and includes a `py.typed` marker so editors and downstream type checkers can consume the inline annotations.

## Caveats

- Arrow upload currently does not preserve units or labels because the backend importer ignores them.
- JSON catalog endpoints are returned as plain dictionaries because the DCAT payloads are flexible and still evolving.
- The SDK is sync-first on purpose. An async variant can be added later if the current API settles.
