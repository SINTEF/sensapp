# Python SDK

SensApp now includes a small Python SDK in `python/sensapp-sdk`.

## Design goals

- keep the client thin and predictable
- make Apache Arrow the default choice for data exchange
- stay ready for PyPI publication with a standard `pyproject.toml`
- keep examples and tests short enough to copy into real code

## Install locally

```bash
cd python/sensapp-sdk
python -m pip install -e '.[dev]'
python -m ruff check .
python -m ruff format --check .
python -m pytest
```

## Quick example

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

    rows = client.query_rows("temperature[1h]")
    print(rows)
```

## Arrow conventions

The backend currently exposes two Arrow layouts:

- upload uses a typed per-sensor table with `timestamp` and `value`, plus optional `sensor_id` and `sensor_name`
- download uses a long-form table with `timestamp`, `sensor_id`, `sensor_name`, `value`, `type`, and `labels`

The SDK makes this explicit through separate upload helpers and Arrow parsing helpers.

## What is included

- `SensAppClient` for the HTTP API
- `SamplePoint` for lightweight upload payloads
- `build_upload_table()` to build Arrow upload tables explicitly
- `serialize_arrow_table()` and `read_arrow_table()` helpers
- examples in `python/sensapp-sdk/examples`
- mocked unit tests in `python/sensapp-sdk/tests`

## Packaging

The package uses a normal `pyproject.toml` and can be built with:

```bash
cd python/sensapp-sdk
python -m build
```

That keeps the path to a future PyPI release straightforward.

## Quality checks

The SDK now uses Ruff for both linting and formatting. The intended local loop is:

```bash
cd python/sensapp-sdk
python -m ruff check .
python -m ruff format .
python -m pytest
```
