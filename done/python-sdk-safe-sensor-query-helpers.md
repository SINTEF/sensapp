# Python SDK Safe Sensor Query Helpers

## Goal

- let SDK callers query by sensor name without hand-writing PromQL
- support hyphenated sensor names safely by generating `__name__` matchers
- cover the behavior with unit and live integration tests

## Delivered

- added structured sensor-name query helpers to `SensAppClient`
- generated safe `__name__="..."` selectors so hyphenated sensor names work without raw PromQL
- added unit tests for helper query construction
- added a live integration test covering a hyphenated sensor name against a real SensApp process
- updated the demo dashboard to use the helper API instead of interpolating raw PromQL
- updated the SDK README quick start and API list

## Validation

- `cd python/sensapp-sdk && uv run python -m pytest tests/test_client.py`
- `cd python/sensapp-sdk && uv run python -m pytest -m integration`
- `cd python/sensapp-sdk && uv run python -m pytest`

## Notes

- the live integration tests already start SensApp through `cargo run`, so extra Docker orchestration was not required for this fix
